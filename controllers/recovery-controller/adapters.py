"""DynamoDB durable inbox/job CAS and fencing progress records."""
import json
import os
import time
from datetime import datetime, timezone


class RecoveryBlocked(ValueError):
    """A decision that needs review, not an endlessly retried write."""


class RetryLimitReached(RecoveryBlocked):
    pass


class Store:
    def __init__(self, table, fencing_table, max_recovery_attempts=3):
        if type(max_recovery_attempts) is not int or max_recovery_attempts < 1:
            raise ValueError("max_recovery_attempts must be a positive integer")
        self.max_recovery_attempts = max_recovery_attempts
        self.table = table  # Our durable event inbox and placement/template metadata.
        self.fencing = fencing_table  # Hyunsu's table: PK job_id, sole generation/owner authority.

    def accept(self, event, digest):
        key = 'event#' + event['event_id']
        try:
            self.table.put_item(Item=dict(pk=key, kind='event', event=event, digest=digest,
                                          state='RECEIVED', created_at=int(time.time()), published=False),
                                ConditionExpression='attribute_not_exists(pk)')
        except Exception as exc:
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') != 'ConditionalCheckFailedException':
                raise
            old = self.get(event['event_id'])
            if old.get('digest') != digest:
                raise ValueError('event_id reused with different content') from exc

    def get(self, rid):
        return self.table.get_item(Key={'pk':'event#'+rid}, ConsistentRead=True)['Item']

    def save(self, rid, **values):
        values['updated_at'] = int(time.time())
        names = {'#'+str(i):k for i,k in enumerate(values)}
        vals = {':'+str(i):v for i,v in enumerate(values.values())}
        return self.table.update_item(Key={'pk':'event#'+rid},
            UpdateExpression='SET '+', '.join('#'+str(i)+' = :'+str(i) for i in range(len(values))),
            ExpressionAttributeNames=names, ExpressionAttributeValues=vals,
            ReturnValues='ALL_NEW')['Attributes']

    def context(self, event, recovering=False):
        item = self.table.get_item(Key={'pk':'job#'+event['job_id']}, ConsistentRead=True).get('Item')
        if not item:
            raise RecoveryBlocked('registered recovery job metadata missing')
        if not recovering and (item.get('cluster') != event['cluster'] or (event.get('node') is not None and event['node'] not in item.get('active_nodes',[]))):
            return None
        if not 1 <= int(item.get('gpu_count',0)) <= 4:
            raise RecoveryBlocked('GPU count must be 1..4')
        uri=item.get('jobset_uri','')
        digest=item.get('jobset_sha256','')
        import re
        if not uri.startswith('s3://') or not re.fullmatch('[a-f0-9]{64}',digest):
            raise RecoveryBlocked('submitted jobset.json URI and sha256 are required')
        return item

    def claim(self, event):
        key = {'job_id':event['job_id']}
        current = self.fencing.get_item(Key=key, ConsistentRead=True).get('Item')
        if not current:
            raise RecoveryBlocked('fencing job record missing')
        metadata = self.context(event, recovering=current.get('recovery_id') == event['event_id'])
        if metadata is None:
            return None
        if current.get('recovery_id') == event['event_id']:
            phase = current.get('recovery_phase')
            if phase == 'CLAIMED':
                return self.advance_generation(event, metadata, current)
            if (int(current.get('generation',0)) != event['generation']+1
                    or int(current.get('recovery_from_gen',0)) != event['generation']
                    or int(current.get('recovery_to_gen',0)) != event['generation']+1):
                raise RecoveryBlocked('recovery record generation mismatch')
            # Never revoke an owner already acquired by the new init on replay.
            return dict(metadata, generation=current['generation'],
                        resume_step=int(current.get('recovery_resume_step', current.get('approved_step', 0))),
                        resume_gen=current.get('recovery_resume_gen', current.get('approved_gen')),
                        allow_restart_from_scratch=current.get('recovery_resume_gen') is None,
                        checkpoint_warning=('NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH'
                                            if current.get('recovery_resume_gen') is None else None))
        if int(current.get('generation',0)) != event['generation']:
            return None
        if current.get('recovery_id'):
            raise RecoveryBlocked('another recovery holds job lock')
        if 'owner' in current and current['owner'] != event['cluster']:
            raise RecoveryBlocked('source cluster does not own this generation')
        if int(current.get('retry_count', 0)) >= self.max_recovery_attempts:
            raise RetryLimitReached(f'automatic recovery limit reached ({self.max_recovery_attempts})')
        approved = ('approved_gen' in current, 'approved_step' in current)
        if approved[0] != approved[1]:
            raise RecoveryBlocked('incomplete approved checkpoint fields')
        if not all(approved) and metadata.get('allow_restart_from_scratch', True) is not True:
            raise RecoveryBlocked('no approved checkpoint; explicit from-scratch policy required')
        if all(approved) and not (1 <= int(current['approved_gen']) <= event['generation']
                                  and int(current['approved_step']) >= 0):
            raise RecoveryBlocked('invalid approved checkpoint fields')
        # Lock the exact approved baseline. A simultaneous sidecar approval causes a
        # conditional failure and a reread on the next reconciliation cycle.
        values = {':old':event['generation'], ':new':event['generation']+1,
                  ':rid':event['event_id'], ':source':event['cluster'],
                  ':phase':'CLAIMED', ':now':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  ':resume':int(current.get('approved_step', 0)),
                  ':resume_gen':current.get('approved_gen'), ':limit':self.max_recovery_attempts}
        approval_condition = 'attribute_not_exists(approved_gen) AND attribute_not_exists(approved_step)'
        if all(approved):
            values.update({':approved_gen':current['approved_gen'], ':approved_step':current['approved_step']})
            approval_condition = 'approved_gen = :approved_gen AND approved_step = :approved_step'
        self.fencing.update_item(Key=key,
            UpdateExpression=('SET recovery_id = :rid, recovery_from_gen = :old, recovery_to_gen = :new, '
                              'recovery_phase = :phase, recovery_updated_at = :now, '
                              'recovery_resume_step = :resume, recovery_resume_gen = :resume_gen'),
            ConditionExpression=('generation = :old AND (attribute_not_exists(#owner) OR #owner = :source) '
                                 'AND attribute_not_exists(recovery_id) AND (attribute_not_exists(retry_count) OR retry_count < :limit) AND ('+approval_condition+')'),
            ExpressionAttributeNames={'#owner':'owner'}, ExpressionAttributeValues=values)
        # This turn only claims the recovery: old generation/owner/retry_count remain.
        return dict(metadata, generation=event['generation'], claim_pending=True,
                    checkpoint_warning=('NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH' if not all(approved) else None))

    def advance_generation(self, event, metadata, current):
        old = event['generation']
        if (int(current.get('generation',0)) != old
                or int(current.get('recovery_from_gen',0)) != old
                or int(current.get('recovery_to_gen',0)) != old+1):
            raise RecoveryBlocked('claimed recovery generation mismatch')
        if int(current.get('retry_count', 0)) >= self.max_recovery_attempts:
            raise RetryLimitReached(f'automatic recovery limit reached ({self.max_recovery_attempts})')
        self.fencing.update_item(Key={'job_id':event['job_id']},
            UpdateExpression=('SET generation = :new, recovery_phase = :phase, recovery_updated_at = :now, '
                              'retry_count = if_not_exists(retry_count, :zero) + :one REMOVE #owner'),
            ConditionExpression=('generation = :old AND recovery_id = :rid AND recovery_phase = :claimed '
                                 'AND recovery_from_gen = :old AND recovery_to_gen = :new '
                                 'AND (attribute_not_exists(retry_count) OR retry_count < :limit) '
                                 'AND (attribute_not_exists(#owner) OR #owner = :source)'),
            ExpressionAttributeNames={'#owner':'owner'},
            ExpressionAttributeValues={':old':old, ':new':old+1, ':rid':event['event_id'],
                ':claimed':'CLAIMED', ':phase':'GENERATION_ADVANCED', ':source':event['cluster'],
                ':now':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'), ':zero':0, ':one':1, ':limit':self.max_recovery_attempts})
        return dict(metadata, generation=old+1,
                    resume_step=int(current.get('recovery_resume_step', 0)),
                    resume_gen=current.get('recovery_resume_gen'),
                    allow_restart_from_scratch=current.get('recovery_resume_gen') is None,
                    checkpoint_warning=('NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH'
                                        if current.get('recovery_resume_gen') is None else None))

    def owner_matches(self, job_id, generation, cluster):
        item=self.fencing.get_item(Key={'job_id':job_id},ConsistentRead=True).get('Item',{})
        return int(item.get('generation',0))==generation and item.get('owner')==cluster

    def finish(self, record):
        if record['state'] != 'RECOVERED':
            return
        event=record['event'];generation=int(record['generation']);target=record['target_cluster']
        key={'job_id':event['job_id']}
        current=self.fencing.get_item(Key=key,ConsistentRead=True).get('Item',{})
        if int(current.get('generation',0))>generation:
            return  # A later completed recovery already advanced the job.
        if int(current.get('generation',0))!=generation or current.get('owner')!=target:
            raise RecoveryBlocked('new owner/generation not confirmed')
        if current.get('recovery_id') not in (None,event['event_id']):
            raise RecoveryBlocked('different recovery owns job lock')
        # Placement is controller metadata, never a second fencing authority.
        self.table.update_item(Key={'pk':'job#'+event['job_id']},
            UpdateExpression='SET #cluster = :cluster, active_nodes = :nodes, placement_generation = :generation',
            ConditionExpression='attribute_not_exists(placement_generation) OR placement_generation <= :generation',
            ExpressionAttributeNames={'#cluster':'cluster'},
            ExpressionAttributeValues={':cluster':target, ':nodes':record['active_nodes'], ':generation':generation})
        if current.get('recovery_id')==event['event_id']:
            self.fencing.update_item(Key=key,
                UpdateExpression='SET recovery_phase = :phase, recovery_updated_at = :now REMOVE recovery_id, recovery_from_gen, recovery_to_gen',
                ConditionExpression='recovery_id = :rid AND generation = :generation AND #owner = :target',
                ExpressionAttributeNames={'#owner':'owner'},
                ExpressionAttributeValues={':rid':event['event_id'], ':generation':generation, ':target':target, ':phase':'COMPLETED',
                                           ':now':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')})
        # owner and approved checkpoint fields remain owned by init/sidecar.

    def records(self):
        # Foundation-scale scan; replace with a pending/outbox GSI for larger workloads.
        args = {'ConsistentRead':True}
        while True:
            response = self.table.scan(**args)
            for item in response.get('Items', []):
                if item.get('kind') == 'event':
                    yield item
            if not response.get('LastEvaluatedKey'):
                break
            args['ExclusiveStartKey'] = response['LastEvaluatedKey']

    def fencing_record(self, job_id):
        return self.fencing.get_item(Key={'job_id':job_id}, ConsistentRead=True).get('Item', {})

    def placement(self, job_id):
        return self.table.get_item(Key={'pk':'job#'+job_id}, ConsistentRead=True).get('Item', {})

    def resuming(self, event, generation):
        self.fencing.update_item(
            Key={'job_id':event['job_id']},
            UpdateExpression='SET recovery_phase = :phase, recovery_updated_at = :now',
            ConditionExpression='recovery_id = :rid AND generation = :gen AND recovery_phase = :submitted',
            ExpressionAttributeValues={':rid':event['event_id'], ':gen':generation,
                ':phase':'RESUMING', ':submitted':'NEW_RUN_SUBMITTED',
                ':now':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')})

    def watchdog_state(self):
        return self.table.get_item(Key={'pk':'watchdog#monitoring'}, ConsistentRead=True).get('Item', {})

    def watchdog_save(self, **values):
        names = {'#'+str(i):key for i,key in enumerate(values)}
        vals = {':'+str(i):value for i,value in enumerate(values.values())}
        self.table.update_item(Key={'pk':'watchdog#monitoring'},
            UpdateExpression='SET '+', '.join('#'+str(i)+' = :'+str(i) for i in range(len(values))),
            ExpressionAttributeNames=names, ExpressionAttributeValues=vals)

    def watchdog_notice(self, rid):
        try:
            self.table.put_item(
                Item=dict(pk='event#'+rid, kind='event', state='HOLD', action='HOLD',
                          reason='watchdog_missing_and_no_gpu_fault', published=False,
                          event=dict(event_id=rid, job_id=None, generation=None)),
                ConditionExpression='attribute_not_exists(pk)')
        except Exception as exc:
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') != 'ConditionalCheckFailedException':
                raise
