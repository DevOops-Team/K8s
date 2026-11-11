"""DynamoDB durable inbox/job CAS and fencing progress records."""
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from recovery.evidence import whole


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
        state = values.get('state')
        count_transition = state in ('HOLD', 'REJECTED')
        previous = self.table.update_item(Key={'pk':'event#'+rid},
            UpdateExpression='SET '+', '.join('#'+str(i)+' = :'+str(i) for i in range(len(values))),
            ExpressionAttributeNames=names, ExpressionAttributeValues=vals,
            ReturnValues='ALL_OLD' if count_transition else 'ALL_NEW')['Attributes']
        if count_transition:
            if previous.get('state') != state:
                from runtime.metrics import record_hold, record_rejected
                record = record_hold if state == 'HOLD' else record_rejected
                record(previous['event'].get('job_id'), values.get('reason'))
            return {**previous, **values}
        return previous

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

    def block_checkpoint(self, event):
        failed = event['checkpoint']
        key = f"checkpoint#{event['job_id']}#{failed['generation']}#{failed['step']}"
        # Idempotent across different Pod events and response loss; never mutate approvals.
        self.table.update_item(Key={'pk': key},
            UpdateExpression='SET #kind = :kind, blocked = :blocked, job_id = :job, '
                             'checkpoint_generation = :gen, checkpoint_step = :step, '
                             'first_event_id = if_not_exists(first_event_id, :event)',
            ExpressionAttributeNames={'#kind': 'kind'},
            ExpressionAttributeValues={':kind': 'checkpoint_block', ':blocked': True,
                ':job': event['job_id'], ':gen': failed['generation'], ':step': failed['step'],
                ':event': event['event_id']})

    def require_usable_checkpoint(self, job_id, generation, step):
        if generation is None:
            return
        key = f"checkpoint#{job_id}#{int(generation)}#{int(step)}"
        item = self.table.get_item(Key={'pk': key}, ConsistentRead=True).get('Item', {})
        if item.get('blocked'):
            raise RecoveryBlocked('selected checkpoint quarantined after ckpt-init code 7')

    def checkpoint_blocked(self, job_id, generation, step):
        key = f"checkpoint#{job_id}#{int(generation)}#{int(step)}"
        item = self.table.get_item(Key={'pk': key}, ConsistentRead=True).get('Item', {})
        return item.get('blocked') is True

    def select_checkpoint(self, job_id, current):
        """A blocked approved baseline falls back once; never treats transport errors as corruption."""
        generation, step = current.get('approved_gen'), current.get('approved_step')
        if generation is None:
            return None, 0, 'NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH', False
        if not self.checkpoint_blocked(job_id, generation, step):
            return generation, int(step), None, False
        previous_gen, previous_step = current.get('prev_approved_gen'), current.get('prev_approved_step')
        valid = (not isinstance(previous_gen, bool) and isinstance(previous_gen, (int, Decimal))
                 and previous_gen == int(previous_gen) and 1 <= previous_gen <= generation
                 and not isinstance(previous_step, bool) and isinstance(previous_step, (int, Decimal))
                 and previous_step == int(previous_step) and 0 <= previous_step < step)
        if valid and not self.checkpoint_blocked(job_id, previous_gen, previous_step):
            return previous_gen, int(previous_step), 'PREVIOUS_APPROVED_CHECKPOINT', True
        return None, 0, 'CHECKPOINTS_UNUSABLE_RESTART_FROM_SCRATCH', True

    @staticmethod
    def checkpoint_snapshot_condition(current, values):
        conditions = []
        for name in ('approved_gen', 'approved_step', 'prev_approved_gen', 'prev_approved_step'):
            if name in current:
                values[':' + name] = current[name]
                conditions.append(name + ' = :' + name)
            else:
                conditions.append('attribute_not_exists(' + name + ')')
        return ' AND '.join(conditions)

    def claim(self, event):
        key = {'job_id':event['job_id']}
        current = self.fencing.get_item(Key=key, ConsistentRead=True).get('Item')
        if not current:
            raise RecoveryBlocked('fencing job record missing')
        handoff = (event.get('component') == 'ckpt-init' and event.get('exit_code') == 7
                   and current.get('recovery_phase') in ('NEW_RUN_SUBMITTED', 'RESUMING')
                   and current.get('new_jobset_uid') == event.get('jobset_uid')
                   and event.get('checkpoint') == {'generation': current.get('recovery_resume_gen'),
                                                   'step': current.get('recovery_resume_step')})
        metadata = self.context(event, recovering=current.get('recovery_id') == event['event_id'] or handoff)
        if metadata is None:
            return None
        if current.get('recovery_id') == event['event_id']:
            self.require_usable_checkpoint(event['job_id'], current.get('recovery_resume_gen'), current.get('recovery_resume_step', 0))
            phase = current.get('recovery_phase')
            if phase == 'CLAIMED':
                return self.advance_generation(event, metadata, current)
            if (int(current.get('generation',0)) != event['generation']+1
                    or int(current.get('recovery_from_gen',0)) != event['generation']
                    or int(current.get('recovery_to_gen',0)) != event['generation']+1):
                raise RecoveryBlocked('recovery record generation mismatch')
            # Never revoke an owner already acquired by the new init on replay.
            return dict(metadata, generation=current['generation'], retry_count=int(current.get('retry_count', 0)),
                        resume_step=int(current.get('recovery_resume_step', current.get('approved_step', 0))),
                        resume_gen=current.get('recovery_resume_gen', current.get('approved_gen')),
                        allow_restart_from_scratch=current.get('recovery_resume_gen') is None,
                        checkpoint_warning=current.get('recovery_checkpoint_warning',
                            'NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH' if current.get('recovery_resume_gen') is None else None))
        if int(current.get('generation',0)) != event['generation']:
            return None
        if current.get('recovery_id') and not handoff:
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
        resume_gen, resume_step, warning, fallback = self.select_checkpoint(event['job_id'], current)
        # Lock the exact approved baseline. A simultaneous sidecar approval causes a
        # conditional failure and a reread on the next reconciliation cycle.
        values = {':old':event['generation'], ':new':event['generation']+1,
                  ':rid':event['event_id'], ':source':event['cluster'],
                  ':phase':'CLAIMED', ':now':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  ':resume':resume_step, ':resume_gen':resume_gen, ':warning':warning,
                  ':limit':self.max_recovery_attempts}
        approval_condition = self.checkpoint_snapshot_condition(current, values)
        lock_condition = 'attribute_not_exists(recovery_id)'
        if current.get('recovery_id'):
            values.update({':previous_rid': current['recovery_id'], ':previous_phase': current['recovery_phase'],
                           ':failed_uid': current['new_jobset_uid'],
                           ':failed_resume_gen': current.get('recovery_resume_gen'),
                           ':failed_resume_step': current.get('recovery_resume_step')})
            lock_condition = ('recovery_id = :previous_rid AND recovery_phase = :previous_phase '
                              'AND new_jobset_uid = :failed_uid AND recovery_resume_gen = :failed_resume_gen '
                              'AND recovery_resume_step = :failed_resume_step')
        update = ('SET recovery_id = :rid, recovery_from_gen = :old, recovery_to_gen = :new, '
                  'recovery_phase = :phase, recovery_updated_at = :now, '
                  'recovery_resume_step = :resume, recovery_resume_gen = :resume_gen, '
                  'recovery_checkpoint_warning = :warning')
        if fallback and resume_gen is not None:
            update += ', approved_gen = :resume_gen, approved_step = :resume'
        elif fallback:
            update += ' REMOVE approved_gen, approved_step'
        self.fencing.update_item(Key=key,
            UpdateExpression=update,
            ConditionExpression=('generation = :old AND (attribute_not_exists(#owner) OR #owner = :source) '
                                 'AND ('+lock_condition+') AND (attribute_not_exists(retry_count) OR retry_count < :limit) AND ('+approval_condition+')'),
            ExpressionAttributeNames={'#owner':'owner'}, ExpressionAttributeValues=values)
        # This turn only claims the recovery: old generation/owner/retry_count remain.
        return dict(metadata, generation=event['generation'], retry_count=int(current.get('retry_count', 0)), claim_pending=True,
                    checkpoint_warning=warning)

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
        return dict(metadata, generation=old+1, retry_count=int(current.get('retry_count', 0))+1,
                    resume_step=int(current.get('recovery_resume_step', 0)),
                    resume_gen=current.get('recovery_resume_gen'),
                    allow_restart_from_scratch=current.get('recovery_resume_gen') is None,
                    checkpoint_warning=current.get('recovery_checkpoint_warning',
                        'NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH' if current.get('recovery_resume_gen') is None else None))

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
        if (not record.get('evidence_progress_id') or not record.get('evidence_placement_id')
                or not record.get('evidence_jobset_uid')
                or record['evidence_jobset_uid'] != current.get('new_jobset_uid')
                or not whole(record.get('evidence_completed_step'))
                or record['evidence_completed_step'] <= int(current.get('recovery_resume_step', 0))):
            raise RecoveryBlocked('verified recovery evidence snapshot required before COMPLETED')
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
                ConditionExpression='recovery_id = :rid AND generation = :generation AND #owner = :target AND new_jobset_uid = :uid',
                ExpressionAttributeNames={'#owner':'owner'},
                ExpressionAttributeValues={':rid':event['event_id'], ':generation':generation, ':target':target, ':phase':'COMPLETED', ':uid':record['evidence_jobset_uid'],
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

    def write_evidence(self, body):
        """Conditional current-state projection; never creates an unregistered job."""
        job_id, kind = body['job_id'], body['kind']
        current = self.placement(job_id)
        if not current:
            raise RecoveryBlocked('registered recovery job metadata missing')
        generation, uid, rid = int(body['generation']), body['jobset_uid'], body['recovery_id']
        meta_key = 'placement_evidence' if kind == 'placement' else 'progress'
        old = current.get(meta_key, {})
        if old.get('event_id') == body['event_id']:
            if old.get('digest') != body['digest']:
                raise ValueError('evidence event_id reused with different content')
            return False  # Never refresh timestamps on a duplicate.
        old_gen = int(current.get('placement_generation', 0))
        if old_gen > generation:
            return False
        if kind == 'progress' or old_gen == generation:
            if (old_gen != generation or current.get('jobset_uid') != uid
                    or current.get('recovery_id') != rid or current.get('cluster') != body['cluster']):
                raise RecoveryBlocked('progress/placement identity does not match current placement')
        if old_gen == generation:
            if body['observed_at'] < old.get('observed_at', 0):
                return False
            if kind == 'progress':
                if old.get('outcome') == 'failed':
                    raise RecoveryBlocked('failure evidence is latched until a new recovery')
                if body.get('completed_step', 0) < old.get('completed_step', 0) and body.get('outcome') != 'failed':
                    return False
        names = {'#cluster':'cluster', '#meta':meta_key}
        values = {':gen':generation, ':uid':uid, ':rid':rid, ':cluster':body['cluster']}
        if kind == 'placement':
            if len(body['active_nodes']) != int(current['gpu_count']):
                raise ValueError('placement node count must match registered GPU count')
            meta = {key:body[key] for key in ('schema_version','event_id','producer','observed_at','received_at','digest')}
            values.update({':meta':meta, ':nodes':body['active_nodes']})
            update = ('SET placement_generation = :gen, jobset_uid = :uid, recovery_id = :rid, '
                      '#cluster = :cluster, active_nodes = :nodes, #meta = :meta')
            if old_gen < generation:
                update += ' REMOVE progress'
            condition = ('attribute_exists(pk) AND (attribute_not_exists(placement_generation) OR placement_generation < :gen '
                         'OR (placement_generation = :gen AND jobset_uid = :uid AND recovery_id = :rid AND #cluster = :cluster))')
        else:
            values[':meta'] = {key:value for key,value in body.items() if key != 'kind'}
            update = 'SET #meta = :meta'
            condition = ('attribute_exists(pk) AND placement_generation = :gen AND jobset_uid = :uid '
                         'AND recovery_id = :rid AND #cluster = :cluster')
        if old_gen == generation:
            if old.get('event_id'):
                values[':prior'] = old['event_id']
                condition += ' AND #meta.event_id = :prior'
            else:
                condition += ' AND attribute_not_exists(#meta)'
        try:
            self.table.update_item(Key={'pk':'job#'+job_id}, UpdateExpression=update,
                ConditionExpression=condition, ExpressionAttributeNames=names,
                ExpressionAttributeValues=values)
        except Exception as exc:
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                return False  # Concurrent newer evidence won; caller can reread/reconcile.
            raise
        return True

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
        else:
            from runtime.metrics import record_hold
            record_hold(None, 'watchdog_missing_and_no_gpu_fault')
