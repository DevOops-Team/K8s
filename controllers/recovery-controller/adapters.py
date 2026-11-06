"""DynamoDB durable inbox/job CAS and authenticated scheduler HTTP adapter."""
import json
import os
import time
import urllib.request
import urllib.parse


class RecoveryBlocked(ValueError):
    """A decision that needs review, not an endlessly retried write."""


class Store:
    def __init__(self, table, fencing_table):
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

    def context(self, event):
        item = self.table.get_item(Key={'pk':'job#'+event['job_id']}, ConsistentRead=True).get('Item')
        if not item:
            raise RecoveryBlocked('registered recovery job metadata missing')
        if item.get('cluster') != event['cluster'] or event['node'] not in item.get('active_nodes',[]):
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
        metadata = self.context(event)
        if metadata is None:
            return None
        key = {'job_id':event['job_id']}
        current = self.fencing.get_item(Key=key, ConsistentRead=True).get('Item')
        if not current:
            raise RecoveryBlocked('fencing job record missing')
        if current.get('recovery_id') == event['event_id']:
            if (int(current.get('generation',0)) != event['generation']+1
                    or int(current.get('recovery_from_gen',0)) != event['generation']):
                raise RecoveryBlocked('recovery record generation mismatch')
            # Do not REMOVE owner again: a new init may already have acquired it.
            return dict(metadata, generation=current['generation'])
        if int(current.get('generation',0)) != event['generation']:
            return None
        if current.get('recovery_id'):
            raise RecoveryBlocked('another recovery holds job lock')
        if 'owner' in current and current['owner'] != event['cluster']:
            raise RecoveryBlocked('source cluster does not own this generation')
        approved = ('approved_gen' in current, 'approved_step' in current)
        if approved[0] != approved[1]:
            raise RecoveryBlocked('incomplete approved checkpoint fields')
        if not all(approved) and metadata.get('allow_restart_from_scratch') is not True:
            raise RecoveryBlocked('no approved checkpoint; explicit from-scratch policy required')
        if all(approved) and not (1 <= int(current['approved_gen']) <= event['generation']
                                  and int(current['approved_step']) >= 0):
            raise RecoveryBlocked('invalid approved checkpoint fields')
        updated = self.fencing.update_item(Key=key,
            UpdateExpression='SET generation = :new, recovery_id = :rid, recovery_from_gen = :old, recovery_to_gen = :new REMOVE #owner',
            ConditionExpression='generation = :old AND (attribute_not_exists(#owner) OR #owner = :source) AND attribute_not_exists(recovery_id)',
            ExpressionAttributeNames={'#owner':'owner'},
            ExpressionAttributeValues={':new':event['generation']+1, ':old':event['generation'],
                                       ':rid':event['event_id'], ':source':event['cluster']},
            ReturnValues='ALL_NEW')['Attributes']
        # approved_gen, approved_step and heartbeat are never overwritten here.
        return dict(metadata, generation=updated['generation'])

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
                UpdateExpression='REMOVE recovery_id, recovery_from_gen, recovery_to_gen',
                ConditionExpression='recovery_id = :rid AND generation = :generation AND #owner = :target',
                ExpressionAttributeNames={'#owner':'owner'},
                ExpressionAttributeValues={':rid':event['event_id'], ':generation':generation, ':target':target})
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


class Executor:
    def __init__(self, url, token):
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError('executor URL must be HTTPS without embedded credentials')
        if not token:
            raise ValueError('executor bearer token required')
        self.url, self.token = url.rstrip('/'), token

    def call(self, operation, payload):
        # Execution service MUST durably deduplicate this ID, including response-loss retries.
        req = urllib.request.Request(self.url+'/v1/recoveries/'+operation,
            data=json.dumps(payload).encode(), method='POST',
            headers={'Content-Type':'application/json', 'Authorization':'Bearer '+self.token,
                     'Idempotency-Key':payload['recovery_id']+':'+operation})
        # Refuse redirects: do not forward bearer tokens to another origin.
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *_): return None
        with urllib.request.build_opener(NoRedirect).open(req, timeout=10) as response:
            result = json.loads(response.read(1024*1024))
        if not isinstance(result,dict) or (result.get('recovery_id') != payload['recovery_id']
                or result.get('generation') != payload['generation']):
            raise ValueError('executor response recovery_id mismatch')
        return result
