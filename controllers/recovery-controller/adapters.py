"""DynamoDB durable inbox/job CAS and authenticated scheduler HTTP adapter."""
import json
import os
import time
import urllib.request
import urllib.parse


class Store:
    def __init__(self, table):
        self.table = table

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

    def claim(self, event):
        key = {'pk':'job#'+event['job_id']}
        current = self.table.get_item(Key=key, ConsistentRead=True).get('Item')
        if current and current.get('recovery_id') == event['event_id']:
            return current  # Crash after CAS: do not advance generation twice.
        if not current:
            raise ValueError('authoritative job record missing')
        if (int(current.get('generation',0)) != event['generation']
                or current.get('cluster') != event['cluster']):
            return None
        if current.get('recovery_id'):
            raise RuntimeError('another recovery holds job lock')
        if event['node'] not in current.get('active_nodes',[]):
            return None  # A reported node must belong to this authoritative execution.
        if type(current.get('gpu_count')) not in (int,) and not str(current.get('gpu_count','')).isdigit():
            raise ValueError('invalid authoritative gpu_count')
        if not 1 <= int(current['gpu_count']) <= 4 or not current.get('checkpoint_uri','').startswith('s3://'):
            raise ValueError('approved checkpoint and GPU count 1..4 required')
        return self.table.update_item(Key=key,
            UpdateExpression='SET generation = :new, recovery_id = :rid',
            ConditionExpression='generation = :old AND #cluster = :cluster AND contains(active_nodes, :node) AND attribute_not_exists(recovery_id)',
            ExpressionAttributeNames={'#cluster':'cluster'},
            ExpressionAttributeValues={':new':event['generation']+1, ':old':event['generation'],
                                       ':rid':event['event_id'], ':cluster':event['cluster'], ':node':event['node']},
            ReturnValues='ALL_NEW')['Attributes']

    def finish(self, record):
        if record['state'] != 'RECOVERED':
            return
        key={'pk':'job#'+record['event']['job_id']}
        current=self.table.get_item(Key=key,ConsistentRead=True).get('Item',{})
        if (current.get('recovery_id') != record['event']['event_id']
                and (current.get('generation',0) > record['generation']
                     or (current.get('generation') == record['generation']
                         and current.get('cluster') == record['target_cluster']))):
            return
        self.table.update_item(Key=key,
            UpdateExpression='SET #cluster = :cluster REMOVE recovery_id',
            ConditionExpression='recovery_id = :rid AND generation = :generation',
            ExpressionAttributeNames={'#cluster':'cluster'},
            ExpressionAttributeValues={':cluster':record['target_cluster'], ':rid':record['event']['event_id'],
                                       ':generation':record['generation']})

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
