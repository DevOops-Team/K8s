"""Restartable recovery orchestration; all external commands are idempotent contracts."""
import hashlib
import json
from controller import plan
from adapters import RecoveryBlocked

TERMINAL = {'RECOVERED', 'REJECTED', 'REVIEW', 'STALE', 'OBSERVED'}


def validate_event(event):
    plan(event)
    if type(event.get('generation')) is not int or event['generation'] < 1:
        raise ValueError('generation must be a positive integer')
    if not isinstance(event.get('node'), str) or not event['node'].strip():
        raise ValueError('node is required')
    return event


class Workflow:
    def __init__(self, store, executor, execute=False):
        self.store, self.executor, self.execute = store, executor, execute

    def accept(self, event):
        validate_event(event)
        canonical = json.dumps(event, sort_keys=True, separators=(',', ':'))
        self.store.accept(event, hashlib.sha256(canonical.encode()).hexdigest())

    def tick(self, record):
        e = record['event']
        rid = e['event_id']
        action = plan(e)['suggested_action']
        state = record.get('state', 'RECEIVED')
        if state in TERMINAL:
            return record
        if not self.execute:
            return self.store.save(rid, state='OBSERVED', action=action)
        if action in ('REVIEW', 'REJECT'):
            return self.store.save(rid, state='REJECTED' if action == 'REJECT' else 'REVIEW', action=action)
        # The authoritative job record, not the event, supplies checkpoint/GPU count.
        try:
            job = self.store.claim(e)
        except RecoveryBlocked as exc:
            return self.store.save(rid, state='REVIEW', reason=str(exc))
        if job is None:
            return self.store.save(rid, state='STALE')
        request = dict(recovery_id=rid, job_id=e['job_id'], source_cluster=e['cluster'],
                       failed_node=e['node'], old_generation=int(e['generation']),
                       generation=int(job['generation']), gpu_count=int(job['gpu_count']),
                       jobset_uri=job['jobset_uri'], jobset_sha256=job['jobset_sha256'],
                       checkpoint_source='dynamodb-approved', action=action)
        if state == 'RECEIVED':
            # CAS already revoked old result authority. This must confirm actual execution stop.
            result = self.executor.call('fence', request)
            if result.get('old_execution_stopped') is not True:
                return record
            self.store.save(rid, state='FENCED', request=request)
            return self.store.get(rid)
        if state == 'FENCED':
            result = self.executor.call('restart', request)
            if result.get('accepted') is not True:
                return record
            target = result.get('target_cluster')
            if target not in ('eks-a', 'eks-b', 'eks-c'):
                raise ValueError('executor returned unknown target cluster')
            if action != 'HOLD_FOR_FENCING' and target != e['cluster']:
                raise ValueError('same-cluster action cannot move to another cluster')
            if action == 'HOLD_FOR_FENCING' and target == e['cluster']:
                raise ValueError('AZ evacuation must choose another cluster')
            return self.store.save(rid, state='SUBMITTED', target_cluster=target)
        if state == 'SUBMITTED':
            request['target_cluster'] = record['target_cluster']
            result = self.executor.call('status', request)
            if not (result.get('generation') == int(job['generation'])
                    and result.get('target_cluster') == record['target_cluster']
                    and result.get('checkpoint_loaded') is True
                    and result.get('step_completed') is True):
                return record
            nodes=result.get('active_nodes')
            if (not isinstance(nodes,list) or len(nodes)!=int(job['gpu_count'])
                    or any(not isinstance(n,str) or not n for n in nodes) or len(set(nodes))!=len(nodes)):
                return record
            if action in ('RELOCATE_SAME_CLUSTER','HOLD_FOR_FENCING') and e['node'] in nodes:
                return record
            if not self.store.owner_matches(e['job_id'],int(job['generation']),record['target_cluster']):
                return record
            # Keep job lock until durable terminal record exists; replay can finish unlock.
            return self.store.save(rid, state='RECOVERED', action=action,
                                   generation=int(job['generation']), target_cluster=record['target_cluster'], active_nodes=nodes)
        raise ValueError('unknown recovery state')
