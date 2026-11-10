"""Versioned recovery evidence contract and conditional writer.

The caller's authenticated transport/IAM adapter must supply producer identity;
never copy it from a message header/body. This module does not authenticate a network client.
"""
import hashlib
import json
import time
from decimal import Decimal

DEFAULT_POLICY = dict(max_age_seconds=300, future_skew_seconds=30,
                      placement_producers=['scheduling'], progress_producers=['training'])


def integer(value):
    return type(value) is int or (isinstance(value, Decimal) and value.is_finite())


def whole(value):
    return integer(value) and value == int(value)


def policy_config(policy=None):
    policy = dict(DEFAULT_POLICY, **(policy or {}))
    if set(policy) != set(DEFAULT_POLICY):
        raise ValueError('unknown evidence policy field')
    for key in ('max_age_seconds', 'future_skew_seconds'):
        if type(policy[key]) is not int or policy[key] < (1 if key == 'max_age_seconds' else 0):
            raise ValueError('invalid evidence time policy')
    for key in ('placement_producers', 'progress_producers'):
        if not isinstance(policy[key], list) or not policy[key] or any(not isinstance(x, str) or not x for x in policy[key]):
            raise ValueError('explicit evidence producer allowlist required')
    return policy


def provenance_error(meta, producer_kind, now, policy):
    if not isinstance(meta, dict) or not whole(meta.get('schema_version')) or meta.get('schema_version') != 1:
        return 'versioned evidence required'
    if meta.get('producer') not in policy[producer_kind + '_producers']:
        return 'unapproved evidence producer'
    if not isinstance(meta.get('event_id'), str) or not meta['event_id']:
        return 'evidence event_id required'
    for key in ('observed_at', 'received_at'):
        value = meta.get(key)
        if not whole(value) or not now - policy['max_age_seconds'] <= value <= now + policy['future_skew_seconds']:
            return 'fresh evidence timestamps required'
    if meta['observed_at'] > meta['received_at'] + policy['future_skew_seconds']:
        return 'evidence clock order invalid'
    return None


def assess(placement, fence, job, recovery_id, generation, cluster, action, excluded_node, now, policy=None):
    """Return (ready | waiting | failed, reason); malformed data never completes recovery."""
    policy = policy_config(policy)
    if not isinstance(placement, dict):
        return 'waiting', 'placement evidence required'
    progress = placement.get('progress')
    if not isinstance(progress, dict):
        return 'waiting', 'training progress evidence required'
    for meta, kind in ((placement.get('placement_evidence'), 'placement'), (progress, 'progress')):
        error = provenance_error(meta, kind, now, policy)
        if error:
            return 'waiting', error
    for item, field in ((placement, 'placement_generation'), (progress, 'generation')):
        if (item.get('recovery_id') != recovery_id or not whole(item.get(field))
                or item.get(field) != generation
                or item.get('cluster') != cluster or not fence.get('new_jobset_uid')
                or item.get('jobset_uid') != fence['new_jobset_uid']):
            return 'waiting', 'current recovery/generation/cluster/JobSet evidence required'
    if progress.get('outcome') == 'failed':
        return 'failed', 'trusted restore/training failure: ' + str(progress.get('failure_reason', 'unspecified'))[:256]
    if progress.get('outcome') != 'succeeded':
        return 'waiting', 'successful progress evidence required'
    baseline = job.get('resume_step', 0)
    restored = (progress.get('checkpoint_loaded') is True
                and whole(progress.get('restored_gen')) and progress.get('restored_gen') == job.get('resume_gen')
                and whole(progress.get('restored_step')) and progress.get('restored_step') == baseline)
    if job.get('resume_gen') is None:
        restored = job.get('allow_restart_from_scratch') is True and progress.get('started_from_scratch') is True
    completed = progress.get('completed_step')
    if not restored or not whole(completed) or completed <= baseline:
        return 'waiting', 'verified restore and first completed step required'
    nodes = placement.get('active_nodes')
    if (not isinstance(nodes, list) or len(nodes) != int(job['gpu_count'])
            or any(not isinstance(n, str) or not n for n in nodes) or len(set(nodes)) != len(nodes)
            or (action != 'RETRY_GROUP' and excluded_node in nodes)):
        return 'waiting', 'fresh placement evidence required'
    return 'ready', 'verified placement, restore and first completed step'


class EvidenceWriter:
    """Integration boundary for a trusted writer; SDK auth/producer adapters are external."""
    def __init__(self, store, policy=None, clock=time.time):
        self.store, self.policy, self.clock = store, policy_config(policy), clock

    def write(self, message, *, authenticated_producer):
        kind = message.get('kind')
        if kind not in ('placement', 'progress'):
            raise ValueError('evidence kind must be placement or progress')
        if 'producer' in message or 'received_at' in message:
            raise ValueError('producer and receipt time must come from authenticated ingestion')
        now = int(self.clock())
        body = dict(message, producer=authenticated_producer, received_at=now)
        error = provenance_error(body, kind, now, self.policy)
        if error:
            raise ValueError(error)
        for field in ('job_id', 'recovery_id', 'jobset_uid', 'event_id'):
            if not isinstance(body.get(field), str) or not body[field]:
                raise ValueError('evidence identity required: ' + field)
        if not whole(body.get('generation')) or body['generation'] < 1 or body.get('cluster') not in ('eks-a', 'eks-b', 'eks-c'):
            raise ValueError('valid evidence generation and cluster required')
        fence = self.store.fencing_record(body['job_id'])
        if (fence.get('generation') != body['generation'] or fence.get('recovery_id') != body['recovery_id']
                or fence.get('new_jobset_uid') != body['jobset_uid']
                or fence.get('recovery_phase') not in ('NEW_RUN_SUBMITTED', 'RESUMING')
                or (fence.get('owner') is not None and fence['owner'] != body['cluster'])):
            raise ValueError('evidence does not match current fencing authority')
        if kind == 'placement':
            nodes = body.get('active_nodes')
            if not isinstance(nodes, list) or not nodes or any(not isinstance(n, str) or not n for n in nodes) or len(set(nodes)) != len(nodes):
                raise ValueError('distinct active_nodes required')
        elif body.get('outcome') == 'failed':
            if not isinstance(body.get('failure_reason'), str) or not body['failure_reason']:
                raise ValueError('failure reason required')
        elif (body.get('outcome') != 'succeeded' or not whole(body.get('completed_step')) or body['completed_step'] < 1):
            raise ValueError('completed step or explicit failure required')
        if kind == 'progress' and body.get('outcome') == 'succeeded':
            baseline = fence.get('recovery_resume_step', 0)
            if body['completed_step'] <= baseline:
                raise ValueError('first completed step must exceed the fixed restore baseline')
            if fence.get('recovery_resume_gen') is None:
                if body.get('started_from_scratch') is not True:
                    raise ValueError('from-scratch execution evidence required')
            elif (body.get('checkpoint_loaded') is not True or not whole(body.get('restored_gen'))
                  or body['restored_gen'] != fence['recovery_resume_gen']
                  or not whole(body.get('restored_step')) or body['restored_step'] != baseline):
                raise ValueError('restoration must match the fixed checkpoint baseline')
        digest = hashlib.sha256(json.dumps(message, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        body['digest'] = digest
        return self.store.write_evidence(body)
