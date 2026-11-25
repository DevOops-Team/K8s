"""ADR-49 verified stop reports. Disabled until the complete producer contract is tested."""
from datetime import datetime
import json
import time
from integrations.dynamodb import RecoveryBlocked
from recovery.evidence import whole

ANNOTATION = 'devoops/self-fence-verified'


def self_fence_config(value=None):
    value = {} if value is None else value
    if not isinstance(value, dict) or set(value) - {'enabled', 'producer_contract_confirmed'}:
        raise ValueError('unknown self_fence_input setting')
    result = {'enabled': False, 'producer_contract_confirmed': False}
    result.update(value)
    if any(type(v) is not bool for v in result.values()):
        raise ValueError('self fence flags must be boolean')
    if result['enabled'] and not result['producer_contract_confirmed']:
        raise ValueError('self fence requires tested jobspec/sidecar producer contract')
    return result


def pod_count(request):
    spec = request['spec']['jobSet']['spec']
    rjs = spec['replicatedJobs']
    if spec.get('failurePolicy', {}).get('maxRestarts') != 0 or len(rjs) != 1 or rjs[0].get('replicas', 1) != 1:
        raise ValueError('self fence requires one non-restarting replicatedJob')
    job = rjs[0]['template']['spec']
    n = job['parallelism']
    if (type(n) is not int or not 1 <= n <= 4 or job.get('completions', n) != n
            or job.get('completionMode') != 'Indexed' or job.get('backoffLimit') != 0
            or job.get('podReplacementPolicy') != 'Failed'
            or job['template']['spec'].get('restartPolicy') != 'Never'):
        raise ValueError('self fence requires tested no-replacement jobspec')
    return n


def check_authority(event, fence):
    if (not all(whole(fence.get(k)) for k in ('generation', 'recovery_from_gen', 'recovery_to_gen'))
            or fence['generation'] != event['generation'] + 1
            or fence['recovery_from_gen'] != event['generation']
            or fence['recovery_to_gen'] != event['generation'] + 1
            or fence.get('recovery_id') != event['event_id']):
        raise RecoveryBlocked('self fence lost recovery authority')


def proof(event, request, value, now, started_at):
    """Validate the exact RR annotation contract, including all Pod indexes and time."""
    n = pod_count(request)
    if (not isinstance(value, dict) or value.get('requestId') != event['event_id']
            or type(value.get('generation')) is not int or value['generation'] != event['generation']
            or request['spec'].get('requestId') != event['event_id']
            or request['spec'].get('jobRef') != f"{request['metadata']['namespace']}/job-{event['job_id']}-g{event['generation']:04d}"
            or request['spec'].get('toGeneration') != event['generation'] + 1
            or value.get('cluster') != event['cluster'] or type(value.get('worldSize')) is not int
            or value['worldSize'] != n):
        raise ValueError('self fence identity mismatch')
    ranks, pods = value.get('ranks'), value.get('pods')
    if (not isinstance(ranks, list) or any(type(v) is not int for v in ranks)
            or len(ranks) != n or set(ranks) != set(range(n))
            or not isinstance(pods, list) or len(pods) != n):
        raise ValueError('incomplete self fence reports')
    indexes, uids = set(), set()
    for pod in pods:
        index, sep, uid = pod.partition(':') if isinstance(pod, str) else ('', '', '')
        if not sep or not index.isdigit() or str(int(index)) != index or not uid.strip() or len(uid) > 128:
            raise ValueError('invalid self fence Pod identity')
        indexes.add(int(index)); uids.add(uid)
    if indexes != set(range(n)) or len(uids) != n:
        raise ValueError('duplicate self fence Pod identity')
    recorded = datetime.fromisoformat(value['recordedAt'].replace('Z', '+00:00'))
    if not whole(started_at) or not 0 < started_at <= now + 30:
        raise ValueError('invalid fencing stop_wait_started_at')
    if recorded.tzinfo is None or not int(started_at) - 30 <= recorded.timestamp() <= now + 30:
        raise ValueError('self fence time outside recovery window')
    return value


class SelfFenceInput:
    def __init__(self, store, clock=time.time):
        self.store, self.clock = store, clock

    def __call__(self, event, request):
        fence = self.store.fencing_record(event['job_id'])  # Store uses ConsistentRead.
        check_authority(event, fence)
        if fence.get('recovery_phase') != 'GENERATION_ADVANCED':
            return None
        try:
            if not whole(fence.get('self_fence_gen')) or not fence['generation'] > fence['self_fence_gen']:
                return None
            ranks, pods = fence.get('self_fence_ranks', []), fence.get('self_fence_pods', [])
            if not all(whole(v) for v in ranks):
                return None
            size = fence.get('self_fence_world_size')
            if not whole(size):
                return None
            value = dict(requestId=fence['recovery_id'], generation=int(fence['self_fence_gen']),
                         cluster=fence['self_fence_cluster'], worldSize=int(size),
                         ranks=sorted(int(v) for v in ranks), pods=sorted(pods), recordedAt=fence['self_fence_at'])
            proof(event, request, value, self.clock(), fence.get('stop_wait_started_at'))
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            return None  # Partial/invalid reports never grant stop permission; normal stop deadline remains.
        # Once complete, keep the attestation stable across duplicate late Pod reports.
        existing = (request.get('metadata', {}).get('annotations') or {}).get(ANNOTATION)
        if existing:
            try:
                old = proof(event, request, json.loads(existing), self.clock(), fence.get('stop_wait_started_at'))
                if all(old[k] == value[k] for k in value if k != 'recordedAt'):
                    return existing
            except (ValueError, TypeError, KeyError, AttributeError):
                pass
            raise RecoveryBlocked('conflicting verified self fence annotation')
        return json.dumps(value, sort_keys=True, separators=(',', ':'))


def validate_status(event, request, evidence, now, fence):
    """A status kind alone cannot authorize self-fence: require our matching RR proof."""
    check_authority(event, fence)
    try:
        value = json.loads(request['metadata']['annotations'][ANNOTATION])
        proof(event, request, value, now, fence.get('stop_wait_started_at'))
        checks = evidence['checks']
        if (not any(c['kind'] == 'self_fence' for c in checks)
                or any(c['cluster'] != value['cluster'] for c in checks if c['kind'] == 'self_fence')
                or evidence['kind'] != ('ec2' if any(c['kind'] == 'ec2' for c in checks) else 'self_fence')):
            raise ValueError('self fence status does not match verified cluster/kind')
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        raise RecoveryBlocked('verified self fence annotation required') from exc
