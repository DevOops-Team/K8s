"""ckpt-init failure adapter and conservative recovery decisions (no worker API calls)."""
import hashlib
from decimal import Decimal

POLICY = {
    2: ('HOLD', 'GENERATION_MISMATCH', 'init generation mismatch; do not take execution authority'),
    3: ('HOLD', 'OWNER_CONFLICT', 'init owner claim denied; do not take execution authority'),
    4: ('RETRY_GROUP', 'TRANSIENT_CONNECTION', 'init connection retries exhausted; bounded group retry'),
    5: ('NEEDS_CONFIG_FIX', 'CONFIG_MISMATCH', 'init configuration mismatch requires correction'),
    6: ('NEEDS_CONFIG_FIX', 'REGISTRATION_MISSING', 'init fencing registration missing; do not create it'),
    7: ('RETRY_GROUP', 'CHECKPOINT_CORRUPT', 'restore previous approved checkpoint, or restart from scratch if absent/corrupt'),
}


def _integer(value):
    # DynamoDB resource reads integer attributes as Decimal after inbox persistence.
    return (not isinstance(value, bool) and isinstance(value, (int, Decimal))
            and value == int(value))


def decision(event):
    if event.get('component') != 'ckpt-init':
        return None
    code = event.get('exit_code')
    if not _integer(code) or code not in POLICY:
        raise ValueError('ckpt-init exit_code must be one of 2,3,4,5,6,7')
    if not all(isinstance(event.get(key), str) and event[key].strip()
               for key in ('job_id', 'jobset_uid', 'pod_uid')):
        raise ValueError('ckpt-init requires job_id, jobset_uid and pod_uid')
    if not _integer(event.get('generation')) or event['generation'] < 1:
        raise ValueError('ckpt-init requires a positive integer generation')
    if code == 7:
        checkpoint = event.get('checkpoint')
        if (not isinstance(checkpoint, dict)
                or not _integer(checkpoint.get('generation'))
                or not 1 <= checkpoint['generation'] <= event['generation']
                or not _integer(checkpoint.get('step')) or checkpoint['step'] < 0):
            raise ValueError('code 7 requires the failed checkpoint generation and step')
    action, classification, reason = POLICY[code]
    return dict(action=action, failure_class=classification, reason=reason)


def collect(pod, *, cluster, job_id, generation, jobset_uid, checkpoint=None):
    """Called by a trusted Pod-status producer; never derives identity from arbitrary logs.

    The caller must verify Pod -> Job -> JobSet ownership/UID and provide the actual
    attempted checkpoint baseline. Current terminated status only; not lastState.
    """
    meta = pod.get('metadata', {})
    if meta.get('namespace') != 'training' or not meta.get('uid'):
        raise ValueError('expected an identified training Pod')
    labels = meta.get('labels', {})
    if labels.get('devoops/job-id') != job_id or labels.get('devoops/generation') != str(generation):
        raise ValueError('Pod labels do not match the verified job identity')
    statuses = [s for s in pod.get('status', {}).get('initContainerStatuses', [])
                if s.get('name') == 'ckpt-init']
    if len(statuses) != 1:
        return None
    terminated = statuses[0].get('state', {}).get('terminated')
    if not terminated or terminated.get('exitCode') == 0:
        return None
    signature = ':'.join((meta['uid'], str(terminated.get('exitCode')),
                          str(terminated.get('containerID', '')), str(terminated.get('finishedAt', ''))))
    event = dict(event_id='ckpt-init-' + hashlib.sha256(signature.encode()).hexdigest()[:40],
                 component='ckpt-init', exit_code=terminated.get('exitCode'), cluster=cluster,
                 job_id=job_id, generation=generation, jobset_uid=jobset_uid, pod_uid=meta['uid'],
                 node=pod.get('spec', {}).get('nodeName'), category='app')
    if checkpoint is not None:
        event['checkpoint'] = checkpoint
    decision(event)
    return event
