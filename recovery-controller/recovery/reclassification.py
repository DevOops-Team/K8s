"""Correlate classifier revisions without rewriting an existing recovery.

event_id remains the inbox/idempotency key. dedup_key only relates decisions
for the same cluster/job/generation; it never consumes or suppresses a key.
"""
from datetime import datetime
from recovery.evidence import whole


def identity(event):
    key, job, generation = (event.get(k) for k in ('dedup_key', 'job_id', 'generation'))
    if (not isinstance(key, str) or not key.strip() or not isinstance(job, str) or not job
            or not whole(generation) or generation < 1):
        return None
    return event.get('cluster'), job, int(generation), key


def decision_time(event):
    value = event.get('classification_occurred_at')
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return stamp if stamp.tzinfo is not None and stamp.utcoffset() is not None else None
    except ValueError:
        return None


def started(record, fence):
    return (fence.get('recovery_id') == record['event']['event_id']
            or bool(record.get('relocation_request'))
            or record.get('state') in ('CLAIMED', 'FENCED', 'REQUESTED', 'SUBMITTED',
                                       'RESUMING', 'RECOVERED'))


def reconcile(store, record, action):
    event = record['event']
    group = identity(event)
    if group is None:
        return None
    is_hold_revision = (event.get('classification_type') == 'fault.reclassified'
                        and event.get('suggested_action') == 'hold')
    if not is_hold_revision and (record['state'] != 'RECEIVED' or action not in (
            'RETRY_GROUP', 'QUARANTINE_AND_RETRY', 'FENCE_AND_EVACUATE')):
        return None
    # Use the existing consistent, paginated inbox scan. Read errors propagate:
    # an incomplete scan must not be interpreted as "no revision".
    related = [other for other in store.records()
               if other['event']['event_id'] != event['event_id']
               and identity(other['event']) == group]
    if is_hold_revision:
        fence = store.fencing_record(event['job_id']) if related else {}
        active = next((other for other in related if started(other, fence)), None)
        if active is not None:
            return store.save(event['event_id'], state='IGNORED', action='NONE',
                              reason='reclassification_after_recovery_claim',
                              recommended_action='HOLD',
                              related_event_id=active['event']['event_id'],
                              related_recovery_state=active['state'],
                              related_recovery_phase=(fence.get('recovery_phase')
                                  if fence.get('recovery_id') == active['event']['event_id'] else None))
        return store.save(event['event_id'], state='HOLD', action='HOLD',
                          reason='classifier_reclassified_hold')
    revisions = [other for other in related
                 if other['event'].get('classification_type') == 'fault.reclassified'
                 and other['event'].get('suggested_action') == 'hold']
    if not revisions:
        return None
    # A CLAIMED write may have succeeded before its response/state save was lost.
    # Once authority is claimed, finish the same recovery; never cancel its lock,
    # roll back generation/owner/retry_count, or delete its RR on a late HOLD.
    if started(record, store.fencing_record(event['job_id'])):
        return None
    current_time = decision_time(event)
    for revision in revisions:
        revised_time = decision_time(revision['event'])
        if current_time is None or revised_time is None or revised_time >= current_time:
            reason = ('classification_order_unresolved' if current_time is None or revised_time is None
                      else 'classification_hold_before_claim')
            return store.save(event['event_id'], state='HOLD', action='HOLD', reason=reason,
                              related_event_id=revision['event']['event_id'])
    return None
