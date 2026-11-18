"""Read-only correlation safety net; never claims authority or restarts a job."""
import json
from recovery.evidence import whole


def reconcile(runner, record, now):
    store, event = runner.store, record['event']
    rid = event['event_id']
    if record['state'] in ('HOLD', 'IGNORED', 'STALE', 'OBSERVED'):
        return record
    if not runner.workflow.execute:
        return store.save(rid, state='OBSERVED', action='NONE', reason='job.failed safety net observe mode')
    # Receipt time is durable and never refreshed on duplicate Kafka delivery/restart.
    if 'job_failed_deadline' not in record:
        received = record.get('created_at')
        if not whole(received) or received < 0:
            raise ValueError('durable job.failed receipt timestamp required')
        record = store.save(rid, job_failed_deadline=int(received) + runner.policy['job_failed_wait_seconds'],
                            stage='WAITING_FOR_FAULT', action='NONE')
    fence = store.fencing_record(event['job_id'])
    if whole(fence.get('generation')) and fence['generation'] > event['generation']:
        return store.save(rid, state='STALE', action='NONE', reason='job.failed generation already superseded')
    if (fence.get('recovery_id') and fence.get('recovery_from_gen') == event['generation']
            and fence.get('owner', event['cluster']) == event['cluster']):
        return store.save(rid, state='IGNORED', action='NONE', reason='related recovery already recorded',
                          related_event_id=fence['recovery_id'])
    # Full consistent paginated scan: an incomplete/failed read must never mean "no fault".
    for other in store.records():
        source = other['event']
        if (source.get('event_id') != rid and source.get('lifecycle_type') != 'job.failed'
                and source.get('job_id') == event['job_id']
                and source.get('generation') == event['generation']
                and source.get('cluster') == event['cluster']
                and other.get('state') not in ('STALE', 'IGNORED')):
            return store.save(rid, state='IGNORED', action='NONE', reason='related fault received',
                              related_event_id=source['event_id'])
    if now < record['job_failed_deadline']:
        return record
    print(json.dumps(dict(component='recovery', event_id=rid,
          job_id=event['job_id'], generation=event['generation'], state='HOLD',
          reason='unclassified_job_failure')), flush=True)
    return store.save(rid, state='HOLD', action='HOLD', reason='unclassified_job_failure',
                      failure_class='UNCLASSIFIED_JOB_FAILURE', stopped_stage='WAITING_FOR_FAULT')
