"""Controller-owned fencing writes from an explicitly trusted RR status contract."""
from datetime import datetime
import json
import time

from integrations.dynamodb import RecoveryBlocked


def status_config(value=None):
    value = {} if value is None else value
    if not isinstance(value, dict) or set(value) - {'enabled', 'producer_contract_confirmed'}:
        raise ValueError('unknown relocation_status_input setting')
    result = {'enabled': False, 'producer_contract_confirmed': False, **value}
    if any(type(flag) is not bool for flag in result.values()):
        raise ValueError('relocation status flags must be boolean')
    if result['enabled'] and not result['producer_contract_confirmed']:
        raise ValueError('confirm relocator-only RR status writer and DB-write handoff before enabling')
    return result


class RelocationStatusInput:
    def __init__(self, store, namespace, clock=time.time, allow_self_fence=False):
        self.store, self.namespace, self.clock = store, namespace, clock
        self.allow_self_fence = allow_self_fence

    def __call__(self, event, request):
        """No field inference from phase/message/timestamps in the old status format."""
        progress = request.get('status', {}).get('recoveryProgress')
        if progress is None:
            return False
        spec = request.get('spec', {})
        old_name = f"job-{event['job_id']}-g{event['generation']:04d}"
        if (request.get('metadata', {}).get('namespace') != self.namespace
                or spec.get('requestId') != event['event_id']
                or spec.get('jobRef') != f'{self.namespace}/{old_name}'
                or not isinstance(progress, dict)
                or type(progress.get('schemaVersion')) is not int or progress['schemaVersion'] != 1
                or progress.get('requestId') != event['event_id']
                or progress.get('jobId') != event['job_id']
                or type(progress.get('fromGeneration')) is not int
                or progress['fromGeneration'] != event['generation']
                or type(progress.get('toGeneration')) is not int
                or progress['toGeneration'] != event['generation'] + 1
                or progress.get('phase') not in ('OLD_RUN_STOPPED', 'NEW_RUN_SUBMITTED')):
            raise RecoveryBlocked('RR progress identity/phase mismatch')
        evidence = progress.get('oldRunStopEvidence')
        if (not isinstance(evidence, dict)
                or type(evidence.get('generation')) is not int
                or evidence['generation'] != event['generation']
                or evidence.get('namespace') != self.namespace
                or evidence.get('jobset_name') != old_name
                or evidence.get('kind') not in ('automatic', 'ec2', 'self_fence')
                or not isinstance(evidence.get('clusters'), list)
                or not evidence['clusters']
                or any(not isinstance(c, str) or not c for c in evidence['clusters'])
                or event['cluster'] not in evidence['clusters']
                or not isinstance(evidence.get('checks'), list) or not evidence['checks']
                or any(not isinstance(c, dict) or c.get('cluster') not in evidence['clusters']
                       or c.get('kind') not in ('automatic', 'ec2', 'self_fence')
                       or not isinstance(c.get('detail'), str) or not c['detail']
                       for c in evidence['checks'])
                or set(c['cluster'] for c in evidence['checks']) != set(evidence['clusters'])):
            raise RecoveryBlocked('complete old-run stop evidence required')
        if evidence['kind'] == 'self_fence' or any(c['kind'] == 'self_fence' for c in evidence['checks']):
            if not self.allow_self_fence:
                raise RecoveryBlocked('self fence input is disabled')
            from integrations.self_fence import validate_status
            validate_status(event, request, evidence, self.clock())
        try:
            confirmed = datetime.fromisoformat(evidence['confirmed_at'].replace('Z', '+00:00'))
            created = datetime.fromisoformat(request['metadata']['creationTimestamp'].replace('Z', '+00:00'))
            if (confirmed.tzinfo is None or created.tzinfo is None
                    or confirmed.timestamp() < created.timestamp() - 30
                    or confirmed.timestamp() > self.clock() + 30):
                raise ValueError('stop evidence time outside request lifetime')
        except (KeyError, TypeError, AttributeError, ValueError) as exc:
            raise RecoveryBlocked('invalid stop evidence time') from exc
        encoded = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
        if len(encoded.encode()) > 64_000:
            raise RecoveryBlocked('stop evidence exceeds budget')
        new = progress.get('newJobSet')
        if progress['phase'] == 'NEW_RUN_SUBMITTED':
            if (not isinstance(new, dict)
                    or new.get('name') != f"job-{event['job_id']}-g{event['generation'] + 1:04d}"
                    or not isinstance(new.get('uid'), str) or not new['uid'].strip()
                    or len(new['uid']) > 128):
                raise RecoveryBlocked('new manager JobSet name/UID required')
        # Apply the stop barrier first, even when both phases arrive in one scan.
        self.store.record_relocation_phase(event, 'OLD_RUN_STOPPED',
                                          {'old_run_stop_evidence': encoded})
        if progress['phase'] == 'NEW_RUN_SUBMITTED':
            self.store.record_relocation_phase(event, 'NEW_RUN_SUBMITTED',
                {'new_jobset_name': new['name'], 'new_jobset_uid': new['uid']},
                stop_evidence=encoded)
        return True
