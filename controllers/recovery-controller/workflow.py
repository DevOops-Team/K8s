"""Recovery decisions and fencing belong here; node changes and JobSets belong to relocator."""
import hashlib
import json
import re
from decimal import Decimal
from controller import plan
from adapters import RecoveryBlocked, RetryLimitReached

TERMINAL = {'RECOVERED', 'REJECTED', 'REVIEW', 'HOLD', 'IGNORED', 'STALE', 'OBSERVED'}
RELOCATION_ACTIONS = {'RETRY_GROUP': 'restartInPlace',
                      'QUARANTINE_AND_RETRY': 'moveToSpare',
                      'FENCE_AND_EVACUATE': 'evacuateCluster'}


def validate_event(event):
    plan(event)
    if not event.get('job_id') or event.get('generation') is None:
        return event  # Unresolved signals are retained but never change execution authority.
    if (not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', event['job_id'])
            or len(event['job_id']) > 230):
        raise ValueError('job_id must fit the JobSet DNS name')
    if type(event['generation']) is not int or event['generation'] < 1:
        raise ValueError('generation must be a positive integer')
    if plan(event)['suggested_action'] in ('RETRY_GROUP', 'QUARANTINE_AND_RETRY', 'PLANNED_MIGRATE'):
        if not isinstance(event.get('node'), str) or not event['node'].strip():
            raise ValueError('node is required for a node-scoped recovery')
    return event


class Workflow:
    def __init__(self, store, executor, execute=False):
        self.store, self.executor, self.execute = store, executor, execute

    def accept(self, event):
        validate_event(event)
        canonical = json.dumps(event, sort_keys=True, separators=(',', ':'))
        self.store.accept(event, hashlib.sha256(canonical.encode()).hexdigest())

    def tick(self, record, monitoring_hold=False):
        e = record['event']
        rid = e['event_id']
        action = plan(e)['suggested_action']
        state = record.get('state', 'RECEIVED')
        if state == 'REVIEW':
            return self.store.save(rid, state='HOLD', action='HOLD',
                                   reason=record.get('reason', 'legacy review requires operator inspection'))
        if state in TERMINAL:
            return record
        if not self.execute:
            return self.store.save(rid, state='OBSERVED', action=action)
        if action in ('HOLD', 'NEEDS_CONFIG_FIX', 'NONE'):
            terminal = {'HOLD':'HOLD', 'NEEDS_CONFIG_FIX':'REJECTED', 'NONE':'IGNORED'}[action]
            return self.store.save(rid, state=terminal, action=action, reason=action)
        if monitoring_hold and state == 'RECEIVED':
            return record  # Pending input survives monitoring outage without changing authority.
        if not e.get('job_id') or e.get('generation') is None:
            return self.store.save(rid, state='HOLD', action=action, reason='job identity unresolved')
        if action == 'PLANNED_MIGRATE':
            # The current CRD has no bounded pre-checkpoint handshake. Never claim it was done.
            return self.store.save(rid, state='HOLD', action=action,
                                   reason='pre-checkpoint request/ack contract required')
        try:
            job = self.store.claim(e)
        except RetryLimitReached as exc:
            return self.store.save(rid, state='REJECTED', action='NEEDS_CONFIG_FIX', reason=str(exc))
        except RecoveryBlocked as exc:
            return self.store.save(rid, state='HOLD', action='HOLD', reason=str(exc))
        if job is None:
            return self.store.save(rid, state='STALE')
        if job.get('checkpoint_warning'):
            record = self.store.save(rid, checkpoint_warning=job['checkpoint_warning'])
        if job.get('claim_pending'):
            return self.store.save(rid, state='CLAIMED', action=action)
        generation = int(job['generation'])
        if state in ('RECEIVED', 'CLAIMED', 'FENCED'):
            name = self.executor.submit(e, RELOCATION_ACTIONS[action])
            return self.store.save(rid, state='REQUESTED', action=action,
                                   generation=generation, relocation_request=name)
        if state not in ('REQUESTED', 'SUBMITTED', 'RESUMING'):
            raise ValueError('unknown recovery state')
        fencing = self.store.fencing_record(e['job_id'])
        if fencing.get('recovery_id') != rid or int(fencing.get('generation', 0)) != generation:
            return self.store.save(rid, state='HOLD', action='HOLD', reason='recovery authority changed')
        phase = fencing.get('recovery_phase')
        if record.get('waiting_phase') != phase:
            record = self.store.save(rid, waiting_phase=phase)
        if phase in ('GENERATION_ADVANCED', 'OLD_RUN_STOPPED'):
            return record
        if phase not in ('NEW_RUN_SUBMITTED', 'RESUMING'):
            return self.store.save(rid, state='HOLD', action='HOLD', reason='unexpected fencing phase')
        target = fencing.get('owner')
        if target not in ('eks-a', 'eks-b', 'eks-c'):
            return record
        same_cluster = action in ('RETRY_GROUP', 'QUARANTINE_AND_RETRY')
        if (target == e['cluster']) != same_cluster:
            return self.store.save(rid, state='HOLD', action='HOLD', reason='new owner violates relocation action')
        if phase == 'NEW_RUN_SUBMITTED':
            self.store.resuming(e, generation)
        # Completion uses training progress independent of checkpoint publication.
        placement = self.store.placement(e['job_id'])
        progress = placement.get('progress', {})
        baseline = int(job.get('resume_step', 0))
        restored = (progress.get('checkpoint_loaded') is True
                    and progress.get('restored_gen') == job.get('resume_gen')
                    and progress.get('restored_step') == baseline)
        if job.get('resume_gen') is None:
            restored = (job.get('allow_restart_from_scratch') is True
                        and progress.get('started_from_scratch') is True)
        completed_step = progress.get('completed_step')
        if (progress.get('recovery_id') != rid
                or progress.get('generation') != generation
                or progress.get('cluster') != target
                or not fencing.get('new_jobset_uid')
                or progress.get('jobset_uid') != fencing['new_jobset_uid']
                or not restored or isinstance(completed_step, bool)
                or not isinstance(completed_step, (int, Decimal))
                or completed_step <= baseline or completed_step != int(completed_step)):
            return self.store.save(rid, state='RESUMING', reason='verified training progress required')
        nodes = placement.get('active_nodes', [])
        if (not isinstance(nodes, list)
                or int(placement.get('placement_generation', 0)) != generation
                or placement.get('cluster') != target
                or len(nodes) != int(job['gpu_count'])
                or any(not isinstance(n, str) or not n for n in nodes)
                or len(set(nodes)) != len(nodes)
                or (action != 'RETRY_GROUP' and e.get('node') in nodes)):
            return self.store.save(rid, state='RESUMING', reason='fresh placement evidence required')
        return self.store.save(rid, state='RECOVERED', action=action,
                               generation=generation, target_cluster=target, active_nodes=nodes)
