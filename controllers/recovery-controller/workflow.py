"""Recovery decisions and fencing belong here; node changes and JobSets belong to relocator."""
import hashlib
import json
import re
import time
from evidence import assess, policy_config
from controller import plan
from init_failure import decision as init_decision
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
    if plan(event)['suggested_action'] in ('RETRY_GROUP', 'QUARANTINE_AND_RETRY'):
        if not isinstance(event.get('node'), str) or not event['node'].strip():
            raise ValueError('node is required for a node-scoped recovery')
    return event


class Workflow:
    def __init__(self, store, executor, execute=False, evidence_policy=None, clock=time.time):
        self.store, self.executor, self.execute = store, executor, execute
        self.evidence_policy, self.clock = policy_config(evidence_policy), clock

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
            init = init_decision(e)
            details = dict(stage='CKPT_INIT', exit_code=e['exit_code'], failure_class=init['failure_class']) if init else {}
            return self.store.save(rid, state='OBSERVED', action=action, **details)
        init = init_decision(e)
        if init and state == 'RECEIVED':
            fence = self.store.fencing_record(e['job_id'])
            context = dict(stage='CKPT_INIT', exit_code=e['exit_code'],
                           failure_class=init['failure_class'],
                           retry_count=int(fence['retry_count']) if 'retry_count' in fence else None)
            record = self.store.save(rid, **context)
            own_replay = e['exit_code'] in (4, 7) and fence.get('recovery_id') == rid
            if fence and not own_replay and int(fence.get('generation', 0)) != e['generation']:
                return self.store.save(rid, state='STALE', action='NONE', reason='stale init generation')
            if e['exit_code'] in (4, 7) and not own_replay:
                placement = self.store.placement(e['job_id'])
                expected_uid = fence.get('new_jobset_uid') if fence.get('recovery_id') else placement.get('jobset_uid')
                expected_cluster = fence.get('owner') or placement.get('cluster')
                placement_current = fence.get('recovery_id') or placement.get('placement_generation') == e['generation']
                if (not fence or not placement_current or expected_uid != e['jobset_uid'] or expected_cluster != e['cluster']):
                    return self.store.save(rid, state='HOLD', action='HOLD', reason='verified current init JobSet identity required')
            if e['exit_code'] == 7 and not own_replay:
                failed = e['checkpoint']
                expected_gen = fence.get('recovery_resume_gen') if fence.get('recovery_id') else fence.get('approved_gen')
                expected_step = fence.get('recovery_resume_step') if fence.get('recovery_id') else fence.get('approved_step')
                if failed != {'generation': expected_gen, 'step': expected_step}:
                    return self.store.save(rid, state='HOLD', action='HOLD', reason='failed checkpoint does not match verified restore baseline')
                self.store.block_checkpoint(e)
                record = self.store.save(rid, blocked_checkpoint=failed, reason=init['reason'])
            if e['exit_code'] not in (4, 7):
                terminal = 'REJECTED' if action == 'NEEDS_CONFIG_FIX' else 'HOLD'
                return self.store.save(rid, state=terminal, action=action, reason=init['reason'])
        if action in ('HOLD', 'NEEDS_CONFIG_FIX', 'NONE'):
            terminal = {'HOLD':'HOLD', 'NEEDS_CONFIG_FIX':'REJECTED', 'NONE':'IGNORED'}[action]
            return self.store.save(rid, state=terminal, action=action, reason=action)
        if monitoring_hold and state == 'RECEIVED':
            return record  # Pending input survives monitoring outage without changing authority.
        if not e.get('job_id') or e.get('generation') is None:
            return self.store.save(rid, state='HOLD', action=action, reason='job identity unresolved')
        try:
            job = self.store.claim(e)
        except RetryLimitReached as exc:
            return self.store.save(rid, state='REJECTED', action='NEEDS_CONFIG_FIX', reason=str(exc),
                                   retry_count=self.store.fencing_record(e['job_id']).get('retry_count'))
        except RecoveryBlocked as exc:
            return self.store.save(rid, state='HOLD', action='HOLD', reason=str(exc),
                                   retry_count=self.store.fencing_record(e['job_id']).get('retry_count'))
        if job is None:
            return self.store.save(rid, state='STALE')
        record = self.store.save(rid, retry_count=job.get('retry_count'))
        if job.get('checkpoint_warning'):
            record = self.store.save(rid, checkpoint_warning=job['checkpoint_warning'])
        if job.get('claim_pending'):
            return self.store.save(rid, state='CLAIMED', action=action, stage='CLAIMED')
        generation = int(job['generation'])
        if state in ('RECEIVED', 'CLAIMED', 'FENCED'):
            name = self.executor.submit(e, RELOCATION_ACTIONS[action])
            return self.store.save(rid, state='REQUESTED', action=action, stage='GENERATION_ADVANCED',
                                   generation=generation, relocation_request=name)
        if state not in ('REQUESTED', 'SUBMITTED', 'RESUMING'):
            raise ValueError('unknown recovery state')
        fencing = self.store.fencing_record(e['job_id'])
        if fencing.get('recovery_id') != rid or int(fencing.get('generation', 0)) != generation:
            return self.store.save(rid, state='HOLD', action='HOLD', reason='recovery authority changed')
        phase = fencing.get('recovery_phase')
        if record.get('waiting_phase') != phase:
            record = self.store.save(rid, waiting_phase=phase, stage=phase)
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
        verdict, reason = assess(placement, fencing, job, rid, generation, target, action,
                                 e.get('node'), int(self.clock()), self.evidence_policy)
        if verdict == 'failed':
            return self.store.save(rid, state='HOLD', stage='RESUMING', stopped_stage='RESUMING',
                                   action='HOLD', reason=reason)
        if verdict != 'ready':
            return self.store.save(rid, state='RESUMING', stage='RESUMING', reason=reason)
        return self.store.save(rid, state='RECOVERED', stage='COMPLETED', action=action,
                               generation=generation, target_cluster=target, active_nodes=placement['active_nodes'],
                               evidence_jobset_uid=fencing['new_jobset_uid'],
                               evidence_progress_id=placement['progress']['event_id'],
                               evidence_placement_id=placement['placement_evidence']['event_id'],
                               evidence_completed_step=placement['progress']['completed_step'])
