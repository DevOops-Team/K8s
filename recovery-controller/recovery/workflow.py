"""Recovery decisions and fencing belong here; node changes and JobSets belong to relocator."""
import hashlib
import json
import re
import time
from recovery.evidence import assess, policy_config, whole
from recovery.policy import is_straggler, plan
from recovery.init_failure import decision as init_decision
from integrations.dynamodb import RecoveryBlocked, RetryLimitReached

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
    def __init__(self, store, executor, execute=False, evidence_policy=None, clock=time.time, straggler_capacity=None):
        self.straggler_capacity = straggler_capacity
        self.store, self.executor, self.execute = store, executor, execute
        self.evidence_policy, self.clock = policy_config(evidence_policy), clock

    def accept(self, event):
        validate_event(event)
        canonical = json.dumps(event, sort_keys=True, separators=(',', ':'))
        self.store.accept(event, hashlib.sha256(canonical.encode()).hexdigest())

    def held_relocation_progress(self, record):
        """Read relocator-owned fencing progress; never create a request or claim here.

        OLD_RUN_STOPPED is the trusted relocator contract, not a user-supplied
        RR status/annotation. Relocator authenticates/validates the stop evidence.
        """
        if not self.execute or record.get('state') != 'HOLD':
            return None
        stop_stages = ('REQUESTED', 'GENERATION_ADVANCED')
        expected_reason = 'stage deadline exceeded: ' + str(record.get('stopped_stage'))
        if (record.get('reason') != expected_reason or record.get('stopped_stage') not in stop_stages
                or not record.get('relocation_request') or record.get('budget_stage') not in stop_stages):
            return None
        event = record['event']
        if not event.get('job_id') or type(event.get('generation')) is not int:
            return None
        generation = event['generation'] + 1
        if record.get('generation') != generation:
            return None
        action = 'QUARANTINE_AND_RETRY' if is_straggler(event) else plan(event)['suggested_action']
        if action not in RELOCATION_ACTIONS:
            return None
        fence = self.store.fencing_record(event['job_id'])
        if (not all(whole(fence.get(key)) for key in ('generation', 'recovery_from_gen', 'recovery_to_gen'))
                or fence.get('recovery_id') != event['event_id']
                or fence.get('generation') != generation
                or fence.get('recovery_from_gen') != event['generation']
                or fence.get('recovery_to_gen') != generation):
            return None
        phase = fence.get('recovery_phase')
        if phase not in ('OLD_RUN_STOPPED', 'NEW_RUN_SUBMITTED', 'RESUMING'):
            return None
        if phase != 'OLD_RUN_STOPPED' and not fence.get('new_jobset_uid'):
            return None
        return dict(phase=phase, action=action, generation=generation)

    def straggler_hold_reason(self, event):
        if not event.get('job_id') or not event.get('generation') or not event.get('node'):
            return 'straggler_identity_unresolved'
        fence = self.store.fencing_record(event['job_id'])
        if fence.get('recovery_id') == event['event_id']:
            return None
        if self.straggler_capacity is None:
            return 'straggler_auto_migration_disabled'
        if fence.get('straggler_migration_count', 0) >= 1:
            return 'straggler_migration_limit'
        facts = event.get('straggler_evidence')
        if (event.get('collector_healthy') is not True
                or event.get('suggested_action') != 'evacuate_preemptive'
                or not isinstance(facts, dict)
                or type(facts.get('duration_seconds')) is not int
                or facts['duration_seconds'] < 600
                or facts.get('cause_node') != event['node']
                or type(facts.get('estimated_gain_seconds')) is not int
                or facts['estimated_gain_seconds'] <= 0):
            return 'straggler_evidence_incomplete'
        job = self.store.placement(event['job_id'])
        try:
            available = self.straggler_capacity.check(event, int(job.get('gpu_count', 0)))
        except (OSError, ValueError, TypeError):
            return 'straggler_capacity_unknown'
        return None if available else 'straggler_capacity_insufficient'

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
            if init['action'] == 'HOLD':
                return self.store.save(rid, state='HOLD', action='HOLD', reason=init['reason'])
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
        if is_straggler(e):
            if monitoring_hold and state == 'RECEIVED':
                return record
            reason = self.straggler_hold_reason(e)
            if reason:
                return self.store.save(rid, state='HOLD', action='HOLD', reason=reason)
            action = 'QUARANTINE_AND_RETRY'
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
        record = self.store.save(rid, retry_count=job.get('retry_count'),
                                 **{key:job[key] for key in ('stop_wait_started_at', 'stop_wait_deadline') if key in job})
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
        # ADR-38: dispatcher may place node recovery in another healthy cluster.
        # Cluster evacuation must still leave the failed source cluster.
        if action == 'FENCE_AND_EVACUATE' and target == e['cluster']:
            return self.store.save(rid, state='HOLD', action='HOLD', reason='new owner violates relocation action')
        if phase == 'NEW_RUN_SUBMITTED':
            self.store.resuming(e, generation)
        # Completion uses training progress independent of checkpoint publication.
        placement = self.store.placement(e['job_id'])
        verdict, reason = assess(placement, fencing, job, rid, generation, target, action,
                                 e.get('node') if target == e['cluster'] else None,
                                 int(self.clock()), self.evidence_policy)
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
