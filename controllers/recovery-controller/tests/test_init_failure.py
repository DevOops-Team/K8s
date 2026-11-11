import copy
import json
import unittest
from decimal import Decimal
from adapters import Store
from controller import plan
from init_failure import collect, decision
from workflow import Workflow
from retry_policy import Runner
from test_fencing import RecoveryTable, FencingTable
from test_workflow import MemoryStore, Requests, EVENT
from test_checkpoint_contract import WarningProducer


def event(code=7, **changes):
    value = dict(EVENT, component='ckpt-init', exit_code=code,
                 jobset_uid='initial-uid', pod_uid='pod-uid')
    value.pop('fault_type')
    if code == 7:
        value['checkpoint'] = dict(generation=1, step=1000)
    value.update(changes)
    return value


class QuarantineTable(RecoveryTable):
    def __init__(self):
        super().__init__()
        self.blocks = {}
    def get_item(self, **kw):
        if kw['Key']['pk'].startswith('checkpoint#'):
            self.assert_consistent = kw['ConsistentRead']
            item = self.blocks.get(kw['Key']['pk'])
            return {'Item': copy.deepcopy(item)} if item else {}
        return super().get_item(**kw)
    def update_item(self, **kw):
        if kw['Key']['pk'].startswith('checkpoint#'):
            self.blocks.setdefault(kw['Key']['pk'], dict(blocked=True))
            return {}
        return super().update_item(**kw)


class ConnectedStore(MemoryStore):
    def __init__(self):
        super().__init__()
        self.table = QuarantineTable()
        self.fencing = FencingTable()
        self.adapter = Store(self.table, self.fencing)
        self.nodes.update(placement_generation=1, jobset_uid='initial-uid')
    def fencing_record(self, job_id):
        return self.adapter.fencing_record(job_id)
    def claim(self, signal):
        return self.adapter.claim(signal)
    def block_checkpoint(self, signal):
        return self.adapter.block_checkpoint(signal)


class InitPolicyTests(unittest.TestCase):
    def setUp(self):
        self.store = ConnectedStore()
        self.requests = Requests()
        self.flow = Workflow(self.store, self.requests, True)
    def process(self, signal):
        self.flow.accept(signal)
        return self.flow.tick(self.store.get(signal['event_id']))

    def test_exit_codes_override_generic_recommendation(self):
        expected = {2:'HOLD', 3:'HOLD', 4:'RETRY_GROUP', 5:'NEEDS_CONFIG_FIX', 6:'NEEDS_CONFIG_FIX', 7:'RETRY_GROUP'}
        for code, action in expected.items():
            with self.subTest(code=code):
                self.assertEqual(plan(event(code, suggested_action='fence_evacuate'))['suggested_action'], action)

    def test_authority_and_configuration_errors_never_advance(self):
        for code, state in [(2,'HOLD'),(3,'HOLD'),(5,'REJECTED'),(6,'REJECTED')]:
            result = self.process(event(code, event_id='code-'+str(code)))
            self.assertEqual(result['state'], state)
            self.assertEqual(result['exit_code'], code)
        self.assertEqual(self.store.fencing.item['generation'], 1)
        self.assertEqual(self.requests.calls, [])

    def test_missing_registration_is_rejected_without_creating_it(self):
        self.store.fencing.item = {}
        self.assertEqual(self.process(event(6))['state'], 'REJECTED')
        self.assertEqual(self.store.fencing.calls, [])

    def test_connection_failure_uses_existing_generation_and_retry_budget(self):
        signal = event(4)
        self.assertEqual(self.process(signal)['state'], 'CLAIMED')
        result = self.flow.tick(self.store.get(signal['event_id']))
        self.assertEqual(result['state'], 'REQUESTED')
        self.assertEqual(result['retry_count'], 1)
        self.assertEqual(self.requests.calls[0][1], 'restartInPlace')
        self.assertEqual(self.store.fencing.item['generation'], 2)
        self.flow.tick(self.store.get(signal['event_id']))
        self.assertEqual(self.store.fencing.item['retry_count'], 1)

    def test_connection_failure_limit_emits_actual_count(self):
        self.store.fencing.item['retry_count'] = 3
        result = self.process(event(4))
        self.assertEqual(result['state'], 'REJECTED')
        self.assertEqual(result['retry_count'], 3)
        self.assertEqual(self.store.fencing.calls, [])

    def test_stale_signal_never_quarantines_current_checkpoint(self):
        self.store.fencing.item['generation'] = 2
        self.assertEqual(self.process(event())['state'], 'STALE')
        self.assertEqual(self.store.table.blocks, {})

    def test_unverified_jobset_or_checkpoint_does_not_quarantine(self):
        for signal in [event(jobset_uid='wrong'), event(checkpoint=dict(generation=1,step=999))]:
            result = self.process(signal)
            self.assertEqual(result['state'], 'HOLD')
            self.assertEqual(self.store.table.blocks, {})
            self.store.events.clear()

    def test_code7_never_reuses_corrupt_baseline_after_restart(self):
        result = self.process(event())
        self.assertEqual(result['state'], 'CLAIMED')
        self.assertEqual(result['checkpoint_warning'], 'CHECKPOINTS_UNUSABLE_RESTART_FROM_SCRATCH')
        self.flow = Workflow(self.store, self.requests, True)
        result = self.flow.tick(self.store.get('e1'))
        self.assertEqual(result['state'], 'REQUESTED')
        self.assertIsNone(self.store.fencing.item['recovery_resume_gen'])
        self.assertNotIn('approved_gen', self.store.fencing.item)
        self.assertEqual(self.store.fencing.item['retry_count'], 1)
        self.assertEqual(self.requests.calls[0][1], 'restartInPlace')
        self.assertIn('checkpoint#j1#1#1000', self.store.table.blocks)

    def test_previous_checkpoint_is_selected_with_atomic_approval_snapshot(self):
        self.store.fencing.item.update(prev_approved_gen=1, prev_approved_step=900)
        result = self.process(event())
        self.assertEqual(result['state'], 'CLAIMED')
        self.assertEqual(self.store.fencing.item['approved_step'], 900)
        self.assertEqual(self.store.fencing.item['recovery_resume_step'], 900)
        self.assertEqual(result['checkpoint_warning'], 'PREVIOUS_APPROVED_CHECKPOINT')
        condition = self.store.fencing.calls[0]['ConditionExpression']
        self.assertIn('prev_approved_step = :prev_approved_step', condition)
        self.assertIn('approved_step = :approved_step', condition)
        self.flow.tick(self.store.get('e1'))
        self.assertEqual(self.store.fencing.item['generation'], 2)
        self.assertEqual(self.store.fencing.item['retry_count'], 1)

    def test_known_corrupt_previous_checkpoint_starts_from_scratch(self):
        self.store.fencing.item.update(prev_approved_gen=1, prev_approved_step=900)
        self.store.adapter.block_checkpoint(event(checkpoint=dict(generation=1,step=900)))
        self.assertEqual(self.process(event())['state'], 'CLAIMED')
        self.assertNotIn('approved_gen', self.store.fencing.item)
        self.assertIsNone(self.store.fencing.item['recovery_resume_gen'])
        self.assertEqual(self.store.fencing.item['recovery_resume_step'], 0)

    def test_corruption_in_resuming_replaces_only_failed_recovery_lock(self):
        self.store.fencing.item.update(generation=2, recovery_id='old-recovery',
            recovery_phase='RESUMING', recovery_from_gen=1, recovery_to_gen=2,
            recovery_resume_gen=1, recovery_resume_step=1000, new_jobset_uid='failed-uid',
            prev_approved_gen=1, prev_approved_step=900, retry_count=1)
        signal = event(generation=2, jobset_uid='failed-uid')
        self.assertEqual(self.process(signal)['state'], 'CLAIMED')
        self.assertEqual(self.store.fencing.item['recovery_id'], 'e1')
        self.assertEqual(self.store.fencing.item['approved_step'], 900)
        self.flow.tick(self.store.get('e1'))
        self.assertEqual(self.store.fencing.item['generation'], 3)
        self.assertEqual(self.store.fencing.item['retry_count'], 2)
        self.assertIn('new_jobset_uid = :failed_uid', self.store.fencing.calls[0]['ConditionExpression'])

    def test_previous_then_corrupt_previous_restarts_from_scratch(self):
        self.store.fencing.item.update(prev_approved_gen=1, prev_approved_step=900)
        self.process(event())
        self.flow.tick(self.store.get('e1'))
        self.store.fencing.item.update(recovery_phase='RESUMING', owner='eks-a', new_jobset_uid='previous-attempt')
        signal = event(event_id='previous-corrupt',generation=2,jobset_uid='previous-attempt',
                       checkpoint=dict(generation=1,step=900))
        self.assertEqual(self.process(signal)['state'], 'CLAIMED')
        self.assertNotIn('approved_gen', self.store.fencing.item)
        self.flow.tick(self.store.get('previous-corrupt'))
        self.assertEqual(self.store.fencing.item['generation'], 3)
        self.assertEqual(self.store.fencing.item['retry_count'], 2)
        self.assertIsNone(self.store.fencing.item['recovery_resume_gen'])

    def test_transport_failure_does_not_fall_back_or_change_approvals(self):
        self.store.fencing.item.update(prev_approved_gen=1, prev_approved_step=900)
        self.process(event(4))
        self.assertEqual(self.store.fencing.item['approved_step'], 1000)
        self.assertEqual(self.store.fencing.item['recovery_resume_step'], 1000)

    def test_generic_fault_cannot_retry_a_quarantined_approved_baseline(self):
        self.store.adapter.block_checkpoint(event())
        self.store.fencing.item.update(prev_approved_gen=1, prev_approved_step=900)
        result = self.process(dict(EVENT,event_id='generic-fault'))
        self.assertEqual(result['state'], 'CLAIMED')
        self.assertEqual(self.store.fencing.item['recovery_resume_step'], 900)

    def test_claimed_replay_checks_quarantine_before_generation_advance(self):
        transient = event(4)
        self.process(transient)
        self.store.adapter.block_checkpoint(event())
        result = self.flow.tick(self.store.get(transient['event_id']))
        self.assertEqual(result['state'], 'HOLD')
        self.assertEqual(self.store.fencing.item['generation'], 1)

    def test_observe_does_not_quarantine_or_change_authority(self):
        self.flow = Workflow(self.store, None, False)
        result = self.process(event())
        self.assertEqual(result['state'], 'OBSERVED')
        self.assertEqual(result['failure_class'], 'CHECKPOINT_CORRUPT')
        self.assertEqual(self.store.table.blocks, {})

    def test_unknown_or_boolean_exit_code_and_missing_checkpoint_rejected(self):
        for signal in [event(True), event(1), event(7, checkpoint=None)]:
            with self.assertRaises(ValueError):
                self.flow.accept(signal)

    def test_result_event_exposes_stopped_stage_count_and_failure_class(self):
        self.store.fencing.item['retry_count'] = 3
        result = self.process(event())
        producer = WarningProducer()
        Runner(self.store, self.flow, producer, 'recovery.action').deliver(result, 0)
        self.assertEqual(producer.payload['stage'], 'CKPT_INIT')
        self.assertIn('retry_count', producer.payload)
        self.assertEqual(producer.payload['exit_code'], 7)
        self.assertEqual(producer.payload['failure_class'], 'CHECKPOINT_CORRUPT')

    def test_dynamodb_integer_roundtrip_keeps_init_decision_and_result_serializable(self):
        signal = event()
        self.flow.accept(signal)
        stored = self.store.events['e1']['event']
        stored.update(exit_code=Decimal(7), generation=Decimal(1),
                      checkpoint=dict(generation=Decimal(1), step=Decimal(1000)))
        self.store.fencing.item['retry_count'] = 3
        result = self.flow.tick(self.store.get('e1'))
        self.assertEqual(result['state'], 'REJECTED')
        result['generation'] = Decimal(1)
        result['retry_count'] = Decimal(2)
        producer = WarningProducer()
        Runner(self.store, self.flow, producer, 'recovery.action').deliver(result, 0)
        self.assertEqual(producer.payload['generation'], 1)
        self.assertEqual(producer.payload['exit_code'], 7)
        self.assertTrue(self.store.get('e1')['published'])

    def test_lost_claim_response_reuses_original_init_event(self):
        signal = event(4)
        self.flow.accept(signal)
        self.store.adapter.claim(signal)  # CAS completed, but inbox state did not advance.
        result = self.flow.tick(self.store.get('e1'))
        self.assertEqual(result['state'], 'REQUESTED')
        self.assertEqual(self.store.fencing.item['retry_count'], 1)

    def test_timeout_stage_and_decimal_retry_count_are_serializable(self):
        self.store.events['timeout'] = dict(event=dict(event_id='timeout',job_id='j1',generation=2),
                                            state='REQUESTED', waiting_phase='OLD_RUN_STOPPED',
                                            retry_count=Decimal(2))
        producer = WarningProducer()
        runner = Runner(self.store, self.flow, producer, 'recovery.action')
        result = runner.hold(self.store.get('timeout'), 'deadline')
        runner.deliver(result, 0)
        self.assertEqual(producer.payload['stage'], 'OLD_RUN_STOPPED')
        self.assertEqual(producer.payload['retry_count'], 2)


class PodAdapterTests(unittest.TestCase):
    def pod(self):
        return dict(metadata=dict(namespace='training',uid='p1',labels={'devoops/job-id':'j1','devoops/generation':'1'}),
                    spec=dict(nodeName='n1'), status=dict(initContainerStatuses=[dict(name='ckpt-init',
                        state=dict(terminated=dict(exitCode=4,containerID='c1',finishedAt='now')))]))
    def collect(self, pod):
        return collect(pod,cluster='eks-a',job_id='j1',generation=1,jobset_uid='uid')
    def test_terminated_status_produces_stable_deduplicated_event(self):
        self.assertEqual(self.collect(self.pod()), self.collect(self.pod()))
        self.assertEqual(self.collect(self.pod())['exit_code'], 4)
    def test_success_running_and_previous_termination_are_not_failures(self):
        pod = self.pod()
        pod['status']['initContainerStatuses'][0]['state']['terminated']['exitCode']=0
        self.assertIsNone(self.collect(pod))
        status=pod['status']['initContainerStatuses'][0]
        status['lastState']=status.pop('state')
        status['state']={'running':{}}
        self.assertIsNone(self.collect(pod))
    def test_wrong_job_label_is_rejected(self):
        pod=self.pod();pod['metadata']['labels']['devoops/generation']='2'
        with self.assertRaises(ValueError):self.collect(pod)
