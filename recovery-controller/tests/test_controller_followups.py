"""Shared deadline and optional fenced training input boundaries, without AWS calls."""
import copy
import unittest
from decimal import Decimal
from integrations.dynamodb import Store
from test_fencing import RecoveryTable, FencingTable, EVENT
from test_retry_policy import Store as RunnerStore, Workflow as RunnerWorkflow, Producer
from recovery.retry_policy import Runner
from test_evidence import Table, Fence, NOW
from recovery.evidence import EvidenceWriter
from integrations.fencing_input import FencingProgressInput, input_config


class DeadlineFence(FencingTable):
    def update_item(self, **kw):
        result=super().update_item(**kw)
        values=kw['ExpressionAttributeValues']
        if ':stop_started' in values:
            self.item.update(stop_wait_started_at=values[':stop_started'],stop_wait_deadline=values[':stop_deadline'])
        return result


class SharedDeadlineTests(unittest.TestCase):
    def test_generation_and_deadline_are_one_write_and_replay_keeps_deadline(self):
        recovery,fence=RecoveryTable(),DeadlineFence()
        store=Store(recovery,fence,clock=lambda:1000)
        store.claim(EVENT)
        result=store.claim(EVENT)
        self.assertEqual((result['stop_wait_started_at'],result['stop_wait_deadline']),(1000,1900))
        call=fence.calls[-1]
        self.assertIn('stop_wait_started_at = :stop_started',call['UpdateExpression'])
        self.assertIn('stop_wait_deadline = :stop_deadline',call['UpdateExpression'])
        self.assertIn('recovery_id = :rid',call['ConditionExpression'])
        store.clock=lambda:5000
        replay=store.claim(EVENT)
        self.assertEqual(replay['stop_wait_deadline'],1900)
        self.assertEqual(len(fence.calls),2)
        self.assertEqual(fence.item['retry_count'],1)
    def test_new_recovery_replaces_previous_timer_without_resetting_count(self):
        recovery,fence=RecoveryTable(),DeadlineFence()
        fence.item.update(stop_wait_started_at=1,stop_wait_deadline=901,retry_count=1)
        store=Store(recovery,fence,stop_timeout_seconds=60,clock=lambda:1000)
        store.claim(EVENT);store.claim(EVENT)
        self.assertEqual(fence.item['stop_wait_deadline'],1060)
        self.assertEqual(fence.item['retry_count'],2)
    def test_controller_uses_shared_deadline_despite_local_stage_transition(self):
        store=RunnerStore();workflow=RunnerWorkflow(store)
        store.fence.update(stop_wait_started_at=100, stop_wait_deadline=1000)
        store.record.update(stop_wait_started_at=100,stop_wait_deadline=2000,
                            stage_started_at=999,budget_stage='REQUESTED')
        runner=Runner(store,workflow,Producer(),'result')
        self.assertEqual(runner.operation(copy.deepcopy(store.record),999,False)['state'],'REQUESTED')
        self.assertEqual(runner.operation(copy.deepcopy(store.record),1000,False)['state'],'HOLD')
        self.assertEqual(workflow.calls,1)
    def test_incomplete_or_invalid_shared_deadline_holds(self):
        for values in ({'stop_wait_deadline':1000},
                       {'stop_wait_started_at':100,'stop_wait_deadline':True},
                       {'stop_wait_started_at':100,'stop_wait_deadline':99}):
            store=RunnerStore();workflow=RunnerWorkflow(store);store.fence.update(values)
            result=Runner(store,workflow,Producer(),'result').operation(copy.deepcopy(store.record),200,False)
            self.assertEqual(result['state'],'HOLD')
            self.assertEqual(workflow.calls,0)
    def test_other_recovery_deadline_is_never_used(self):
        store = RunnerStore()
        workflow = RunnerWorkflow(store)
        store.fence.update(recovery_id='other', stop_wait_started_at=100, stop_wait_deadline=1000)
        result = Runner(store, workflow, Producer(), 'result').operation(store.record, 200, False)
        self.assertEqual(result['reason'], 'recovery authority changed')
        self.assertEqual(workflow.calls, 0)

    def test_confirmed_stop_can_advance_after_deadline(self):
        store = RunnerStore()
        workflow = RunnerWorkflow(store)
        store.fence.update(recovery_phase='OLD_RUN_STOPPED',
                           stop_wait_started_at=100, stop_wait_deadline=1000)
        result = Runner(store, workflow, Producer(), 'result').operation(store.record, 1001, False)
        self.assertEqual(result['state'], 'REQUESTED')
        self.assertEqual(workflow.calls, 1)

    def test_stop_timeout_constructor_rejects_invalid_values(self):
        for value in (True,0,-1,1.5,'900'):
            with self.assertRaises(ValueError):Store(RecoveryTable(),DeadlineFence(),stop_timeout_seconds=value)


class EvidenceInputTests(unittest.TestCase):
    def setUp(self):
        self.table,self.fence=Table(),Fence()
        self.fence.item['new_jobset_name']='job-j1-g0002'
        self.store=Store(self.table,self.fence)
        self.writer=EvidenceWriter(self.store,clock=lambda:NOW)
        placement=dict(schema_version=1,kind='placement',event_id='p1',job_id='j1',recovery_id='r1',
                       generation=2,cluster='eks-a',jobset_uid='uid',observed_at=NOW,active_nodes=['n1','n2'])
        self.writer.write(placement,authenticated_producer='scheduling')
        self.message=dict(schema_version=Decimal(1),kind='progress',event_id='step-1',job_id='j1',
                          recovery_id='r1',generation=Decimal(2),cluster='eks-a',jobset_uid='uid',
                          observed_at=Decimal(NOW),outcome='succeeded',checkpoint_loaded=True,
                          restored_gen=Decimal(1),restored_step=Decimal(10),completed_step=Decimal(11))
        self.fence.item['recovery_training_evidence']=self.message
        self.record=dict(event=dict(event_id='r1',job_id='j1'),state='RESUMING',generation=2)
        self.input=FencingProgressInput(self.store,self.writer,
            {'enabled':True,'producer_contract_confirmed':True})
    def test_explicitly_enabled_iam_contract_projects_current_progress(self):
        self.assertTrue(self.input.ingest(self.record))
        progress=self.table.item['progress']
        self.assertEqual(progress['producer'],'training')
        self.assertEqual(progress['completed_step'],11)
        self.assertEqual(progress['received_at'],NOW)
    def test_disabled_by_default_and_confirmation_required(self):
        disabled=FencingProgressInput(self.store,self.writer)
        self.assertEqual(disabled.reconcile(self.record,NOW),self.record)
        self.assertNotIn('progress',self.table.item)
        with self.assertRaises(ValueError):input_config({'enabled':True})
        for value in ({'enabled':'true'},{'attribute':'generation'},{'unknown':True}):
            with self.assertRaises(ValueError):input_config(value)
    def test_stale_identity_or_spoofed_producer_is_rejected(self):
        for changes in (dict(recovery_id='old'),dict(generation=1),dict(jobset_uid='old'),
                        dict(cluster='eks-b'),dict(job_id='other'),dict(producer='training'),
                        dict(received_at=NOW),dict(observed_at=NOW-301),dict(completed_step=10)):
            self.fence.item['recovery_training_evidence']=dict(self.message,**changes)
            with self.subTest(changes=changes),self.assertRaises(ValueError):self.input.ingest(self.record)
        self.assertNotIn('progress',self.table.item)
    def test_owner_must_be_confirmed(self):
        self.fence.item.pop('owner')
        with self.assertRaises(ValueError):self.input.ingest(self.record)
    def test_manager_jobset_name_and_uid_must_be_recorded(self):
        original=copy.deepcopy(self.fence.item)
        for changes in (dict(new_jobset_name=None),dict(new_jobset_name='job-other-g0002'),
                        dict(new_jobset_name='job-j1-g0001'),dict(new_jobset_uid=None),
                        dict(new_jobset_uid='')):
            self.fence.item=dict(original,**changes)
            with self.subTest(changes=changes),self.assertRaises(ValueError):
                self.input.ingest(self.record)
        self.assertNotIn('progress',self.table.item)
    def test_worker_copy_uid_is_not_the_manager_correlation_uid(self):
        self.fence.item['recovery_training_evidence']=dict(self.message,jobset_uid='worker-copy-uid')
        with self.assertRaises(ValueError):self.input.ingest(self.record)
        self.assertNotIn('progress',self.table.item)
        self.fence.item['recovery_training_evidence']=self.message
        self.assertTrue(self.input.ingest(self.record))
    def test_refresh_with_new_observation_id_updates_step_and_receipt(self):
        self.assertTrue(self.input.ingest(self.record))
        self.writer.clock=lambda:NOW+60
        self.fence.item['recovery_training_evidence']=dict(self.message,event_id='step-2',
                                                         observed_at=NOW+60,completed_step=12)
        self.assertTrue(self.input.ingest(self.record))
        progress=self.table.item['progress']
        self.assertEqual((progress['event_id'],progress['completed_step'],progress['received_at']),
                         ('step-2',12,NOW+60))
        self.assertFalse(self.input.ingest(self.record))
    def test_refresh_reusing_event_id_with_changed_content_is_rejected(self):
        self.input.ingest(self.record)
        before=copy.deepcopy(self.table.item)
        self.writer.clock=lambda:NOW+60
        self.fence.item['recovery_training_evidence']=dict(self.message,observed_at=NOW+60,completed_step=12)
        with self.assertRaisesRegex(ValueError,'event_id reused'):
            self.input.ingest(self.record)
        self.assertEqual(self.table.item,before)
    def test_duplicate_does_not_refresh_receipt(self):
        self.input.ingest(self.record)
        self.writer.clock=lambda:NOW+20
        self.assertFalse(self.input.ingest(self.record))
        self.assertEqual(self.table.item['progress']['received_at'],NOW)
    def test_missing_map_waits_and_failure_evidence_is_preserved(self):
        self.fence.item.pop('recovery_training_evidence')
        self.assertFalse(self.input.ingest(self.record))
        self.fence.item['recovery_training_evidence']=dict(self.message,outcome='failed',failure_reason='restore failed')
        self.assertTrue(self.input.ingest(self.record))
        self.assertEqual(self.table.item['progress']['outcome'],'failed')
    def test_ingestion_failure_backs_off_without_changing_recovery(self):
        state=copy.deepcopy(self.record)
        def save(rid,**values):
            state.update(values)
            return copy.deepcopy(state)
        self.store.save=save
        self.fence.item['recovery_training_evidence']=dict(self.message,completed_step=10)
        result=self.input.reconcile(state,NOW)
        self.assertEqual(result['state'],'RESUMING')
        self.assertEqual(result['evidence_input_next_at'],NOW+5)
        before=copy.deepcopy(result)
        self.assertEqual(self.input.reconcile(result,NOW+1),before)
        self.assertEqual(len(self.table.calls),1)  # only placement, no malformed progress write
