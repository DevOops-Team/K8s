import copy
import time
import unittest
from decimal import Decimal
from evidence import EvidenceWriter, assess, DEFAULT_POLICY
from adapters import Store, RecoveryBlocked
from test_workflow import MemoryStore, Requests, EVENT
from workflow import Workflow
from retry_policy import Runner
from test_retry_policy import Producer

NOW = 2000000000


class ConditionalError(Exception):
    response = {'Error': {'Code': 'ConditionalCheckFailedException'}}


class Table:
    def __init__(self):
        self.item = dict(pk='job#j1',gpu_count=2,placement_generation=1,cluster='eks-a')
        self.calls = []
        self.conflict = False
    def get_item(self, **kw):
        assert kw['Key'] == {'pk':'job#j1'} and kw['ConsistentRead'] is True
        return {'Item':copy.deepcopy(self.item)}
    def update_item(self, **kw):
        self.calls.append(kw)
        if self.conflict:
            raise ConditionalError()
        values = kw['ExpressionAttributeValues']
        if kw['ExpressionAttributeNames']['#meta'] == 'placement_evidence':
            self.item.update(placement_generation=values[':gen'], jobset_uid=values[':uid'],
                             recovery_id=values[':rid'], cluster=values[':cluster'],
                             active_nodes=values[':nodes'], placement_evidence=values[':meta'])
            if 'REMOVE progress' in kw['UpdateExpression']:
                self.item.pop('progress',None)
        else:
            self.item['progress'] = copy.deepcopy(values[':meta'])


class Fence:
    def __init__(self):
        self.item = dict(generation=2,recovery_id='r1',new_jobset_uid='uid',owner='eks-a',
                         recovery_phase='RESUMING',recovery_resume_gen=1,recovery_resume_step=10)
    def get_item(self, **kw):
        return {'Item': copy.deepcopy(self.item)}


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.table, self.fence = Table(), Fence()
        self.store = Store(self.table,self.fence)
        self.writer = EvidenceWriter(self.store,clock=lambda:NOW)
        self.base = dict(schema_version=1,job_id='j1',recovery_id='r1',generation=2,
                         cluster='eks-a',jobset_uid='uid',observed_at=NOW)
    def place(self, **changes):
        message=dict(self.base,kind='placement',event_id='p1',active_nodes=['n1','n2'])
        message.update(changes)
        return self.writer.write(message,authenticated_producer='scheduling')
    def progress(self, **changes):
        message=dict(self.base,kind='progress',event_id='s1',outcome='succeeded',completed_step=11,
                     checkpoint_loaded=True,restored_gen=1,restored_step=10)
        message.update(changes)
        return self.writer.write(message,authenticated_producer='training')
    def test_projection_and_receipt_from_trusted_adapter(self):
        self.assertTrue(self.place()); self.assertTrue(self.progress())
        self.assertEqual(self.table.item['placement_evidence']['producer'],'scheduling')
        self.assertEqual(self.table.item['progress']['received_at'],NOW)
        self.assertIn('attribute_exists(pk)',self.table.calls[0]['ConditionExpression'])
        self.assertIn('jobset_uid = :uid',self.table.calls[1]['ConditionExpression'])
    def test_body_cannot_supply_producer_or_receipt(self):
        for key,value in [('producer','scheduling'),('received_at',NOW)]:
            with self.assertRaises(ValueError):self.place(**{key:value})
        self.assertEqual(self.table.calls,[])
    def test_unapproved_authenticated_producer_rejected(self):
        with self.assertRaises(ValueError):
            self.writer.write(dict(self.base,kind='placement',event_id='x',active_nodes=['n1']),authenticated_producer='unknown')
    def test_stale_future_and_foreign_generation_rejected(self):
        for changes in [dict(observed_at=NOW-301),dict(observed_at=NOW+31),dict(generation=1),dict(jobset_uid='old')]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):self.place(**changes)
        self.assertEqual(self.table.calls,[])
    def test_duplicate_does_not_refresh_receipt_and_conflicting_id_rejected(self):
        self.place(); self.progress()
        before=copy.deepcopy(self.table.item)
        self.writer.clock=lambda:NOW+10
        self.assertFalse(self.progress())
        self.assertEqual(self.table.item,before)
        with self.assertRaises(ValueError):self.progress(completed_step=12)
    def test_lower_step_and_delayed_evidence_cannot_overwrite(self):
        self.place(); self.progress(completed_step=12)
        self.assertFalse(self.progress(event_id='older-step',completed_step=11))
        self.assertFalse(self.progress(event_id='older-time',completed_step=13,observed_at=NOW-1))
        self.assertEqual(self.table.item['progress']['completed_step'],12)
    def test_wrong_restore_or_no_new_step_rejected(self):
        self.place()
        for changes in [dict(restored_step=9),dict(checkpoint_loaded=False),dict(completed_step=10),dict(restored_gen=True)]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):self.progress(**changes)
    def test_progress_before_placement_is_rejected(self):
        with self.assertRaises(RecoveryBlocked):self.progress()
    def test_failure_latched_and_generation_reset_clears_old_progress(self):
        self.place();self.progress(event_id='failed',outcome='failed',failure_reason='restore failed')
        with self.assertRaises(RecoveryBlocked):self.progress(event_id='later-success')
        self.fence.item.update(generation=3,recovery_id='r2',new_jobset_uid='uid2')
        self.place(event_id='p2',generation=3,recovery_id='r2',jobset_uid='uid2')
        self.assertNotIn('progress',self.table.item)
    def test_atomic_conflict_is_not_reported_as_written(self):
        self.table.conflict=True
        self.assertFalse(self.place())
        self.assertNotIn('placement_evidence',self.table.item)
    def test_scratch_requires_explicit_evidence(self):
        self.fence.item.update(recovery_resume_gen=None,recovery_resume_step=0)
        self.place()
        with self.assertRaises(ValueError):self.progress()
        self.assertTrue(self.progress(started_from_scratch=True))


class ControllerEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.store=MemoryStore();self.requests=Requests()
        self.flow=Workflow(self.store,self.requests,True,clock=lambda:NOW)
        for meta in (self.store.nodes['placement_evidence'],self.store.nodes['progress']):
            meta.update(observed_at=NOW,received_at=NOW)
        self.flow.accept(EVENT)
        self.flow.tick(self.store.get('e1'))
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED',owner='eks-a')
    def tick(self):return self.flow.tick(self.store.get('e1'))
    def test_success_keeps_evidence_audit_ids(self):
        result=self.tick()
        self.assertEqual(result['state'],'RECOVERED')
        self.assertEqual(result['evidence_jobset_uid'],'new-uid')
        self.assertEqual(result['evidence_completed_step'],1200)
    def test_missing_forged_stale_and_wrong_placement_do_not_complete(self):
        original=copy.deepcopy(self.store.nodes)
        cases=[('placement_evidence','producer','unknown'),('progress','received_at',NOW-301),
               ('progress','observed_at',NOW+31),('progress','schema_version',True),
               ('progress','outcome','pending')]
        for section,field,value in cases:
            self.store.nodes=copy.deepcopy(original);self.store.nodes[section][field]=value
            with self.subTest(field=field):self.assertEqual(self.tick()['state'],'RESUMING')
        self.store.nodes=copy.deepcopy(original);self.store.nodes['jobset_uid']='old'
        self.assertEqual(self.tick()['state'],'RESUMING')
    def test_trusted_failure_holds_immediately(self):
        self.store.nodes['progress'].update(outcome='failed',failure_reason='checkpoint load failed')
        result=self.tick();self.assertEqual(result['state'],'HOLD')
        self.assertEqual(result['stopped_stage'],'RESUMING')
    def test_missing_evidence_times_out_without_relocation_resubmission(self):
        self.store.nodes.pop('progress')
        runner=Runner(self.store,self.flow,Producer(),'result',dict(resume_timeout_seconds=10))
        self.assertEqual(runner.operation(self.store.get('e1'),0,False)['state'],'RESUMING')
        self.assertEqual(runner.operation(self.store.get('e1'),9,False)['state'],'RESUMING')
        result=runner.operation(self.store.get('e1'),10,False)
        self.assertEqual(result['state'],'HOLD')
        self.assertEqual(len(self.requests.calls),1)
    def test_resume_phase_change_does_not_reset_deadline(self):
        self.store.nodes.pop('progress')
        self.store.events['e1'].update(waiting_phase='NEW_RUN_SUBMITTED',budget_stage='NEW_RUN_SUBMITTED',stage_started_at=0)
        runner=Runner(self.store,self.flow,Producer(),'result',dict(resume_timeout_seconds=10))
        runner.operation(self.store.get('e1'),9,False)
        self.assertEqual(self.store.get('e1')['stage_started_at'],0)
        self.assertEqual(runner.operation(self.store.get('e1'),10,False)['state'],'HOLD')
    def test_decimal_evidence_and_bool_rejection(self):
        self.store.nodes['progress'].update(generation=Decimal(2),completed_step=Decimal(1200),restored_gen=Decimal(1))
        self.assertEqual(self.tick()['state'],'RECOVERED')


if __name__=='__main__':unittest.main()
