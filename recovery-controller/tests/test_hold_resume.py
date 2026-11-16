"""HOLD follows only trusted relocator progress, without a new recovery."""
import copy
import json
import unittest
from recovery.workflow import Workflow
from recovery.retry_policy import Runner
from test_workflow import MemoryStore, Requests, EVENT


class Producer:
    def __init__(self):
        self.fail = False
        self.messages = []
    def produce(self, topic, **kw):
        self.messages.append(json.loads(kw['value']))
        kw['on_delivery']('unavailable' if self.fail else None, None)
    def flush(self, timeout): return 0


class HoldResumeTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.requests = Requests()
        self.flow = Workflow(self.store, self.requests, True)
        self.flow.accept(EVENT)
        record = self.flow.tick(self.store.get('e1'))
        self.store.fence.update(recovery_from_gen=1, recovery_to_gen=2, retry_count=1)
        self.store.job['retry_count'] = 1
        self.producer = Producer()
        self.runner = Runner(self.store, self.flow, self.producer, 'recovery.action')
        self.store.save('e1', budget_stage='GENERATION_ADVANCED', waiting_phase='GENERATION_ADVANCED',
                        stage_started_at=0, retry_count=1)
        self.runner.hold(self.store.get('e1'), 'stage deadline exceeded: GENERATION_ADVANCED')
    def step(self, now=1000):
        return self.runner.operation(self.store.get('e1'), now, False)
    def test_stop_confirmation_resumes_same_request_and_counter(self):
        self.store.fence.update(recovery_phase='OLD_RUN_STOPPED')
        result = self.step()
        self.assertEqual(result['state'], 'REQUESTED')
        self.assertEqual(result['waiting_phase'], 'OLD_RUN_STOPPED')
        self.assertEqual(result['stage_started_at'], 1000)
        self.assertEqual(result['retry_count'], 1)
        self.assertEqual(self.store.job['generation'], 2)
        self.assertEqual(len(self.requests.calls), 1)
        self.assertEqual(result['hold_resume_history'][0]['recovery_id'], 'e1')
        self.assertEqual(self.step(1899)['state'], 'REQUESTED')
        self.assertEqual(self.step(1900)['state'], 'HOLD')
        self.assertEqual(self.step(1901)['state'], 'HOLD')  # no repeated budget reset
    def test_skipped_intermediate_phase_still_requires_progress_evidence(self):
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='eks-a')
        self.store.nodes['progress']['completed_step'] = 1000
        result = self.step()
        self.assertEqual(result['state'], 'RESUMING')
        self.assertEqual(result['budget_stage'], 'RESUME')
        self.assertEqual(result['stage_started_at'], 1000)
        self.store.nodes['progress']['completed_step'] = 1001
        self.assertEqual(self.step(1005)['state'], 'RECOVERED')
        self.assertEqual(len(self.requests.calls), 1)
    def test_other_recovery_or_generation_never_releases_hold(self):
        baseline = copy.deepcopy(self.store.fence)
        for key, value in [('recovery_id','other'),('generation',3),('recovery_from_gen',2),('recovery_to_gen',3)]:
            self.store.fence = dict(baseline, recovery_phase='OLD_RUN_STOPPED', **{key:value})
            self.assertEqual(self.step()['state'], 'HOLD', key)
        self.assertEqual(len(self.requests.calls), 1)
    def test_unconfirmed_stop_or_missing_uid_never_releases_hold(self):
        self.assertEqual(self.step()['state'], 'HOLD')
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', new_jobset_uid=None)
        self.assertEqual(self.step()['state'], 'HOLD')
    def test_api_config_and_resume_holds_never_release(self):
        self.store.fence.update(recovery_phase='OLD_RUN_STOPPED')
        for reason in ('API attempt budget exhausted: GENERATION_ADVANCED', 'configuration invalid',
                       'stage deadline exceeded: RESUME'):
            self.store.save('e1', reason=reason)
            self.assertEqual(self.step()['state'], 'HOLD')
        self.assertEqual(len(self.requests.calls), 1)
    def test_monitoring_hold_blocks_resume(self):
        self.store.fence.update(recovery_phase='OLD_RUN_STOPPED')
        result=self.runner.operation(self.store.get('e1'),1000,True)
        self.assertEqual(result['state'],'HOLD')
    def test_observe_mode_never_resumes(self):
        self.flow.execute=False
        self.store.fence.update(recovery_phase='OLD_RUN_STOPPED')
        self.assertEqual(self.step()['state'],'HOLD')
    def test_poll_failure_backs_off_and_recovers_without_new_claim(self):
        original=self.store.fencing_record
        calls=[]
        def unavailable(job):
            calls.append(job)
            raise TimeoutError()
        self.store.fencing_record=unavailable
        self.assertEqual(self.step()['state'],'HOLD')
        self.assertEqual(self.store.get('e1')['hold_poll_next_at'],1005)
        self.step(1001)
        self.assertEqual(len(calls),1)
        self.store.fencing_record=original
        self.store.fence.update(recovery_phase='OLD_RUN_STOPPED')
        self.assertEqual(self.step(1005)['state'],'REQUESTED')
    def test_kafka_outage_preserves_hold_and_later_result_with_distinct_ids(self):
        self.producer.fail=True
        self.runner.deliver(self.store.get('e1'),999)
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED',owner='eks-a')
        result=self.step()
        self.assertEqual(result['state'],'RECOVERED')
        self.assertEqual(len(result['pending_results']),1)
        self.assertEqual(len(self.requests.calls),1)
        self.producer.fail=False
        # Simulate finalization so this test exercises only the outbox contract.
        self.store.save('e1',finalized=True)
        self.runner=Runner(self.store,self.flow,self.producer,'recovery.action')
        self.runner.deliver(self.store.get('e1'),1005)
        old,new=self.producer.messages[-2:]
        self.assertEqual(old['type'],'recovery.held')
        self.assertEqual(new['state'],'RECOVERED')
        self.assertNotEqual(old['event_id'],new['event_id'])
        self.assertEqual(old['occurred_at'],self.producer.messages[0]['occurred_at'])
        self.assertTrue(self.store.get('e1')['pending_results'][0]['published'])
        self.assertTrue(self.store.get('e1')['published'])
    def test_already_published_hold_is_not_lost_or_republished_on_resume(self):
        self.runner.deliver(self.store.get('e1'),999)
        self.store.fence.update(recovery_phase='OLD_RUN_STOPPED')
        result=self.step()
        self.assertEqual(result['pending_results'],[])
        self.assertFalse(result['published'])
        self.assertEqual(result['result_sequence'],1)
        self.runner.deliver(result,1005)
        self.assertEqual(len(self.producer.messages),1)
