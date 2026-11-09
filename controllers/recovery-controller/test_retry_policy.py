import copy
import unittest
from retry_policy import Runner

class Store:
    def __init__(self):
        self.record = dict(event=dict(event_id='e',job_id='j',generation=1),state='REQUESTED')
        self.finished = 0
    def save(self, rid, **values):
        self.record.update(values)
        return copy.deepcopy(self.record)
    def finish(self, record): self.finished += 1

class Workflow:
    def __init__(self, store): self.store=store; self.calls=0; self.fail=False
    def tick(self, record, monitoring_hold=False):
        self.calls += 1
        if self.fail: raise TimeoutError()
        return record

class Producer:
    def __init__(self): self.calls=0;self.fail=True
    def produce(self, *args, **kwargs):
        self.calls+=1
        kwargs['on_delivery']('unavailable' if self.fail else None, None)
    def flush(self, timeout): return 0

class RetryTests(unittest.TestCase):
    def setUp(self):
        self.store=Store();self.flow=Workflow(self.store);self.producer=Producer()
        self.runner=Runner(self.store,self.flow,self.producer,'result')
    def step(self, now): return self.runner.operation(copy.deepcopy(self.store.record),now,False)
    def test_three_attempts_survive_runner_restart(self):
        self.flow.fail=True
        self.step(0);self.step(5)
        self.runner=Runner(self.store,self.flow,self.producer,'result')
        result=self.step(10)
        self.assertEqual(result['state'],'HOLD')
        self.step(15)
        self.assertEqual(self.flow.calls,3)
    def test_successful_polling_does_not_slide_deadline(self):
        self.step(0);self.step(899)
        self.assertEqual(self.flow.calls,2)
        self.assertEqual(self.step(900)['state'],'HOLD')
        self.assertEqual(self.flow.calls,2)
    def test_new_phase_has_separate_deadline(self):
        self.step(0)
        self.store.record['waiting_phase']='OLD_RUN_STOPPED'
        self.step(899)
        self.assertEqual(self.store.record['stage_started_at'],899)
        self.assertEqual(self.step(1799)['state'],'HOLD')
    def test_third_inflight_attempt_is_not_repeated_after_crash(self):
        self.store.record.update(budget_stage='REQUESTED',stage_started_at=0,api_attempts=3)
        self.assertEqual(self.step(5)['state'],'HOLD')
        self.assertEqual(self.flow.calls,0)
    def test_kafka_backoff_never_reexecutes_recovery(self):
        self.store.record.update(state='HOLD',action='HOLD')
        for now in (0,1,5,6,15):
            result=self.step(now);self.runner.deliver(result,now)
        self.assertEqual(self.producer.calls,3)
        self.assertEqual(self.store.record['publish_next_at'],35)
        self.assertEqual(self.flow.calls,0)
        self.producer.fail=False
        self.runner.deliver(copy.deepcopy(self.store.record),35)
        self.assertTrue(self.store.record['published'])
    def test_recovered_finalizes_only_once_during_delivery_failure(self):
        self.store.record.update(state='RECOVERED')
        for now in (0,5,15):
            self.runner.deliver(self.step(now),now)
        self.assertEqual(self.store.finished,1)
        self.assertEqual(self.flow.calls,0)
    def test_db_intent_write_failure_does_not_start_operation(self):
        def fail(*args,**kwargs): raise TimeoutError()
        self.store.save=fail
        with self.assertRaises(TimeoutError):self.step(0)
        self.assertEqual(self.flow.calls,0)
