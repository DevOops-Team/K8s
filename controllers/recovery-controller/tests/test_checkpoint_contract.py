"""Confirmed checkpoint contract boundaries; no AWS/Kubernetes calls."""
import copy
import json
import unittest
from test_retry_policy import Store, Workflow, Runner

class WarningProducer:
    def __init__(self): self.payload=None
    def produce(self,topic,**kw):
        self.payload=json.loads(kw['value']);kw['on_delivery'](None,None)
    def flush(self,timeout): return 0

class WarningContractTests(unittest.TestCase):
    def test_scratch_warning_is_durable_and_in_result_event(self):
        store=Store();producer=WarningProducer()
        warning='NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH'
        store.record.update(state='HOLD',action='HOLD',checkpoint_warning=warning)
        Runner(store,Workflow(store),producer,'recovery.action').deliver(copy.deepcopy(store.record),0)
        self.assertEqual(producer.payload['checkpoint_warning'],warning)
        self.assertTrue(store.record['published'])
