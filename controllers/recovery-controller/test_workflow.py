import copy
import unittest
from workflow import Workflow

EVENT=dict(event_id='e1',job_id='j1',cluster='eks-a',node='gpu1',generation=1,fault_type='node_hardware')

class MemoryStore:
    def __init__(self):
        self.events={}
        self.job=dict(generation=1,cluster='eks-a',gpu_count=4,jobset_uri='s3://bucket/code/j1/jobset.json',jobset_sha256='a'*64)
    def accept(self,e,digest):
        if e['event_id'] in self.events:
            if self.events[e['event_id']]['digest']!=digest: raise ValueError('collision')
            return
        self.events[e['event_id']]=dict(event=copy.deepcopy(e),digest=digest,state='RECEIVED')
    def get(self,rid): return copy.deepcopy(self.events[rid])
    def save(self,rid,**values):
        self.events[rid].update(values)
        return self.get(rid)
    def owner_matches(self,*_): return getattr(self, "owner_ok", True)
    def claim(self,e):
        if self.job.get('recovery_id')==e['event_id']: return self.job.copy()
        if self.job['generation']!=e['generation']: return None
        self.job.update(generation=e['generation']+1,recovery_id=e['event_id'])
        return self.job.copy()

class Executor:
    stopped=True
    resumed=True
    fail_restart=False
    def __init__(self): self.calls=[]
    def call(self,op,payload):
        self.calls.append(op)
        if op=='fence': return dict(old_execution_stopped=self.stopped)
        if op=='restart':
            if self.fail_restart: raise TimeoutError()
            return dict(accepted=True,target_cluster='eks-a')
        return dict(generation=payload['generation'],target_cluster='eks-a',checkpoint_loaded=self.resumed,step_completed=self.resumed,active_nodes=['new1','new2','new3','new4'])

class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.store=MemoryStore(); self.executor=Executor()
        self.flow=Workflow(self.store,self.executor,True);self.flow.accept(EVENT)
    def tick(self): return self.flow.tick(self.store.get('e1'))
    def test_full_recovery(self):
        self.assertEqual(self.tick()['state'],'FENCED')
        self.assertEqual(self.tick()['state'],'SUBMITTED')
        self.assertEqual(self.tick()['state'],'RECOVERED')
        self.assertEqual(self.store.job['generation'],2)
        self.assertEqual(self.executor.calls,['fence','restart','status'])
    def test_duplicate_does_not_advance_generation(self):
        self.tick();self.flow.accept(EVENT);self.tick()
        self.assertEqual(self.store.job['generation'],2)
    def test_conflicting_event_id_rejected(self):
        with self.assertRaises(ValueError): self.flow.accept(EVENT | {'node':'other'})
    def test_no_restart_before_stop_confirmation(self):
        self.executor.stopped=False
        self.assertEqual(self.tick()['state'],'RECEIVED')
        self.assertNotIn('restart',self.executor.calls)
    def test_response_loss_retries_same_stage_and_generation(self):
        self.tick(); self.executor.fail_restart=True
        with self.assertRaises(TimeoutError): self.tick()
        self.executor.fail_restart=False
        self.flow=Workflow(self.store,self.executor,True)
        self.assertEqual(self.tick()['state'],'SUBMITTED')
        self.assertEqual(self.store.job['generation'],2)
    def test_running_pod_is_not_recovery_success(self):
        self.tick();self.tick();self.executor.resumed=False
        self.assertEqual(self.tick()['state'],'SUBMITTED')
    def test_stale_event_does_not_restart(self):
        self.store.job['generation']=3
        self.assertEqual(self.tick()['state'],'STALE')
        self.assertEqual(self.executor.calls,[])
    def test_observe_does_not_claim(self):
        self.flow.execute=False
        self.assertEqual(self.tick()['state'],'OBSERVED')
        self.assertEqual(self.store.job['generation'],1)
    def test_unknown_goes_to_review(self):
        self.store.events['e1']['event']['fault_type']='unknown'
        self.assertEqual(self.tick()['state'],'REVIEW')
        self.assertEqual(self.store.job['generation'],1)
    def test_missing_node_rejected(self):
        e=EVENT.copy();del e['node']
        with self.assertRaises(ValueError): self.flow.accept(e)

    def test_owner_mismatch_blocks_success(self):
        self.tick();self.tick();self.store.owner_ok=False
        self.assertEqual(self.tick()['state'],'SUBMITTED')
    def test_request_contains_jobset_reference_not_checkpoint_uri(self):
        seen=[]
        original=self.executor.call
        def call(operation,payload):
            seen.append(payload.copy())
            return original(operation,payload)
        self.executor.call=call
        self.tick()
        self.assertEqual(seen[0]['checkpoint_source'],'dynamodb-approved')
        self.assertNotIn('checkpoint_uri',seen[0])
        self.assertEqual(seen[0]['jobset_sha256'],'a'*64)
    def test_missing_new_node_evidence_blocks_success(self):
        self.tick();self.tick()
        self.executor.call=lambda op,payload: dict(generation=2,target_cluster='eks-a',checkpoint_loaded=True,step_completed=True)
        self.assertEqual(self.tick()['state'],'SUBMITTED')
