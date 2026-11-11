import copy
import unittest
import time
from recovery.workflow import Workflow
from test_fencing import EVENT as BASE_EVENT

EVENT = dict(BASE_EVENT, fault_type='node_hardware')


class MemoryStore:
    def __init__(self):
        self.events = {}
        self.job = dict(resume_gen=1, resume_step=1000, generation=1, cluster='eks-a', gpu_count=1,
                        jobset_uri='s3://bucket/code/j1/jobset.json', jobset_sha256='a'*64)
        self.fence = {}
        self.nodes = dict(placement_generation=2, cluster='eks-a', active_nodes=['new'],
                          progress=dict(recovery_id='e1',generation=2,cluster='eks-a',
                                        jobset_uid='new-uid',checkpoint_loaded=True,
                                        restored_gen=1,restored_step=1000,completed_step=1200))
        now = int(time.time())
        self.nodes.update(recovery_id='e1', jobset_uid='new-uid',
            placement_evidence=dict(schema_version=1,event_id='placement-1',producer='scheduling',observed_at=now,received_at=now))
        self.nodes['progress'].update(schema_version=1,event_id='progress-1',producer='training',
                                      observed_at=now,received_at=now,outcome='succeeded')
    def accept(self, event, digest):
        old = self.events.get(event['event_id'])
        if old:
            if old['digest'] != digest:
                raise ValueError('collision')
            return
        self.events[event['event_id']] = dict(event=copy.deepcopy(event), digest=digest, state='RECEIVED')
    def get(self, rid):
        return copy.deepcopy(self.events[rid])
    def save(self, rid, **values):
        self.events[rid].update(values)
        return self.get(rid)
    def claim(self, event):
        if self.job.get('recovery_id') == event['event_id']:
            return self.job.copy()
        if self.job['generation'] != event['generation']:
            return None
        self.job.update(generation=2, recovery_id=event['event_id'])
        self.fence.update(generation=2, recovery_id=event['event_id'], recovery_phase='GENERATION_ADVANCED',new_jobset_uid='new-uid')
        return self.job.copy()
    def fencing_record(self, _):
        return self.fence.copy()
    def placement(self, _):
        return self.nodes.copy()
    def resuming(self, event, generation):
        self.fence['recovery_phase'] = 'RESUMING'


class Requests:
    def __init__(self):
        self.calls = []
        self.fail = False
    def submit(self, event, action):
        self.calls.append((event.copy(), action))
        if self.fail:
            raise TimeoutError()
        return 'recovery-stable'


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.requests = Requests()
        self.flow = Workflow(self.store, self.requests, True)
        self.flow.accept(EVENT)
    def tick(self):
        return self.flow.tick(self.store.get('e1'))
    def test_completes_on_training_progress_before_new_checkpoint(self):
        self.assertEqual(self.tick()['state'], 'REQUESTED')
        self.assertEqual(self.tick()['state'], 'REQUESTED')
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='eks-a',
                                approved_gen=1,approved_step=1000)
        self.store.nodes['progress']['completed_step']=1000
        self.assertEqual(self.tick()['state'], 'RESUMING')
        self.store.nodes['progress']['completed_step']=1001
        self.assertEqual(self.tick()['state'], 'RECOVERED')
        self.assertEqual(self.store.fence['approved_gen'],1)
        self.assertEqual(self.store.fence['approved_step'],1000)
        self.assertEqual(self.requests.calls[0][1], 'moveToSpare')
    def test_lost_create_response_reuses_id_and_generation(self):
        self.requests.fail = True
        with self.assertRaises(TimeoutError):
            self.tick()
        self.requests.fail = False
        self.assertEqual(self.tick()['state'], 'REQUESTED')
        self.assertEqual(self.store.job['generation'], 2)
        self.assertEqual(self.requests.calls[0], self.requests.calls[1])
    def test_hold_refuse_none_never_claim_or_submit(self):
        for recommendation, state in [('hold','HOLD'), ('refuse','REJECTED'), ('none','IGNORED')]:
            event = dict(EVENT, event_id=recommendation, suggested_action=recommendation)
            self.flow.accept(event)
            self.assertEqual(self.flow.tick(self.store.get(recommendation))['state'], state)
        self.assertEqual(self.store.job['generation'], 1)
        self.assertEqual(self.requests.calls, [])
    def test_site_without_node_creates_evacuation_request(self):
        self.store.events['e1']['event'].update(fault_type='az_failure', node=None)
        self.assertEqual(self.tick()['state'], 'REQUESTED')
        self.assertEqual(self.requests.calls[0][1], 'evacuateCluster')
    def test_preemptive_claims_before_spare_request_and_resumes_approved_step(self):
        self.store.events['e1']['event']['suggested_action'] = 'evacuate_preemptive'
        submit = self.requests.submit
        def check_generation_before_submit(event, action):
            self.assertEqual(self.store.fence['generation'], 2)
            self.assertEqual(self.store.fence['recovery_phase'], 'GENERATION_ADVANCED')
            self.assertEqual(event['generation'], 1)  # Relocator must stop the old run.
            return submit(event, action)
        self.requests.submit = check_generation_before_submit
        result = self.tick()
        self.assertEqual(result['state'], 'REQUESTED')
        self.assertEqual(result['action'], 'QUARANTINE_AND_RETRY')
        self.assertEqual(self.requests.calls[0][1], 'moveToSpare')
        self.assertEqual(self.tick()['state'], 'REQUESTED')  # Old-run stop not confirmed.
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='eks-a',
                                approved_gen=1, approved_step=1000)
        self.assertEqual(self.tick()['state'], 'RECOVERED')
        self.assertEqual(self.store.fence['approved_step'], 1000)

    def test_preemptive_requires_target_node_before_claim(self):
        with self.assertRaises(ValueError):
            self.flow.accept(dict(EVENT, suggested_action='evacuate_preemptive', node=None))
        self.assertEqual(self.store.job['generation'], 1)
        self.assertEqual(self.requests.calls, [])

    def test_preemptive_does_not_resume_in_another_cluster(self):
        self.store.events['e1']['event']['suggested_action'] = 'evacuate_preemptive'
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='eks-b')
        self.assertEqual(self.tick()['state'], 'HOLD')

    def test_no_fresh_placement_cannot_complete(self):
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='eks-a', approved_gen=2)
        self.store.nodes['placement_generation'] = 1
        self.assertEqual(self.tick()['state'], 'RESUMING')
    def test_stale_event_and_observe_cannot_submit(self):
        self.store.job['generation'] = 3
        self.assertEqual(self.tick()['state'], 'STALE')
        self.assertEqual(self.requests.calls, [])
    def test_conflicting_id_rejected(self):
        with self.assertRaises(ValueError):
            self.flow.accept(dict(EVENT, node='different'))
    def test_missing_node_rejected_for_node_action(self):
        with self.assertRaises(ValueError):
            self.flow.accept(dict(EVENT, node=None))

    def test_same_step_reapproval_is_not_progress(self):
        self.tick()
        self.store.nodes['progress']['completed_step'] = 1000
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='eks-a',
                                approved_gen=2, approved_step=1200)
        self.assertEqual(self.tick()['state'], 'RESUMING')

    def test_unknown_owner_does_not_complete(self):
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='other',
                                approved_gen=2, approved_step=1200)
        self.assertEqual(self.tick()['state'], 'REQUESTED')

    def test_invalid_job_name_is_rejected_before_claim(self):
        with self.assertRaises(ValueError):
            self.flow.accept(dict(EVENT, job_id='../wrong'))
        self.assertEqual(self.store.job['generation'], 1)

    def test_legacy_review_is_migrated_to_hold_without_execution(self):
        self.store.events['e1'].update(state='REVIEW',reason='manual inspection')
        result=self.tick()
        self.assertEqual(result['state'],'HOLD')
        self.assertEqual(result['reason'],'manual inspection')
        self.assertEqual(self.requests.calls,[])

    def test_monitoring_hold_blocks_new_recovery_and_keeps_input_pending(self):
        record=self.flow.tick(self.store.get('e1'), monitoring_hold=True)
        self.assertEqual(record['state'],'RECEIVED')
        self.assertEqual(self.store.job['generation'],1)
        self.assertEqual(self.requests.calls,[])
        self.assertEqual(self.tick()['state'],'REQUESTED')

    def test_monitoring_hold_does_not_interrupt_existing_recovery(self):
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED',owner='eks-a',
                                approved_gen=2,approved_step=1200)
        record=self.flow.tick(self.store.get('e1'), monitoring_hold=True)
        self.assertEqual(record['state'],'RECOVERED')

    def test_approved_checkpoint_without_progress_is_not_completion(self):
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED',owner='eks-a',
                                approved_gen=2,approved_step=1500)
        self.store.nodes.pop('progress')
        self.assertEqual(self.tick()['state'],'RESUMING')

    def test_progress_must_match_recovery_generation_jobset_and_restore(self):
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED',owner='eks-a')
        for field,value in [('recovery_id','other'),('generation',1),('jobset_uid','old-uid'),
                            ('cluster','eks-b'),('restored_step',999),('checkpoint_loaded',False),
                            ('completed_step',True),('completed_step',1000.5)]:
            original=self.store.nodes['progress'].copy()
            self.store.nodes['progress'][field]=value
            self.assertEqual(self.tick()['state'],'RESUMING')
            self.store.nodes['progress']=original

    def test_claimed_is_recorded_before_any_relocation_request(self):
        actual_claim=self.store.claim
        pending=[True]
        def claim(event):
            if pending:
                pending.pop()
                return dict(self.store.job,claim_pending=True)
            return actual_claim(event)
        self.store.claim=claim
        self.assertEqual(self.tick()['state'],'CLAIMED')
        self.assertEqual(self.requests.calls,[])
        self.assertEqual(self.tick()['state'],'REQUESTED')

    def test_nodes_must_be_a_list(self):
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED',owner='eks-a')
        self.store.nodes['active_nodes']='x'
        self.assertEqual(self.tick()['state'],'RESUMING')

    def test_scratch_requires_actual_start_and_step_evidence(self):
        self.store.job.update(resume_gen=None,resume_step=0,allow_restart_from_scratch=True,
                              checkpoint_warning='NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH')
        self.tick()
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED',owner='eks-a')
        progress=self.store.nodes['progress']
        progress.update(restored_gen=None,restored_step=0,completed_step=1)
        self.assertEqual(self.tick()['state'],'RESUMING')
        progress['started_from_scratch']=True
        result=self.tick()
        self.assertEqual(result['state'],'RECOVERED')
        self.assertEqual(result['checkpoint_warning'],'NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH')
