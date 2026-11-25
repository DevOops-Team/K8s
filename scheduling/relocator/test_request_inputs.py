import copy
import json
import unittest
from unittest.mock import Mock
from request_inputs import ANNOTATION, InputsUnavailable, RequestStore


def request():
    data = dict(schema_version=1, job_id='abc', request_id='r1', observed_at=100,
                fencing=dict(generation=2, recovery_from_gen=1, recovery_to_gen=2,
                             recovery_id='r1', recovery_phase='GENERATION_ADVANCED'),
                original_jobset={'kind': 'JobSet', 'metadata': {'name': 'job-abc-g0001'}})
    return dict(metadata={'annotations': {ANNOTATION: json.dumps(data)}},
                spec={'requestId': 'r1', 'jobRef': 'training/job-abc-g0001'})


class RequestInputTests(unittest.TestCase):
    def setUp(self):
        self.writer = Mock(spec=['set_phase', 'check_authority'])
        self.writer.set_phase.return_value = True
        self.writer.check_authority.return_value = True

    def test_all_reads_use_payload_not_writer(self):
        store = RequestStore(request(), self.writer, clock=lambda: 100)
        self.assertEqual(store.get_fencing('abc')['generation'], 2)
        self.assertEqual(store.get_jobset('abc')['kind'], 'JobSet')
        self.writer.set_phase.assert_called_once_with('abc', 'r1', 2,
            ('GENERATION_ADVANCED',), 'GENERATION_ADVANCED')

    def test_missing_stale_future_wrong_identity_fail_closed(self):
        for clock in (lambda: 161, lambda: 94):
            with self.assertRaises(InputsUnavailable): RequestStore(request(), self.writer, clock)
        rr = request()
        rr['metadata']['annotations'] = {}
        with self.assertRaises(InputsUnavailable): RequestStore(rr, self.writer, lambda: 100)
        rr = request()
        rr['spec']['requestId'] = 'other'
        with self.assertRaises(InputsUnavailable): RequestStore(rr, self.writer, lambda: 100)
        self.writer.set_phase.assert_not_called()

    def test_authority_change_blocks_before_side_effects(self):
        store = RequestStore(request(), self.writer, lambda: 100)
        self.writer.check_authority.return_value = False
        with self.assertRaises(PermissionError): store.get_fencing('abc')
        self.writer.set_phase.assert_not_called()

    def test_same_authority_new_phase_waits_for_refresh(self):
        store = RequestStore(request(), self.writer, lambda: 100)
        self.writer.set_phase.return_value = False
        with self.assertRaises(InputsUnavailable): store.get_fencing('abc')

    def test_progress_updates_local_snapshot_after_cas_only(self):
        store = RequestStore(request(), self.writer, lambda: 100)
        self.assertTrue(store.set_phase('abc', 'r1', 2, ('GENERATION_ADVANCED',), 'OLD_RUN_STOPPED'))
        self.assertEqual(store.get_fencing('abc')['recovery_phase'], 'OLD_RUN_STOPPED')
        self.writer.set_phase.return_value = False
        self.assertFalse(store.set_phase('abc', 'r1', 2, ('OLD_RUN_STOPPED',), 'NEW_RUN_SUBMITTED'))
        self.assertEqual(store.data['fencing']['recovery_phase'], 'OLD_RUN_STOPPED')

    def test_job_and_recovery_cannot_be_swapped(self):
        store = RequestStore(request(), self.writer, lambda: 100)
        with self.assertRaises(PermissionError): store.get_jobset('other')
        with self.assertRaises(PermissionError): store.set_phase('abc', 'other', 2, (), 'OLD_RUN_STOPPED')

    def test_returned_original_cannot_mutate_request(self):
        store = RequestStore(request(), self.writer, lambda: 100)
        orig = store.get_jobset('abc')
        orig['metadata']['name'] = 'changed'
        self.assertEqual(store.get_jobset('abc')['metadata']['name'], 'job-abc-g0001')


class ControllerInputExecutionTests(unittest.TestCase):
    def execute(self, stolen=False, phase_advanced=False):
        import time
        import relocator
        from test_relocator_flow import FakeCO
        rr = request()
        data = json.loads(rr['metadata']['annotations'][ANNOTATION])
        data['observed_at'] = int(time.time())
        data['fencing']['recovery_phase'] = 'OLD_RUN_STOPPED'
        data['original_jobset']['spec'] = {}
        rr['metadata'].update(name='rr1', namespace='training', uid='r-uid')
        rr['metadata']['annotations'][ANNOTATION] = json.dumps(data)
        rr['spec']['action'] = 'restartInPlace'
        rr['status'] = {'sourceCluster': 'eks-a'}
        current = copy.deepcopy(data['fencing'])
        writer = Mock(spec=['set_phase', 'check_authority', 'require_controller_inputs'])
        writer.require_controller_inputs = True
        writer.check_authority.side_effect = lambda job, rid, gen: current['recovery_id'] == rid

        def cas(job, rid, gen, allowed, phase, extra=None):
            if phase == 'NEW_RUN_SUBMITTED' and stolen:
                current['recovery_id'] = 'other'
                return False
            if phase == 'NEW_RUN_SUBMITTED' and phase_advanced:
                current['recovery_phase'] = 'NEW_RUN_SUBMITTED'
                return False
            if current['recovery_id'] != rid or current['recovery_phase'] not in allowed:
                return False
            current.update(recovery_phase=phase, **(extra or {}))
            return True

        writer.set_phase.side_effect = cas
        co = FakeCO()
        x = relocator.Ctx(None, None, writer, rr)
        x.co = co
        return relocator, x, co, current

    def test_submits_from_rr_with_no_db_or_s3_read_interface(self):
        relocator, x, co, current = self.execute()
        relocator.resubmit(x)
        self.assertEqual(co.created[0]['metadata']['name'], 'job-abc-g0002')
        self.assertEqual(current['recovery_phase'], 'NEW_RUN_SUBMITTED')

    def test_authority_loss_still_withdraws_only_created_uid(self):
        relocator, x, co, current = self.execute(stolen=True)
        with self.assertRaises(PermissionError): relocator.resubmit(x)
        self.assertEqual(co.deleted, [('training', 'job-abc-g0002', 'uid-new')])

    def test_lost_phase_response_waits_instead_of_deleting_valid_job(self):
        relocator, x, co, current = self.execute(phase_advanced=True)
        with self.assertRaises(InputsUnavailable): relocator.resubmit(x)
        self.assertEqual(co.deleted, [])
