import copy
import hashlib
import io
import json
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError
from integrations.dynamodb import RecoveryBlocked
from integrations.relocation import CANCEL_ANNOTATION, RelocationClient
from integrations.relocation_inputs import RelocationInputs

EVENT = dict(event_id='r1', job_id='abc', generation=1, cluster='eks-a', node='bad', category='hw')


class InputTests(unittest.TestCase):
    def setUp(self):
        self.raw = json.dumps({'kind': 'JobSet', 'metadata': {'name': 'job-abc-g0001'}, 'spec': {}}).encode()
        self.fence = dict(generation=2, recovery_id='r1', recovery_from_gen=1,
                          recovery_to_gen=2, recovery_phase='GENERATION_ADVANCED',
                          stop_wait_started_at=1000, stop_wait_deadline=1900)
        self.store, self.s3 = Mock(), Mock()
        self.store.fencing_record.return_value = self.fence
        self.store.placement.return_value = dict(jobset_uri='s3://code/code/abc/jobset.json',
            jobset_sha256=hashlib.sha256(self.raw).hexdigest(), cluster='eks-a',
            placement_generation=1, active_nodes=['bad', 'other'], gpu_count=2)
        self.s3.get_object.side_effect = lambda **kw: {'Body': io.BytesIO(self.raw)}
        self.provider = RelocationInputs(self.store, self.s3)

    def test_spec_carries_verified_original_and_old_placement(self):
        self.fence['credential'] = 'not-for-wire'
        fields = self.provider(EVENT)
        self.assertEqual(set(fields), {'toGeneration', 'stopDeadline', 'jobSet', 'sourceCluster', 'nodes'})
        self.assertEqual(fields['toGeneration'], 2)
        self.assertEqual(fields['stopDeadline'], 1900)
        self.assertEqual(fields['jobSet']['metadata']['name'], 'job-abc-g0001')
        self.assertNotIn('not-for-wire', json.dumps(fields))
        self.s3.get_object.assert_called_once_with(Bucket='code', Key='code/abc/jobset.json')

    def test_dynamodb_decimal_deadline_becomes_epoch_integer(self):
        from decimal import Decimal
        self.fence['stop_wait_deadline'] = Decimal('1900')
        self.assertIs(type(self.provider(EVENT)['stopDeadline']), int)

    def test_missing_deadline_fails_closed(self):
        del self.fence['stop_wait_deadline']
        with self.assertRaisesRegex(RecoveryBlocked, 'stop_wait_deadline'): self.provider(EVENT)
        self.s3.get_object.assert_not_called()

    def test_missing_and_mutated_original_fail_closed(self):
        self.store.placement.return_value = {}
        with self.assertRaises(RecoveryBlocked): self.provider(EVENT)
        self.s3.get_object.assert_not_called()

    def test_digest_mismatch(self):
        self.raw = b'{"kind":"JobSet","spec":{"changed":true}}'
        with self.assertRaisesRegex(RecoveryBlocked, 'digest'): self.provider(EVENT)

    def test_other_recovery_does_not_read_s3(self):
        self.fence['recovery_id'] = 'another'
        with self.assertRaises(RecoveryBlocked): self.provider(EVENT)
        self.s3.get_object.assert_not_called()

    def test_create_puts_inputs_in_spec_without_annotation_and_retry_matches(self):
        saved, calls = {}, []
        def transport(method, path, document=None):
            calls.append((method, copy.deepcopy(document)))
            if method == 'POST':
                if saved:
                    raise HTTPError(path, 409, 'AlreadyExists', {}, None)
                saved.update(copy.deepcopy(document))
                saved['metadata']['resourceVersion'] = '7'
            return copy.deepcopy(saved)
        client = RelocationClient('training', transport, self.provider)
        name = client.submit(EVENT, 'moveToSpare')
        self.assertEqual(saved['spec']['toGeneration'], 2)
        self.assertEqual(saved['spec']['jobSet']['kind'], 'JobSet')
        self.assertEqual(saved['spec']['excludeNodes'], ['bad'])
        self.assertNotIn('annotations', saved['metadata'])
        self.assertEqual(client.submit(EVENT, 'moveToSpare'), name)  # response loss: 409 + same spec
        self.assertEqual([c[0] for c in calls], ['POST', 'POST', 'GET'])

    def test_pod_restart_uses_full_placement_when_fault_node_is_unknown(self):
        transport = Mock()
        client = RelocationClient('training', transport, self.provider)
        client.submit(dict(EVENT, node=None, category='app'), 'restartInPlace')
        spec = transport.call_args.args[2]['spec']
        self.assertEqual(spec['sourceCluster'], 'eks-a')
        self.assertEqual(spec['nodes'], [{'name': 'bad'}, {'name': 'other'}])
        self.assertNotIn('excludeNodes', spec)

    def test_stale_or_incomplete_placement_cannot_create_rr(self):
        registered = self.store.placement.return_value
        for changes in ({'placement_generation': 2}, {'cluster': 'eks-b'},
                        {'active_nodes': ['bad']}, {'active_nodes': ['bad', 'bad']}):
            with self.subTest(changes=changes):
                self.store.placement.return_value = dict(registered, **changes)
                transport = Mock()
                with self.assertRaisesRegex(RecoveryBlocked, 'old generation'):
                    RelocationClient('training', transport, self.provider).submit(EVENT, 'restartInPlace')
                transport.assert_not_called()

    def test_retry_with_different_inputs_is_not_adopted(self):
        existing = {'spec': {'requestId': 'r1', 'jobRef': 'training/job-abc-g0001',
                             'action': 'moveToSpare', 'reason': 'hardware', 'excludeNodes': ['bad'],
                             'toGeneration': 2, 'stopDeadline': 1, 'jobSet': {'kind': 'JobSet'}}}
        def transport(method, path, document=None):
            if method == 'POST':
                raise HTTPError(path, 409, 'AlreadyExists', {}, None)
            return existing
        with self.assertRaises(ValueError):
            RelocationClient('training', transport, self.provider).submit(EVENT, 'moveToSpare')

    def test_refresh_never_patches_inputs(self):
        rr = {'metadata': {'resourceVersion': '7'},
              'spec': {'requestId': 'r1', 'jobRef': 'training/job-abc-g0001'}, 'status': {'phase': 'Stopping'}}
        transport = Mock(return_value=rr)
        status_input = Mock(return_value=False)
        RelocationClient('training', transport, self.provider, status_input).refresh(EVENT, 'rr')
        self.assertEqual([c.args[0] for c in transport.call_args_list], ['GET'])
        self.s3.get_object.assert_not_called()

    def test_refresh_cannot_target_someone_else(self):
        client = RelocationClient('training', lambda *a: {'spec': {'requestId': 'other'}},
                                  self.provider, Mock())
        with self.assertRaises(ValueError): client.refresh(EVENT, 'rr')

    def test_refresh_failure_is_bounded_by_runner_not_worker_polling(self):
        from recovery.workflow import Workflow
        from recovery.retry_policy import Runner
        from test_workflow import MemoryStore, Requests, EVENT as fault
        store, requests = MemoryStore(), Requests()
        flow = Workflow(store, requests, execute=True)
        flow.accept(fault)
        flow.tick(store.get('e1'))
        requests.refresh = Mock(side_effect=TimeoutError())
        runner = Runner(store, flow, None, 'result')
        for now in (0, 5, 10):
            result = runner.operation(store.get('e1'), now, False)
        self.assertEqual(result['state'], 'HOLD')
        self.assertEqual(requests.refresh.call_count, 3)
        self.assertEqual(len(requests.calls), 1)

    def test_cancelled_request_is_not_acknowledged(self):
        rr = {'metadata': {'resourceVersion': '7'},
              'spec': {'requestId': 'r1', 'jobRef': 'training/job-abc-g0001'}, 'status': {'phase': 'Cancelled'}}
        transport = Mock(return_value=rr)
        RelocationClient('training', transport, status_input=Mock(return_value=True)).refresh(EVENT, 'rr')
        self.assertEqual([c.args[0] for c in transport.call_args_list], ['GET'])


class CancelTests(unittest.TestCase):
    def rr(self, phase='Submitted', annotations=None):
        return {'metadata': {'resourceVersion': '9', 'annotations': annotations or {}},
                'spec': {'requestId': 'r1', 'jobRef': 'training/job-abc-g0001'}, 'status': {'phase': phase}}

    def test_cancel_annotates_with_reason_even_after_done(self):
        for phase in ('Submitted', 'Done', 'Failed'):
            transport = Mock(return_value=self.rr(phase))
            self.assertTrue(RelocationClient('training', transport).cancel(EVENT, 'rr', 'authority lost'))
            method, path, body = transport.call_args.args
            self.assertEqual(method, 'PATCH')
            self.assertEqual(body, {'metadata': {'resourceVersion': '9',
                                                 'annotations': {CANCEL_ANNOTATION: 'authority lost'}}})

    def test_cancel_is_idempotent_and_skips_finished_withdrawals(self):
        for rr in (self.rr(annotations={CANCEL_ANNOTATION: 'x'}), self.rr('Cancelled'), self.rr('Duplicate')):
            transport = Mock(return_value=rr)
            self.assertFalse(RelocationClient('training', transport).cancel(EVENT, 'rr', 'again'))
            self.assertEqual(transport.call_count, 1)

    def test_cancel_cannot_target_someone_else(self):
        transport = Mock(return_value={'spec': {'requestId': 'other'}})
        with self.assertRaises(ValueError):
            RelocationClient('training', transport).cancel(EVENT, 'rr', 'x')

    def test_workflow_cancels_when_fencing_authority_moves(self):
        from recovery.workflow import Workflow
        from test_workflow import MemoryStore, Requests, EVENT as fault
        store, requests = MemoryStore(), Requests()
        requests.cancel = Mock()
        flow = Workflow(store, requests, execute=True)
        flow.accept(fault)
        self.assertEqual(flow.tick(store.get('e1'))['state'], 'REQUESTED')
        store.fence['recovery_id'] = 'someone-else'
        result = flow.tick(store.get('e1'))
        self.assertEqual((result['state'], result['reason']), ('HOLD', 'recovery authority changed'))
        requests.cancel.assert_called_once_with(store.get('e1')['event'], 'recovery-stable',
                                                'recovery authority changed')

    def test_lost_authority_during_status_projection_cancels(self):
        from recovery.workflow import Workflow
        from test_workflow import MemoryStore, Requests, EVENT as fault
        store, requests = MemoryStore(), Requests()
        flow = Workflow(store, requests, execute=True)
        flow.accept(fault)
        flow.tick(store.get('e1'))
        store.fence['recovery_id'] = 'someone-else'
        requests.refresh = Mock(side_effect=RecoveryBlocked('relocation progress lost fencing authority'))
        requests.cancel = Mock()
        result = flow.tick(store.get('e1'))
        self.assertEqual(result['state'], 'HOLD')
        requests.cancel.assert_called_once()

    def test_other_status_errors_are_not_cancellations(self):
        from recovery.workflow import Workflow
        from test_workflow import MemoryStore, Requests, EVENT as fault
        store, requests = MemoryStore(), Requests()
        flow = Workflow(store, requests, execute=True)
        flow.accept(fault)
        flow.tick(store.get('e1'))
        requests.refresh = Mock(side_effect=RecoveryBlocked('complete old-run stop evidence required'))
        requests.cancel = Mock()
        with self.assertRaises(RecoveryBlocked):
            flow.tick(store.get('e1'))
        requests.cancel.assert_not_called()


class RunnerCancelTests(unittest.TestCase):
    """Review P1: the Runner's own authority check must cancel too, and a failed cancel is retried."""
    def setUp(self):
        from recovery.workflow import Workflow
        from recovery.retry_policy import Runner
        from test_workflow import MemoryStore, Requests, EVENT as fault
        self.store, self.requests = MemoryStore(), Requests()
        self.requests.cancel = Mock()
        self.flow = Workflow(self.store, self.requests, execute=True, clock=lambda: 0)
        self.flow.accept(fault)
        self.runner = Runner(self.store, self.flow, None, 'result')
        self.assertEqual(self.runner.operation(self.store.get('e1'), 0, False)['state'], 'REQUESTED')
        self.store.fence['recovery_id'] = 'someone-else'

    def test_runner_stop_wait_check_cancels(self):
        result = self.runner.operation(self.store.get('e1'), 5, False)
        self.assertEqual((result['state'], result['reason']), ('HOLD', 'recovery authority changed'))
        self.requests.cancel.assert_called_once_with(self.store.get('e1')['event'], 'recovery-stable',
                                                     'recovery authority changed')
        self.assertIs(self.store.get('e1')['relocation_cancelled'], True)

    def test_failed_cancel_is_retried_until_done_and_never_releases_hold(self):
        self.requests.cancel.side_effect = [TimeoutError(), ConnectionError(), None]
        result = self.runner.operation(self.store.get('e1'), 5, False)
        self.assertEqual(result['state'], 'HOLD')
        self.assertIs(self.store.get('e1')['relocation_cancelled'], False)
        self.assertEqual(self.store.get('e1')['relocation_cancel_next_at'], 5)  # clock 0 + 5s backoff
        self.runner.operation(self.store.get('e1'), 4, False)                   # backoff: not yet
        self.assertEqual(self.requests.cancel.call_count, 1)
        self.runner.operation(self.store.get('e1'), 5, False)                   # second failure
        self.assertEqual(self.store.get('e1')['relocation_cancel_errors'], 2)
        record = self.runner.operation(self.store.get('e1'), 15, False)         # third try succeeds
        self.assertEqual(self.requests.cancel.call_count, 3)
        self.assertIs(record['relocation_cancelled'], True)
        self.assertEqual(record['state'], 'HOLD')
        self.runner.operation(self.store.get('e1'), 100, False)                 # no duplicate cancel
        self.assertEqual(self.requests.cancel.call_count, 3)

    def test_workflow_path_failure_is_also_retried(self):
        self.requests.cancel.side_effect = [TimeoutError(), None]
        # Past the stop wait the Runner skips its check; the tick's own authority check fires.
        self.store.events['e1'].update(state='SUBMITTED', waiting_phase='NEW_RUN_SUBMITTED')
        result = self.runner.operation(self.store.get('e1'), 5, False)
        self.assertEqual(result['state'], 'HOLD')
        self.assertIs(self.store.get('e1')['relocation_cancelled'], False)
        self.runner.operation(self.store.get('e1'), 20, False)
        self.assertIs(self.store.get('e1')['relocation_cancelled'], True)
        self.assertEqual(self.requests.cancel.call_count, 2)
