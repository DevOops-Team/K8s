import copy
import hashlib
import io
import json
import unittest
from unittest.mock import Mock
from integrations.dynamodb import RecoveryBlocked
from integrations.relocation import RelocationClient
from integrations.relocation_inputs import ANNOTATION, RelocationInputs

EVENT = dict(event_id='r1', job_id='abc', generation=1, cluster='eks-a', node='bad', category='hw')


class InputTests(unittest.TestCase):
    def setUp(self):
        self.raw = json.dumps({'kind': 'JobSet', 'metadata': {'name': 'job-abc-g0001'}, 'spec': {}}).encode()
        self.fence = dict(generation=2, recovery_id='r1', recovery_from_gen=1,
                          recovery_to_gen=2, recovery_phase='GENERATION_ADVANCED')
        self.store, self.s3 = Mock(), Mock()
        self.store.fencing_record.return_value = self.fence
        self.store.placement.return_value = dict(jobset_uri='s3://code/code/abc/jobset.json',
            jobset_sha256=hashlib.sha256(self.raw).hexdigest())
        self.s3.get_object.side_effect = lambda **kw: {'Body': io.BytesIO(self.raw)}
        self.provider = RelocationInputs(self.store, self.s3, clock=lambda: 100)

    def test_producer_sends_selected_fencing_fields_and_verified_original(self):
        self.fence['credential'] = 'not-for-wire'
        data = json.loads(self.provider(EVENT))
        self.assertEqual(data['fencing']['generation'], 2)
        self.assertNotIn('credential', data['fencing'])
        self.assertEqual(data['original_jobset']['metadata']['name'], 'job-abc-g0001')
        self.s3.get_object.assert_called_once_with(Bucket='code', Key='code/abc/jobset.json')

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

    def test_create_and_refresh_use_same_spec_and_versioned_annotation(self):
        saved, calls = {}, []
        def transport(method, path, document=None):
            calls.append((method, copy.deepcopy(document)))
            if method == 'POST':
                saved.update(copy.deepcopy(document))
                saved['metadata']['resourceVersion'] = '7'
            return saved
        client = RelocationClient('training', transport, self.provider)
        name = client.submit(EVENT, 'moveToSpare')
        before = copy.deepcopy(saved['spec'])
        self.assertEqual(json.loads(saved['metadata']['annotations'][ANNOTATION])['request_id'], 'r1')
        self.fence['recovery_phase'] = 'OLD_RUN_STOPPED'
        client.refresh(EVENT, name)
        patch = calls[-1][1]
        self.assertEqual(calls[-1][0], 'PATCH')
        self.assertEqual(patch['metadata']['resourceVersion'], '7')
        self.assertEqual(json.loads(patch['metadata']['annotations'][ANNOTATION])['fencing']['recovery_phase'], 'OLD_RUN_STOPPED')
        self.assertEqual(saved['spec'], before)

    def test_refresh_cannot_target_someone_else(self):
        client = RelocationClient('training', lambda *a: {'spec': {'requestId': 'other'}}, self.provider)
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

    def test_final_request_is_not_refreshed_or_recreated(self):
        existing = dict(spec={'requestId': 'r1', 'jobRef': 'training/job-abc-g0001'},
                        status={'phase': 'Done'})
        transport = Mock(return_value=existing)
        provider = Mock()
        RelocationClient('training', transport, provider).refresh(EVENT, 'rr')
        provider.assert_not_called()
        self.assertEqual(transport.call_count, 1)
