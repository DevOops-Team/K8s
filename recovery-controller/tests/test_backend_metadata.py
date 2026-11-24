import copy
import io
import json
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from integrations.backend_metadata import BackendMetadata, metadata_config, NoRedirect
from integrations.dynamodb import Store, RecoveryBlocked
from recovery.retry_policy import Runner
from recovery.workflow import Workflow
from test_fencing import EVENT, FencingTable

NOW = 1800000000
CONFIG = dict(enabled=True, base_url='https://backend.example.test')


def metadata():
    return dict(schema_version=1, job_id='j1', generation=1, recovery_eligible=True,
                cluster='eks-a', gpu_count=1, active_nodes=['n1'], placement_generation=1,
                jobset_name='job-j1-g0001', jobset_uid='manager-uid', observed_at=NOW,
                jobset_uri='s3://bucket/code/j1/jobset.json', jobset_sha256='a'*64)


class ConditionalFailure(Exception):
    response = {'Error': {'Code': 'ConditionalCheckFailedException'}}


class Jobs:
    def __init__(self):
        self.item = None
        self.puts = []
        self.race = None
    def get_item(self, **kw):
        assert kw['ConsistentRead'] is True
        return {'Item': copy.deepcopy(self.item)} if self.item else {}
    def put_item(self, **kw):
        self.puts.append(copy.deepcopy(kw))
        assert kw['ConditionExpression'] == 'attribute_not_exists(pk)'
        if self.race:
            self.item = copy.deepcopy(self.race)
            raise ConditionalFailure()
        if self.item:
            raise ConditionalFailure()
        self.item = copy.deepcopy(kw['Item'])


class BackendTests(unittest.TestCase):
    def setUp(self):
        self.api = BackendMetadata(CONFIG, clock=lambda: NOW)
    def test_valid_response_is_allowlisted(self):
        body = metadata() | {'owner': 'wrong', 'progress': {'completed_step': 999}, 'recovery_id': 'wrong'}
        result = self.api.validate(body, EVENT)
        self.assertNotIn('progress', result)
        self.assertNotIn('owner', result)
        self.assertNotIn('recovery_id', result)
    def test_invalid_identity_original_placement_and_time_rejected(self):
        cases = [dict(schema_version=True), dict(job_id='other'), dict(generation=True),
                 dict(generation=2), dict(recovery_eligible=False), dict(cluster='eks-b'),
                 dict(gpu_count=True), dict(gpu_count=5), dict(active_nodes=[]),
                 dict(active_nodes=['n1', 'n1'], gpu_count=2), dict(active_nodes=['wrong']),
                 dict(placement_generation=2), dict(jobset_name='job-other-g0001'),
                 dict(jobset_uid=''), dict(jobset_uri='s3://bucket/code/other/jobset.json'),
                 dict(jobset_uri='https://example.test/jobset.json'), dict(jobset_sha256='config-hash'),
                 dict(observed_at=NOW-301), dict(observed_at=NOW+31), dict(observed_at=True),
                 dict(allow_restart_from_scratch='yes')]
        for update in cases:
            with self.subTest(update=update), self.assertRaises(RecoveryBlocked):
                self.api.validate(metadata() | update, EVENT)
    def test_disabled_default_and_bad_configs(self):
        self.assertFalse(metadata_config()['enabled'])
        for update in [dict(base_url='http://backend'), dict(base_url='https://user:pass@backend'),
                       dict(base_url='https://backend/?token=x'), dict(base_url='https://backend/path'),
                       dict(enabled='yes'), dict(timeout_seconds=100), dict(max_age_seconds=True),
                       dict(token_env='bad-name'), dict(unknown=True)]:
            with self.subTest(update=update), self.assertRaises(ValueError):
                metadata_config(CONFIG | update)
    def test_request_uses_service_auth_and_generation_without_redirects(self):
        response = Mock(status=200)
        response.read.return_value = json.dumps(metadata()).encode()
        self.api.opener.open = Mock(return_value=Mock(__enter__=Mock(return_value=response),
                                                     __exit__=Mock(return_value=False)))
        with patch.dict('os.environ', BACKEND_RECOVERY_API_TOKEN='test-only-token'):
            self.api.fetch(EVENT)
        request = self.api.opener.open.call_args.args[0]
        self.assertEqual(request.full_url, 'https://backend.example.test/internal/v1/recovery/jobs/j1?generation=1')
        self.assertEqual(request.get_header('Authorization'), 'Bearer test-only-token')
        self.assertEqual(self.api.opener.open.call_args.kwargs['timeout'], 5)
        self.assertIsNone(NoRedirect().redirect_request(None, None, 302, '', {}, 'https://other/'))
    def test_missing_credentials_never_calls_network(self):
        self.api.opener.open = Mock()
        with patch.dict('os.environ', {}, clear=True), self.assertRaises(RecoveryBlocked):
            self.api.fetch(EVENT)
        self.api.opener.open.assert_not_called()
    def test_invalid_json_and_oversize_rejected(self):
        for raw in (b'not-json', b'x'*65537):
            response = Mock(status=200)
            response.read.return_value = raw
            self.api.opener.open = Mock(return_value=Mock(__enter__=Mock(return_value=response),
                                                         __exit__=Mock(return_value=False)))
            with patch.dict('os.environ', BACKEND_RECOVERY_API_TOKEN='test'), self.assertRaises(RecoveryBlocked):
                self.api.fetch(EVENT)


class RegistrationTests(unittest.TestCase):
    def setUp(self):
        self.table, self.fence = Jobs(), FencingTable()
        self.api = Mock()
        self.api.fetch.return_value = BackendMetadata(CONFIG, clock=lambda: NOW).validate(metadata(), EVENT)
        self.store = Store(self.table, self.fence, metadata_client=self.api, clock=lambda: NOW)
    def test_fault_registers_once_and_then_existing_claim_can_proceed(self):
        self.assertTrue(self.store.ensure_job(EVENT))
        self.assertEqual(self.fence.calls, [])
        self.assertEqual(self.table.item['pk'], 'job#j1')
        self.assertEqual(self.table.item['registration_event_id'], 'e1')
        self.assertTrue(self.store.ensure_job(EVENT))
        self.api.fetch.assert_called_once()
        self.assertEqual(len(self.table.puts), 1)
        self.store.claim(EVENT)
        self.assertEqual(self.fence.item['recovery_phase'], 'CLAIMED')
    def test_existing_job_never_overwritten_or_refetched(self):
        self.table.item = {'pk': 'job#j1', 'progress': {'completed_step': 123}}
        before = copy.deepcopy(self.table.item)
        self.store.ensure_job(EVENT)
        self.assertEqual(self.table.item, before)
        self.api.fetch.assert_not_called()
    def test_stale_generation_no_fetch_or_registration(self):
        self.fence.item['generation'] = 2
        self.assertFalse(self.store.ensure_job(EVENT))
        self.api.fetch.assert_not_called()
        self.assertFalse(self.table.puts)
    def test_lookup_failure_never_creates_or_changes_fencing(self):
        self.api.fetch.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError): self.store.ensure_job(EVENT)
        self.assertFalse(self.table.puts)
        self.assertEqual(self.fence.calls, [])
    def test_generation_changed_during_lookup_is_not_registered(self):
        def fetch(_):
            self.fence.item['generation'] = 2
            return self.api.fetch.return_value
        self.api.fetch.side_effect = fetch
        self.assertFalse(self.store.ensure_job(EVENT))
        self.assertFalse(self.table.puts)
    def test_other_recovery_or_foreign_owner_blocks_registration(self):
        for change in ({'recovery_id':'other'}, {'owner':'eks-b'}, {'new_jobset_uid':'other-uid'}):
            with self.subTest(change=change):
                self.fence.item = dict(job_id='j1', generation=1, owner='eks-a') | change
                with self.assertRaises(RecoveryBlocked): self.store.ensure_job(EVENT)
                self.assertFalse(self.table.puts)
    def test_concurrent_equal_create_preserves_winner_progress(self):
        self.table.race = self.api.fetch.return_value | {'progress': {'completed_step': 44}}
        self.assertTrue(self.store.ensure_job(EVENT))
        self.assertEqual(self.table.item['progress']['completed_step'], 44)
    def test_concurrent_conflicting_original_blocks(self):
        self.table.race = self.api.fetch.return_value | {'jobset_sha256':'b'*64}
        with self.assertRaises(RecoveryBlocked): self.store.ensure_job(EVENT)
    def test_lost_put_ack_reloads_existing_without_second_api_call(self):
        original = self.table.put_item
        def lost(**kw):
            original(**kw)
            raise TimeoutError()
        self.table.put_item = lost
        with self.assertRaises(TimeoutError): self.store.ensure_job(EVENT)
        self.assertTrue(self.store.ensure_job(EVENT))
        self.api.fetch.assert_called_once()


class WorkflowRegistrationTests(unittest.TestCase):
    def setUp(self):
        from test_workflow import MemoryStore, Requests, EVENT as FAULT
        self.store, self.executor = MemoryStore(), Requests()
        self.event = FAULT
        self.store.ensure_job = Mock(return_value=True)
        self.flow = Workflow(self.store, self.executor, True)
        self.flow.accept(self.event)
    def test_registration_precedes_generation_and_submit(self):
        def register(_):
            self.assertEqual(self.store.job['generation'], 1)
            self.assertEqual(self.executor.calls, [])
            return True
        self.store.ensure_job.side_effect = register
        result = self.flow.tick(self.store.get('e1'))
        self.assertEqual(result['state'], 'REQUESTED')
        self.store.ensure_job.assert_called_once()
    def test_registration_validation_failure_holds_without_recovery(self):
        self.store.ensure_job.side_effect = RecoveryBlocked('backend metadata mismatch')
        self.assertEqual(self.flow.tick(self.store.get('e1'))['state'], 'HOLD')
        self.assertEqual(self.executor.calls, [])
        self.assertEqual(self.store.job['generation'], 1)
    def test_transient_api_failures_use_existing_three_attempt_budget(self):
        self.store.ensure_job.side_effect = TimeoutError()
        runner = Runner(self.store, self.flow, None, 'result')
        for now in (1, 6, 11):
            result = runner.operation(self.store.get('e1'), now, False)
        self.assertEqual(result['state'], 'HOLD')
        self.assertEqual(self.store.ensure_job.call_count, 3)
        self.assertEqual(self.executor.calls, [])
    def test_permanent_http_failure_not_retried(self):
        self.store.ensure_job.side_effect = HTTPError('https://backend', 404, '', {}, io.BytesIO())
        runner = Runner(self.store, self.flow, None, 'result')
        result = runner.operation(self.store.get('e1'), 1, False)
        self.assertEqual(result['state'], 'HOLD')
        self.assertEqual(self.store.ensure_job.call_count, 1)
    def test_observe_hold_and_monitoring_outage_do_not_register(self):
        self.flow.execute = False
        self.flow.tick(self.store.get('e1'))
        self.flow.execute = True
        self.store.events['e1']['state'] = 'RECEIVED'
        self.flow.tick(self.store.get('e1'), monitoring_hold=True)
        self.store.events['e1']['event']['suggested_action'] = 'hold'
        self.flow.tick(self.store.get('e1'))
        self.store.ensure_job.assert_not_called()
    def test_stale_registration_does_not_submit(self):
        self.store.ensure_job.return_value = False
        self.assertEqual(self.flow.tick(self.store.get('e1'))['state'], 'STALE')
        self.assertEqual(self.executor.calls, [])
