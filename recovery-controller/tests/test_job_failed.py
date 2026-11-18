import copy
import json
import unittest
from unittest.mock import Mock
from integrations.kafka import accept_message
from recovery.workflow import Workflow
from recovery.retry_policy import Runner
from test_workflow import MemoryStore, Requests


class FailedStore(MemoryStore):
    def accept(self, event, digest):
        super().accept(event, digest)
        self.events[event['event_id']].setdefault('created_at', 1000)
    def records(self):
        return list(copy.deepcopy(self.events).values())


class JobFailedTests(unittest.TestCase):
    def setUp(self):
        self.store = FailedStore()
        self.executor = Requests()
        self.workflow = Workflow(self.store, self.executor, execute=True)
        self.runner = Runner(self.store, self.workflow, Mock(), 'recovery.action')
        self.consumer, self.message = Mock(), Mock()
        self.message.topic.return_value = 'job.lifecycle'
        self.message.headers.return_value = []
        self.envelope = dict(schema_version=1, type='job.failed', event_id='job-j1-g0001-failed',
                             job_id='j1', generation=1, cluster='eks-a', node=None,
                             payload=dict(jobset_name='j1', cluster='eks-a'))
        self.message.value.return_value = json.dumps(self.envelope).encode()
        self.assertFalse(accept_message(self.consumer, self.workflow, self.message))
        self.rid = self.envelope['event_id']

    def tick(self, now, monitoring_hold=False):
        return self.runner.operation(self.store.get(self.rid), now, monitoring_hold)

    def test_wait_then_hold_without_claim_or_restart(self):
        self.assertEqual(self.tick(1000)['job_failed_deadline'], 1300)
        self.assertEqual(self.tick(1299)['state'], 'RECEIVED')
        result = self.tick(1300, monitoring_hold=True)
        self.assertEqual(result['state'], 'HOLD')
        self.assertEqual(result['reason'], 'unclassified_job_failure')
        self.assertEqual(result['stopped_stage'], 'WAITING_FOR_FAULT')
        self.assertNotIn('retry_count', result)
        self.assertEqual(self.store.job['generation'], 1)
        self.assertEqual(self.executor.calls, [])
        self.assertEqual(self.tick(1400), result)

    def test_duplicate_and_restart_do_not_slide_deadline(self):
        self.tick(1000)
        accept_message(self.consumer, self.workflow, self.message)
        self.runner = Runner(self.store, self.workflow, Mock(), 'recovery.action')
        self.assertEqual(self.tick(1200)['job_failed_deadline'], 1300)
        self.assertEqual(self.tick(1300)['state'], 'HOLD')

    def test_matching_fault_before_or_after_failure_suppresses_hold(self):
        for component in (None, 'ckpt-init'):
            self.store.events['fault'] = dict(state='RECEIVED', event=dict(
                event_id='fault', job_id='j1', generation=1, cluster='eks-a', component=component))
            self.assertEqual(self.tick(1300)['state'], 'IGNORED')
            self.store.events[self.rid]['state'] = 'RECEIVED'

    def test_unrelated_job_generation_cluster_do_not_suppress(self):
        for changes in (dict(job_id='j2'), dict(generation=2), dict(cluster='eks-b')):
            source = dict(event_id='fault', job_id='j1', generation=1, cluster='eks-a')
            source.update(changes)
            self.store.events['fault'] = dict(state='RECEIVED', event=source)
            self.assertEqual(self.tick(1300)['state'], 'HOLD')
            self.store.events[self.rid]['state'] = 'RECEIVED'

    def test_other_job_failed_is_not_causal_evidence(self):
        source = dict(self.store.events[self.rid]['event'], event_id='other-failed')
        self.store.events['other-failed'] = dict(state='RECEIVED', event=source)
        self.assertEqual(self.tick(1300)['state'], 'HOLD')

    def test_fencing_superseded_or_claimed_suppresses(self):
        self.store.fence = dict(generation=2)
        self.assertEqual(self.tick(1000)['state'], 'STALE')
        self.store.events[self.rid]['state'] = 'RECEIVED'
        self.store.fence = dict(generation=1, recovery_id='fault', recovery_from_gen=1, owner='eks-a')
        self.assertEqual(self.tick(1000)['state'], 'IGNORED')

    def test_failed_reads_do_not_turn_into_unclassified_hold(self):
        self.store.records = Mock(side_effect=OSError('database unreachable'))
        with self.assertRaises(OSError):
            self.tick(1300)
        self.assertEqual(self.store.get(self.rid)['state'], 'RECEIVED')

    def test_observe_mode_never_holds(self):
        self.workflow.execute = False
        self.assertEqual(self.tick(1300)['state'], 'OBSERVED')

    def test_late_fault_does_not_automatically_release_hold(self):
        self.tick(1300)
        self.store.events['fault'] = dict(state='RECEIVED', event=dict(
            event_id='fault', job_id='j1', generation=1, cluster='eks-a'))
        self.assertEqual(self.tick(1400)['state'], 'HOLD')

    def test_hold_uses_existing_result_outbox(self):
        result = self.tick(1300)
        producer = self.runner.producer
        producer.flush.return_value = 0
        producer.produce.side_effect = lambda *a, **kw: kw['on_delivery'](None, None)
        self.runner.deliver(result, 1300)
        envelope = json.loads(producer.produce.call_args.kwargs['value'])
        self.assertEqual(envelope['type'], 'recovery.held')
        self.assertEqual(envelope['payload']['reason'], 'unclassified_job_failure')
        self.assertEqual(envelope['payload']['stage'], 'WAITING_FOR_FAULT')
        self.assertTrue(self.store.get(self.rid)['published'])

    def test_fault_scan_second_page_and_db_commit_order(self):
        from integrations.dynamodb import Store
        table = Mock()
        table.scan.side_effect = [dict(Items=[], LastEvaluatedKey={'pk': 'cursor'}),
                                 dict(Items=[dict(kind='event', event=dict(event_id='fault',
                                      job_id='j1', generation=1, cluster='eks-a'), state='RECEIVED')])]
        self.store.records = lambda: Store(table, Mock()).records()
        self.assertEqual(self.tick(1300)['state'], 'IGNORED')
        self.assertEqual(table.scan.call_args.kwargs,
                         dict(ConsistentRead=True, ExclusiveStartKey={'pk': 'cursor'}))

    def test_storage_failure_never_commits_job_failed(self):
        self.consumer.reset_mock()
        self.workflow.store.accept = Mock(side_effect=OSError('ddb unavailable'))
        with self.assertRaises(OSError):
            accept_message(self.consumer, self.workflow, self.message)
        self.consumer.commit.assert_not_called()

    def test_wait_duration_configurable(self):
        self.runner.policy['job_failed_wait_seconds'] = 60
        self.assertEqual(self.tick(1059)['state'], 'RECEIVED')
        self.assertEqual(self.tick(1060)['state'], 'HOLD')
