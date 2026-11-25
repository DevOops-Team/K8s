"""GPU-276: recovery job metadata from job.lifecycle (Backend job.submitted, job-watcher job.placement)."""
import json
import unittest
from unittest.mock import Mock
from integrations.dynamodb import RecoveryBlocked, Store
from integrations.job_registration import JobRegistry, actual_placement, submitted_original
from integrations.kafka import JOB_LIFECYCLE_TOPIC, accept_message

SHA = 'a' * 64


def submitted(**payload):
    body = dict(jobset_name='job-abc-g0001', gpus=2, strategy='fsdp', queue='heavy',
                jobset_sha256=SHA, jobset_uri='s3://ckpt/code/abc/jobset.json')
    body.update(payload)
    return dict(event_id='submitted-abc-g0001', schema_version=1, type='job.submitted',
                occurred_at='2026-11-25T01:00:00.000Z', source='backend', job_id='abc',
                generation=1, payload={k: v for k, v in body.items() if v is not None})


def placement(generation=1, **payload):
    body = dict(jobset_name=f'job-abc-g{generation:04d}', jobset_uid='uid-1', cluster='eks-a',
                placement_generation=generation, placement_cluster='eks-a',
                active_nodes=['n2', 'n1'], observed_at=1000, placement_version=1)
    body.update(payload)
    return dict(event_id=f'job-abc-g{generation:04d}-placement-x', schema_version=1, type='job.placement',
                occurred_at='2026-11-25T01:00:00Z', source='job-watcher', cluster='eks-a', node=None,
                job_id='abc', generation=generation, payload={k: v for k, v in body.items() if v is not None})


class ConditionalFailure(Exception):
    response = {'Error': {'Code': 'ConditionalCheckFailedException'}}


class ParseTests(unittest.TestCase):
    def test_backend_submitted_carries_the_original(self):
        self.assertEqual(submitted_original(submitted()), dict(
            job_id='abc', jobset_uri='s3://ckpt/code/abc/jobset.json', jobset_sha256=SHA,
            gpu_count=2, event_id='submitted-abc-g0001'))

    def test_submitted_without_hash_or_from_watcher_is_not_registration(self):
        self.assertIsNone(submitted_original(submitted(jobset_sha256=None, jobset_uri=None)))
        self.assertIsNone(submitted_original(dict(submitted(), source='job-watcher')))

    def test_submitted_rejects_non_canonical_original(self):
        for bad in (dict(jobset_uri='s3://ckpt/code/other/jobset.json'), dict(jobset_uri='https://x/code/abc/jobset.json'),
                    dict(jobset_sha256='A' * 64), dict(gpus=5), dict(gpus='2'), dict(jobset_name='job-abc-g0002')):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                submitted_original(submitted(**bad))

    def test_placement_fields_and_sorted_nodes(self):
        fields = actual_placement(placement())
        self.assertEqual(fields['active_nodes'], ['n1', 'n2'])
        self.assertEqual((fields['cluster'], fields['generation'], fields['jobset_uid'], fields['recovery_id']),
                         ('eks-a', 1, 'uid-1', None))

    def test_placement_rejects_inconsistent_or_incomplete_messages(self):
        for bad in (dict(placement_generation=2), dict(placement_cluster='eks-z'), dict(active_nodes=[]),
                    dict(active_nodes=['n1', 'n1']), dict(jobset_uid=None), dict(observed_at='1000'),
                    dict(jobset_name='job-abc-g0009')):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                actual_placement(placement(**bad))
        with self.assertRaises(ValueError):
            actual_placement(dict(placement(), source='backend'))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.table = Mock()
        self.store = Store(self.table, Mock(), clock=lambda: 50)

    def test_original_is_registered_once_and_same_replay_is_accepted(self):
        self.assertTrue(self.store.register_original('abc', 's3://ckpt/code/abc/jobset.json', SHA, 2, 'e'))
        kwargs = self.table.update_item.call_args.kwargs
        self.assertEqual(kwargs['Key'], {'pk': 'job#abc'})
        self.assertIn('attribute_not_exists(jobset_sha256)', kwargs['ConditionExpression'])

    def test_different_original_latches_conflict_and_context_holds(self):
        self.table.update_item.side_effect = [ConditionalFailure(), {}]
        self.assertFalse(self.store.register_original('abc', 's3://ckpt/code/abc/jobset.json', 'b' * 64, 2, 'e2'))
        self.assertEqual(self.table.update_item.call_args.kwargs['UpdateExpression'],
                         'SET registration_conflict = :event')
        self.table.get_item.return_value = {'Item': dict(pk='job#abc', registration_conflict='e2', cluster='eks-a',
            gpu_count=2, active_nodes=['n1'], jobset_uri='s3://ckpt/code/abc/jobset.json', jobset_sha256=SHA)}
        with self.assertRaisesRegex(RecoveryBlocked, 'conflicting'):
            self.store.context(dict(job_id='abc', cluster='eks-a', node='n1'))

    def test_new_generation_placement_clears_previous_recovery_evidence(self):
        self.assertTrue(self.store.register_placement(**actual_placement(placement(3))))
        kwargs = self.table.update_item.call_args.kwargs
        self.assertIn('REMOVE recovery_id, progress, placement_evidence', kwargs['UpdateExpression'])
        self.assertEqual(kwargs['ExpressionAttributeValues'][':nodes'], ['n1', 'n2'])

    def test_same_generation_move_then_stale_placement(self):
        self.table.update_item.side_effect = [ConditionalFailure(), {}]
        self.assertTrue(self.store.register_placement(**actual_placement(placement())))
        self.assertIn('placement_observed_at <= :observed',
                      self.table.update_item.call_args.kwargs['ConditionExpression'])
        self.table.update_item.side_effect = [ConditionalFailure(), ConditionalFailure()]
        self.assertFalse(self.store.register_placement(**actual_placement(placement())))

    def test_infrastructure_errors_propagate(self):
        self.table.update_item.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            self.store.register_placement(**actual_placement(placement()))

    def test_registered_job_is_usable_by_fault_matching(self):
        self.table.get_item.return_value = {'Item': dict(pk='job#abc', kind='job', cluster='eks-a', gpu_count=2,
            active_nodes=['n1', 'n2'], jobset_uri='s3://ckpt/code/abc/jobset.json', jobset_sha256=SHA,
            placement_generation=1, jobset_uid='uid-1')}
        self.assertEqual(self.store.context(dict(job_id='abc', cluster='eks-a', node='n2'))['gpu_count'], 2)
        self.assertIsNone(self.store.context(dict(job_id='abc', cluster='eks-b', node='n2')))


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.store, self.writer = Mock(), Mock()

    def test_routes_registration_messages(self):
        registry = JobRegistry(self.store, self.writer)
        self.assertEqual(registry.ingest(submitted()), 'registered')
        self.store.register_original.assert_called_once()
        self.assertEqual(registry.ingest(placement()), 'registered')
        self.store.register_placement.assert_called_once()
        self.writer.write.assert_not_called()

    def test_bad_content_is_logged_not_raised(self):
        registry = JobRegistry(self.store, self.writer)
        self.assertEqual(registry.ingest(placement(active_nodes=[])), 'rejected')
        self.store.register_placement.assert_not_called()

    def test_recovery_placement_needs_trusted_producer(self):
        message = placement(2, recovery_id='r1')
        self.assertEqual(JobRegistry(self.store, self.writer).ingest(message), 'ignored')
        self.writer.write.assert_not_called()
        self.store.register_placement.assert_not_called()
        trusted = JobRegistry(self.store, self.writer, dict(placement_producers=['scheduling', 'job-watcher']))
        self.assertEqual(trusted.ingest(message), 'evidence')
        body = self.writer.write.call_args.args[0]
        self.assertEqual(self.writer.write.call_args.kwargs, {'authenticated_producer': 'job-watcher'})
        self.assertEqual((body['kind'], body['recovery_id'], body['generation'], body['jobset_uid']),
                         ('placement', 'r1', 2, 'uid-1'))
        self.assertNotIn('producer', body)

    def test_recovery_placement_feeds_completion_evidence(self):
        from recovery.evidence import EvidenceWriter
        store = Mock()
        store.fencing_record.return_value = dict(generation=2, recovery_id='r1', new_jobset_uid='uid-1',
                                                 recovery_phase='RESUMING', owner='eks-a')
        store.write_evidence.return_value = True
        policy = dict(placement_producers=['job-watcher'])
        writer = EvidenceWriter(store, dict(policy), clock=lambda: 1010)
        self.assertEqual(JobRegistry(store, writer, policy).ingest(placement(2, recovery_id='r1')), 'evidence')
        written = store.write_evidence.call_args.args[0]
        self.assertEqual((written['producer'], written['received_at']), ('job-watcher', 1010))
        # A recovery placement for another JobSet is not evidence.
        bad = placement(2, recovery_id='r1', jobset_uid='other')
        self.assertEqual(JobRegistry(store, writer, policy).ingest(bad), 'rejected')


class IntakeTests(unittest.TestCase):
    def message(self, envelope):
        msg = Mock()
        msg.value.return_value = json.dumps(envelope).encode()
        msg.topic.return_value = JOB_LIFECYCLE_TOPIC
        msg.headers.return_value = []
        return msg

    def test_registration_is_committed_without_a_fault(self):
        consumer, workflow, registry = Mock(), Mock(), Mock()
        msg = self.message(submitted())
        self.assertFalse(accept_message(consumer, workflow, msg, registry))
        registry.ingest.assert_called_once()
        workflow.accept.assert_not_called()
        consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_store_outage_leaves_offset_uncommitted(self):
        consumer, registry = Mock(), Mock()
        registry.ingest.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            accept_message(consumer, Mock(), self.message(placement()), registry)
        consumer.commit.assert_not_called()


class ConsumerPathTests(unittest.TestCase):
    """Review P2: malformed active_nodes through the real Kafka consumer path must commit, not wedge."""
    def run_message(self, envelope):
        table = Mock()
        registry = JobRegistry(Store(table, Mock()), Mock())
        consumer = Mock()
        msg = IntakeTests.message(None, envelope)
        self.assertFalse(accept_message(consumer, Mock(), msg, registry))
        consumer.commit.assert_called_once_with(message=msg, asynchronous=False)
        return table

    def test_unhashable_active_nodes_are_rejected_and_committed(self):
        for nodes in ([{'name': 'n1'}, 'n2'], [['n1']], ['n1', None], 'n1'):
            with self.subTest(nodes=nodes):
                self.run_message(placement(active_nodes=nodes)).update_item.assert_not_called()

    def test_wrong_payload_shapes_are_rejected_and_committed(self):
        for envelope in (dict(placement(), payload=['x']), dict(placement(), generation='1'),
                         dict(submitted(), payload=dict(submitted()['payload'], gpus=[2]))):
            with self.subTest(envelope=envelope):
                self.run_message(envelope).update_item.assert_not_called()

    def test_valid_placement_reaches_dynamodb(self):
        table = self.run_message(placement())
        self.assertEqual(table.update_item.call_args.kwargs['Key'], {'pk': 'job#abc'})
