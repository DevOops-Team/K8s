import copy
import json
import unittest
from decimal import Decimal
from unittest.mock import Mock

from integrations.kafka import accept_message
from recovery.workflow import Workflow
from test_init_failure import ConnectedStore
from test_workflow import MemoryStore, Requests


def event(event_id='site-1', action='fence_evacuate', time='2026-11-24T06:00:00Z', **changes):
    body = dict(event_id=event_id, cluster='eks-a', job_id='j1', generation=1,
                node=None, category='site', source='site', collector_healthy=False,
                suggested_action=action, dedup_key='eks-a/j1|site|1795500000',
                classification_type='fault.detected', classification_occurred_at=time)
    body.update(changes)
    return body


def revision(**changes):
    return event('revision-1', 'hold', '2026-11-24T06:00:01Z',
                 classification_type='fault.reclassified', **changes)


class InboxStore(MemoryStore):
    def records(self):
        yield from copy.deepcopy(list(self.events.values()))


class SiteTests(unittest.TestCase):
    def setUp(self):
        self.store, self.requests = InboxStore(), Requests()
        self.flow = Workflow(self.store, self.requests, execute=True)

    def accept(self, body):
        self.flow.accept(body)
        return self.store.get(body['event_id'])

    def test_unknown_cluster_is_saved_committed_and_next_message_recovers(self):
        consumer, message = Mock(), Mock()
        message.topic.return_value = 'gpu.fault'
        message.headers.return_value = []
        body = event(cluster='unexpected-alias', node=None)
        self.store.ensure_job = Mock(return_value=True)
        message.value.return_value = json.dumps(body).encode()
        accept_message(consumer, self.flow, message)
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)
        result = self.flow.tick(self.store.get('site-1'))
        self.assertEqual((result['state'], result['reason']), ('HOLD', 'unknown_cluster'))
        self.assertEqual(self.requests.calls, [])
        self.assertEqual(self.store.job['generation'], 1)
        self.store.ensure_job.assert_not_called()
        message.value.return_value = json.dumps(event('valid-next')).encode()
        accept_message(consumer, self.flow, message)
        self.assertEqual(self.flow.tick(self.store.get('valid-next'))['state'], 'REQUESTED')
        self.assertEqual(self.requests.calls[0][1], 'evacuateCluster')

    def test_storage_failure_does_not_commit_unknown_cluster(self):
        consumer, message = Mock(), Mock()
        message.topic.return_value = 'gpu.fault'
        message.headers.return_value = []
        message.value.return_value = json.dumps(event(cluster='other')).encode()
        self.store.accept = Mock(side_effect=OSError('DynamoDB unavailable'))
        with self.assertRaises(OSError):
            accept_message(consumer, self.flow, message)
        consumer.commit.assert_not_called()

    def test_unresolved_site_is_hold_without_claim(self):
        result = self.flow.tick(self.accept(event(job_id=None, generation=None)))
        self.assertEqual(result['state'], 'HOLD')
        self.assertEqual(self.store.job['generation'], 1)
        self.assertEqual(self.requests.calls, [])

    def test_envelope_metadata_is_persisted_with_payload_and_rule_version(self):
        consumer, message = Mock(), Mock()
        body = event()
        body.pop('classification_type')
        body.pop('classification_occurred_at')
        message.topic.return_value = 'gpu.fault'
        message.headers.return_value = [('rule_version', b'abc1234')]
        message.value.return_value = json.dumps(dict(type='fault.reclassified',
            occurred_at='2026-11-24T06:01:00Z', payload=dict(body, suggested_action='hold'))).encode()
        accept_message(consumer, self.flow, message)
        stored = self.store.get('site-1')['event']
        self.assertEqual(stored['classification_type'], 'fault.reclassified')
        self.assertEqual(stored['classification_occurred_at'], '2026-11-24T06:01:00Z')
        self.assertEqual(stored['rule_version'], 'abc1234')
        consumer.commit.assert_called_once()

    def test_late_hold_is_audited_without_changing_active_evacuation(self):
        active = self.flow.tick(self.accept(event()))
        original = self.store.get('site-1')
        fence = copy.deepcopy(self.store.fence)
        self.assertEqual(active['state'], 'REQUESTED')
        revised = self.flow.tick(self.accept(revision()))
        self.assertEqual(revised['state'], 'IGNORED')
        self.assertEqual(revised['related_event_id'], 'site-1')
        self.assertEqual(revised['recommended_action'], 'HOLD')
        self.assertEqual(self.store.get('site-1'), original)
        self.assertEqual(self.store.fence, fence)
        self.flow.tick(self.store.get('site-1'))
        self.assertEqual(len(self.requests.calls), 1)
        self.assertEqual(self.store.job['generation'], 2)
        self.store.fence.update(recovery_phase='NEW_RUN_SUBMITTED', owner='eks-b')
        self.store.nodes.update(cluster='eks-b', recovery_id='site-1')
        self.store.nodes['progress'].update(cluster='eks-b', recovery_id='site-1')
        self.assertEqual(self.flow.tick(self.store.get('site-1'))['state'], 'RECOVERED')
        self.assertEqual(len(self.requests.calls), 1)

    def test_other_malformed_fields_still_do_not_commit(self):
        consumer, message = Mock(), Mock()
        message.topic.return_value = 'gpu.fault'
        message.headers.return_value = []
        for change in (dict(cluster=''), dict(event_id=''), dict(generation='1'), dict(job_id=3)):
            with self.subTest(change=change):
                message.value.return_value = json.dumps(event(**change)).encode()
                with self.assertRaises(ValueError):
                    accept_message(consumer, self.flow, message)
                consumer.commit.assert_not_called()

    def test_hold_before_claim_prevents_original_evacuation(self):
        original = self.accept(event())
        held = self.flow.tick(self.accept(revision()))
        self.assertEqual(held['state'], 'HOLD')
        result = self.flow.tick(original)
        self.assertEqual(result['reason'], 'classification_hold_before_claim')
        self.assertEqual(self.store.job['generation'], 1)
        self.assertEqual(self.requests.calls, [])

    def test_dynamodb_decimal_generations_are_correlated(self):
        self.accept(event())
        self.accept(revision())
        for record in self.store.events.values():
            record['event']['generation'] = Decimal('1')
        result = self.flow.tick(self.store.get('site-1'))
        self.assertEqual(result['reason'], 'classification_hold_before_claim')
        self.assertEqual(self.requests.calls, [])

    def test_later_action_can_follow_hold_without_consuming_dedup_key(self):
        self.flow.tick(self.accept(revision()))
        newer = event('new-decision', time='2026-11-24T06:00:02Z',
                      classification_type='fault.reclassified')
        self.assertEqual(self.flow.tick(self.accept(newer))['state'], 'REQUESTED')
        self.assertEqual(len(self.requests.calls), 1)

    def test_older_hold_delivered_last_does_not_block_newer_action(self):
        original = self.accept(event(time='2026-11-24T06:00:02Z'))
        self.flow.tick(self.accept(revision()))
        self.assertEqual(self.flow.tick(original)['state'], 'REQUESTED')

    def test_unresolved_order_is_hold_and_equal_times_prefer_hold(self):
        for value in (None, 'invalid', '2026-11-24T06:00:01', '2026-11-24T06:00:00Z'):
            with self.subTest(value=value):
                self.setUp()
                original = self.accept(event())
                self.accept(revision(classification_occurred_at=value))
                self.assertEqual(self.flow.tick(original)['state'], 'HOLD')
                self.assertEqual(self.requests.calls, [])

    def test_identity_boundaries_do_not_cross_jobs_clusters_generations_or_keys(self):
        for changes in (dict(job_id='j2'), dict(cluster='eks-b'), dict(generation=2),
                        dict(dedup_key='different'), dict(job_id=None, generation=None)):
            with self.subTest(changes=changes):
                self.setUp()
                original = self.accept(event())
                self.accept(revision(**changes))
                self.assertEqual(self.flow.tick(original)['state'], 'REQUESTED')

    def test_duplicate_reclassification_does_not_touch_original_or_create_request(self):
        self.flow.tick(self.accept(event()))
        self.flow.tick(self.accept(revision()))
        before = copy.deepcopy(self.store.events)
        self.accept(revision())
        self.flow.tick(self.store.get('revision-1'))
        self.assertEqual(self.store.events, before)
        self.assertEqual(len(self.requests.calls), 1)

    def test_new_envelope_fields_do_not_collide_with_old_inbox_payload(self):
        old = event()
        old.pop('classification_type')
        old.pop('classification_occurred_at')
        self.accept(old)
        before = self.store.get('site-1')
        self.accept(event())
        self.assertEqual(self.store.get('site-1'), before)
        with self.assertRaises(ValueError):
            self.accept(dict(event(), suggested_action='hold'))

    def test_incomplete_correlation_scan_must_not_start_recovery(self):
        self.accept(event())
        def broken_scan():
            yield self.store.get('site-1')
            raise OSError('second page unavailable')
        self.store.records = broken_scan
        with self.assertRaises(OSError):
            self.flow.tick(self.store.get('site-1'))
        self.assertEqual(self.requests.calls, [])
        self.assertEqual(self.store.job['generation'], 1)

    def test_claimed_and_crash_before_state_save_do_not_rollback_on_hold(self):
        for lose_state in (False, True):
            with self.subTest(lose_state=lose_state):
                store = ConnectedStore()
                store.records = lambda: iter(copy.deepcopy(list(store.events.values())))
                requests = Requests()
                flow = Workflow(store, requests, execute=True)
                flow.accept(event())
                claimed = flow.tick(store.get('site-1'))
                self.assertEqual(claimed['state'], 'CLAIMED')
                if lose_state:
                    store.save('site-1', state='RECEIVED')
                fence = copy.deepcopy(store.fencing.item)
                flow.accept(revision())
                revised = flow.tick(store.get('revision-1'))
                self.assertEqual(revised['state'], 'IGNORED')
                self.assertEqual(store.fencing.item, fence)
                resumed = flow.tick(store.get('site-1'))
                self.assertEqual(resumed['state'], 'REQUESTED')
                self.assertEqual(store.fencing.item['generation'], 2)
                self.assertEqual(store.fencing.item['retry_count'], 1)
                self.assertEqual(len(requests.calls), 1)


if __name__ == '__main__':
    unittest.main()
