import unittest
from integrations.kafka import accept_message
from integrations.dynamodb import Store

class InboxTests(unittest.TestCase):
    def test_save_before_commit(self):
        calls=[]
        class Message:
            def value(self): return b'{"event_id":"e1"}'
        class Workflow:
            def accept(self,e): calls.append('saved')
        class Consumer:
            def commit(self,**kw):
                self_async=kw['asynchronous']
                if self_async: raise AssertionError('commit must be synchronous')
                calls.append('committed')
        accept_message(Consumer(),Workflow(),Message())
        self.assertEqual(calls,['saved','committed'])
    def test_storage_failure_never_commits(self):
        class Message:
            def value(self): return b'{}'
        class Workflow:
            def accept(self,e): raise OSError('db unavailable')
        class Consumer:
            def commit(self,**kw): raise AssertionError('must not commit')
        with self.assertRaises(OSError): accept_message(Consumer(),Workflow(),Message())

    def test_envelope_and_rule_version_are_preserved(self):
        seen=[]
        class Message:
            def value(self):
                return b'{"occurred_at":"2026-11-09T00:00:00Z","payload":{"event_id":"e1"}}'
            def headers(self):
                return [('rule_version', b'abc1234')]
        class Workflow:
            def accept(self, event):
                seen.append(event)
        class Consumer:
            def commit(self, **kw):
                seen.append('committed')
        accept_message(Consumer(), Workflow(), Message())
        self.assertEqual(seen, [{'event_id':'e1','rule_version':'abc1234'}, 'committed'])

    def test_lifecycle_only_accepts_init_failures(self):
        import json
        from unittest.mock import Mock
        from recovery.init_failure import decision

        consumer, workflow, message = Mock(), Mock(), Mock()
        message.topic.return_value = 'job.lifecycle'
        message.headers.return_value = []
        envelope = dict(schema_version=1, type='job.progress', event_id='init-1', job_id='j1',
                        generation=2, cluster='eks-a')
        message.value.return_value = json.dumps(envelope).encode()
        self.assertFalse(accept_message(consumer, workflow, message))
        workflow.accept.assert_not_called()
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)

        envelope.update(type='job.init_failed', payload=dict(
            exit_code=7, reason='corrupt', pod_uid='pod', jobset_uid='js',
            checkpoint_gen=1, checkpoint_step=750))
        message.value.return_value = json.dumps(envelope).encode()
        self.assertFalse(accept_message(consumer, workflow, message))
        event = workflow.accept.call_args.args[0]
        self.assertEqual(event['event_id'], 'init-1')
        self.assertEqual(event['checkpoint'], {'generation': 1, 'step': 750})
        self.assertEqual(decision(event)['action'], 'RETRY_GROUP')
        self.assertEqual(consumer.commit.call_count, 2)

    def test_data33_payload_without_uids_is_durable_hold(self):
        import json
        from unittest.mock import Mock
        from recovery.workflow import Workflow
        from test_init_failure import ConnectedStore, Requests
        store, requests = ConnectedStore(), Requests()
        workflow = Workflow(store, requests, execute=True)
        message, consumer = Mock(), Mock()
        message.topic.return_value = 'job.lifecycle'
        message.headers.return_value = []
        message.value.return_value = json.dumps(dict(
            schema_version=1, type='job.init_failed', event_id='init-j1-g0001-r0-corrupt',
            job_id='j1', generation=1, cluster='eks-a', node='node-1',
            payload=dict(exit_code=7, reason='corrupt', rank=0,
                         checkpoint_gen=1, checkpoint_step=1000, detail={}))).encode()
        self.assertFalse(accept_message(consumer, workflow, message))
        consumer.commit.assert_called_once()
        record = store.events['init-j1-g0001-r0-corrupt']
        result = workflow.tick(record)
        self.assertEqual(result['state'], 'HOLD')
        self.assertEqual(result['failure_class'], 'INIT_IDENTITY_MISSING')
        self.assertEqual(result['event']['rank'], 0)
        self.assertEqual(store.adapter.fencing.calls, [])

    def test_cancelled_and_failed_do_not_initiate_recovery(self):
        import json
        from unittest.mock import Mock
        for kind in ('job.cancelled', 'job.stopped', 'job.failed', 'checkpoint.approved'):
            with self.subTest(kind=kind):
                workflow, consumer, message = Mock(), Mock(), Mock()
                message.topic.return_value = 'job.lifecycle'
                message.value.return_value = json.dumps(dict(type=kind)).encode()
                self.assertFalse(accept_message(consumer, workflow, message))
                workflow.accept.assert_not_called()
                consumer.commit.assert_called_once()
