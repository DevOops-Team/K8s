import unittest
from runtime import accept_message
from adapters import Store

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
