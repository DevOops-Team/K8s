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
