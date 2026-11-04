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
    def test_dynamo_claim_cas_and_replay(self):
        class Table:
            item=dict(pk='job#j1',generation=1,cluster='eks-a',gpu_count=4,
                      active_nodes=['n1'],checkpoint_uri='s3://bucket/checkpoint')
            count=0
            def get_item(self,**kw): return {'Item':self.item.copy()}
            def update_item(self,**kw):
                self.count+=1
                assert 'generation = :old' in kw['ConditionExpression']
                assert 'contains(active_nodes, :node)' in kw['ConditionExpression']
                assert kw['ExpressionAttributeValues'][':old']==1
                self.item.update(generation=2,recovery_id='e1')
                return {'Attributes':self.item.copy()}
        table=Table();store=Store(table)
        event=dict(event_id='e1',job_id='j1',cluster='eks-a',generation=1,node='n1')
        self.assertEqual(store.claim(event)['generation'],2)
        self.assertEqual(store.claim(event)['generation'],2)
        self.assertEqual(table.count,1)
    def test_unrelated_node_cannot_claim(self):
        class Table:
            def get_item(self,**kw):
                return {'Item':dict(generation=1,cluster='eks-a',active_nodes=['n1'])}
            def update_item(self,**kw): raise AssertionError('must not write')
        self.assertIsNone(Store(Table()).claim(dict(event_id='e1',job_id='j1',cluster='eks-a',generation=1,node='other')))
