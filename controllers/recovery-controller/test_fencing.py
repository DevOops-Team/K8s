import copy
import unittest
from adapters import Store, RecoveryBlocked

EVENT=dict(event_id='e1',job_id='j1',cluster='eks-a',node='n1',generation=1)

class RecoveryTable:
    def __init__(self):
        self.item=dict(pk='job#j1',cluster='eks-a',active_nodes=['n1'],gpu_count=1,
                       jobset_uri='s3://bucket/code/j1/jobset.json',jobset_sha256='a'*64)
        self.calls=[]
    def get_item(self,**kw):
        assert kw['Key']=={'pk':'job#j1'}
        return {'Item':copy.deepcopy(self.item)}
    def update_item(self,**kw): self.calls.append(kw);return {}

class FencingTable:
    def __init__(self):
        self.item=dict(job_id='j1',generation=1,owner='eks-a',approved_gen=1,approved_step=1000,heartbeat_at='original')
        self.calls=[]
    def get_item(self,**kw):
        assert kw['Key']=={'job_id':'j1'}
        assert kw['ConsistentRead'] is True
        return {'Item':copy.deepcopy(self.item)}
    def update_item(self,**kw):
        self.calls.append(kw)
        values=kw['ExpressionAttributeValues']
        if kw['UpdateExpression'].startswith('SET generation'):
            assert self.item['generation']==values[':old']
            assert self.item.get('owner',values[':source'])==values[':source']
            assert 'recovery_id' not in self.item
            self.item.update(generation=values[':new'],recovery_id=values[':rid'],
                             recovery_from_gen=values[':old'],recovery_to_gen=values[':new'])
            self.item.pop('owner',None)
        else:
            assert self.item['owner']==values[':target']
            for key in ['recovery_id','recovery_from_gen','recovery_to_gen']:self.item.pop(key,None)
        return {'Attributes':copy.deepcopy(self.item)}

class FencingTests(unittest.TestCase):
    def setUp(self):
        self.recovery=RecoveryTable();self.fence=FencingTable();self.store=Store(self.recovery,self.fence)
    def test_single_atomic_generation_advance_removes_owner(self):
        result=self.store.claim(EVENT)
        self.assertEqual(result['generation'],2)
        self.assertNotIn('owner',self.fence.item)
        call=self.fence.calls[0]
        self.assertIn('REMOVE #owner',call['UpdateExpression'])
        self.assertIn('generation = :old',call['ConditionExpression'])
        self.assertIn('#owner = :source',call['ConditionExpression'])
        self.assertEqual(self.fence.item['approved_gen'],1)
        self.assertEqual(self.fence.item['approved_step'],1000)
        self.assertEqual(self.fence.item['heartbeat_at'],'original')
        self.assertNotIn('checkpoint_uri',result)
    def test_replay_does_not_clear_new_owner(self):
        self.store.claim(EVENT);self.fence.item['owner']='eks-b'
        self.assertEqual(self.store.claim(EVENT)['generation'],2)
        self.assertEqual(self.fence.item['owner'],'eks-b')
        self.assertEqual(len(self.fence.calls),1)
    def test_owner_absent_can_advance(self):
        self.fence.item.pop('owner');self.store.claim(EVENT)
        self.assertEqual(self.fence.item['generation'],2)
    def test_foreign_owner_is_review_not_takeover(self):
        self.fence.item['owner']='eks-b'
        with self.assertRaises(RecoveryBlocked):self.store.claim(EVENT)
        self.assertEqual(self.fence.calls,[])
    def test_stale_generation_no_write(self):
        self.fence.item['generation']=3
        self.assertIsNone(self.store.claim(EVENT));self.assertEqual(self.fence.calls,[])
    def test_unrelated_node_no_write(self):
        self.assertIsNone(self.store.claim(EVENT|{'node':'other'}));self.assertEqual(self.fence.calls,[])
    def test_incomplete_or_missing_approval_blocks(self):
        self.fence.item.pop('approved_step')
        with self.assertRaises(RecoveryBlocked):self.store.claim(EVENT)
        self.fence.item.pop('approved_gen')
        with self.assertRaises(RecoveryBlocked):self.store.claim(EVENT)
        self.assertEqual(self.fence.calls,[])
    def test_explicit_scratch_policy(self):
        self.fence.item.pop('approved_gen');self.fence.item.pop('approved_step')
        self.recovery.item['allow_restart_from_scratch']=True
        self.assertEqual(self.store.claim(EVENT)['generation'],2)
    def test_finish_retains_owner_and_approved_fields(self):
        self.store.claim(EVENT);self.fence.item['owner']='eks-b'
        self.store.finish(dict(state='RECOVERED',event=EVENT,generation=2,target_cluster='eks-b',active_nodes=['new']))
        self.assertEqual(self.fence.item['owner'],'eks-b')
        self.assertEqual(self.fence.item['approved_gen'],1)
        self.assertNotIn('recovery_id',self.fence.item)
        self.assertEqual(self.recovery.calls[0]['ExpressionAttributeValues'][':nodes'],['new'])
    def test_finish_refuses_unclaimed_new_cluster(self):
        self.store.claim(EVENT)
        with self.assertRaises(RecoveryBlocked):
            self.store.finish(dict(state='RECOVERED',event=EVENT,generation=2,target_cluster='eks-b',active_nodes=['new']))
    def test_consistent_owner_read(self):
        self.assertTrue(self.store.owner_matches('j1',1,'eks-a'))
        self.assertFalse(self.store.owner_matches('j1',1,'eks-b'))
    def test_other_recovery_lock_blocks(self):
        self.fence.item['recovery_id']='other'
        with self.assertRaises(RecoveryBlocked):self.store.claim(EVENT)
