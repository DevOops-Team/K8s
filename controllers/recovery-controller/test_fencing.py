import copy
import unittest
from adapters import Store, RecoveryBlocked, RetryLimitReached

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
        if kw['UpdateExpression'].startswith('SET recovery_id'):
            assert self.item['generation'] == values[':old']
            assert 'recovery_id' not in self.item
            if ':approved_gen' in values:
                assert self.item['approved_gen'] == values[':approved_gen']
                assert self.item['approved_step'] == values[':approved_step']
            self.item.update(recovery_id=values[':rid'], recovery_from_gen=values[':old'],
                             recovery_to_gen=values[':new'], recovery_phase='CLAIMED',
                             recovery_resume_step=values[':resume'], recovery_resume_gen=values[':resume_gen'])
        elif kw['UpdateExpression'].startswith('SET generation'):
            assert self.item['generation']==values[':old']
            assert self.item.get('owner',values[':source'])==values[':source']
            assert self.item['recovery_id']==values[':rid']
            assert self.item['recovery_phase']=='CLAIMED'
            self.item.update(generation=values[':new'],recovery_id=values[':rid'],
                             recovery_from_gen=values[':old'],recovery_to_gen=values[':new'])
            self.item.update(recovery_phase=values[':phase'],
                             recovery_resume_step=self.item['recovery_resume_step'],
                             retry_count=self.item.get('retry_count',0)+1)
            self.item.pop('owner',None)
        else:
            assert self.item['owner']==values[':target']
            self.item['recovery_phase']=values[':phase']
            for key in ['recovery_id','recovery_from_gen','recovery_to_gen']:self.item.pop(key,None)
        return {'Attributes':copy.deepcopy(self.item)}

class FencingTests(unittest.TestCase):
    def setUp(self):
        self.recovery=RecoveryTable();self.fence=FencingTable();self.store=Store(self.recovery,self.fence)
    def advance(self):
        self.store.claim(EVENT)
        return self.store.claim(EVENT)
    def test_fourth_recovery_rejected_without_writes(self):
        self.fence.item['retry_count']=3
        with self.assertRaises(RetryLimitReached):self.store.claim(EVENT)
        self.assertEqual(self.fence.calls,[])
    def test_third_recovery_replay_allowed_and_budget_is_atomic(self):
        self.fence.item['retry_count']=2
        self.advance()
        self.assertEqual(self.fence.item['retry_count'],3)
        self.assertEqual(self.store.claim(EVENT)['generation'],2)
        self.assertEqual(self.fence.item['retry_count'],3)
        for call in self.fence.calls:
            self.assertIn('retry_count < :limit',call['ConditionExpression'])
            self.assertEqual(call['ExpressionAttributeValues'][':limit'],3)
    def test_single_atomic_generation_advance_removes_owner(self):
        result=self.advance()
        self.assertEqual(result['generation'],2)
        self.assertNotIn('owner',self.fence.item)
        call=self.fence.calls[1]
        self.assertIn('REMOVE #owner',call['UpdateExpression'])
        self.assertIn('generation = :old',call['ConditionExpression'])
        self.assertIn('#owner = :source',call['ConditionExpression'])
        self.assertEqual(self.fence.item['approved_gen'],1)
        self.assertEqual(self.fence.item['recovery_phase'],'GENERATION_ADVANCED')
        self.assertEqual(self.fence.item['retry_count'],1)
        self.assertEqual(self.fence.item['recovery_resume_step'],1000)
        self.assertEqual(self.fence.item['approved_step'],1000)
        self.assertEqual(self.fence.item['heartbeat_at'],'original')
        self.assertNotIn('checkpoint_uri',result)
    def test_replay_does_not_clear_new_owner(self):
        self.advance();self.fence.item['owner']='eks-b'
        self.assertEqual(self.advance()['generation'],2)
        self.assertEqual(self.fence.item['owner'],'eks-b')
        self.assertEqual(len(self.fence.calls),2)
        self.assertEqual(self.fence.item['retry_count'],1)
    def test_owner_absent_can_advance(self):
        self.fence.item.pop('owner');self.advance()
        self.assertEqual(self.fence.item['generation'],2)
    def test_foreign_owner_is_review_not_takeover(self):
        self.fence.item['owner']='eks-b'
        with self.assertRaises(RecoveryBlocked):self.advance()
        self.assertEqual(self.fence.calls,[])
    def test_stale_generation_no_write(self):
        self.fence.item['generation']=3
        self.assertIsNone(self.advance());self.assertEqual(self.fence.calls,[])
    def test_unrelated_node_no_write(self):
        self.assertIsNone(self.store.claim(EVENT|{'node':'other'}));self.assertEqual(self.fence.calls,[])
    def test_incomplete_or_explicitly_disallowed_missing_approval_blocks(self):
        self.fence.item.pop('approved_step')
        with self.assertRaises(RecoveryBlocked):self.advance()
        self.fence.item.pop('approved_gen')
        self.recovery.item['allow_restart_from_scratch']=False
        with self.assertRaises(RecoveryBlocked):self.advance()
        self.assertEqual(self.fence.calls,[])
    def test_explicit_scratch_policy(self):
        self.fence.item.pop('approved_gen');self.fence.item.pop('approved_step')
        self.recovery.item['allow_restart_from_scratch']=True
        self.assertEqual(self.advance()['generation'],2)
    def test_finish_retains_owner_and_approved_fields(self):
        self.advance();self.fence.item['owner']='eks-b'
        self.store.finish(dict(state='RECOVERED',event=EVENT,generation=2,target_cluster='eks-b',active_nodes=['new']))
        self.assertEqual(self.fence.item['owner'],'eks-b')
        self.assertEqual(self.fence.item['approved_gen'],1)
        self.assertNotIn('recovery_id',self.fence.item)
        self.assertEqual(self.recovery.calls[0]['ExpressionAttributeValues'][':nodes'],['new'])
    def test_finish_refuses_unclaimed_new_cluster(self):
        self.advance()
        with self.assertRaises(RecoveryBlocked):
            self.store.finish(dict(state='RECOVERED',event=EVENT,generation=2,target_cluster='eks-b',active_nodes=['new']))
    def test_consistent_owner_read(self):
        self.assertTrue(self.store.owner_matches('j1',1,'eks-a'))
        self.assertFalse(self.store.owner_matches('j1',1,'eks-b'))
    def test_other_recovery_lock_blocks(self):
        self.fence.item['recovery_id']='other'
        with self.assertRaises(RecoveryBlocked):self.advance()

    def test_replay_allows_registered_new_placement(self):
        self.advance()
        self.recovery.item.update(cluster='eks-b',active_nodes=['new'],placement_generation=2)
        self.assertEqual(self.advance()['generation'],2)
        self.assertEqual(self.fence.item['retry_count'],1)

    def test_claimed_preserves_old_generation_owner_and_retry_count(self):
        self.fence.item['retry_count']=0
        result=self.store.claim(EVENT)
        self.assertTrue(result['claim_pending'])
        self.assertEqual(self.fence.item['generation'],1)
        self.assertEqual(self.fence.item['owner'],'eks-a')
        self.assertEqual(self.fence.item['retry_count'],0)
        self.assertEqual(self.fence.item['recovery_phase'],'CLAIMED')
        self.assertEqual(self.fence.item['approved_step'],1000)
        # Crash after the CLAIMED write: next call resumes with a single increment.
        self.store=Store(self.recovery,self.fence)
        self.assertEqual(self.store.claim(EVENT)['generation'],2)
        self.assertEqual(self.fence.item['retry_count'],1)
        self.store.claim(EVENT)
        self.assertEqual(self.fence.item['retry_count'],1)

    def test_complete_preserves_previous_approval_until_sidecar_updates_it(self):
        self.advance()
        self.fence.item.update(owner='eks-b',recovery_phase='RESUMING')
        record=dict(state='RECOVERED',event=EVENT,generation=2,
                    target_cluster='eks-b',active_nodes=['new'])
        self.store.finish(record)
        self.assertEqual(self.fence.item['recovery_phase'],'COMPLETED')
        self.assertEqual(self.fence.item['approved_gen'],1)
        self.assertEqual(self.fence.item['approved_step'],1000)
        self.assertEqual(self.fence.item['retry_count'],1)
        self.store.finish(record)  # Retry after terminal persistence must be safe.
        self.fence.item.update(approved_gen=2,approved_step=1500)
        self.store.finish(record)
        self.assertEqual(self.fence.item['approved_gen'],2)
        self.assertEqual(self.fence.item['approved_step'],1500)

    def test_no_approval_defaults_to_scratch_and_retains_warning_on_replay(self):
        self.fence.item.pop('approved_gen'); self.fence.item.pop('approved_step')
        claimed=self.store.claim(EVENT)
        self.assertTrue(claimed['claim_pending'])
        result=self.store.claim(EVENT)
        self.assertTrue(result['allow_restart_from_scratch'])
        self.assertIsNone(result['resume_gen'])
        self.assertEqual(result['resume_step'],0)
        self.assertEqual(result['checkpoint_warning'],'NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH')
        self.assertEqual(self.store.claim(EVENT)['checkpoint_warning'],result['checkpoint_warning'])
        self.assertEqual(self.fence.item['retry_count'],1)
        self.assertNotIn('approved_gen',self.fence.item)

    def test_custom_recovery_budget_is_used_by_conditional_writes(self):
        self.store=Store(self.recovery,self.fence,max_recovery_attempts=1)
        self.advance()
        for call in self.fence.calls:
            self.assertEqual(call['ExpressionAttributeValues'][':limit'],1)
        self.assertEqual(self.store.claim(EVENT)['generation'],2)
        self.fence.item.pop('recovery_id')
        self.fence.item['owner']='eks-a'
        with self.assertRaises(RetryLimitReached):
            self.store.claim(dict(EVENT,event_id='next',generation=2))

    def test_invalid_recovery_budget_is_rejected(self):
        for value in (0,-1,True,1.5,'3'):
            with self.assertRaises(ValueError):Store(self.recovery,self.fence,value)
