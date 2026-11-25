import copy
import json
import unittest
from decimal import Decimal
from unittest.mock import Mock
from integrations.self_fence import SelfFenceInput, ANNOTATION, self_fence_config
from integrations.relocation import RelocationClient
from integrations.relocation_status import RelocationStatusInput
from integrations.dynamodb import RecoveryBlocked
from test_relocation_status import EVENT, NOW, STAMP, request


def rr():
    r = request()
    r['spec'].update(toGeneration=2, jobSet={'spec': {'failurePolicy': {'maxRestarts': 0},
        'replicatedJobs': [{'replicas': 1, 'template': {'spec': {'parallelism': 2,
        'completions': 2, 'completionMode': 'Indexed', 'backoffLimit': 0,
        'podReplacementPolicy': 'Failed', 'template': {'spec': {'restartPolicy': 'Never'}}}}}]}})
    return r


def fence():
    return dict(stop_wait_started_at=Decimal(int(NOW)), generation=Decimal(2), recovery_from_gen=Decimal(1), recovery_to_gen=Decimal(2),
        recovery_id='r1', recovery_phase='GENERATION_ADVANCED', self_fence_gen=Decimal(1),
        self_fence_cluster='eks-a', self_fence_world_size=Decimal(2),
        self_fence_ranks={Decimal(0), Decimal(1)}, self_fence_pods={'0:uid0', '1:uid1'}, self_fence_at=STAMP)


class SelfFenceTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.store.fencing_record.return_value = fence()
        self.input = SelfFenceInput(self.store, lambda: NOW)

    def test_complete_decimal_sets_to_canonical_annotation(self):
        value = json.loads(self.input(EVENT, rr()))
        self.assertEqual(value['ranks'], [0, 1])
        self.assertEqual(value['pods'], ['0:uid0', '1:uid1'])
        self.assertEqual(value['generation'], 1)

    def test_partial_invalid_old_future_and_replacement_reports_never_authorize(self):
        for changes in [dict(self_fence_ranks={0}), dict(self_fence_gen=0),
                        dict(self_fence_cluster='eks-b'), dict(self_fence_world_size=True),
                        dict(self_fence_pods={'0:uid0','1:uid0'}), dict(self_fence_at='bad'),
                        dict(self_fence_at='2020-01-01T00:00:00Z'),
                        dict(self_fence_at='2099-01-01T00:00:00Z')]:
            with self.subTest(changes=changes):
                self.store.fencing_record.return_value = fence() | changes
                self.assertIsNone(self.input(EVENT, rr()))
        self.store.fencing_record.return_value = fence()
        r = rr(); r['spec']['jobSet']['spec']['replicatedJobs'][0]['template']['spec']['podReplacementPolicy']='TerminatingOrFailed'
        self.assertIsNone(self.input(EVENT,r))

    def test_authority_loss_fails_closed_and_later_phase_does_not_reissue(self):
        for changes in [dict(generation=3),dict(recovery_id='other'),dict(recovery_from_gen=0),dict(recovery_to_gen=3)]:
            self.store.fencing_record.return_value=fence() | changes
            with self.assertRaises(RecoveryBlocked):self.input(EVENT,rr())
        self.store.fencing_record.return_value=fence() | dict(recovery_phase='RESUMING')
        self.assertIsNone(self.input(EVENT,rr()))

    def test_annotation_patch_then_status_re_read_and_replay(self):
        r=rr();r['status']={}
        def transport(method,path,document=None):
            if method=='PATCH':
                r['metadata'].setdefault('annotations',{}).update(document['metadata']['annotations'])
                r['metadata']['resourceVersion']='8'
            return copy.deepcopy(r)
        transport=Mock(side_effect=transport)
        client=RelocationClient('training',transport,status_input=Mock(return_value=False),self_fence_input=self.input)
        client.refresh(EVENT,'rr');client.refresh(EVENT,'rr')
        self.assertEqual([c.args[0] for c in transport.call_args_list],['GET','PATCH','GET','GET'])
        self.assertIn(ANNOTATION,r['metadata']['annotations'])

    def test_cancel_never_produces_proof(self):
        r=rr();r['metadata']['annotations']={'devoops/recovery-cancel':'lost'}
        transport=Mock(return_value=r)
        RelocationClient('training',transport,status_input=Mock(),self_fence_input=self.input).refresh(EVENT,'rr')
        self.store.fencing_record.assert_not_called()
        self.assertEqual(transport.call_count,1)

    def test_status_requires_enabled_matching_verified_proof(self):
        r=rr(); r['metadata']['annotations']={ANNOTATION:self.input(EVENT,r)}
        e=r['status']['recoveryProgress']['oldRunStopEvidence']
        e['kind']='self_fence';e['checks'][0]['kind']='self_fence'
        with self.assertRaises(RecoveryBlocked):RelocationStatusInput(self.store,'training',lambda:NOW)(EVENT,r)
        project=RelocationStatusInput(self.store,'training',lambda:NOW,allow_self_fence=True)
        self.assertTrue(project(EVENT,r))
        self.assertEqual(self.store.record_relocation_phase.call_count,2)
        for change in ['missing','cluster','ranks','kind']:
            bad=copy.deepcopy(r)
            if change=='missing':bad['metadata']['annotations']={}
            elif change=='kind':bad['status']['recoveryProgress']['oldRunStopEvidence']['kind']='automatic'
            else:
                value=json.loads(bad['metadata']['annotations'][ANNOTATION]);value[change]='bad'
                bad['metadata']['annotations'][ANNOTATION]=json.dumps(value)
            with self.subTest(change=change),self.assertRaises(RecoveryBlocked):project(EVENT,bad)

    def test_mixed_ec2_self_fence_and_automatic_status(self):
        r=rr();r['metadata']['annotations']={ANNOTATION:self.input(EVENT,r)}
        e=r['status']['recoveryProgress']['oldRunStopEvidence'];e.update(kind='ec2',clusters=['eks-a','eks-b'])
        e['checks']=[dict(cluster='eks-a',kind='self_fence',detail='all exited'),dict(cluster='eks-b',kind='ec2',detail='terminated')]
        self.assertTrue(RelocationStatusInput(self.store,'training',lambda:NOW,allow_self_fence=True)(EVENT,r))

    def test_validated_annotation_is_stable_after_late_duplicate(self):
        r=rr(); value=self.input(EVENT,r)
        r['metadata']['annotations']={ANNOTATION:value}
        self.store.fencing_record.return_value['self_fence_at']='2026-11-25T01:00:01Z'
        self.assertEqual(self.input(EVENT,r), value)

    def test_execute_and_status_input_required(self):
        from main import validate_config
        from pathlib import Path
        config=json.loads((Path(__file__).resolve().parents[1]/'examples/config.kafka.example.json').read_text())
        config['recovery_table']='test-recovery'
        config['self_fence_input']=dict(enabled=True,producer_contract_confirmed=True)
        with self.assertRaises(ValueError):validate_config(config)
        config.update(mode='execute',execution_enabled=True,scheduler_policy_confirmed=True)
        with self.assertRaises(ValueError):validate_config(config)
        config['relocation_status_input']=dict(enabled=True,producer_contract_confirmed=True)
        validate_config(config)

    def test_report_before_delayed_rr_uses_generation_advance_time(self):
        r = rr()
        r['metadata']['creationTimestamp'] = '2026-11-25T01:01:00Z'
        r['status']['recoveryProgress']['oldRunStopEvidence'].update(
            kind='self_fence', confirmed_at=r['metadata']['creationTimestamp'])
        r['status']['recoveryProgress']['oldRunStopEvidence']['checks'][0]['kind'] = 'self_fence'
        value = SelfFenceInput(self.store, lambda: NOW + 60)(EVENT, r)
        self.assertIsNotNone(value)
        r['metadata']['annotations'] = {ANNOTATION: value}
        project = RelocationStatusInput(self.store, 'training', lambda: NOW + 60, allow_self_fence=True)
        self.assertTrue(project(EVENT, r))
        for invalid_start in (None, True, Decimal('1.5'), 0, int(NOW) + 31):
            self.store.fencing_record.return_value['stop_wait_started_at'] = invalid_start
            self.assertIsNone(SelfFenceInput(self.store, lambda: NOW + 60)(EVENT, r))
            with self.assertRaises(RecoveryBlocked):
                project(EVENT, r)

    def test_config_default_and_contract_gate(self):
        self.assertFalse(self_fence_config()['enabled'])
        for v in [True,{'enabled':True},{'enabled':1},{'typo':False}]:
            with self.assertRaises(ValueError):self_fence_config(v)
        self.assertTrue(self_fence_config(dict(enabled=True,producer_contract_confirmed=True))['enabled'])
