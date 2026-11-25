import copy
import unittest
from datetime import datetime
from unittest.mock import Mock
from integrations.dynamodb import RecoveryBlocked, Store
from integrations.relocation import RelocationClient
from integrations.relocation_status import RelocationStatusInput, status_config

EVENT = dict(event_id='r1', job_id='abc', generation=1, cluster='eks-a')
STAMP = '2026-11-25T01:00:00Z'
NOW = datetime.fromisoformat(STAMP.replace('Z', '+00:00')).timestamp()


def request():
    return dict(metadata=dict(namespace='training', creationTimestamp=STAMP, resourceVersion='7'),
        spec=dict(requestId='r1', jobRef='training/job-abc-g0001'),
        status=dict(phase='Done', recoveryProgress=dict(schemaVersion=1, requestId='r1',
            jobId='abc', fromGeneration=1, toGeneration=2, phase='NEW_RUN_SUBMITTED',
            oldRunStopEvidence=dict(generation=1, namespace='training', jobset_name='job-abc-g0001',
                clusters=['eks-a'], kind='automatic', confirmed_at=STAMP,
                checks=[dict(cluster='eks-a', kind='automatic', detail='JobSet/Workload/Pods absent')]),
            newJobSet=dict(name='job-abc-g0002', uid='manager-uid'))))


class ConditionalFailure(Exception):
    response = {'Error': {'Code': 'ConditionalCheckFailedException'}}


class Fence:
    def __init__(self):
        self.item = dict(generation=2, recovery_id='r1', recovery_from_gen=1,
                         recovery_to_gen=2, recovery_phase='GENERATION_ADVANCED')
        self.calls = []
        self.lose_authority = False

    def get_item(self, **kwargs):
        assert kwargs['ConsistentRead'] is True
        return {'Item': copy.deepcopy(self.item)}

    def update_item(self, **kwargs):
        self.calls.append(kwargs)
        if self.lose_authority: self.item['recovery_id'] = 'new-recovery'
        v = kwargs['ExpressionAttributeValues']
        if (self.item['recovery_id'] != v[':rid'] or self.item['generation'] != v[':gen']
                or self.item['recovery_to_gen'] != v[':gen']
                or self.item['recovery_from_gen'] != v[':from']
                or self.item['recovery_phase'] != v[':previous']
                or (':stop' in v and self.item.get('old_run_stop_evidence') != v[':stop'])):
            raise ConditionalFailure()
        self.item['recovery_phase'] = v[':phase']
        for key in ('old_run_stop_evidence', 'new_jobset_name', 'new_jobset_uid'):
            if ':'+key in v: self.item[key] = v[':'+key]


class StatusTests(unittest.TestCase):
    def setUp(self):
        self.fence = Fence()
        self.store = Store(Mock(), self.fence)
        self.input = RelocationStatusInput(self.store, 'training', lambda: NOW)

    def test_done_projects_both_barriers_and_replay_does_not_write(self):
        rr = request()
        rr['metadata']['annotations'] = {'devoops/recovery-stop-recorded': 'true'}
        transport = Mock(return_value=rr)
        client = RelocationClient('training', transport, status_input=self.input)
        client.refresh(EVENT, 'rr')
        self.assertEqual(self.fence.item['new_jobset_uid'], 'manager-uid')
        self.assertEqual([x['ExpressionAttributeValues'][':phase'] for x in self.fence.calls],
                         ['OLD_RUN_STOPPED', 'NEW_RUN_SUBMITTED'])
        self.assertEqual([x.args[0] for x in transport.call_args_list], ['GET'])
        client.refresh(EVENT, 'rr')
        self.assertEqual(len(self.fence.calls), 2)
        self.assertNotIn('owner', self.fence.item)
        self.assertEqual(self.fence.item['generation'], 2)

    def test_stop_recorded_annotation_follows_the_fencing_write(self):
        rr = request()
        rr['status'].update(phase='Stopping')
        rr['status']['recoveryProgress']['phase'] = 'OLD_RUN_STOPPED'
        del rr['status']['recoveryProgress']['newJobSet']
        calls = []
        def transport(method, path, document=None):
            calls.append((method, copy.deepcopy(document)))
            if method == 'PATCH':
                # Barrier order: the fencing write must already be done.
                self.assertEqual(self.fence.item['recovery_phase'], 'OLD_RUN_STOPPED')
            return rr
        RelocationClient('training', transport, status_input=self.input).refresh(EVENT, 'rr')
        self.assertEqual(calls[-1], ('PATCH', {'metadata': {'resourceVersion': '7',
            'annotations': {'devoops/recovery-stop-recorded': 'true'}}}))

    def test_no_progress_means_no_stop_acknowledgement(self):
        rr = request()
        del rr['status']['recoveryProgress']
        transport = Mock(return_value=rr)
        RelocationClient('training', transport, status_input=self.input).refresh(EVENT, 'rr')
        self.assertEqual([x.args[0] for x in transport.call_args_list], ['GET'])

    def test_failed_fencing_write_is_not_acknowledged(self):
        rr = request()
        rr['status']['recoveryProgress']['fromGeneration'] = 9
        transport = Mock(return_value=rr)
        with self.assertRaises(RecoveryBlocked):
            RelocationClient('training', transport, status_input=self.input).refresh(EVENT, 'rr')
        self.assertEqual([x.args[0] for x in transport.call_args_list], ['GET'])

    def test_old_status_is_not_inferred_as_stop_proof(self):
        rr = request()
        del rr['status']['recoveryProgress']
        self.assertFalse(self.input(EVENT, rr))
        self.assertEqual(self.fence.calls, [])

    def test_invalid_identity_or_proof_never_writes(self):
        mutations = [lambda r: r['metadata'].update(namespace='other'),
            lambda r: r['spec'].update(requestId='other'),
            lambda r: r['status']['recoveryProgress'].update(requestId='other'),
            lambda r: r['status']['recoveryProgress'].update(toGeneration=3),
            lambda r: r['status']['recoveryProgress'].update(toGeneration=True),
            lambda r: r['status']['recoveryProgress'].update(oldRunStopEvidence={}),
            lambda r: r['status']['recoveryProgress']['oldRunStopEvidence'].update(kind='manual'),
            lambda r: r['status']['recoveryProgress']['oldRunStopEvidence'].update(clusters=['other']),
            lambda r: r['status']['recoveryProgress']['oldRunStopEvidence'].update(checks=[]),
            lambda r: r['status']['recoveryProgress']['oldRunStopEvidence'].update(clusters=['eks-a', 'eks-b']),
            lambda r: r['status']['recoveryProgress']['oldRunStopEvidence'].update(confirmed_at='2099-01-01T00:00:00Z'),
            lambda r: r['status']['recoveryProgress']['oldRunStopEvidence'].update(confirmed_at='2020-01-01T00:00:00Z'),
            lambda r: r['status']['recoveryProgress']['newJobSet'].update(uid=''),
            lambda r: r['status']['recoveryProgress']['newJobSet'].update(name='job-other-g0002')]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                rr = request()
                mutate(rr)
                with self.assertRaises(RecoveryBlocked): self.input(EVENT, rr)
                self.assertEqual(self.fence.calls, [])

    def test_stop_only_then_submitted(self):
        rr = request()
        rr['status']['recoveryProgress']['phase'] = 'OLD_RUN_STOPPED'
        rr['status']['recoveryProgress'].pop('newJobSet')
        self.input(EVENT, rr)
        self.assertEqual(self.fence.item['recovery_phase'], 'OLD_RUN_STOPPED')
        self.input(EVENT, request())
        self.assertEqual(self.fence.item['recovery_phase'], 'NEW_RUN_SUBMITTED')

    def test_stale_authority_and_closed_recovery_never_write(self):
        for change in (dict(recovery_id='another'), dict(generation=3),
                       dict(recovery_phase='COMPLETED'), dict(recovery_from_gen=0)):
            with self.subTest(change=change):
                fence = Fence()
                fence.item.update(change)
                with self.assertRaises(RecoveryBlocked):
                    RelocationStatusInput(Store(Mock(), fence), 'training', lambda: NOW)(EVENT, request())
                self.assertEqual(fence.calls, [])

    def test_authority_race_is_checked_by_conditional_write(self):
        self.fence.lose_authority = True
        with self.assertRaises(ConditionalFailure): self.input(EVENT, request())
        self.assertEqual(self.fence.item['recovery_phase'], 'GENERATION_ADVANCED')
        self.assertNotIn('new_jobset_uid', self.fence.item)

    def test_conflicting_uid_or_stop_evidence_cannot_overwrite(self):
        self.input(EVENT, request())
        for field in ('uid', 'evidence'):
            rr = request()
            if field == 'uid': rr['status']['recoveryProgress']['newJobSet']['uid'] = 'different'
            else: rr['status']['recoveryProgress']['oldRunStopEvidence']['checks'][0]['detail'] = 'changed'
            with self.assertRaises(RecoveryBlocked): self.input(EVENT, rr)
        self.assertEqual(len(self.fence.calls), 2)

    def test_status_config_requires_explicit_handoff(self):
        self.assertFalse(status_config()['enabled'])
        self.assertTrue(status_config(dict(enabled=True, producer_contract_confirmed=True))['enabled'])
        for value in (dict(enabled=True), dict(enabled=1), dict(unknown=True), []):
            with self.assertRaises(ValueError): status_config(value)
