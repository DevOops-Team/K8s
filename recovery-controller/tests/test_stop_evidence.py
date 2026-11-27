import base64
import copy
import hashlib
import hmac
import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock
from integrations.dynamodb import RecoveryBlocked, Store
from integrations.relocation import RelocationClient
from integrations.relocation_status import RelocationStatusInput
from integrations.stop_evidence import (ALGORITHM, ANNOTATION, DOMAIN, S3StopApprovals,
                                        VerifiedStopInput, approval_key, canonical, stop_config)
from recovery.workflow import Workflow
from recovery.retry_policy import Runner

NOW = 1795651200
KEY = 'arn:aws:kms:us-east-2:111111111111:key/12345678-1234-1234-1234-123456789012'
EVENT = dict(event_id='r1', job_id='abc', generation=1, cluster='eks-a', node='n1',
             category='hw', suggested_action='isolate_migrate')


def config():
    return dict(enabled=True, producer_contract_confirmed=True, signing_keys=[KEY], ec2_enabled=True,
        approval_bucket='test-approvals', cluster_accounts={
            'eks-a': dict(account_id='111111111111', region='us-east-2')})


def request():
    return dict(metadata=dict(namespace='training', uid='rr-uid', resourceVersion='7',
        creationTimestamp=datetime.fromtimestamp(NOW-60, timezone.utc).isoformat()),
        spec=dict(requestId='r1', jobRef='training/job-abc-g0001', toGeneration=2, sourceCluster='eks-a',
                  action='moveToSpare', nodes=[dict(name='n1'), dict(name='n2')]),
    status=dict(phase='Held', seenClusters=['eks-a']))


def document():
    return dict(domain=DOMAIN, schemaVersion=1, requestId='r1', requestUid='rr-uid', jobId='abc',
        namespace='training', fromGeneration=1, toGeneration=2, sourceCluster='eks-a',
        oldJobsetUid='old-uid', approvedAt=NOW, actorArn='arn:aws:sts::111111111111:assumed-role/operator/session',
        scopeComplete=True, recreationBlocked=True, executionStopped=True, mode='manual',
        blockEvidence='ticket: old worker recreation and replacement paths disabled',
        reason='verified all old ranks stopped', clusters=['eks-a'], participants=[
            dict(rank=i, podUid=f'pod-{i}', node=f'n{i+1}', instanceId=f'i-{i+1:017x}',
                 cluster='eks-a', accountId='111111111111', region='us-east-2') for i in range(2)])


def signed(doc):
    digest = hashlib.sha256(canonical(doc)).digest()
    signature = hmac.digest(b'test-only', digest, 'sha256')
    return json.dumps(dict(document=doc, key_id=KEY, signature=base64.b64encode(signature).decode()))


def kms_verify(**args):
    assert args['MessageType'] == 'DIGEST' and args['SigningAlgorithm'] == ALGORITHM
    return dict(KeyId=KEY, SignatureValid=hmac.compare_digest(
        args['Signature'], hmac.digest(b'test-only', args['Message'], 'sha256')))


class MemoryStore:
    def __init__(self):
        self.record = dict(event=EVENT, state='HOLD', stopped_stage='REQUESTED', budget_stage='REQUESTED',
            reason='stage deadline exceeded: REQUESTED', generation=2, relocation_request='rr', published=True)
        self.fence = dict(generation=2, recovery_id='r1', recovery_from_gen=1, recovery_to_gen=2,
                          recovery_phase='GENERATION_ADVANCED')
        self.metadata = dict(placement_generation=1, cluster='eks-a', jobset_uid='old-uid',
                             gpu_count=2, active_nodes=['n1', 'n2'])
        self.writes = []
    def get(self, rid): return copy.deepcopy(self.record)
    def save(self, rid, **changes):
        self.record.update(changes)
        return self.get(rid)
    def fencing_record(self, job): return copy.deepcopy(self.fence)
    def placement(self, job): return copy.deepcopy(self.metadata)
    def freeze_stop_evidence(self, event, proof):
        value = canonical(proof).decode()
        if self.record.get('verified_stop_evidence', value) != value: raise RecoveryBlocked('conflict')
        self.record['verified_stop_evidence'] = value
        return proof
    def record_relocation_phase(self, event, phase, extra, **kwargs):
        self.writes.append(phase)
        self.fence.update(recovery_phase=phase, **extra)


class StopEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.store, self.rr, self.doc = MemoryStore(), request(), document()
        self.kms, self.ec2 = Mock(), Mock()
        self.kms.verify.side_effect = kms_verify
        self.source = Mock(side_effect=lambda e: signed(self.doc))
        self.verifier = VerifiedStopInput(self.store, lambda r: self.kms, lambda r: self.ec2,
            config(), clock=lambda: NOW, approval_source=self.source)

    def accept(self):
        value = self.verifier(EVENT, self.rr)
        self.assertIsNotNone(value, self.store.record.get('stop_evidence_error'))
        self.rr['metadata']['annotations'] = {ANNOTATION: value}
        return json.loads(value)

    def reject(self):
        self.assertIsNone(self.verifier(EVENT, self.rr))
        self.assertNotIn('verified_stop_evidence', self.store.record)
        self.assertTrue(self.store.record.get('stop_evidence_error'))

    def test_disabled_and_missing_approval_do_not_call_aws(self):
        self.assertIsNone(VerifiedStopInput(self.store, Mock(), Mock())(EVENT, self.rr))
        self.source.side_effect, self.source.return_value = None, None
        self.assertIsNone(self.verifier(EVENT, self.rr))
        self.kms.verify.assert_not_called()

    def test_manual_signed_proof_survives_restart_and_age(self):
        proof = self.accept()
        self.assertEqual(proof['confirmed_by'], 'kms:' + KEY)
        self.ec2.describe_instances.assert_not_called()
        self.verifier.clock = lambda: NOW + 10000
        self.assertEqual(json.loads(self.verifier(EVENT, self.rr)), proof)
        self.assertEqual(self.kms.verify.call_count, 1)

    def test_tampered_document_is_rejected(self):
        envelope = json.loads(signed(self.doc))
        envelope['document']['reason'] = 'altered'
        self.source.side_effect, self.source.return_value = None, json.dumps(envelope)
        self.reject()

    def test_incomplete_identity_scope_or_attestation_rejected(self):
        for changes in [dict(requestId='other'), dict(requestUid='replacement'), dict(fromGeneration=True),
            dict(toGeneration=3), dict(oldJobsetUid='other'), dict(approvedAt=NOW-301), dict(approvedAt=NOW+31),
            dict(scopeComplete=False), dict(recreationBlocked=False), dict(blockEvidence=''),
            dict(executionStopped=False), dict(clusters=[]), dict(clusters=['eks-a', 'eks-b']),
            dict(participants=[]), dict(mode='unknown'), dict(participants=[document()['participants'][0]]),
            dict(participants=[document()['participants'][0]] * 2)]:
            with self.subTest(changes=changes):
                self.doc = dict(document(), **changes)
                self.reject()

    def test_participant_identity_errors(self):
        for changes in (dict(accountId='222222222222'), dict(region='us-west-2'), dict(rank=4),
                        dict(podUid='pod-1'), dict(node='foreign'), dict(instanceId='bad')):
            with self.subTest(changes=changes):
                self.doc = document()
                self.doc['participants'][0].update(changes)
                self.reject()

    def test_lost_authority_never_emits_proof(self):
        self.store.fence['recovery_id'] = 'new'
        with self.assertRaises(RecoveryBlocked): self.verifier(EVENT, self.rr)
        self.kms.verify.assert_not_called()

    def test_changed_scope_or_rr_invalidates_frozen_proof(self):
        self.accept()
        self.rr['status']['seenClusters'].append('eks-b')
        self.assertIsNone(self.verifier(EVENT, self.rr))
        self.rr = request()
        self.rr['metadata']['uid'] = 'other'
        self.assertIsNone(self.verifier(EVENT, self.rr))

    def response(self, state='stopped', owner='111111111111'):
        return dict(Reservations=[dict(OwnerId=owner, Instances=[
            dict(InstanceId=p['instanceId'], State=dict(Name=state)) for p in self.doc['participants']])])

    def test_ec2_complete_stopped_or_terminated(self):
        for state in ('stopped', 'terminated'):
            with self.subTest(state=state):
                self.store.record.pop('verified_stop_evidence', None)
                self.doc['mode'] = 'ec2'
                self.ec2.describe_instances.return_value = self.response(state)
                proof = self.accept()
                self.assertEqual(len(proof['ec2Observations']), 2)
                self.assertEqual(self.ec2.describe_instances.call_args.kwargs['InstanceIds'],
                                 ['i-00000000000000001', 'i-00000000000000002'])

    def test_ec2_missing_running_stopping_owner_and_pagination_rejected(self):
        self.doc['mode'] = 'ec2'
        responses = [self.response(s) for s in ('running', 'stopping', 'shutting-down', 'pending')]
        responses += [dict(Reservations=[]), self.response(owner='222222222222'),
                      dict(self.response(), NextToken='more')]
        partial = self.response()
        partial['Reservations'][0]['Instances'].pop()
        responses.append(partial)
        for response in responses:
            with self.subTest(response=response):
                self.ec2.describe_instances.return_value = response
                self.reject()

    def test_sdk_errors_never_fabricate_proof(self):
        self.doc['mode'] = 'ec2'
        self.ec2.describe_instances.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError): self.verifier(EVENT, self.rr)
        self.assertNotIn('verified_stop_evidence', self.store.record)

    def test_manual_status_must_exactly_echo_verified_proof(self):
        status_input = RelocationStatusInput(self.store, 'training', lambda: NOW, verified_stop_input=self.verifier)
        proof = self.accept()
        self.rr['status']['recoveryProgress'] = dict(schemaVersion=1, requestId='r1', jobId='abc',
            fromGeneration=1, toGeneration=2, phase='OLD_RUN_STOPPED', oldRunStopEvidence=proof)
        for mutate in ('empty', 'forged', 'strip-verification'):
            changed = copy.deepcopy(self.rr)
            if mutate == 'empty': changed['status']['recoveryProgress']['oldRunStopEvidence']['checks'] = []
            elif mutate == 'forged': changed['status']['recoveryProgress']['oldRunStopEvidence']['approval']['reason'] = 'different'
            else:
                stopped = changed['status']['recoveryProgress']['oldRunStopEvidence']
                stopped.pop('verificationId')
                stopped['kind'] = 'ec2'
                stopped['checks'][0]['kind'] = 'ec2'
            with self.assertRaises(RecoveryBlocked): status_input(EVENT, changed)
        self.assertEqual(self.store.writes, [])

    def test_deadline_hold_accepts_status_then_fencing_then_ack_without_new_claim(self):
        proof = self.accept()
        self.rr['status']['recoveryProgress'] = dict(schemaVersion=1, requestId='r1', jobId='abc',
            fromGeneration=1, toGeneration=2, phase='OLD_RUN_STOPPED', oldRunStopEvidence=proof)
        methods = []
        def transport(method, path, body=None):
            methods.append(method)
            if method == 'PATCH':
                annotations = body['metadata']['annotations']
                if 'devoops/recovery-stop-recorded' in annotations:
                    self.assertEqual(self.store.fence['recovery_phase'], 'OLD_RUN_STOPPED')
                self.rr['metadata'].setdefault('annotations', {}).update(annotations)
            return copy.deepcopy(self.rr)
        status_input = RelocationStatusInput(self.store, 'training', lambda: NOW, verified_stop_input=self.verifier)
        executor = RelocationClient('training', transport, status_input=status_input, verified_stop_input=self.verifier)
        workflow = Workflow(self.store, executor, execute=True, clock=lambda: NOW)
        result = Runner(self.store, workflow, Mock(), 'result').poll_held(self.store.record, NOW, False)
        self.assertEqual((result['state'], result['stage'], result['generation']), ('REQUESTED', 'OLD_RUN_STOPPED', 2))
        self.assertEqual(result['event']['event_id'], 'r1')
        self.assertEqual(self.rr['metadata']['annotations']['devoops/recovery-stop-recorded'], 'true')
        self.assertNotIn('POST', methods)
        self.assertEqual(len(result['hold_resume_history']), 1)

    def test_other_holds_not_released(self):
        executor = Mock()
        workflow = Workflow(self.store, executor, execute=True)
        for reason in ('API attempt budget exhausted: REQUESTED', 'restore failed', 'observe_mode_cancel'):
            self.assertIsNone(workflow.held_relocation_progress(dict(self.store.record, reason=reason)))
        executor.refresh.assert_not_called()

    def test_config_explicit_and_keys_pinned(self):
        self.assertFalse(stop_config()['enabled'])
        for value in ({'enabled': True}, {'enabled': 'true'}, {'signing_keys': ['alias/foo']},
                      {'signing_keys': [{}]}, {'max_age_seconds': 0}, {'unknown': True}):
            with self.subTest(value=value), self.assertRaises(ValueError): stop_config(value)

    def test_freeze_is_conditional_and_json_serialized(self):
        table = Mock()
        Store(table, Mock()).freeze_stop_evidence(EVENT, dict(verificationId='digest'))
        call = table.update_item.call_args.kwargs
        self.assertIn('attribute_not_exists(verified_stop_evidence)', call['ConditionExpression'])
        self.assertEqual(json.loads(call['ExpressionAttributeValues'][':proof']), {'verificationId': 'digest'})

    def test_s3_input_closes_stream_and_does_not_hide_access_errors(self):
        s3, stream = Mock(), io.BytesIO(b'{}')
        s3.get_object.return_value = dict(Body=stream)
        reader = S3StopApprovals(s3, 'test-approvals')
        self.assertEqual(reader(EVENT), '{}')
        self.assertTrue(stream.closed)
        self.assertEqual(s3.get_object.call_args.kwargs['Key'], approval_key(EVENT))
        s3.get_object.side_effect = PermissionError()
        with self.assertRaises(PermissionError): reader(EVENT)


if __name__ == '__main__': unittest.main()
