import copy
import io
import json
import unittest
from unittest.mock import Mock, patch

from tools.stop_evidence import collect, publish
from test_stop_evidence import EVENT, KEY, NOW, document, config, MemoryStore, request
from integrations.stop_evidence import S3StopApprovals, VerifiedStopInput


class ToolTests(unittest.TestCase):
    def test_collect_maps_actual_provider_ids_and_rejects_incomplete_or_replaced_ranks(self):
        job = dict(metadata=dict(uid='manager', labels={'devoops/job-id':'abc', 'devoops/generation':'1'}))
        pods = [dict(metadata=dict(uid=f'p{i}', labels={'devoops/job-id':'abc', 'devoops/generation':'1',
                'batch.kubernetes.io/job-completion-index':str(i)}), spec=dict(nodeName=f'n{i}')) for i in range(2)]
        def kube(context, ns, *args):
            if args[1].startswith('jobsets'): return job
            if args[1] == 'pods': return dict(items=pods)
            return dict(spec=dict(providerID='aws:///us-east-2a/i-00000000000000001'))
        with patch('tools.stop_evidence.kubectl', side_effect=kube):
            result = collect('worker', 'manager', 'training', 'abc', 1, 'eks-a', '111111111111', 'us-east-2', 2)
            self.assertEqual(result['oldJobsetUid'], 'manager')
            self.assertEqual(result['participants'][0]['instanceId'], 'i-00000000000000001')
            pods[1]['metadata']['labels']['batch.kubernetes.io/job-completion-index']='0'
            with self.assertRaises(ValueError): collect('w','m','training','abc',1,'eks-a','111111111111','us-east-2',2)
            pods.pop()
            with self.assertRaises(ValueError): collect('w','m','training','abc',1,'eks-a','111111111111','us-east-2',2)

    def test_publish_uses_sts_identity_digest_signature_and_conditional_s3_write(self):
        session, kms, s3, sts = Mock(), Mock(), Mock(), Mock()
        session.client.side_effect = lambda service, **kw: {'kms':kms,'s3':s3,'sts':sts}[service]
        sts.get_caller_identity.return_value = dict(Arn='arn:aws:iam::111111111111:role/approver')
        kms.sign.return_value = dict(KeyId=KEY, Signature=b'signature', ResponseMetadata=dict(RequestId='audit'))
        result = publish(document(), KEY, 'approval-bucket', session, now=NOW)
        put = s3.put_object.call_args.kwargs
        doc = json.loads(put['Body'])['document']
        self.assertEqual(doc['actorArn'], sts.get_caller_identity()['Arn'])
        self.assertEqual(result['signingRequestId'], 'audit')
        self.assertEqual(put['IfNoneMatch'], '*')
        self.assertEqual(kms.sign.call_args.kwargs['MessageType'], 'DIGEST')
        publish(document(), KEY, 'approval-bucket', session, now=NOW, expected_etag='old')
        self.assertEqual(s3.put_object.call_args.kwargs['IfMatch'], 'old')
        with self.assertRaises(ValueError): publish(dict(document(), recreationBlocked=False), KEY, 'b', session)

    def test_real_botocore_request_shapes_for_sign_verify_and_s3_cas(self):
        import boto3
        from botocore.stub import Stubber
        from integrations.stop_evidence import ALGORITHM, canonical
        import hashlib
        session = boto3.Session(aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-2')
        kms = session.client('kms')
        digest = hashlib.sha256(canonical(document())).digest()
        expected = dict(KeyId=KEY, Message=digest, MessageType='DIGEST', Signature=b'x', SigningAlgorithm=ALGORITHM)
        with Stubber(kms) as stub:
            stub.add_response('verify', dict(KeyId=KEY, SignatureValid=True, SigningAlgorithm=ALGORITHM), expected)
            self.assertTrue(kms.verify(**expected)['SignatureValid'])
        model = session.client('s3').meta.service_model.operation_model('PutObject').input_shape.members
        self.assertIn('IfMatch', model)
        self.assertIn('IfNoneMatch', model)

    def test_conditional_audit_failure_never_acknowledges(self):
        from integrations.dynamodb import Store, RecoveryBlocked
        class Conflict(Exception):
            response={'Error':{'Code':'ConditionalCheckFailedException'}}
        table=Mock()
        table.update_item.side_effect=Conflict()
        with self.assertRaises(RecoveryBlocked): Store(table,Mock()).freeze_stop_evidence(EVENT, {'x':1})

    def test_enable_requires_execute_and_status_input(self):
        from main import validate_config
        cfg = dict(schema_version=1, mode='observe', event_source='disabled', execution_enabled=False,
                   verified_stop_input=config())
        with self.assertRaisesRegex(ValueError, 'requires relocation status'):
            validate_config(cfg)


if __name__ == '__main__': unittest.main()
