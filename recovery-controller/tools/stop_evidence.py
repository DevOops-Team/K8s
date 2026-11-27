"""Operator-side GPU-304 evidence tool; read-only collect, explicit signed approval.

Run from recovery-controller with Python/boto3 and an existing kubectl context.
Never stops instances, force-deletes Pods, patches RR or changes a generation.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from integrations.stop_evidence import ALGORITHM, DOMAIN, MAX_BYTES, approval_key, canonical, stop_config


def kubectl(context, namespace, *args):
    return json.loads(subprocess.check_output(
        ['kubectl', '--context', context, '-n', namespace, *args, '-o', 'json'], text=True))


def collect(context, manager_context, namespace, job_id, generation, cluster, account, region, gpus):
    """Capture one observed placement, NOT a proof of complete historical execution scope."""
    name = f'job-{job_id}-g{generation:04d}'
    job = kubectl(manager_context, namespace, 'get', 'jobsets.jobset.x-k8s.io', name)
    labels = job['metadata'].get('labels', {})
    if labels.get('devoops/job-id') != job_id or labels.get('devoops/generation') != str(generation):
        raise ValueError('manager JobSet identity mismatch')
    pods = kubectl(context, namespace, 'get', 'pods', '-l',
                   f'jobset.sigs.k8s.io/jobset-name={name}')['items']
    if len(pods) != gpus:
        raise ValueError('capture requires exactly the expected GPU/rank count, no partial or replacement duplicates')
    participants = []
    for pod in pods:
        meta, spec = pod['metadata'], pod['spec']
        labels = meta.get('labels', {})
        if (labels.get('devoops/job-id') != job_id
                or labels.get('devoops/generation') != str(generation)):
            raise ValueError('Pod identity differs from requested job/generation')
        index = labels.get('batch.kubernetes.io/job-completion-index')
        if index is None or not index.isdigit():
            raise ValueError('Indexed Job rank label required')
        node_name = spec.get('nodeName')
        if not node_name:
            raise ValueError('unbound Pod cannot supply an EC2 participant')
        node = kubectl(context, namespace, 'get', 'node', node_name)
        provider = node['spec'].get('providerID', '')
        if not provider.startswith('aws:///') or not provider.rsplit('/', 1)[-1].startswith('i-'):
            raise ValueError('AWS Node providerID required')
        participants.append(dict(rank=int(index), podUid=meta['uid'], node=node_name,
            instanceId=provider.rsplit('/', 1)[-1], cluster=cluster, accountId=account, region=region))
    if {p['rank'] for p in participants} != set(range(gpus)):
        raise ValueError('complete unique 0..N-1 ranks required')
    return dict(jobId=job_id, fromGeneration=generation, sourceCluster=cluster,
                oldJobsetUid=job['metadata']['uid'], capturedAt=int(time.time()),
                participants=sorted(participants, key=lambda p: p['rank']),
                warning='Observed snapshot only; review replacement history and every seen worker before approving.')


def private_write(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as out:
        out.write(canonical(value) + b'\n')


def publish(document, key_id, bucket, session, now=None, expected_etag=None):
    """KMS signing authority attests scope/block; STS identity is retained for audit."""
    stop_config({'signing_keys': [key_id]})
    if document.get('mode') not in ('manual', 'ec2'):
        raise ValueError('explicit manual or ec2 approval mode required')
    doc = dict(document, domain=DOMAIN, schemaVersion=1,
               approvedAt=int(time.time() if now is None else now),
               actorArn=session.client('sts').get_caller_identity()['Arn'])
    if doc.get('scopeComplete') is not True or doc.get('recreationBlocked') is not True:
        raise ValueError('operator must explicitly attest complete scope and recreation block')
    if doc.get('mode') == 'manual' and doc.get('executionStopped') is not True:
        raise ValueError('manual mode requires actual execution stopped attestation')
    digest = hashlib.sha256(canonical(doc)).digest()
    signed = session.client('kms', region_name=key_id.split(':')[3]).sign(
        KeyId=key_id, Message=digest, MessageType='DIGEST', SigningAlgorithm=ALGORITHM)
    if signed['KeyId'] != key_id:
        raise ValueError('use an immutable KMS key ARN')
    envelope = dict(document=doc, key_id=key_id, signature=base64.b64encode(signed['Signature']).decode())
    raw = canonical(envelope)
    if len(raw) > MAX_BYTES:
        raise ValueError('approval exceeds budget')
    key = approval_key(dict(job_id=doc['jobId'], event_id=doc['requestId']))
    condition = {'IfMatch': expected_etag} if expected_etag else {'IfNoneMatch': '*'}
    session.client('s3').put_object(Bucket=bucket, Key=key, Body=raw,
                                  ContentType='application/json', **condition)
    return dict(key=key, verificationId=digest.hex(),
                signingRequestId=signed.get('ResponseMetadata', {}).get('RequestId'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    capture = commands.add_parser('collect')
    for field in ('context', 'manager-context', 'job-id', 'cluster', 'account', 'region', 'output'):
        capture.add_argument('--' + field, required=True)
    capture.add_argument('--namespace', default='training')
    capture.add_argument('--generation', type=int, required=True)
    capture.add_argument('--gpus', type=int, choices=range(1, 5), required=True)
    sign = commands.add_parser('approve')
    for field in ('document', 'key-id', 'bucket', 'region'):
        sign.add_argument('--' + field, required=True)
    sign.add_argument('--expected-etag', help='Explicit CAS replacement of an unaccepted approval')
    sign.add_argument('--confirm-scope-and-block', action='store_true', required=True)
    args = parser.parse_args()
    if args.command == 'collect':
        value = collect(args.context, args.manager_context, args.namespace, args.job_id,
                        args.generation, args.cluster, args.account, args.region, args.gpus)
        private_write(args.output, value)
        print('Placement snapshot written; this is not a stop approval.')
    else:
        import boto3
        result = publish(json.loads(Path(args.document).read_text()), args.key_id, args.bucket,
                         boto3.Session(region_name=args.region), expected_etag=args.expected_etag)
        print(json.dumps(result))


if __name__ == '__main__':
    main()
