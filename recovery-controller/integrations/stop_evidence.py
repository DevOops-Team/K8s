"""GPU-304: authenticated stop attestations + independent EC2 observations.

No instance mutation. A signature attests the execution scope and recreation block;
EC2 state alone never proves either. Disabled until the producer/IAM contract is reviewed.
"""
import base64
import hashlib
import json
import re
import time
from datetime import datetime, timezone

from integrations.dynamodb import RecoveryBlocked
from recovery.evidence import whole

ANNOTATION = 'devoops/old-run-stop-verified'
DOMAIN = 'devoops.old-run-stop.v1'
ALGORITHM = 'RSASSA_PSS_SHA_256'
MAX_BYTES = 24_000


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def stop_config(value=None):
    defaults = dict(enabled=False, producer_contract_confirmed=False, signing_keys=[],
                    ec2_enabled=False, max_age_seconds=300, cluster_accounts={}, approval_bucket='')
    if value is not None and (not isinstance(value, dict) or set(value) - set(defaults)):
        raise ValueError('unknown verified_stop_input setting')
    cfg = dict(defaults, **(value or {}))
    if any(type(cfg[k]) is not bool for k in ('enabled', 'producer_contract_confirmed', 'ec2_enabled')):
        raise ValueError('stop input flags must be boolean')
    if type(cfg['max_age_seconds']) is not int or not 1 <= cfg['max_age_seconds'] <= 900:
        raise ValueError('stop approval max_age_seconds must be 1..900')
    keys = cfg['signing_keys']
    if (not isinstance(keys, list)
            or any(not isinstance(k, str) or not re.fullmatch(r'arn:aws:kms:[a-z0-9-]+:\d{12}:key/[a-zA-Z0-9-]+', k) for k in keys) or len(set(keys)) != len(keys)):
        raise ValueError('pin KMS key ARNs, not mutable aliases')
    accounts = cfg['cluster_accounts']
    if not isinstance(accounts, dict):
        raise ValueError('cluster_accounts must be a map')
    for cluster, account in accounts.items():
        if (cluster not in ('eks-a', 'eks-b', 'eks-c') or not isinstance(account, dict)
                or set(account) != {'account_id', 'region'}
                or not re.fullmatch(r'\d{12}', str(account['account_id']))
                or not isinstance(account['region'], str) or not re.fullmatch(r'[a-z]{2}-[a-z]+-\d', account['region'])):
            raise ValueError('explicit cluster account and region required')
    if cfg['enabled'] and (not cfg['producer_contract_confirmed'] or not keys or not accounts
                           or not isinstance(cfg['approval_bucket'], str)
                           or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', cfg['approval_bucket'])):
        raise ValueError('stop input requires reviewed scope/block producer and pinned keys/accounts')
    return cfg


def _require(condition, reason):
    if not condition:
        raise RecoveryBlocked(reason)


def authority(store, event):
    f = store.fencing_record(event['job_id'])
    _require(f.get('recovery_id') == event['event_id']
             and f.get('generation') == event['generation'] + 1
             and f.get('recovery_from_gen') == event['generation']
             and f.get('recovery_to_gen') == event['generation'] + 1
             and f.get('recovery_phase') in ('GENERATION_ADVANCED', 'OLD_RUN_STOPPED', 'NEW_RUN_SUBMITTED', 'RESUMING'),
             'stop evidence lost current fencing authority')
    return f


def expected_clusters(request):
    spec, status = request.get('spec', {}), request.get('status', {})
    clusters = {spec.get('sourceCluster'), status.get('sourceCluster')}
    seen = status.get('seenClusters', [])
    _require(isinstance(seen, list) and all(isinstance(c, str) for c in seen), 'invalid seen cluster scope')
    clusters.update(seen)
    clusters.discard(None)
    clusters.discard('')
    return clusters


class VerifiedStopInput:
    def __init__(self, store, kms_factory, ec2_factory, config=None, clock=time.time, approval_source=None):
        self.store, self.kms_factory, self.ec2_factory = store, kms_factory, ec2_factory
        self.config, self.clock = stop_config(config), clock
        self.approval_source = approval_source

    def _document(self, event, request, raw):
        _require(isinstance(raw, str) and len(raw.encode()) <= MAX_BYTES, 'bounded signed stop approval required')
        try:
            envelope = json.loads(raw)
            doc = envelope['document']
            key = envelope['key_id']
            signature = base64.b64decode(envelope['signature'], validate=True)
            digest = hashlib.sha256(canonical(doc)).digest()
        except (ValueError, KeyError, TypeError) as exc:
            raise RecoveryBlocked('invalid signed stop approval') from exc
        _require(key in self.config['signing_keys'], 'unapproved stop signing key')
        try:
            result = self.kms_factory(key.split(':')[3]).verify(
                KeyId=key, Message=digest, MessageType='DIGEST', Signature=signature,
                SigningAlgorithm=ALGORITHM)
        except Exception as exc:
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') == 'KMSInvalidSignatureException':
                raise RecoveryBlocked('stop approval signature invalid') from exc
            raise
        _require(result.get('SignatureValid') is True and result.get('KeyId') == key,
                 'stop approval signature invalid')
        spec, meta = request['spec'], request['metadata']
        _require(spec.get('requestId') == event['event_id']
                 and spec.get('jobRef') == f"{meta['namespace']}/job-{event['job_id']}-g{event['generation']:04d}"
                 and spec.get('toGeneration') == event['generation'] + 1
                 and spec.get('sourceCluster') == event['cluster'], 'RR stop input identity mismatch')
        placement = self.store.placement(event['job_id'])
        _require(isinstance(doc, dict) and doc.get('domain') == DOMAIN and type(doc.get('schemaVersion')) is int
                 and doc['schemaVersion'] == 1, 'stop approval domain/version mismatch')
        identity = dict(requestId=event['event_id'], jobId=event['job_id'], namespace=meta['namespace'],
                        fromGeneration=event['generation'], toGeneration=event['generation'] + 1,
                        requestUid=meta.get('uid'), sourceCluster=event['cluster'])
        _require(bool(identity['requestUid'])
                 and all(type(doc.get(k)) is int for k in ('fromGeneration', 'toGeneration'))
                 and all(doc.get(k) == v for k, v in identity.items()),
                 'stop approval request identity mismatch')
        _require(placement.get('placement_generation') == event['generation']
                 and placement.get('cluster') == event['cluster']
                 and doc.get('oldJobsetUid') == placement.get('jobset_uid')
                 and bool(doc.get('oldJobsetUid')), 'stop approval old placement mismatch')
        approved = doc.get('approvedAt')
        created = datetime.fromisoformat(meta['creationTimestamp'].replace('Z', '+00:00')).timestamp()
        _require(type(approved) is int and created - 30 <= approved <= self.clock() + 30
                 and self.clock() - approved <= self.config['max_age_seconds'], 'stop approval expired or outside RR lifetime')
        _require(doc.get('scopeComplete') is True and doc.get('recreationBlocked') is True
                 and isinstance(doc.get('blockEvidence'), str) and 1 <= len(doc['blockEvidence']) <= 2048
                 and isinstance(doc.get('reason'), str) and 1 <= len(doc['reason']) <= 1024
                 and isinstance(doc.get('actorArn'), str) and doc['actorArn'].startswith('arn:aws:'),
                 'signed complete-scope and recreation-block attestation required')
        clusters = doc.get('clusters')
        _require(isinstance(clusters, list) and all(isinstance(c, str) for c in clusters)
                 and len(set(clusters)) == len(clusters) and set(clusters) == expected_clusters(request)
                 and event['cluster'] in clusters, 'stop approval must cover all seen clusters')
        count = placement.get('gpu_count')
        _require(whole(count) and 1 <= count <= 4, 'registered GPU count required')
        participants = doc.get('participants')
        _require(isinstance(participants, list) and len(participants) == len(clusters) * count,
                 'complete participants required for each cluster')
        pod_uids, instances = set(), set()
        for cluster in clusters:
            account = self.config['cluster_accounts'].get(cluster)
            _require(account is not None, 'unconfigured cluster account')
            rows = [p for p in participants if isinstance(p, dict) and p.get('cluster') == cluster]
            _require(len(rows) == count and all(type(p.get('rank')) is int for p in rows)
                     and {p['rank'] for p in rows} == set(range(int(count))), 'full rank coverage required')
            for p in rows:
                _require(p.get('accountId') == account['account_id'] and p.get('region') == account['region']
                         and isinstance(p.get('node'), str) and bool(p['node'])
                         and isinstance(p.get('podUid'), str) and bool(p['podUid'])
                         and isinstance(p.get('instanceId'), str)
                         and re.fullmatch(r'i-(?:[0-9a-f]{8}|[0-9a-f]{17})', p['instanceId']), 'invalid participant identity')
                instance = (p['accountId'], p['region'], p['instanceId'])
                _require(p['podUid'] not in pod_uids and instance not in instances,
                         'duplicate Pod or instance in stop scope')
                pod_uids.add(p['podUid'])
                instances.add(instance)
            _require(len({p['node'] for p in rows}) == count, 'distinct participant nodes required')
            if cluster == event['cluster']:
                nodes = {p['node'] for p in rows}
                _require(nodes == set(placement.get('active_nodes', []))
                         == {n.get('name') for n in spec.get('nodes', [])}, 'old participant nodes mismatch')
        mode = doc.get('mode')
        _require(mode in ('manual', 'ec2'), 'unknown stop approval mode')
        if mode == 'manual':
            _require(doc.get('executionStopped') is True, 'manual approval must attest actual execution stop')
        else:
            _require(self.config['ec2_enabled'], 'EC2 stop verification disabled')
        return doc, key, digest.hex()

    def __call__(self, event, request):
        if not self.config['enabled']:
            return None
        try:
            return self._accept(event, request)
        except (RecoveryBlocked, ValueError, TypeError, KeyError) as exc:
            # Bad/partial approvals must not stop ordinary automatic termination checks.
            # Keep waiting; a corrected, CAS-replaced approval may be accepted later.
            authority(self.store, event)
            self.store.save(event['event_id'], stop_evidence_error=str(exc)[:256])
            return None

    def _accept(self, event, request):
        if not self.config['enabled']:
            return None
        authority(self.store, event)
        record = self.store.get(event['event_id'])
        frozen = record.get('verified_stop_evidence')
        if isinstance(frozen, str):
            frozen = json.loads(frozen)
        if frozen:
            # Accepted proof survives timeout/restart, but never a changed RR/scope/authority.
            _require(frozen.get('approval', {}).get('keyId') in self.config['signing_keys']
                     and frozen.get('requestUid') == request['metadata'].get('uid')
                     and frozen.get('requestId') == event['event_id']
                     and frozen.get('generation') == event['generation']
                     and set(frozen.get('clusters', [])) == expected_clusters(request), 'verified stop scope changed')
            return canonical(frozen).decode()
        if authority(self.store, event).get('recovery_phase') != 'GENERATION_ADVANCED':
            return None  # Ordinary automatic progress needs no new approval.
        raw = self.approval_source(event) if self.approval_source else None
        if raw is None:
            return None
        doc, key, digest = self._document(event, request, raw)
        observations = []
        if doc['mode'] == 'ec2':
            groups = {}
            for p in doc['participants']:
                groups.setdefault((p['accountId'], p['region']), []).append(p['instanceId'])
            for (account, region), ids in sorted(groups.items()):
                response = self.ec2_factory(region).describe_instances(InstanceIds=sorted(ids))
                _require(not response.get('NextToken'), 'incomplete EC2 response')
                found = {}
                for reservation in response.get('Reservations', []):
                    _require(reservation.get('OwnerId') == account, 'EC2 owner mismatch')
                    for instance in reservation.get('Instances', []):
                        iid, state = instance.get('InstanceId'), instance.get('State', {}).get('Name')
                        _require(iid in ids and iid not in found and state in ('stopped', 'terminated'),
                                 'EC2 instance not confirmed stopped/terminated')
                        found[iid] = state
                _require(set(found) == set(ids), 'missing EC2 instance is not termination evidence')
                observations.extend(dict(accountId=account, region=region, instanceId=i, state=found[i]) for i in sorted(found))
        now = int(self.clock())
        kind = doc['mode']
        proof = dict(generation=event['generation'], namespace=request['metadata']['namespace'],
            jobset_name=f"job-{event['job_id']}-g{event['generation']:04d}",
            clusters=sorted(doc['clusters']), kind=kind,
            checks=[dict(cluster=c, kind=kind, detail=doc['blockEvidence']) for c in sorted(doc['clusters'])],
            confirmed_at=datetime.fromtimestamp(now, timezone.utc).isoformat(),
            confirmed_by='kms:' + key, verificationId=digest, requestUid=request['metadata']['uid'],
            requestId=event['event_id'], jobId=event['job_id'], toGeneration=event['generation'] + 1,
            oldJobsetUid=doc['oldJobsetUid'], participants=doc['participants'],
            recreationBlocked=True, blockEvidence=doc['blockEvidence'],
            approval=dict(keyId=key, actorArn=doc['actorArn'], approvedAt=doc['approvedAt'], reason=doc['reason']),
            ec2Observations=observations)
        _require(len(canonical(proof)) <= MAX_BYTES, 'verified stop proof too large')
        authority(self.store, event)
        # CAS binds the accepted proof to the exact signed document and current event.
        frozen = self.store.freeze_stop_evidence(event, proof)
        authority(self.store, event)
        return canonical(frozen).decode()

    def validate_status(self, event, request, evidence):
        _require(self.config['enabled'], 'signed stop input disabled')
        authority(self.store, event)
        frozen = self.store.get(event['event_id']).get('verified_stop_evidence')
        if isinstance(frozen, str):
            frozen = json.loads(frozen)
        _require(isinstance(frozen, dict) and frozen.get('approval', {}).get('keyId') in self.config['signing_keys']
                 and evidence == frozen
                 and request.get('metadata', {}).get('annotations', {}).get(ANNOTATION) == canonical(frozen).decode()
                 and frozen.get('requestUid') == request['metadata'].get('uid')
                 and set(frozen.get('clusters', [])) == expected_clusters(request),
                 'status does not match controller-verified stop proof')


def approval_key(event):
    token = hashlib.sha256(event['event_id'].encode()).hexdigest()
    return f"recovery-stop-approvals/{event['job_id']}/{token}.json"


class S3StopApprovals:
    """Dedicated approval prefix: operators must not have recovery-table write access."""
    def __init__(self, s3, bucket):
        self.s3, self.bucket = s3, bucket

    def __call__(self, event):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=approval_key(event))
        except Exception as exc:
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404'):
                return None
            raise
        body = response['Body']
        try:
            raw = body.read(MAX_BYTES + 1)
        finally:
            body.close()
        _require(len(raw) <= MAX_BYTES, 'stop approval exceeds size limit')
        return raw.decode('utf-8')
