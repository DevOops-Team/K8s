"""Register recovery job metadata from job.lifecycle (GPU-276; producers GPU-271, GPU-275).

The Backend metadata API is gone (GPU-246). The data owners now publish what the
recovery table `job#<id>` needs:

  job.submitted (source backend)  payload jobset_uri, jobset_sha256, gpus
                                  -> the submitted original (S3 object + hash) and GPU count
  job.placement (job-watcher)     payload placement_cluster, active_nodes, placement_generation,
                                  observed_at, jobset_uid (manager JobSet metadata.uid)
                                  -> where the current generation actually runs

A placement of a recovery-made generation (payload recovery_id) is not registration:
it is placement evidence for COMPLETED and is only accepted when evidence_policy
placement_producers lists "job-watcher" (the topic carries no producer authentication).

Malformed or conflicting messages never overwrite stored data. They are logged and
committed so one bad message cannot stop the shared job.lifecycle consumer.
"""
import json
import re
from urllib.parse import urlsplit

from integrations.dynamodb import RecoveryBlocked

CLUSTERS = ('eks-a', 'eks-b', 'eks-c')
WATCHER = 'job-watcher'


def _log(**fields):
    print(json.dumps(dict(component='job-registration', **fields), ensure_ascii=False), flush=True)


def _identity(envelope):
    job_id, generation = envelope.get('job_id'), envelope.get('generation')
    if (type(envelope.get('schema_version')) is not int or envelope['schema_version'] != 1
            or not isinstance(job_id, str) or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', job_id)
            or len(job_id) > 230 or type(generation) is not int or generation < 1
            or not isinstance(envelope.get('event_id'), str) or not envelope['event_id']):
        raise ValueError('job_id, generation, event_id and schema_version 1 required')
    payload = envelope.get('payload')
    if not isinstance(payload, dict):
        raise ValueError('payload required')
    return job_id, generation, payload


def submitted_original(envelope):
    """Backend job.submitted -> registration fields, or None when it carries no original hash."""
    job_id, generation, payload = _identity(envelope)
    if envelope.get('source') != 'backend' or 'jobset_sha256' not in payload:
        return None  # job-watcher resubmissions and older Backend images carry no hash.
    digest, uri, gpus = payload.get('jobset_sha256'), payload.get('jobset_uri'), payload.get('gpus')
    parts = urlsplit(uri if isinstance(uri, str) else '')
    if (parts.scheme != 's3' or not parts.netloc or parts.username or parts.password
            or parts.path != f'/code/{job_id}/jobset.json' or parts.query or parts.fragment):
        raise ValueError('canonical s3://<bucket>/code/<job_id>/jobset.json URI required')
    if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
        raise ValueError('lowercase hex jobset_sha256 required')
    if type(gpus) is not int or not 1 <= gpus <= 4:
        raise ValueError('gpus must be 1..4')
    if payload.get('jobset_name') != f'job-{job_id}-g{generation:04d}':
        raise ValueError('jobset_name must match job_id and generation')
    return dict(job_id=job_id, jobset_uri=uri, jobset_sha256=digest, gpu_count=gpus,
                event_id=envelope['event_id'])


def actual_placement(envelope):
    """job-watcher job.placement -> placement fields (validated like the dispatcher annotation)."""
    job_id, generation, payload = _identity(envelope)
    if envelope.get('source') != WATCHER:
        raise ValueError('job.placement is a job-watcher event')
    nodes, cluster = payload.get('active_nodes'), payload.get('placement_cluster')
    uid, observed = payload.get('jobset_uid'), payload.get('observed_at')
    if payload.get('placement_generation') != generation:
        raise ValueError('placement_generation must equal the envelope generation')
    if payload.get('jobset_name') != f'job-{job_id}-g{generation:04d}':
        raise ValueError('jobset_name must match job_id and generation')
    if cluster not in CLUSTERS:
        raise ValueError('placement_cluster must be a known worker cluster')
    if (not isinstance(nodes, list) or not nodes or len(set(nodes)) != len(nodes)
            or any(not isinstance(n, str) or not n.strip() or len(n) > 253 for n in nodes)):
        raise ValueError('distinct active_nodes required')
    if not isinstance(uid, str) or not uid.strip() or len(uid) > 128:
        raise ValueError('manager jobset_uid required')
    if type(observed) is not int or observed <= 0:
        raise ValueError('observed_at must be Unix seconds')
    recovery_id = payload.get('recovery_id')
    if recovery_id is not None and (not isinstance(recovery_id, str) or not recovery_id):
        raise ValueError('recovery_id must be a non-empty string')
    return dict(job_id=job_id, generation=generation, cluster=cluster, active_nodes=sorted(nodes),
                jobset_uid=uid, observed_at=observed, recovery_id=recovery_id,
                event_id=envelope['event_id'])


class JobRegistry:
    def __init__(self, store, evidence_writer=None, evidence_policy=None):
        from recovery.evidence import policy_config
        self.store, self.writer = store, evidence_writer
        self.policy = policy_config(evidence_policy)

    def ingest(self, envelope):
        """Return what happened. Never raises for bad content (logged, then committed)."""
        kind = envelope.get('type')
        try:
            if kind == 'job.submitted':
                fields = submitted_original(envelope)
                if fields is None:
                    return 'ignored'
                return 'registered' if self.store.register_original(**fields) else 'conflict'
            if kind == 'job.placement':
                fields = actual_placement(envelope)
                if fields['recovery_id']:
                    return self.recovery_placement(fields)
                return 'registered' if self.store.register_placement(**fields) else 'stale'
        except (ValueError, RecoveryBlocked) as exc:
            _log(event_id=envelope.get('event_id'), type=kind, result='rejected', reason=str(exc)[:256])
            return 'rejected'
        return 'ignored'

    def recovery_placement(self, fields):
        if self.writer is None or WATCHER not in self.policy['placement_producers']:
            return 'ignored'  # Not a trusted placement producer in this deployment.
        message = dict(schema_version=1, kind='placement', job_id=fields['job_id'],
                       recovery_id=fields['recovery_id'], jobset_uid=fields['jobset_uid'],
                       event_id=fields['event_id'], generation=fields['generation'],
                       cluster=fields['cluster'], active_nodes=fields['active_nodes'],
                       observed_at=fields['observed_at'])
        return 'evidence' if self.writer.write(message, authenticated_producer=WATCHER) else 'stale'
