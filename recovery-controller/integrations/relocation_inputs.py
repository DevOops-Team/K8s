"""Controller-owned DB/S3 inputs, fixed in the RelocationRequest spec (ADR-50, GPU-276).

relocator 2.0.0 never reads S3 or DynamoDB. It takes the new generation, the old-run
stop deadline and the verified original from the immutable RR spec
(K8s scheduling/relocator/RECOVERY_CONTRACT.md section 2).
"""
import hashlib
import json
import re
from urllib.parse import urlparse

from integrations.dynamodb import RecoveryBlocked
from recovery.evidence import whole

MAX_BYTES = 180_000  # Keeps the RR well under the API server's request/etcd object limits.


class RelocationInputs:
    def __init__(self, store, s3):
        self.store, self.s3 = store, s3

    def __call__(self, event):
        """Return verified recovery inputs, including the old generation placement."""
        fence = self.store.fencing_record(event['job_id'])
        if (fence.get('recovery_id') != event['event_id']
                or fence.get('generation') != event['generation'] + 1
                or fence.get('recovery_from_gen') != event['generation']
                or fence.get('recovery_to_gen') != event['generation'] + 1):
            raise RecoveryBlocked('relocation input fencing identity mismatch')
        deadline = fence.get('stop_wait_deadline')
        if not whole(deadline) or deadline <= 0:
            raise RecoveryBlocked('fencing stop_wait_deadline required for relocation')
        metadata = self.store.placement(event['job_id'])
        uri = urlparse(metadata.get('jobset_uri', ''))
        digest = metadata.get('jobset_sha256', '')
        if (uri.scheme != 's3' or not uri.netloc or uri.query or uri.fragment
                or uri.path != f"/code/{event['job_id']}/jobset.json"
                or not re.fullmatch('[0-9a-f]{64}', digest)):
            raise RecoveryBlocked('verified original JobSet URI/digest missing')
        cluster = metadata.get('cluster')
        nodes = metadata.get('active_nodes')
        gpu_count = metadata.get('gpu_count')
        if (cluster not in ('eks-a', 'eks-b', 'eks-c') or cluster != event['cluster']
                or not whole(metadata.get('placement_generation'))
                or metadata['placement_generation'] != event['generation']
                or not whole(gpu_count) or not 1 <= gpu_count <= 4
                or not isinstance(nodes, list) or len(nodes) != gpu_count
                or any(not isinstance(node, str) or not node.strip() for node in nodes)
                or len(set(nodes)) != len(nodes)):
            raise RecoveryBlocked('complete placement for the old generation required for relocation')
        body = self.s3.get_object(Bucket=uri.netloc, Key=uri.path[1:])['Body']
        try:
            raw = body.read(MAX_BYTES + 1)
        finally:
            body.close()
        if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != digest:
            raise RecoveryBlocked('original JobSet size/digest mismatch')
        original = json.loads(raw)
        if not isinstance(original, dict) or original.get('kind') != 'JobSet':
            raise RecoveryBlocked('original object must be a JobSet')
        return {'toGeneration': event['generation'] + 1,
                'stopDeadline': int(deadline),  # epoch seconds = fencing stop_wait_deadline
                'jobSet': original,
                'sourceCluster': cluster,
                'nodes': [{'name': node} for node in sorted(nodes)]}

    def prepare_failed_nodes(self, event):
        """Validate the old node placement and persist exclusions before fencing."""
        metadata = self.store.placement(event['job_id'])
        nodes = metadata.get('active_nodes')
        if (metadata.get('cluster') != event['cluster']
                or metadata.get('placement_generation') != event['generation']
                or not isinstance(nodes, list) or not nodes
                or any(not isinstance(node, str) or not node.strip() for node in nodes)
                or len(set(nodes)) != len(nodes)
                or event.get('node') not in nodes):
            raise RecoveryBlocked('fault node outside matching old placement')
        placed = [{'name': node} for node in nodes]
        return self.store.snapshot_excluded_nodes(
            event, lambda: self._collect_failed_nodes(event, placed))

    def failed_nodes(self, event, placed_nodes):
        """Use the durable snapshot, never recompute on 409/retry."""
        return self.store.snapshot_excluded_nodes(
            event, lambda: self._collect_failed_nodes(event, placed_nodes))

    def _collect_failed_nodes(self, event, placed_nodes):
        """Collect actionable hardware faults once, excluding unrelated events."""

        """Collect hardware faults already recorded for the same old placement."""
        from recovery.policy import plan
        known = {item['name'] for item in placed_nodes}
        primary = event.get('node')
        if primary not in known:
            raise RecoveryBlocked('fault node outside old placement')
        faults = {primary}
        for record in self.store.records():
            other = record.get('event', {})
            if (other.get('job_id') != event['job_id']
                    or other.get('generation') != event['generation']
                    or other.get('cluster') != event['cluster']
                    or not isinstance(other.get('node'), str)
                    or other['node'] not in known
                    or other.get('category') != 'hw'):
                continue
            try:
                if plan(other)['suggested_action'] == 'QUARANTINE_AND_RETRY':
                    faults.add(other['node'])
            except ValueError:
                continue
        return sorted(faults)
