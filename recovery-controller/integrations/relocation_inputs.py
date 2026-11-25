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
        """Return the spec fields toGeneration, stopDeadline and jobSet for this recovery."""
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
        # Only the three fields relocator needs; no fencing snapshot, progress or credentials.
        return {'toGeneration': event['generation'] + 1,
                'stopDeadline': int(deadline),  # epoch seconds = fencing stop_wait_deadline
                'jobSet': original}
