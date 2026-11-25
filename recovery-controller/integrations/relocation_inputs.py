"""Controller-owned DB/S3 inputs carried by the existing RelocationRequest API."""
import hashlib
import json
import re
import time
from urllib.parse import urlparse

from integrations.dynamodb import RecoveryBlocked

ANNOTATION = 'devoops/recovery-inputs'
MAX_BYTES = 180_000  # Leave room under Kubernetes' 256 KiB annotation limit.


class RelocationInputs:
    def __init__(self, store, s3, clock=time.time):
        self.store, self.s3, self.clock = store, s3, clock

    def __call__(self, event):
        fence = self.store.fencing_record(event['job_id'])
        if (fence.get('recovery_id') != event['event_id']
                or fence.get('generation') != event['generation'] + 1
                or fence.get('recovery_from_gen') != event['generation']
                or fence.get('recovery_to_gen') != event['generation'] + 1):
            raise RecoveryBlocked('relocation input fencing identity mismatch')
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
        # Do not send progress evidence, credentials, or the entire fencing item.
        keys = ('generation', 'recovery_id', 'recovery_from_gen', 'recovery_to_gen',
                'recovery_phase', 'stop_wait_started_at', 'stop_wait_deadline',
                'new_jobset_name', 'new_jobset_uid')
        result = dict(schema_version=1, job_id=event['job_id'],
                      request_id=event['event_id'], observed_at=int(self.clock()),
                      fencing={k: fence[k] for k in keys if k in fence},
                      original_jobset=original, original_sha256=digest)
        # DynamoDB numbers are Decimal. They are whole numbers in this contract.
        def number(v):
            if v != int(v):
                raise RecoveryBlocked('non-integral fencing input')
            return int(v)
        encoded = json.dumps(result, separators=(',', ':'), default=number)
        if len(encoded.encode()) > MAX_BYTES:
            raise RecoveryBlocked('RelocationRequest input exceeds annotation budget')
        return encoded
