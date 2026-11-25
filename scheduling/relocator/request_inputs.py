"""Read controller inputs from RR, never fetch DB/S3 inputs in the executor."""
import copy
import json
import time

ANNOTATION = 'devoops/recovery-inputs'
MAX_BYTES = 180_000


class InputsUnavailable(RuntimeError):
    """Wait for controller refresh; absence is never proof that a run stopped."""


class RequestStore:
    def __init__(self, rr, writer, clock=time.time):
        raw = (rr.get('metadata', {}).get('annotations') or {}).get(ANNOTATION, '')
        if not raw or len(raw.encode()) > MAX_BYTES:
            raise InputsUnavailable('controller inputs missing or oversized')
        try:
            self.data = json.loads(raw)
            d = self.data
            ref = rr['spec']['jobRef'].split('/')[-1]
            old = int(ref.rsplit('-g', 1)[1])
            fence = d['fencing']
            valid = (type(d['schema_version']) is int and d['schema_version'] == 1 and d['request_id'] == rr['spec']['requestId']
                     and ref == f"job-{d['job_id']}-g{old:04d}"
                     and type(d['observed_at']) is int
                     and -5 <= clock() - d['observed_at'] <= 60
                     and fence['recovery_id'] == d['request_id']
                     and type(fence['generation']) is int and fence['generation'] == old + 1
                     and type(fence['recovery_from_gen']) is int and fence['recovery_from_gen'] == old
                     and type(fence['recovery_to_gen']) is int and fence['recovery_to_gen'] == old + 1
                     and isinstance(d['original_jobset'], dict)
                     and d['original_jobset'].get('kind') == 'JobSet')
            if not valid:
                raise ValueError('identity/freshness mismatch')
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise InputsUnavailable('invalid or stale controller inputs') from exc
        self.writer, self.clock = writer, clock

    def _job(self, job_id):
        if job_id != self.data['job_id']:
            raise PermissionError('controller inputs belong to another job')

    def get_fencing(self, job_id):
        self._job(job_id)
        if not -5 <= self.clock() - self.data['observed_at'] <= 60:
            raise InputsUnavailable('controller inputs expired during execution')
        f = self.data['fencing']
        if not self.writer.check_authority(job_id, f['recovery_id'], f['recovery_to_gen']):
            raise PermissionError('recovery execution authority changed')
        # Atomic authority check with a no-op phase update: no stale snapshot may
        # authorize isolation/submission after the authoritative lock changed.
        if not self.writer.set_phase(job_id, f['recovery_id'], f['recovery_to_gen'],
                                     (f['recovery_phase'],), f['recovery_phase']):
            raise InputsUnavailable('fencing changed; wait for controller refresh')
        if not -5 <= self.clock() - self.data['observed_at'] <= 60:
            raise InputsUnavailable('controller inputs expired during authority check')
        return copy.deepcopy(f)

    def get_jobset(self, job_id):
        self._job(job_id)
        return copy.deepcopy(self.data['original_jobset'])

    def set_phase(self, job_id, recovery_id, to_gen, allowed_from, new_phase, extra=None):
        self._job(job_id)
        f = self.data['fencing']
        if recovery_id != f['recovery_id'] or to_gen != f['recovery_to_gen']:
            raise PermissionError('phase update belongs to another recovery')
        ok = self.writer.set_phase(job_id, recovery_id, to_gen, allowed_from, new_phase, extra)
        if ok:
            f.update(recovery_phase=new_phase, **(extra or {}))
        return ok
