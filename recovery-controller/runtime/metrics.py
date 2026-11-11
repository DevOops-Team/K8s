"""Prometheus counters for durable recovery decisions."""
from prometheus_client import Counter

HOLD_TOTAL = Counter(
    'recovery_hold_total',
    'Recovery events entering HOLD',
    ('job_id', 'reason'),
)

REJECTED_TOTAL = Counter(
    'recovery_rejected_total',
    'Recovery events entering REJECTED',
    ('job_id', 'reason'),
)

_REJECTED_REASONS = {
    'NEEDS_CONFIG_FIX': 'classifier_refuse',
    'init configuration mismatch requires correction': 'config_mismatch',
    'init fencing registration missing; do not create it': 'registration_missing',
}


def record_rejected(job_id, reason):
    if (reason or '').startswith('automatic recovery limit reached ('):
        code = 'retry_limit_reached'
    else:
        code = _REJECTED_REASONS.get(reason, 'other')
    REJECTED_TOTAL.labels(job_id=job_id or '', reason=code).inc()


_REASON_PREFIXES = {
    'stage deadline exceeded:': 'stage_timeout',
    'API attempt budget exhausted:': 'api_attempts_exhausted',
    'operation error:': 'operation_error',
    'init generation mismatch': 'generation_mismatch',
    'init owner claim denied': 'owner_conflict',
    'selected checkpoint quarantined': 'checkpoint_corrupt',
    'trusted restore/training failure:': 'restore_or_training_failed',
}

_REASON_CODES = {
    'HOLD': 'classifier_hold',
    'verified current init JobSet identity required': 'init_jobset_unverified',
    'failed checkpoint does not match verified restore baseline': 'restore_baseline_mismatch',
    'job identity unresolved': 'job_identity_unresolved',
    'recovery authority changed': 'authority_changed',
    'unexpected fencing phase': 'unexpected_fencing_phase',
    'new owner violates relocation action': 'invalid_relocation_owner',
    'watchdog_missing_and_no_gpu_fault': 'watchdog_missing_and_no_gpu_fault',
}


def record_hold(job_id, reason):
    code = _REASON_CODES.get(reason)
    if code is None:
        code = next((value for prefix, value in _REASON_PREFIXES.items()
                     if (reason or '').startswith(prefix)), 'other')
    HOLD_TOTAL.labels(job_id=job_id or '', reason=code).inc()
