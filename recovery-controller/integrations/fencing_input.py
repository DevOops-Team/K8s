"""Optional IAM-trusted fencing progress projection. Producer/IAM contract is not deployed.

Only reads an explicitly agreed attribute in the fencing item, never Kafka claims.
No network endpoint, no training/placement evidence fabrication, no generation writes.
"""
from decimal import Decimal
from recovery.evidence import whole


def input_config(value=None):
    value = {} if value is None else value
    if not isinstance(value, dict) or set(value) - {'enabled','producer_contract_confirmed','attribute'}:
        raise ValueError('unknown fencing_progress_input setting')
    config = dict(enabled=False, producer_contract_confirmed=False,
                  attribute='recovery_training_evidence')
    config.update(value)
    if any(type(config[k]) is not bool for k in ('enabled','producer_contract_confirmed')):
        raise ValueError('fencing progress flags must be boolean')
    attribute = config['attribute']
    if not isinstance(attribute, str) or not attribute.startswith('recovery_training_') or not attribute.isidentifier():
        raise ValueError('dedicated recovery_training_* attribute required')
    if config['enabled'] and not config['producer_contract_confirmed']:
        raise ValueError('review producer conditional-write and IAM contract before enabling fencing input')
    return config


def normalise(value):
    if isinstance(value, Decimal):
        if not whole(value):
            raise ValueError('non-integral evidence number')
        return int(value)
    if isinstance(value, dict):
        return {key:normalise(item) for key,item in value.items()}
    if isinstance(value, list):
        return [normalise(item) for item in value]
    return value


class FencingProgressInput:
    def __init__(self, store, writer, config=None):
        self.store, self.writer, self.config = store, writer, input_config(config)

    def ingest(self, record):
        event = record['event']
        fence = self.store.fencing_record(event['job_id'])  # Store uses ConsistentRead.
        message = fence.get(self.config['attribute'])
        if message is None:
            return False
        if not isinstance(message, dict):
            raise ValueError('fencing training evidence must be a Map')
        message = normalise(message)
        if (message.get('kind') != 'progress' or message.get('job_id') != event['job_id']
                or message.get('recovery_id') != event['event_id']
                or message.get('generation') != record.get('generation')
                or message.get('generation') != fence.get('generation')
                or message.get('recovery_id') != fence.get('recovery_id')
                or not fence.get('owner') or message.get('cluster') != fence['owner']
                or message.get('jobset_uid') != fence.get('new_jobset_uid')):
            raise ValueError('training evidence must match current owner, recovery, generation and JobSet')
        # Producer identity is a configured IAM/attribute-writer contract, never a body field.
        # The sidecar must conditionally write the entire Map for generation/owner/recovery_id.
        # EvidenceWriter rereads fencing before projecting and rejects stale timestamps/baselines.
        return self.writer.write(message, authenticated_producer='training')

    def reconcile(self, record, now):
        if (not self.config['enabled'] or record.get('state') not in ('REQUESTED','SUBMITTED','RESUMING')
                or not record['event'].get('job_id') or not whole(record.get('generation'))
                or now < int(record.get('evidence_input_next_at',0))):
            return record
        rid = record['event']['event_id']
        try:
            self.ingest(record)
        except Exception as exc:
            failures = int(record.get('evidence_input_errors',0)) + 1
            delay = min(300,5 * 2 ** min(failures-1,6))
            return self.store.save(rid,evidence_input_errors=failures,
                                   evidence_input_error=type(exc).__name__,evidence_input_next_at=now+delay)
        return self.store.save(rid,evidence_input_errors=0,evidence_input_error=None,evidence_input_next_at=0)
