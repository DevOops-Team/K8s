"""Durable operation budgets, stage deadlines and independent result outbox."""
import json
from urllib.error import HTTPError

DEFAULTS = dict(max_recovery_attempts=3, api_attempts=3, claim_timeout_seconds=300,
                stop_timeout_seconds=900, submit_timeout_seconds=900,
                resume_timeout_seconds=1800, kafka_backoff_seconds=5,
                kafka_max_backoff_seconds=300)


class Runner:
    def __init__(self, store, workflow, producer, topic, policy=None):
        self.store, self.workflow, self.producer, self.topic = store, workflow, producer, topic
        self.policy = dict(DEFAULTS, **(policy or {}))

    def hold(self, record, reason):
        print(json.dumps(dict(component='recovery', event_id=record['event']['event_id'],
                              state='HOLD', reason=reason)), flush=True)
        return self.store.save(record['event']['event_id'], state='HOLD', action='HOLD', reason=reason)

    def operation(self, record, now, monitoring_hold):
        from workflow import TERMINAL
        rid = record['event']['event_id']
        finish = record['state'] == 'RECOVERED'
        if finish and record.get('finalized'):
            return record
        if record['state'] in TERMINAL and not finish and record['state'] != 'REVIEW':
            return record
        if monitoring_hold and record['state'] == 'RECEIVED':
            return record
        stage = 'FINALIZE' if finish else record.get('waiting_phase', record['state'])
        if record.get('budget_stage') != stage:
            record = self.store.save(rid, budget_stage=stage, stage_started_at=now, api_attempts=0)
        limits = {'RECEIVED':'claim', 'CLAIMED':'claim', 'GENERATION_ADVANCED':'stop',
                  'OLD_RUN_STOPPED':'submit', 'NEW_RUN_SUBMITTED':'resume', 'RESUMING':'resume',
                  'REQUESTED':'stop', 'SUBMITTED':'resume', 'FINALIZE':'claim'}
        timeout = self.policy[limits.get(stage, 'claim') + '_timeout_seconds']
        if now - int(record['stage_started_at']) >= timeout:
            return self.hold(record, 'stage deadline exceeded: ' + stage)
        attempts = int(record.get('api_attempts', 0))
        if attempts >= self.policy['api_attempts']:
            return self.hold(record, 'API attempt budget exhausted: ' + stage)
        # Persist intent BEFORE any external operation. Crashes conservatively consume an attempt.
        record = self.store.save(rid, api_attempts=attempts + 1)
        try:
            if finish:
                self.store.finish(record)
                result = self.store.save(rid, finalized=True)
            else:
                result = self.workflow.tick(record, monitoring_hold=monitoring_hold)
        except Exception as exc:
            code = getattr(exc, 'response', {}).get('Error', {}).get('Code', '')
            transient = ((isinstance(exc, (TimeoutError, ConnectionError, OSError)) and not isinstance(exc, HTTPError))
                         or type(exc).__name__ in ('EndpointConnectionError', 'ReadTimeoutError', 'ConnectTimeoutError')
                         or code in ('ConditionalCheckFailedException', 'ThrottlingException',
                                     'ProvisionedThroughputExceededException', 'RequestLimitExceeded',
                                     'InternalServerError', 'ServiceUnavailable')
                         or getattr(exc, 'code', 0) in (408, 409, 429, 500, 502, 503, 504))
            if not transient or attempts + 1 >= self.policy['api_attempts']:
                return self.hold(record, ('API attempt budget exhausted: ' if transient else 'operation error: ')
                                 + stage + ' (' + type(exc).__name__ + ')')
            return self.store.save(rid, last_api_error=type(exc).__name__)
        # Successful status polling is not an error retry and does not slide the deadline.
        return self.store.save(rid, api_attempts=0)

    def deliver(self, record, now):
        from workflow import TERMINAL
        if record['state'] not in TERMINAL or record.get('published'):
            return
        if record['state'] == 'RECOVERED' and not record.get('finalized'):
            return
        if now < int(record.get('publish_next_at', 0)):
            return
        rid = record['event']['event_id']
        attempt = int(record.get('publish_attempts', 0)) + 1
        delay = min(self.policy['kafka_max_backoff_seconds'],
                    self.policy['kafka_backoff_seconds'] * 2 ** min(attempt - 1, 20))
        # Keep durable outbox even if delivery succeeds but its acknowledgement is lost.
        self.store.save(rid, publish_attempts=attempt, publish_next_at=now + delay)
        errors = []
        payload = dict(event_id=rid + ':result', job_id=record['event'].get('job_id'),
                       state=record['state'], generation=record.get('generation', record['event'].get('generation')),
                       action=record.get('action'), reason=record.get('reason'),
                       checkpoint_warning=record.get('checkpoint_warning'))
        try:
            self.producer.produce(self.topic, key=payload['job_id'], value=json.dumps(payload),
                                  on_delivery=lambda err, msg: errors.append(err))
            if self.producer.flush(10) or not errors or errors[0]:
                raise RuntimeError('result delivery unconfirmed')
        except Exception as exc:
            self.store.save(rid, publish_error=type(exc).__name__)
            return
        self.store.save(rid, published=True)
