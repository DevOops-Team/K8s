"""Durable operation budgets, stage deadlines and independent result outbox."""
import json
import copy
from datetime import datetime, timezone
from urllib.error import HTTPError

DEFAULTS = dict(max_recovery_attempts=3, api_attempts=3, claim_timeout_seconds=300,
                stop_timeout_seconds=900, submit_timeout_seconds=900,
                resume_timeout_seconds=1800, kafka_backoff_seconds=5,
                kafka_max_backoff_seconds=300, job_failed_wait_seconds=300)


class Runner:
    def __init__(self, store, workflow, producer, topic, policy=None):
        self.store, self.workflow, self.producer, self.topic = store, workflow, producer, topic
        self.policy = dict(DEFAULTS, **(policy or {}))

    def hold(self, record, reason):
        print(json.dumps(dict(component='recovery', event_id=record['event']['event_id'],
                              state='HOLD', reason=reason)), flush=True)
        return self.store.save(record['event']['event_id'], state='HOLD', action='HOLD', reason=reason,
                               stopped_stage=record.get('waiting_phase', record.get('budget_stage', record['state'])))

    def lost_authority(self, record, reason='recovery authority changed'):
        """HOLD and withdraw relocator's new JobSet (ADR-50 devoops/recovery-cancel)."""
        held = self.hold(record, reason)
        request = getattr(self.workflow, 'request_cancel', None)
        return request(held, reason) if request else held

    def poll_held(self, record, now, monitoring_hold):
        # A pending cancel is retried first: a missed one would leave relocator's
        # new JobSet running without fencing authority.
        retry = getattr(self.workflow, 'retry_cancel', None)
        if retry and record.get('relocation_cancelled') is False:
            record = retry(record, now)
        # Polling is read-only and continues after HOLD. Failed reads back off;
        # they neither release HOLD nor reset an execution/API retry budget.
        if monitoring_hold or now < int(record.get('hold_poll_next_at', 0)):
            return record
        inspect = getattr(self.workflow, 'held_relocation_progress', None)
        if inspect is None:
            return record
        rid = record['event']['event_id']
        try:
            progress = inspect(record)
        except Exception as exc:
            failures = int(record.get('hold_poll_errors', 0)) + 1
            delay = min(300, 5 * 2 ** min(failures - 1, 6))
            return self.store.save(rid, hold_poll_errors=failures, hold_poll_error=type(exc).__name__,
                                   hold_poll_next_at=now + delay)
        if not progress:
            return record
        pending = copy.deepcopy(record.get('pending_results', []))
        if not record.get('published'):
            snapshot = copy.deepcopy(record)
            snapshot.pop('pending_results', None)
            pending.append(snapshot)
        phase = progress['phase']
        budget = 'OLD_RUN_STOPPED' if phase == 'OLD_RUN_STOPPED' else 'RESUME'
        audit = list(record.get('hold_resume_history', []))
        audit.append(dict(recovery_id=rid, generation=progress['generation'],
                          prior_reason=record.get('reason'), observed_phase=phase,
                          observed_at=now, checked_by='recovery-controller',
                          basis='relocator-owned fencing phase'))
        return self.store.save(rid, state='REQUESTED', action=progress['action'],
                               reason='continuing same recovery after relocator stop confirmation',
                               stage=phase, waiting_phase=phase, stopped_stage=None,
                               budget_stage=budget, stage_started_at=now, api_attempts=0,
                               hold_resume_history=audit, hold_poll_errors=0, hold_poll_next_at=0,
                               pending_results=pending, published=False, publish_attempts=0,
                               publish_next_at=0, result_occurred_at=None,
                               result_sequence=int(record.get('result_sequence', 0)) + 1)

    def operation(self, record, now, monitoring_hold):
        from recovery.workflow import TERMINAL
        rid = record['event']['event_id']
        if record['event'].get('lifecycle_type') == 'job.failed':
            from recovery.job_failed import reconcile
            return reconcile(self, record, now)
        if (record.get('relocation_request') and not getattr(self.workflow, 'execute', True)
                and (record['state'] not in TERMINAL or record['state'] == 'HOLD'
                     or (record['state'] == 'RECOVERED' and not record.get('finalized')))):
            reason = 'observe_mode_cancel'
            if record.get('relocation_cancel_reason') != reason:
                record = self.store.save(rid, state='HOLD', action='HOLD', reason=reason,
                                         relocation_cancelled=False, relocation_cancel_reason=reason,
                                         relocation_cancel_next_at=0)
            return self.workflow.retry_cancel(record, now)
        if record['state'] == 'HOLD':
            record = self.poll_held(record, now, monitoring_hold)
        finish = record['state'] == 'RECOVERED'
        if finish and record.get('finalized'):
            return record
        if record['state'] in TERMINAL and not finish and record['state'] != 'REVIEW':
            return record
        if monitoring_hold and record['state'] == 'RECEIVED':
            return record
        stage = 'FINALIZE' if finish else record.get('waiting_phase', record['state'])
        resume_stages = ('NEW_RUN_SUBMITTED', 'SUBMITTED', 'RESUMING', 'RESUME')
        if stage in resume_stages:
            stage = 'RESUME'
            if record.get('budget_stage') in resume_stages:
                record = self.store.save(rid, budget_stage='RESUME')
        if record.get('budget_stage') != stage:
            record = self.store.save(rid, budget_stage=stage, stage_started_at=now, api_attempts=0)
        limits = {'RECEIVED':'claim', 'CLAIMED':'claim', 'GENERATION_ADVANCED':'stop',
                  'OLD_RUN_STOPPED':'submit', 'NEW_RUN_SUBMITTED':'resume', 'RESUMING':'resume',
                  'REQUESTED':'stop', 'SUBMITTED':'resume', 'RESUME':'resume', 'FINALIZE':'claim'}
        timeout = self.policy[limits.get(stage, 'claim') + '_timeout_seconds']
        deadline = int(record['stage_started_at']) + timeout
        waiting_for_stop = stage in ('REQUESTED', 'GENERATION_ADVANCED')
        if not waiting_for_stop and now >= deadline:
            return self.hold(record, 'stage deadline exceeded: ' + stage)
        attempts = int(record.get('api_attempts', 0))
        if attempts >= self.policy['api_attempts']:
            return self.hold(record, 'API attempt budget exhausted: ' + stage)
        # Persist intent BEFORE any external operation. Crashes conservatively consume an attempt.
        record = self.store.save(rid, api_attempts=attempts + 1)
        try:
            if waiting_for_stop:
                from recovery.evidence import whole
                fence = self.store.fencing_record(record['event']['job_id'])
                if fence.get('recovery_id') != rid or fence.get('generation') != record.get('generation'):
                    return self.lost_authority(record)
                if fence.get('recovery_phase') not in ('OLD_RUN_STOPPED', 'NEW_RUN_SUBMITTED', 'RESUMING'):
                    if any(key in source for source in (fence, record)
                           for key in ('stop_wait_started_at', 'stop_wait_deadline')):
                        start = fence.get('stop_wait_started_at')
                        end = fence.get('stop_wait_deadline')
                        if not whole(start) or not whole(end) or start < 0 or end <= start:
                            return self.hold(record, 'invalid shared stop wait deadline')
                        deadline = int(end)
                    if now >= deadline:
                        return self.hold(record, 'stage deadline exceeded: ' + stage)
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
        changes = dict(api_attempts=0)
        if (result.get('state') == 'RESUMING' or result.get('waiting_phase') in resume_stages) and stage != 'RESUME':
            changes.update(budget_stage='RESUME', stage_started_at=now)
        return self.store.save(rid, **changes)

    def deliver(self, record, now):
        rid = record['event']['event_id']
        pending = copy.deepcopy(record.get('pending_results', []))
        for index in range(len(pending)):
            if pending[index].get('published'):
                continue
            def persist(**values):
                pending[index].update(values)
                self.store.save(rid, pending_results=pending)
                return copy.deepcopy(pending[index])
            self._deliver_result(pending[index], now, persist)
        self._deliver_result(record, now, lambda **values: self.store.save(rid, **values))

    def _deliver_result(self, record, now, persist):
        from recovery.workflow import TERMINAL
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
        record = persist(publish_attempts=attempt, publish_next_at=now + delay,
                                 result_occurred_at=record.get('result_occurred_at') or
                                 datetime.fromtimestamp(now, timezone.utc).isoformat().replace('+00:00', 'Z'))
        errors = []
        suffix = ':result' + (':' + str(int(record['result_sequence'])) if record.get('result_sequence') else '')
        payload = dict(event_id=rid + suffix, job_id=record['event'].get('job_id'),
                       state=record['state'], generation=(int(record.get('generation', record['event'].get('generation')))
                           if record.get('generation', record['event'].get('generation')) is not None else None),
                       action=record.get('action'), reason=record.get('reason'),
                       checkpoint_warning=record.get('checkpoint_warning'),
                       stage=record.get('stopped_stage', record.get('stage', record.get('waiting_phase', record.get('budget_stage', record['state'])))),
                       retry_count=int(record['retry_count']) if record.get('retry_count') is not None else None,
                       exit_code=int(record['exit_code']) if record.get('exit_code') is not None else None, failure_class=record.get('failure_class'))
        # fault.held is the classifier's input; recovery.held is this controller's decision.
        # Keep legacy flat result fields while adding the collector's event envelope.
        envelope = dict(payload, schema_version=1,
                        type='recovery.held' if record['state'] in ('HOLD', 'REVIEW') else 'recovery.decided',
                        occurred_at=record['result_occurred_at'], source='recovery-controller',
                        cluster=record['event'].get('cluster'), node=record['event'].get('node'),
                        payload=dict(payload, recovery_id=rid,
                                     source_event_id=rid,
                                     dedup_key=record['event'].get('dedup_key')))
        try:
            self.producer.produce(self.topic, key=payload['job_id'], value=json.dumps(envelope),
                                  on_delivery=lambda err, msg: errors.append(err))
            if self.producer.flush(10) or not errors or errors[0]:
                raise RuntimeError('result delivery unconfirmed')
        except Exception as exc:
            persist(publish_error=type(exc).__name__)
            return
        persist(published=True)
