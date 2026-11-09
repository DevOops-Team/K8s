"""Independent Alertmanager Watchdog observation. Never restarts training."""
import json
import urllib.request
from datetime import datetime


def watchdog_timestamp(alerts, now):
    if not isinstance(alerts, list):
        raise ValueError('Alertmanager response must be an array')
    timestamps = []
    for alert in alerts:
        if alert.get('labels', {}).get('alertname') != 'Watchdog':
            continue
        if alert.get('status', {}).get('state') != 'active':
            continue
        end = datetime.fromisoformat(alert['endsAt'].replace('Z', '+00:00'))
        if end.tzinfo is None:
            raise ValueError('Watchdog endsAt must include timezone')
        updated = datetime.fromisoformat(alert['updatedAt'].replace('Z', '+00:00'))
        if updated.tzinfo is None:
            raise ValueError('Watchdog updatedAt must include timezone')
        if end.timestamp() > now and updated.timestamp() <= now:
            timestamps.append(int(updated.timestamp()))
    return max(timestamps) if timestamps else None


def watchdog_active(alerts, now):
    return watchdog_timestamp(alerts, now) is not None


class Watchdog:
    def __init__(self, store, url, timeout_seconds=300, poll_seconds=30, fetch=None):
        self.store = store
        self.url = url
        self.timeout = timeout_seconds
        self.poll = poll_seconds
        self.fetch = fetch or self._fetch
        self.next_poll = 0
        self.last_seen = None
        self.last_fault = None
        self.started = None
        self.hold = False

    def _fetch(self):
        with urllib.request.urlopen(self.url, timeout=5) as response:
            return json.load(response)

    def fault_received(self, now):
        self.last_fault = now
        self.store.watchdog_save(last_fault_at=now)
        self.hold = False

    def check(self, now):
        if now < self.next_poll:
            return self.hold
        self.next_poll = now + self.poll
        previous = self.store.watchdog_state()
        self.started = previous.get('started_at', now)
        self.last_seen = previous.get('last_seen_at')
        self.last_fault = previous.get('last_fault_at')
        observed = None
        try:
            observed = watchdog_timestamp(self.fetch(), now)
        except (OSError, ValueError, KeyError, TypeError):
            # No observation is treated as silence, never as proof of a GPU failure.
            pass
        if observed is not None:
            self.last_seen = max(observed, self.last_seen or observed)
        baseline = self.last_seen if self.last_seen is not None else self.started
        silent = now - baseline >= self.timeout
        no_fault = self.last_fault is None or now - self.last_fault >= self.timeout
        self.hold = bool(silent and no_fault)
        values = dict(started_at=self.started, hold=self.hold)
        if self.last_seen is not None:
            values['last_seen_at'] = self.last_seen
        self.store.watchdog_save(**values)
        if self.hold:
            # Stable outage ID survives controller restarts. Durable outbox retries Kafka delivery.
            outage_id = 'watchdog-' + str(int(baseline))
            self.store.watchdog_notice(outage_id)
        return self.hold
