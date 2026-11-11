"""Elect a single Kafka recovery worker using a namespaced Kubernetes Lease."""
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

LEASE_SECONDS = 30
RENEW_SECONDS = 12
RETRY_SECONDS = 2
SERVICE_ACCOUNT = Path('/var/run/secrets/kubernetes.io/serviceaccount')


class LeaseClient:
    def __init__(self):
        namespace = (SERVICE_ACCOUNT / 'namespace').read_text().strip()
        self.url = (f'https://kubernetes.default.svc/apis/coordination.k8s.io/v1'
                    f'/namespaces/{namespace}/leases/recovery-controller')
        self.tls = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / 'ca.crt'))

    def request(self, method, document=None):
        token = (SERVICE_ACCOUNT / 'token').read_text().strip()
        body = None if document is None else json.dumps(document).encode()
        request = urllib.request.Request(self.url, data=body, method=method,
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, context=self.tls, timeout=3) as response:
            return json.load(response)


class Election:
    def __init__(self, client, identity, clock=time.monotonic):
        self.client = client
        self.identity = identity
        self.clock = clock
        self.observed = None
        self.observed_at = None

    def step(self, leading):
        """Return the renewal deadline after a successful conditional update."""
        lease = self.client.request('GET')
        spec = lease.setdefault('spec', {})
        holder = spec.get('holderIdentity')
        revision = (holder, spec.get('renewTime'), spec.get('leaseTransitions'))
        now = self.clock()
        if revision != self.observed:
            self.observed, self.observed_at = revision, now
        if leading and holder != self.identity:
            raise RuntimeError('recovery lease was taken over')
        if not leading and holder and now - self.observed_at < max(
                LEASE_SECONDS, spec.get('leaseDurationSeconds', LEASE_SECONDS)):
            return None
        timestamp = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
        spec.update(holderIdentity=self.identity, leaseDurationSeconds=LEASE_SECONDS, renewTime=timestamp)
        if not leading:
            spec.update(acquireTime=timestamp, leaseTransitions=spec.get('leaseTransitions', 0) + 1)
        # PUT carries the GET resourceVersion, so concurrent contenders cannot both acquire it.
        self.client.request('PUT', lease)
        return now + RENEW_SECONDS


def run_elected(run, config, stop, health, identity):
    election = Election(LeaseClient(), identity)
    deadline = None
    worker = None
    worker_failed = threading.Event()

    def watch_lease():
        while True:
            current = deadline
            if current is not None and time.monotonic() >= current:
                # End the entire process, including a worker blocked in external I/O.
                os._exit(1)
            time.sleep(0.2)

    def work():
        try:
            run(config, stop, health)
        finally:
            if not stop.is_set():
                worker_failed.set()

    threading.Thread(target=watch_lease, daemon=True).start()
    try:
        while not stop.is_set():
            if worker_failed.is_set() or (worker is not None and not worker.is_alive()):
                raise RuntimeError('leader recovery worker stopped')
            try:
                renewed = election.step(worker is not None)
                health['lease_connected'] = True
            except (urllib.error.URLError, OSError) as exc:
                health['lease_connected'] = False
                print(json.dumps({'component': 'leader-election', 'error_type': type(exc).__name__}), flush=True)
                renewed = None
            if renewed is not None:
                if time.monotonic() >= renewed or (deadline is not None and time.monotonic() >= deadline):
                    os._exit(1)
                deadline = renewed
                if worker is None and not stop.is_set():
                    health['leader'] = True
                    worker = threading.Thread(target=work, daemon=True)
                    worker.start()
            stop.wait(RETRY_SECONDS)
    finally:
        stop.set()
        health['leader'] = False
        if worker is not None:
            worker.join(timeout=5)
