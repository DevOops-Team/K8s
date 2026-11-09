"""Create immutable ADR-19 requests on the management cluster, never worker APIs."""
import hashlib
import json
import ssl
import urllib.error
import urllib.request
from pathlib import Path

SERVICE_ACCOUNT = Path('/var/run/secrets/kubernetes.io/serviceaccount')


class RelocationClient:
    def __init__(self, namespace, transport=None):
        self.namespace = namespace
        self.transport = transport or self._request

    def _request(self, method, path, document=None):
        token = (SERVICE_ACCOUNT / 'token').read_text().strip()
        tls = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / 'ca.crt'))
        request = urllib.request.Request(
            'https://kubernetes.default.svc' + path,
            data=None if document is None else json.dumps(document).encode(),
            method=method,
            headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, context=tls, timeout=10) as response:
            return json.load(response)

    def submit(self, event, action):
        # Stable across response loss and leader failover. No mutable status is consulted.
        name = 'recovery-' + hashlib.sha256(event['event_id'].encode()).hexdigest()[:40]
        path = (f'/apis/scheduling.devoops.site/v1alpha1/namespaces/'
                f'{self.namespace}/relocationrequests')
        reason = {'hw':'hardware', 'app':'software', 'site':'az', 'straggler':'straggler'}
        legacy = {'node_hardware':'hardware', 'software_transient':'software',
                  'spot_interruption':'spot', 'az_failure':'az'}
        category = reason.get(event.get('category'), legacy.get(event.get('fault_type'), 'unknown'))
        if event.get('source') == 'spot':
            category = 'spot'
        spec = {'requestId': event['event_id'],
                'jobRef': f"{self.namespace}/job-{event['job_id']}-g{event['generation']:04d}",
                'action': action, 'reason': category}
        if action in ('moveToSpare', 'moveToOtherCluster'):
            spec['excludeNodes'] = [event['node']]
        if action in ('moveToOtherCluster', 'evacuateCluster'):
            spec['excludeClusters'] = [event['cluster']]
        document = {'apiVersion': 'scheduling.devoops.site/v1alpha1',
                    'kind': 'RelocationRequest',
                    'metadata': {'name': name, 'namespace': self.namespace}, 'spec': spec}
        try:
            self.transport('POST', path, document)
        except urllib.error.HTTPError as exc:
            if exc.code != 409:
                raise
            existing = self.transport('GET', path + '/' + name)
            if existing.get('spec') != spec:
                raise ValueError('RelocationRequest name belongs to a different request') from exc
        return name
