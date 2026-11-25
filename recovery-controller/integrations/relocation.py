"""Create immutable ADR-19 requests on the management cluster, never worker APIs."""
import hashlib
import json
import ssl
import urllib.error
import urllib.request
from pathlib import Path

SERVICE_ACCOUNT = Path('/var/run/secrets/kubernetes.io/serviceaccount')


class RelocationClient:
    def __init__(self, namespace, transport=None, inputs=None, status_input=None):
        self.namespace = namespace
        self.transport = transport or self._request
        self.inputs = inputs
        self.status_input = status_input

    def _request(self, method, path, document=None):
        token = (SERVICE_ACCOUNT / 'token').read_text().strip()
        tls = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / 'ca.crt'))
        request = urllib.request.Request(
            'https://kubernetes.default.svc' + path,
            data=None if document is None else json.dumps(document).encode(),
            method=method,
            headers={'Authorization': 'Bearer ' + token, 'Content-Type':
                     'application/merge-patch+json' if method == 'PATCH' else 'application/json'})
        with urllib.request.urlopen(request, context=tls, timeout=10) as response:
            return json.load(response)

    def submit(self, event, action):
        if action not in ('restartInPlace', 'moveToSpare', 'evacuateCluster'):
            raise ValueError(f'unsupported relocation action: {action}')
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
        if action == 'moveToSpare':
            spec['excludeNodes'] = [event['node']]
        if action == 'evacuateCluster':
            spec['excludeClusters'] = [event['cluster']]
        document = {'apiVersion': 'scheduling.devoops.site/v1alpha1',
                    'kind': 'RelocationRequest',
                    'metadata': {'name': name, 'namespace': self.namespace}, 'spec': spec}
        if self.inputs:
            from integrations.relocation_inputs import ANNOTATION
            document['metadata']['annotations'] = {ANNOTATION: self.inputs(event)}
        try:
            self.transport('POST', path, document)
        except urllib.error.HTTPError as exc:
            if exc.code != 409:
                raise
            existing = self.transport('GET', path + '/' + name)
            if existing.get('spec') != spec:
                raise ValueError('RelocationRequest name belongs to a different request') from exc
            if self.inputs:
                self.transport('PATCH', path + '/' + name, {'metadata': {
                    'resourceVersion': existing['metadata']['resourceVersion'],
                    'annotations': document['metadata']['annotations']}})
        return name

    def refresh(self, event, name):
        """Refresh only this request; resourceVersion prevents a lost concurrent update."""
        if self.inputs is None and self.status_input is None:
            return
        from integrations.relocation_inputs import ANNOTATION
        path = (f'/apis/scheduling.devoops.site/v1alpha1/namespaces/'
                f'{self.namespace}/relocationrequests/{name}')
        existing = self.transport('GET', path)
        spec = existing.get('spec', {})
        expected_ref = f"{self.namespace}/job-{event['job_id']}-g{event['generation']:04d}"
        if spec.get('requestId') != event['event_id'] or spec.get('jobRef') != expected_ref:
            raise ValueError('cannot refresh another relocation request')
        if self.status_input:
            self.status_input(event, existing)
        if existing.get('status', {}).get('phase') in ('Done', 'Failed', 'Duplicate'):
            return
        if self.inputs is None:
            return
        self.transport('PATCH', path, {'metadata': {
            'resourceVersion': existing['metadata']['resourceVersion'],
            'annotations': {ANNOTATION: self.inputs(event)}}})


class DispatcherCapacity:
    def __init__(self, url):
        if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            raise ValueError('dispatcher_capacity_url must be an HTTP(S) URL')
        self.url = url

    def check(self, event, gpu_count):
        payload = {key: event[key] for key in ('job_id', 'generation', 'cluster', 'node')}
        payload['gpu_count'] = gpu_count
        request = urllib.request.Request(self.url, data=json.dumps(payload).encode(),
                                         headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=8) as response:
            result = json.loads(response.read(4096))
        if not isinstance(result, dict) or type(result.get('available')) is not bool:
            raise ValueError('invalid dispatcher response')
        return result['available']
