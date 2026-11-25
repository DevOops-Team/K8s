"""Create immutable ADR-19 requests on the management cluster, never worker APIs."""
import hashlib
import json
import ssl
import urllib.error
import urllib.request
from pathlib import Path

SERVICE_ACCOUNT = Path('/var/run/secrets/kubernetes.io/serviceaccount')
# relocator 2.0.0 control annotations (K8s scheduling/common/keys.py, RECOVERY_CONTRACT.md section 3)
STOP_RECORDED_ANNOTATION = 'devoops/recovery-stop-recorded'
CANCEL_ANNOTATION = 'devoops/recovery-cancel'


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
        if self.inputs:
            # ADR-50: fixed at creation (spec is immutable); relocator reads nothing else.
            spec.update(self.inputs(event))
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

    def _owned(self, event, name):
        path = (f'/apis/scheduling.devoops.site/v1alpha1/namespaces/'
                f'{self.namespace}/relocationrequests/{name}')
        existing = self.transport('GET', path)
        spec = existing.get('spec', {})
        expected_ref = f"{self.namespace}/job-{event['job_id']}-g{event['generation']:04d}"
        if spec.get('requestId') != event['event_id'] or spec.get('jobRef') != expected_ref:
            raise ValueError('cannot refresh another relocation request')
        return path, existing

    def _annotate(self, path, existing, key, value):
        """Merge-patch one control annotation; resourceVersion prevents a lost concurrent update."""
        if (existing.get('metadata', {}).get('annotations') or {}).get(key) == value:
            return False
        self.transport('PATCH', path, {'metadata': {
            'resourceVersion': existing['metadata']['resourceVersion'],
            'annotations': {key: value}}})
        return True

    def refresh(self, event, name):
        """Project RR status into fencing, then acknowledge the stop barrier (ADR-36 order)."""
        if self.status_input is None:
            return
        path, existing = self._owned(event, name)
        annotations = existing.get('metadata', {}).get('annotations') or {}
        if (self.status_input(event, existing)
                and existing.get('status', {}).get('phase') not in ('Cancelled', 'Duplicate')
                and not annotations.get(CANCEL_ANNOTATION)):
            # OLD_RUN_STOPPED is now in fencing: relocator may submit the new generation.
            self._annotate(path, existing, STOP_RECORDED_ANNOTATION, 'true')

    def cancel(self, event, name, reason):
        """Lost authority: relocator withdraws its new JobSet and ends Cancelled (Done/Failed too)."""
        path, existing = self._owned(event, name)
        annotations = existing.get('metadata', {}).get('annotations') or {}
        if annotations.get(CANCEL_ANNOTATION) or existing.get('status', {}).get('phase') in ('Duplicate', 'Cancelled'):
            return False
        return self._annotate(path, existing, CANCEL_ANNOTATION, str(reason)[:256] or 'cancelled')


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
