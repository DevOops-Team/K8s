"""Authenticated, bounded Backend lookup for fault-triggered job registration."""
import json
import os
import re
import time
from urllib.parse import urlsplit, quote
from urllib.request import Request, build_opener, HTTPRedirectHandler
from integrations.dynamodb import RecoveryBlocked


def metadata_config(value=None):
    config = dict(enabled=False, base_url='', token_env='BACKEND_RECOVERY_API_TOKEN',
                  timeout_seconds=5, max_age_seconds=300, future_skew_seconds=30)
    if value is None:
        return config
    if not isinstance(value, dict) or set(value) - set(config):
        raise ValueError('unknown backend_metadata setting')
    config.update(value)
    if type(config['enabled']) is not bool:
        raise ValueError('backend_metadata.enabled must be boolean')
    for key, maximum in (('timeout_seconds', 10), ('max_age_seconds', 3600), ('future_skew_seconds', 60)):
        if type(config[key]) is not int or not 1 <= config[key] <= maximum:
            raise ValueError('invalid backend_metadata ' + key)
    if not isinstance(config['token_env'], str) or not re.fullmatch('[A-Z][A-Z0-9_]*', config['token_env']):
        raise ValueError('backend_metadata requires a token environment variable name')
    if config['enabled']:
        url = urlsplit(config['base_url'])
        if (url.scheme != 'https' or not url.hostname or url.username or url.password
                or url.path not in ('', '/') or url.query or url.fragment):
            raise ValueError('backend_metadata requires an HTTPS origin without credentials')
    return config


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward service credentials or silently change the trusted origin.


class BackendMetadata:
    def __init__(self, config, clock=time.time):
        self.config, self.clock = metadata_config(config), clock
        self.opener = build_opener(NoRedirect())

    def fetch(self, event):
        token = os.environ.get(self.config['token_env'], '')
        if not token or any(c in token for c in '\r\n'):
            raise RecoveryBlocked('backend metadata API credential missing or invalid')
        job_id = event['job_id']
        url = (self.config['base_url'].rstrip('/') + '/internal/v1/recovery/jobs/'
               + quote(job_id, safe='') + '?generation=' + str(event['generation']))
        request = Request(url, headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json'})
        # No internal retry: Runner persists intent and owns the three-attempt budget.
        with self.opener.open(request, timeout=self.config['timeout_seconds']) as response:
            if response.status != 200:
                raise RecoveryBlocked('backend metadata API response must be 200')
            raw = response.read(65537)
        if len(raw) > 65536:
            raise RecoveryBlocked('backend metadata API response too large')
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise RecoveryBlocked('backend metadata API JSON invalid') from exc
        return self.validate(body, event)

    def validate(self, body, event):
        def reject(message):
            raise RecoveryBlocked('backend metadata ' + message)
        if not isinstance(body, dict) or type(body.get('schema_version')) is not int or body['schema_version'] != 1:
            reject('schema_version must be 1')
        generation = body.get('generation')
        if (body.get('job_id') != event['job_id'] or type(generation) is not int
                or generation != event['generation'] or generation < 1):
            reject('job/generation mismatch')
        # API must distinguish genuine failure from user stop, completion, or deletion.
        if body.get('recovery_eligible') is not True:
            reject('job is not eligible for automatic recovery')
        if body.get('cluster') not in ('eks-a', 'eks-b', 'eks-c') or body['cluster'] != event['cluster']:
            reject('source cluster mismatch')
        if type(body.get('gpu_count')) is not int or not 1 <= body['gpu_count'] <= 4:
            reject('GPU count must be 1..4')
        nodes = body.get('active_nodes')
        if (not isinstance(nodes, list) or len(nodes) != body['gpu_count']
                or any(not isinstance(n, str) or not n.strip() or len(n) > 253 for n in nodes)
                or len(set(nodes)) != len(nodes) or (event.get('node') and event['node'] not in nodes)):
            reject('complete distinct source nodes required')
        if type(body.get('placement_generation')) is not int or body['placement_generation'] != generation:
            reject('placement generation mismatch')
        name = 'job-' + event['job_id'] + '-g' + f'{generation:04d}'
        if body.get('jobset_name') != name:
            reject('manager JobSet name mismatch')
        if not isinstance(body.get('jobset_uid'), str) or not body['jobset_uid'].strip() or len(body['jobset_uid']) > 128:
            reject('manager JobSet UID required')
        uri = urlsplit(body.get('jobset_uri', '') if isinstance(body.get('jobset_uri'), str) else '')
        if (uri.scheme != 's3' or not uri.netloc or uri.username or uri.password
                or uri.path != '/code/' + event['job_id'] + '/jobset.json' or uri.query or uri.fragment):
            reject('canonical S3 JobSet URI required')
        digest = body.get('jobset_sha256')
        if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
            reject('JobSet SHA-256 required')
        now, observed = int(self.clock()), body.get('observed_at')
        if (type(observed) is not int or observed <= 0 or now - observed > self.config['max_age_seconds']
                or observed - now > self.config['future_skew_seconds']):
            reject('placement observation expired or invalid')
        if type(body.get('allow_restart_from_scratch', True)) is not bool:
            reject('invalid from-scratch policy')
        # Explicit allowlist: API cannot inject progress, recovery_id, owner or approval fields.
        keys = ('job_id', 'jobset_uri', 'jobset_sha256', 'gpu_count', 'cluster', 'active_nodes',
                'placement_generation', 'jobset_name', 'jobset_uid', 'observed_at')
        return dict({k: body[k] for k in keys},
                    allow_restart_from_scratch=body.get('allow_restart_from_scratch', True))
