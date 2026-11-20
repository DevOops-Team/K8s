import copy
import unittest

from runtime.leadership import Election, LEASE_SECONDS, RENEW_SECONDS


class FakeLease:
    def __init__(self):
        self.document = {'metadata': {'resourceVersion': '0'}, 'spec': {}}

    def request(self, method, document=None):
        if method == 'GET':
            return copy.deepcopy(self.document)
        if document['metadata']['resourceVersion'] != self.document['metadata']['resourceVersion']:
            raise RuntimeError('stale resourceVersion')
        self.document = copy.deepcopy(document)
        self.document['metadata']['resourceVersion'] = str(int(document['metadata']['resourceVersion']) + 1)


class ElectionTests(unittest.TestCase):
    def test_standby_waits_for_last_observed_renewal_to_expire(self):
        lease = FakeLease()
        now = 0
        leader = Election(lease, 'pod-a', lambda: now)
        standby = Election(lease, 'pod-b', lambda: now)
        self.assertEqual(leader.step(False), RENEW_SECONDS)
        self.assertIsNone(standby.step(False))
        now = RENEW_SECONDS - 1
        self.assertEqual(leader.step(True), now + RENEW_SECONDS)
        self.assertIsNone(standby.step(False))
        now += LEASE_SECONDS - 1
        self.assertIsNone(standby.step(False))
        now += 1
        self.assertEqual(standby.step(False), now + RENEW_SECONDS)
        self.assertEqual(lease.document['spec']['holderIdentity'], 'pod-b')
        self.assertEqual(lease.document['spec']['leaseTransitions'], 2)
        with self.assertRaises(RuntimeError):
            leader.step(True)


class LeaseClientCreateTests(unittest.TestCase):
    """GPU-248: Argo CD excludes Lease by default, so the client creates it on 404."""

    def client(self, responses):
        import io
        import json
        import tempfile
        import urllib.error
        from pathlib import Path
        from unittest.mock import patch
        from runtime import leadership

        folder = Path(tempfile.mkdtemp())
        for name, text in (('namespace', 'platform'), ('token', 't'), ('ca.crt', '')):
            (folder / name).write_text(text)
        calls = []

        def urlopen(request, context=None, timeout=None):
            calls.append((request.get_method(), request.full_url))
            code, body = responses.pop(0)
            if code >= 400:
                raise urllib.error.HTTPError(request.full_url, code, 'x', {}, io.BytesIO(b'{}'))
            response = io.BytesIO(json.dumps(body).encode())
            response.__enter__ = lambda *a: response
            response.__exit__ = lambda *a: None
            return response

        patches = [patch.object(leadership, 'SERVICE_ACCOUNT', folder),
                   patch.object(leadership.ssl, 'create_default_context'),
                   patch.object(leadership.urllib.request, 'urlopen', urlopen)]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        return leadership.LeaseClient(), calls

    def test_missing_lease_is_created(self):
        created = {'metadata': {'name': 'recovery-controller', 'resourceVersion': '1'}, 'spec': {}}
        client, calls = self.client([(404, None), (201, created)])
        self.assertEqual(client.request('GET'), created)
        self.assertEqual(calls[1][0], 'POST')
        self.assertTrue(calls[1][1].endswith('/namespaces/platform/leases'))

    def test_concurrent_create_falls_back_to_get(self):
        lease = {'metadata': {'name': 'recovery-controller', 'resourceVersion': '2'}, 'spec': {}}
        client, calls = self.client([(404, None), (409, None), (200, lease)])
        self.assertEqual(client.request('GET'), lease)
        self.assertEqual([m for m, _ in calls], ['GET', 'POST', 'GET'])

    def test_put_not_found_is_not_turned_into_create(self):
        import urllib.error
        client, calls = self.client([(404, None)])
        with self.assertRaises(urllib.error.HTTPError):
            client.request('PUT', {'metadata': {}, 'spec': {}})
        self.assertEqual(len(calls), 1)
