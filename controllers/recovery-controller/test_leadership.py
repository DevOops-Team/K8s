import copy
import unittest

from leadership import Election, LEASE_SECONDS, RENEW_SECONDS


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
