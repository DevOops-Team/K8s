import unittest
from urllib.error import HTTPError
from integrations.relocation import RelocationClient

EVENT = dict(event_id='event/123', job_id='3f9a1c2e', generation=1,
             cluster='eks-a', node='bad-node', category='hw')


class RequestTests(unittest.TestCase):
    def test_crd_contract_and_response_loss(self):
        saved = {}
        calls = []
        def transport(method, path, document=None):
            calls.append((method, path))
            if method == 'POST':
                if saved:
                    raise HTTPError(path, 409, 'AlreadyExists', {}, None)
                saved.update(document)
                return document
            return saved
        client = RelocationClient('training', transport)
        name = client.submit(EVENT, 'moveToSpare')
        self.assertEqual(client.submit(EVENT, 'moveToSpare'), name)
        self.assertEqual(saved['spec'], {
            'requestId':'event/123', 'jobRef':'training/job-3f9a1c2e-g0001',
            'action':'moveToSpare', 'reason':'hardware', 'excludeNodes':['bad-node']})
        self.assertNotIn('annotations', saved['metadata'])
        self.assertEqual(calls[-1][0], 'GET')
    def test_conflicting_existing_request_rejected(self):
        def transport(method, path, document=None):
            if method == 'POST':
                raise HTTPError(path, 409, 'AlreadyExists', {}, None)
            return {'spec': {'requestId':'other'}}
        with self.assertRaises(ValueError):
            RelocationClient('training', transport).submit(EVENT, 'restartInPlace')
    def test_evacuation_excludes_old_cluster_without_claiming_stop(self):
        saved = []
        client = RelocationClient('training', lambda m,p,d=None: saved.append(d))
        client.submit(dict(EVENT,node=None,category='site'), 'evacuateCluster')
        self.assertEqual(saved[0]['spec']['excludeClusters'], ['eks-a'])
        self.assertNotIn('devoops/old-execution-stopped', str(saved))

    def test_preemptive_request_excludes_only_the_retiring_node(self):
        from recovery.policy import plan
        from recovery.workflow import RELOCATION_ACTIONS
        event = dict(EVENT, source='spot', suggested_action='evacuate_preemptive')
        saved = []
        action = RELOCATION_ACTIONS[plan(event)['suggested_action']]
        RelocationClient('training', lambda m,p,d=None: saved.append(d)).submit(event, action)
        self.assertEqual(saved[0]['spec']['action'], 'moveToSpare')
        self.assertEqual(saved[0]['spec']['reason'], 'spot')
        self.assertEqual(saved[0]['spec']['excludeNodes'], ['bad-node'])
        self.assertNotIn('excludeClusters', saved[0]['spec'])
        self.assertNotIn('annotations', saved[0]['metadata'])
