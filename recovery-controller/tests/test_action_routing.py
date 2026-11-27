import copy
import hashlib
import io
import json
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError

from recovery.policy import plan, native_node_restart
from recovery.workflow import Workflow
from integrations.relocation import RelocationClient
from integrations.relocation_inputs import RelocationInputs
from integrations.dynamodb import RecoveryBlocked
from test_workflow import MemoryStore

BASE = dict(event_id='e1', job_id='j1', generation=1, cluster='eks-a', node='n1', collector_healthy=True)


class RoutingTests(unittest.TestCase):
    def test_native_node_and_pod_recommendations_restart(self):
        for source in ('node', 'pod'):
            for recommendation in ('restart_in_place', 'isolate_migrate', 'reset_then_migrate', 'evacuate_preemptive'):
                with self.subTest(source=source, recommendation=recommendation):
                    event = dict(BASE, source=source, category='infra' if source == 'node' else 'app',
                                 suggested_action=recommendation)
                    self.assertEqual(plan(event)['suggested_action'], 'RETRY_GROUP')
                    self.assertEqual(native_node_restart(event), source == 'node')

    def test_gpu_and_driver_migrations_keep_spare(self):
        for source in ('dcgm', 'dmesg', 'xid', 'gpu', 'driver'):
            event = dict(BASE, source=source, category='hw', suggested_action='isolate_migrate')
            self.assertEqual(plan(event)['suggested_action'], 'QUARANTINE_AND_RETRY')
        for xid in (48, 63, 64, 79, 119, 120):
            # An explicit GPU hardware fault wins over a generic Pod/node wrapper.
            for source in ('node', 'pod'):
                event = dict(BASE, source=source, category='hw', raw={'xid':xid}, suggested_action='restart_in_place')
                self.assertEqual(plan(event)['suggested_action'], 'QUARANTINE_AND_RETRY')
                self.assertFalse(native_node_restart(event))

    def test_gpu_application_xid_and_uuid_alone_do_not_force_migration(self):
        event = dict(BASE, source='dmesg', category='app', raw={'xid':13}, suggested_action='restart_in_place')
        self.assertEqual(plan(event)['suggested_action'], 'RETRY_GROUP')
        event.update(source='pod', gpu_uuid='GPU-123', raw={'xid':None})
        self.assertEqual(plan(event)['suggested_action'], 'RETRY_GROUP')

    def test_correlated_gpu_root_cause_wins_but_generic_node_context_does_not(self):
        event = dict(BASE, source='pod', category='hw', suggested_action='isolate_migrate',
                     correlated=[{'source':'dmesg', 'alert':'GpuXid79', 'ts':'2026-11-26T00:00:00Z'}])
        self.assertEqual(plan(event)['suggested_action'], 'QUARANTINE_AND_RETRY')
        event['correlated'] = [{'source':'node', 'alert':'NodeLost'}]
        self.assertEqual(plan(event)['suggested_action'], 'RETRY_GROUP')

    def test_stop_conditions_spot_site_and_straggler_are_not_relaxed(self):
        for source in ('node','pod','dcgm'):
            for recommendation, expected in [('hold','HOLD'),('refuse','NEEDS_CONFIG_FIX'),('none','NONE'),
                                               ('fence_evacuate','FENCE_AND_EVACUATE')]:
                event = dict(BASE, source=source, category='hw', raw={'xid':79}, suggested_action=recommendation)
                self.assertEqual(plan(event)['suggested_action'], expected)
        event = dict(BASE, source='spot', category='infra', suggested_action='restart_in_place')
        self.assertEqual(plan(event)['suggested_action'], 'QUARANTINE_AND_RETRY')
        event = dict(BASE, source='pod', category='straggler', suggested_action='evacuate_preemptive')
        self.assertEqual(plan(event)['suggested_action'], 'HOLD')
        event = dict(BASE, source='node', category='infra', collector_healthy=False, suggested_action='restart_in_place')
        self.assertEqual(plan(event)['suggested_action'], 'HOLD')

    def test_native_node_without_identity_holds(self):
        event = dict(BASE, source='node', node=None, suggested_action='isolate_migrate')
        self.assertEqual(plan(event)['suggested_action'], 'HOLD')

    def test_inflight_and_legacy_held_requests_keep_their_existing_action(self):
        event = dict(BASE, source='node', category='infra', suggested_action='restart_in_place')
        executor = Mock()
        workflow = Workflow(MemoryStore(), executor, execute=True)
        # Policy now says restart, but this claimed request was already a migration.
        record = dict(event=event, state='REQUESTED', action='QUARANTINE_AND_RETRY', relocation_request='rr')
        self.assertEqual(workflow.recovery_action(record), 'QUARANTINE_AND_RETRY')
        executor.action_for.assert_not_called()
        executor.action_for.return_value = 'moveToSpare'
        record.update(state='HOLD', action='HOLD')
        self.assertEqual(workflow.recovery_action(record), 'QUARANTINE_AND_RETRY')
        executor.action_for.assert_called_once_with(event, 'rr')


class NativeRestartRequestTests(unittest.TestCase):
    def setUp(self):
        self.event = dict(BASE, source='node', category='infra', suggested_action='isolate_migrate')
        self.store, self.s3 = Mock(), Mock()
        raw = json.dumps(dict(kind='JobSet', metadata=dict(name='job-j1-g0001'))).encode()
        self.store.fencing_record.return_value = dict(recovery_id='e1', generation=2, recovery_from_gen=1,
                                                       recovery_to_gen=2, stop_wait_deadline=1900)
        self.store.placement.return_value = dict(placement_generation=1, cluster='eks-a', active_nodes=['n1','n2'],
            gpu_count=2, jobset_uri='s3://bucket/code/j1/jobset.json', jobset_sha256=hashlib.sha256(raw).hexdigest())
        self.s3.get_object.side_effect = lambda **kw: {'Body':io.BytesIO(raw)}
        self.inputs = RelocationInputs(self.store, self.s3)
        self.saved = {}
        def transport(method, path, doc=None):
            if method == 'POST':
                if self.saved: raise HTTPError(path,409,'AlreadyExists',{},None)
                self.saved.update(copy.deepcopy(doc))
            return copy.deepcopy(self.saved)
        self.client = RelocationClient('training', transport, self.inputs)

    def test_native_node_restart_omits_pin_retains_source_and_old_scope_and_retries(self):
        self.inputs.prepare_native_restart(self.event)
        name = self.client.submit(self.event, 'restartInPlace')
        spec = self.saved['spec']
        self.assertEqual(spec['sourceCluster'], 'eks-a')
        self.assertNotIn('nodes', spec)
        self.assertNotIn('excludeNodes', spec)
        scope = json.loads(self.saved['metadata']['annotations']['devoops/recovery-old-placement'])
        self.assertEqual(scope, dict(cluster='eks-a', generation=1, nodes=[{'name':'n1'},{'name':'n2'}]))
        self.assertEqual(self.client.submit(self.event, 'restartInPlace'), name)

    def test_pod_restart_still_carries_nodes_for_existing_pin_contract(self):
        self.client.submit(dict(self.event, source='pod'), 'restartInPlace')
        self.assertEqual(self.saved['spec']['nodes'], [{'name':'n1'},{'name':'n2'}])
        self.assertNotIn('annotations', self.saved['metadata'])

    def test_gpu_migration_still_excludes_bad_node(self):
        self.store.snapshot_excluded_nodes.side_effect = lambda e, fn: fn()
        self.store.records.return_value = []
        self.client.submit(dict(self.event, source='dcgm', category='hw', raw={'xid':79}), 'moveToSpare')
        self.assertEqual(self.saved['spec']['excludeNodes'], ['n1'])
        self.assertEqual(len(self.saved['spec']['nodes']), 2)

    def test_bad_native_placement_holds_before_generation_claim(self):
        for changes in (dict(node='foreign'), dict(cluster='eks-b'), dict(generation=2)):
            event = dict(self.event, **changes)
            flow = Workflow(self.store, self.client, execute=True)
            self.store.save.side_effect = lambda rid, **v: v
            result = flow.tick(dict(event=event, state='RECEIVED'))
            self.assertEqual(result['state'], 'HOLD')
            self.store.claim.assert_not_called()
            self.s3.get_object.assert_not_called()

    def test_response_loss_does_not_accept_a_changed_old_scope_annotation(self):
        self.client.submit(self.event, 'restartInPlace')
        self.saved['metadata']['annotations']['devoops/recovery-old-placement'] = '{}'
        with self.assertRaises(ValueError): self.client.submit(self.event, 'restartInPlace')


if __name__ == '__main__': unittest.main()
