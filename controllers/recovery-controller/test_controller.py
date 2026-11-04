import unittest
from controller import plan

class PolicyTests(unittest.TestCase):
    def event(self, kind):
        return dict(event_id='e1', job_id='j1', cluster='eks-a', fault_type=kind)
    def test_hardware_never_executes(self):
        result=plan(self.event('node_hardware'))
        self.assertEqual(result['suggested_action'], 'RELOCATE_SAME_CLUSTER')
        self.assertFalse(result['executed'])
    def test_az_requires_fencing(self):
        self.assertEqual(plan(self.event('az_failure'))['suggested_action'], 'HOLD_FOR_FENCING')
    def test_permanent_error_rejected(self):
        self.assertEqual(plan(self.event('software_permanent'))['suggested_action'], 'REJECT')
    def test_bad_event_rejected(self):
        for event in ({}, self.event('unknown') | {'cluster':'onprem'}, self.event('unknown') | {'job_id':3}):
            with self.assertRaises(ValueError): plan(event)
    def test_unknown_requires_review(self):
        self.assertEqual(plan(self.event('unknown'))['suggested_action'], 'REVIEW')


class ConfigurationTests(unittest.TestCase):
    def test_execution_cannot_be_enabled(self):
        from controller import validate_config
        with self.assertRaises(ValueError):
            validate_config(dict(schema_version=1, mode='observe', event_source='disabled', execution_enabled=True))
    def test_manual_render_requires_digest(self):
        from render import render
        with self.assertRaises(ValueError): render('ghcr.io/team/controller:latest', {})
    def test_manual_render_is_single_observer(self):
        from render import render
        config=dict(schema_version=1, mode='observe', event_source='disabled', execution_enabled=False)
        docs=render('ghcr.io/team/controller@sha256:'+'a'*64, config)
        dep=next(d for d in docs if d['kind']=='Deployment')
        self.assertEqual(dep['spec']['replicas'],1)
        self.assertEqual(dep['spec']['strategy']['type'],'Recreate')
        self.assertFalse(dep['spec']['template']['spec']['automountServiceAccountToken'])
