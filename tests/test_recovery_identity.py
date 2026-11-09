"""Prevent an identity migration from leaving a dangling Argo/RBAC reference."""
import copy
import importlib.util
from pathlib import Path
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('validation', ROOT/'scripts/validate.py')
validation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validation)


class RecoveryIdentityTests(unittest.TestCase):
    def setUp(self):
        self.resources = list(yaml.safe_load_all((ROOT/'onprem/apps/recovery-controller/resources.yaml').read_text()))
        self.app = yaml.safe_load((ROOT/'argocd/applications/onprem/gpu-recovery-controller.yaml').read_text())

    def test_current_contract(self):
        validation.check_recovery_identity(self.resources, self.app)

    def test_stale_rbac_subject_is_rejected(self):
        binding = next(x for x in self.resources if x['kind']=='RoleBinding' and x['metadata']['namespace']=='training')
        binding['subjects'][0]['namespace']='recovery'
        with self.assertRaises(AssertionError):
            validation.check_recovery_identity(self.resources, self.app)

    def test_partial_argo_migration_is_rejected(self):
        self.app['spec']['ignoreDifferences'][0]['namespace']='recovery'
        with self.assertRaises(AssertionError):
            validation.check_recovery_identity(self.resources, self.app)


if __name__=='__main__':
    unittest.main()
