"""Require direct Pod termination evidence; a 403 must not authorize resubmission."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from kubernetes import client
import relocator

class StopEvidenceTests(unittest.TestCase):
    def check(self,pods=None,error=None):
        remote=Mock()
        remote.get_namespaced_custom_object.side_effect=client.ApiException(status=404)
        remote.list_namespaced_custom_object.return_value={'items':[]}
        core=Mock()
        if error:core.list_namespaced_pod.side_effect=client.ApiException(status=error)
        else:core.list_namespaced_pod.return_value=SimpleNamespace(items=pods or [])
        ctx=SimpleNamespace(st={'sourceCluster':'eks-a'},workers=SimpleNamespace(get=lambda:{'eks-a':object()}))
        with patch.object(relocator.kube,'kueue',return_value=remote), patch.object(relocator.client,'CoreV1Api',return_value=core):
            return relocator.old_gone_on_worker(ctx,'training','job-j1-g0001')
    def test_missing_pod_read_permission_is_not_stop_evidence(self):
        self.assertFalse(self.check(error=403)[0])
    def test_live_pods_prevent_stop_confirmation(self):
        self.assertFalse(self.check(pods=[object()])[0])
    def test_absent_jobset_workload_and_pods_confirm_stop(self):
        self.assertTrue(self.check()[0])
    def test_unreachable_worker_is_not_stop_evidence(self):
        self.assertFalse(self.check(error=503)[0])
