"""Git 상태 변경과 실제 Pod 확인, Argo revision 검증을 확인한다."""
from types import SimpleNamespace as O
import unittest
from unittest.mock import Mock, patch

from kubernetes import client

import eks_spare as S


class SpareTests(unittest.TestCase):
    def setUp(self):
        self.core, self.apps = Mock(), Mock()
        self.core.read_namespaced_config_map.return_value = O(data={"CLUSTER_NAME": "eks-a"})
        self.dep = O(spec=O(replicas=1), metadata=O(generation=2),
                     status=O(observed_generation=2, replicas=1, available_replicas=1))
        self.apps.read_namespaced_deployment.return_value = self.dep
        self.pod = O(spec=O(priority_class_name=S.G.K.PLACEHOLDER_PRIORITY_CLASS),
                     metadata=O(labels={"app": S.DEPLOYMENT}, deletion_timestamp=None),
                     status=O(phase="Running", conditions=[O(type="Ready", status="True")]))
        self.core.list_namespaced_pod.return_value = O(items=[self.pod])

    def ready(self, state="on", cluster="eks-a"):
        return S.ready(self.core, self.apps, state, cluster)

    def test_on_ready(self):
        self.assertTrue(self.ready())

    def test_on_not_ready_or_terminating(self):
        self.pod.status.phase = "Pending"
        self.assertFalse(self.ready())
        self.pod.status.phase = "Running"
        self.pod.status.conditions = []
        self.assertFalse(self.ready())
        self.pod.status.conditions = [O(type="Ready", status="True")]
        self.pod.metadata.deletion_timestamp = "now"
        self.assertFalse(self.ready())

    def test_on_requires_available_replica(self):
        self.dep.status.available_replicas = 0
        self.assertFalse(self.ready())

    def test_stale_rollout_or_wrong_cluster(self):
        self.assertFalse(self.ready(cluster="eks-b"))
        self.dep.status.observed_generation = 1
        self.assertFalse(self.ready())

    def test_off_waits_for_last_pod(self):
        self.assertFalse(self.ready("off"))
        self.dep.spec.replicas = 0
        self.dep.status.replicas = 0
        self.pod.metadata.deletion_timestamp = "now"
        self.assertFalse(self.ready("off"))
        self.core.list_namespaced_pod.return_value.items = []
        self.assertTrue(self.ready("off"))
        self.dep.status.replicas = 1
        self.assertFalse(self.ready("off"))

    def test_off_counts_unlabelled_placeholder(self):
        self.dep.spec.replicas = 0
        self.dep.status.replicas = 0
        self.pod.metadata.labels = {}
        self.assertFalse(self.ready("off"))

    def test_missing_resource_waits_but_forbidden_raises(self):
        self.core.read_namespaced_config_map.side_effect = client.ApiException(status=404)
        self.assertFalse(self.ready())
        self.core.read_namespaced_config_map.side_effect = client.ApiException(status=403)
        with self.assertRaises(client.ApiException):
            self.ready()

    def test_timeout_does_not_change_cluster(self):
        with patch.object(S.config, "new_client_from_config"), \
                patch.object(S, "ready", return_value=False), \
                patch.object(S.time, "monotonic", side_effect=[0, 10]):
            self.assertEqual(S.wait_ready("worker", "off", 5, "eks-a"), 2)

    def test_cli_success(self):
        with patch.object(S.config, "new_client_from_config"), patch.object(S, "ready", return_value=True):
            self.assertEqual(S.main(["eks-a", "on", "--worker-context", "eks-context"]), 0)


if __name__ == "__main__":
    unittest.main()
