"""GPU-297: multiple disappearing Nodes, stale objects, and controller-only inputs."""
import builtins
import copy
import unittest
from types import SimpleNamespace
from unittest import mock

from kubernetes import client

import relocator
from test_relocator_flow import FakeCO, ctx, elapsed, on_worker, rr


def node(name, version="1", taints=()):
    return SimpleNamespace(metadata=SimpleNamespace(name=name, resource_version=version),
                           spec=SimpleNamespace(taints=list(taints)))


class NodeIsolation(unittest.TestCase):
    def context(self, names, core):
        x = ctx(FakeCO(), spec={"excludeNodes": names}, sourceCluster="worker1")
        x.workers = mock.Mock(get=mock.Mock(return_value={"worker1": object()}))
        return x

    def test_multiple_missing_nodes_do_not_skip_remaining_node_or_confirm_stop(self):
        core = mock.Mock()
        core.list_node.return_value.items = [node("live")]
        core.read_node.side_effect = [client.ApiException(status=404), node("live"), client.ApiException(status=404)]
        x = self.context(["gone-a", "live", "gone-b"], core)
        with mock.patch.object(relocator.client, "CoreV1Api", return_value=core):
            relocator.isolate(x)
        self.assertEqual(core.patch_node.call_args.args[0], "live")
        self.assertEqual(x.st["phase"], "Executing")
        self.assertNotIn("stoppedAt", x.st)
        self.assertNotIn("recoveryProgress", x.st)
        # Missing Node objects do not authorize a new generation while old Pods remain.
        with elapsed(121), on_worker(None), mock.patch.object(
                relocator, "gone_on", return_value=(False, "old Pods still Terminating")):
            relocator.stop_old(x)
        self.assertEqual(x.st["phase"], "WaitingForStop")
        self.assertEqual(x.co.created, [])

    def test_all_nodes_missing_completes_only_isolation(self):
        core = mock.Mock()
        core.list_node.return_value.items = []
        core.read_node.side_effect = client.ApiException(status=404)
        x = self.context(["gone-a", "gone-b", "gone-c"], core)
        with mock.patch.object(relocator.client, "CoreV1Api", return_value=core):
            relocator.isolate(x)
        self.assertEqual(core.read_node.call_count, 3)
        core.patch_node.assert_not_called()
        self.assertIn("isolatedAt", x.st)
        self.assertNotIn("stoppedAt", x.st)

    def test_node_disappears_between_list_and_read(self):
        core = mock.Mock()
        core.list_node.return_value.items = [node("gone")]
        core.read_node.side_effect = client.ApiException(status=404)
        x = self.context(["gone"], core)
        with mock.patch.object(relocator.client, "CoreV1Api", return_value=core):
            relocator.isolate(x)
        self.assertIn("isolatedAt", x.st)
        core.patch_node.assert_not_called()

    def test_node_disappears_before_patch_and_next_node_is_still_isolated(self):
        core = mock.Mock()
        core.list_node.return_value.items = [node("a"), node("b")]
        core.read_node.side_effect = [node("a"), node("b")]
        core.patch_node.side_effect = [client.ApiException(status=404), None]
        x = self.context(["a", "b"], core)
        with mock.patch.object(relocator.client, "CoreV1Api", return_value=core):
            relocator.isolate(x)
        self.assertEqual([c.args[0] for c in core.patch_node.call_args_list], ["a", "b"])
        self.assertIn("isolatedAt", x.st)

    def test_existing_non_gpu_node_is_not_touched(self):
        core = mock.Mock()
        core.list_node.return_value.items = []
        core.read_node.return_value = node("system")
        x = self.context(["system"], core)
        with mock.patch.object(relocator.client, "CoreV1Api", return_value=core), self.assertRaises(PermissionError):
            relocator.isolate(x)
        core.patch_node.assert_not_called()
        self.assertNotIn("isolatedAt", x.st)

    def test_read_errors_are_not_treated_as_missing(self):
        for status in (403, 500):
            with self.subTest(status=status):
                core = mock.Mock()
                core.list_node.return_value.items = [node("a")]
                core.read_node.side_effect = client.ApiException(status=status)
                x = self.context(["a"], core)
                with mock.patch.object(relocator.client, "CoreV1Api", return_value=core), self.assertRaises(client.ApiException):
                    relocator.isolate(x)
                self.assertNotIn("isolatedAt", x.st)

    def test_conflict_retries_with_new_version_and_preserves_other_taints(self):
        other = client.V1Taint(key="owner/hold", value="true", effect="NoSchedule")
        core = mock.Mock()
        core.list_node.return_value.items = [node("a")]
        core.read_node.side_effect = [node("a", "1"), node("a", "2", [other])]
        core.patch_node.side_effect = [client.ApiException(status=409), None]
        x = self.context(["a"], core)
        with mock.patch.object(relocator.client, "CoreV1Api", return_value=core):
            with self.assertRaises(client.ApiException):
                relocator.isolate(x)
            self.assertNotIn("isolatedAt", x.st)
            relocator.isolate(x)
        bodies = [c.args[1] for c in core.patch_node.call_args_list]
        self.assertEqual([b["metadata"]["resourceVersion"] for b in bodies], ["1", "2"])
        self.assertIn(other.to_dict(), bodies[1]["spec"]["taints"])


class StopBarrier(unittest.TestCase):
    def test_one_or_multiple_terminating_pods_block_resubmission(self):
        for count in (1, 2, 4):
            with self.subTest(nodes=count):
                remote = mock.Mock()
                remote.get_namespaced_custom_object.side_effect = client.ApiException(status=404)
                remote.list_namespaced_custom_object.return_value = {"items": []}
                core = mock.Mock()
                core.list_namespaced_pod.return_value.items = [
                    SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp="pending"),
                                    spec=SimpleNamespace(node_name=f"node-{i}")) for i in range(count)]
                x = ctx(FakeCO(), sourceCluster="worker1", isolatedAt="t")
                x.workers = mock.Mock(get=mock.Mock(return_value={"worker1": object()}))
                with elapsed(121), on_worker(None), mock.patch.object(relocator.kube, "kueue", return_value=remote), \
                        mock.patch.object(relocator.client, "CoreV1Api", return_value=core):
                    relocator.stop_old(x)
                self.assertEqual(x.st["phase"], "WaitingForStop")
                self.assertNotIn("stoppedAt", x.st)
                self.assertEqual(x.co.created, [])

    def test_zero_nodes_does_not_override_remaining_jobset_or_workload(self):
        for resource in ("jobsets", "workloads"):
            with self.subTest(resource=resource):
                remote = mock.Mock()
                if resource == "workloads":
                    remote.get_namespaced_custom_object.side_effect = client.ApiException(status=404)
                remote.list_namespaced_custom_object.return_value = {"items": [{"metadata": {"ownerReferences": [
                    {"kind": "JobSet", "name": "job-abc-g0001"}]}}]}
                x = ctx(FakeCO(), sourceCluster="worker1", isolatedAt="t")
                x.workers = mock.Mock(get=mock.Mock(return_value={"worker1": object()}))
                with elapsed(121), on_worker(None), mock.patch.object(relocator.kube, "kueue", return_value=remote):
                    relocator.stop_old(x)
                self.assertEqual(x.st["phase"], "WaitingForStop")
                self.assertIn("남아 있음", x.st["message"])
                self.assertEqual(x.co.created, [])

    def test_startup_and_unreachable_worker_never_import_aws_even_with_legacy_env(self):
        class EndLoop(Exception):
            pass

        real_import = builtins.__import__

        def controller_only_import(name, *args, **kwargs):
            if name.split(".")[0] in {"boto3", "botocore", "ec2_evidence"}:
                raise AssertionError(f"external input access: {name}")
            return real_import(name, *args, **kwargs)

        for mode in ("describe", "terminate"):
            with self.subTest(mode=mode):
                request = rr(acceptedAt="t", isolatedAt="t", sourceCluster="worker1")
                co = FakeCO()
                co.list_cluster_custom_object = mock.Mock(return_value={"items": [copy.deepcopy(request)]})
                elector = mock.Mock()
                elector.is_leader.return_value = True
                workers = mock.Mock(get=mock.Mock(return_value={}))
                with mock.patch.dict("os.environ", {"EC2_EVIDENCE": mode}), \
                        mock.patch("builtins.__import__", side_effect=controller_only_import), \
                        mock.patch.object(relocator.kube, "manager_client"), \
                        mock.patch.object(relocator.leader, "Elector", return_value=elector), \
                        mock.patch.object(relocator.kube, "Workers", return_value=workers), \
                        mock.patch.object(relocator.kube, "kueue", return_value=co), \
                        mock.patch.object(relocator, "SELF_FENCE_EVIDENCE", False), \
                        elapsed(121), on_worker(None), \
                        mock.patch.object(relocator.time, "sleep", side_effect=EndLoop):
                    with self.assertRaises(EndLoop):
                        relocator.main()
                self.assertEqual(co.status["phase"], "WaitingForStop")
                self.assertNotIn("stoppedAt", co.status)
                self.assertEqual(co.created, [])


if __name__ == "__main__":
    unittest.main()
