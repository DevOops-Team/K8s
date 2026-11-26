"""클러스터 없이 OFF/ON 판정과 ACK 대기를 확인한다."""
import contextlib
from datetime import datetime, timezone
import io
from types import SimpleNamespace as Obj
import unittest
from unittest.mock import Mock, patch

from kubernetes import client

import eks_gate as G


class GateTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.capture = contextlib.redirect_stdout(self.output)
        self.capture.__enter__()
        self.addCleanup(self.capture.__exit__, None, None, None)
        self.manager = Mock()
        self.manager.get_cluster_custom_object.return_value = {
            "metadata": {}, "status": {"conditions": [{"type": "Active", "status": "True"}]}}
        self.pages = {"workloads": [], G.K.RR_PLURAL: []}
        self.manager.list_namespaced_custom_object.side_effect = lambda g, v, ns, p, **kw: {"items": self.pages[p]}
        self.co, self.core, self.apps = Mock(), Mock(), Mock()
        self.apps.read_namespaced_deployment.return_value = Obj(status=Obj(available_replicas=1))
        self.core.read_namespaced_config_map.return_value = Obj(data={"CLUSTER_NAME": "worker3"})
        self.core.list_namespaced_pod.return_value = Obj(items=[
            Obj(spec=Obj(priority_class_name=G.K.PLACEHOLDER_PRIORITY_CLASS), status=Obj(phase="Running"))])
        self.core.list_node.return_value = Obj(items=[])
        self.gate = G.Gate(self.manager, (self.co, self.core, self.apps))

    def workload(self, **status):
        w = {"metadata": {"name": "wl", "ownerReferences": [{"kind": "JobSet", "name": "job"}]},
             "status": status}
        self.pages["workloads"] = [w]
        return w

    def test_off_workload_blockers(self):
        for status, reserved, text in (
                ({"clusterName": "worker3"}, False, "실행 중"),
                ({"nominatedClusterNames": ["worker3"]}, False, "보내는 중"),
                ({}, True, "자리 예약")):
            with self.subTest(text=text):
                w = self.workload(**status)
                if reserved:
                    w["metadata"]["annotations"] = {G.K.ANN_RESERVED: "worker3"}
                self.assertEqual(self.gate.off_check("worker3"), 1)
                self.assertIn(text, self.output.getvalue())

    def test_off_rr_source_and_target(self):
        for field in ("sourceCluster", "targetCluster"):
            with self.subTest(field=field):
                self.pages[G.K.RR_PLURAL] = [{"metadata": {"name": "rr"},
                                              "status": {"phase": "Accepted", field: "worker3"}}]
                self.assertEqual(self.gate.off_check("worker3"), 1)

    def test_off_rr_without_phase(self):
        for st in (None, {}, {"sourceCluster": "worker1"}):
            with self.subTest(status=st):
                self.pages[G.K.RR_PLURAL] = [{"metadata": {"name": "new"}, "status": st}]
                self.assertEqual(self.gate.off_check("worker3"), 1)
        self.assertIn("처리 전 RR: new (대상 미정)", self.output.getvalue())

    def test_off_finished_workload(self):
        self.workload(clusterName="worker3", conditions=[{"type": "Finished", "status": "True"}])
        self.assertEqual(self.gate.off_check("worker3"), 0)

    def test_off_final_rr(self):
        for phase in G.FINAL:
            self.pages[G.K.RR_PLURAL] = [{"metadata": {"name": phase},
                                          "status": {"phase": phase, "sourceCluster": "worker3"}}]
            self.assertEqual(self.gate.off_check("worker3"), 0)

    def test_off_other_cluster(self):
        w = self.workload(clusterName="worker1", nominatedClusterNames=["worker2"])
        w["metadata"]["annotations"] = {G.K.ANN_RESERVED: "worker1"}
        self.pages[G.K.RR_PLURAL] = [{"metadata": {"name": "rr"},
                                      "status": {"phase": "Accepted", "targetCluster": "worker1"}}]
        self.assertEqual(self.gate.off_check("worker3"), 0)

    def test_off_empty(self):
        self.assertEqual(self.gate.off_check("worker3"), 0)

    def test_off_lists_all_blockers(self):
        w = self.workload(clusterName="worker3", nominatedClusterNames=["worker3"])
        w["metadata"]["annotations"] = {G.K.ANN_RESERVED: "worker3"}
        self.pages[G.K.RR_PLURAL] = [{"metadata": {"name": "new"}}]
        self.assertEqual(self.gate.off_check("worker3"), 1)
        for text in ("실행 중", "보내는 중", "자리 예약", "처리 전 RR"):
            self.assertIn(text, self.output.getvalue())

    def test_off_pagination(self):
        self.manager.list_namespaced_custom_object.side_effect = [
            {"items": [], "metadata": {"continue": "next"}},
            {"items": [{"metadata": {"name": "late"}, "status": {"clusterName": "worker3"}}]},
            {"items": []}]
        self.assertEqual(self.gate.off_check("worker3"), 1)
        self.assertEqual(self.manager.list_namespaced_custom_object.call_args_list[1].kwargs["_continue"], "next")

    def fence_run(self, values, timeout=10):
        def mkc(value):
            return {"metadata": {"labels": {G.K.LABEL_FENCED: "true"},
                                 "annotations": {G.K.ANN_FENCED_ACK: value}}}
        self.manager.get_cluster_custom_object.side_effect = [mkc(None)] + [mkc(v) for v in values]
        with patch.object(G, "datetime", wraps=datetime) as clock, patch.object(G.time, "sleep") as sleep, \
                patch.object(G.time, "monotonic", side_effect=range(100)), \
                patch.object(self.gate, "off_check", return_value=1) as off:
            clock.now.return_value = datetime(2026, 11, 11, tzinfo=timezone.utc)
            result = self.gate.fence("worker3", timeout)
        return result, sleep, off

    def test_fence_equal_ack_waits_then_runs_off(self):
        result, sleep, off = self.fence_run(["2026-11-11T00:00:00Z", "2026-11-11T00:00:01Z"])
        self.assertEqual(result, 1)
        sleep.assert_called_once_with(2)
        off.assert_called_once_with("worker3")
        self.manager.patch_cluster_custom_object.assert_not_called()

    def test_fence_timeout_keeps_label(self):
        self.manager.get_cluster_custom_object.return_value = {"metadata": {}}
        with patch.object(G.time, "monotonic", side_effect=[0, 1]), patch.object(G.time, "sleep"):
            self.assertEqual(self.gate.fence("worker3", 1), 2)
        calls = self.manager.patch_cluster_custom_object.call_args_list
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].args[4], {"metadata": {"labels": {G.K.LABEL_FENCED: "true"}, "annotations": {
            G.K.ANN_FENCED_BY: G.K.FENCED_BY_EKS_OFF, G.K.ANN_FENCED_WORKER: None}}})
        self.assertIn("디스패처 로그 확인", self.output.getvalue())

    def test_fence_marks_eks_off_with_resource_version_and_retries_conflict(self):
        # GPU-277: 읽은 뒤 relocator가 펜싱하면 409 -> 다시 읽고, relocator 펜싱이면 표시하지 않는다
        self.manager.get_cluster_custom_object.side_effect = [
            {"metadata": {"resourceVersion": "5"}},
            {"metadata": {"resourceVersion": "6", "labels": {G.K.LABEL_FENCED: "true"},
                          "annotations": {G.K.ANN_FENCED_BY: G.K.FENCED_BY_RELOCATOR}}}]
        self.manager.patch_cluster_custom_object.side_effect = client.ApiException(status=409)
        self.gate.mark_eks_off("worker3")
        calls = self.manager.patch_cluster_custom_object.call_args_list
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].args[4]["metadata"]["resourceVersion"], "5")
        self.assertIn("이미 펜싱됨 (relocator)", self.output.getvalue())

    def test_fence_keeps_existing_marks(self):
        for ann, text in (({G.K.ANN_FENCED_BY: G.K.FENCED_BY_EKS_OFF}, None), ({}, "붙인 쪽 표시 없음")):
            with self.subTest(ann=ann):
                self.manager.get_cluster_custom_object.return_value = {
                    "metadata": {"labels": {G.K.LABEL_FENCED: "true"}, "annotations": ann}}
                self.output.seek(0)
                self.output.truncate(0)
                self.gate.mark_eks_off("worker3")
                self.manager.patch_cluster_custom_object.assert_not_called()
                if text:
                    self.assertIn(text, self.output.getvalue())
                else:
                    self.assertEqual(self.output.getvalue(), "")

    def test_fence_conflict_gives_up_after_attempts(self):
        self.manager.get_cluster_custom_object.return_value = {"metadata": {"resourceVersion": "5"}}
        self.manager.patch_cluster_custom_object.side_effect = client.ApiException(status=409)
        with self.assertRaises(client.ApiException):
            self.gate.mark_eks_off("worker3")
        self.assertEqual(self.manager.patch_cluster_custom_object.call_count, 3)

    def test_report_auto_unfence(self):
        for ann, text in (({G.K.ANN_FENCED_BY: G.K.FENCED_BY_EKS_OFF, G.K.ANN_FENCED_WORKER: "u"}, "자동 해제 기준 기록됨"),
                          ({G.K.ANN_FENCED_BY: G.K.FENCED_BY_EKS_OFF}, "on-check --unfence로 지운다"),
                          ({G.K.ANN_FENCED_BY: G.K.FENCED_BY_RELOCATOR}, "")):
            with self.subTest(ann=ann):
                self.output.seek(0)
                self.output.truncate(0)
                G.Gate.report_auto_unfence({"metadata": {"annotations": ann}})
                if text:
                    self.assertIn(text, self.output.getvalue())
                else:
                    self.assertEqual(self.output.getvalue(), "")

    def test_fence_bad_ack_waits(self):
        result, sleep, _ = self.fence_run(["bad", "2026-11-11T00:00:01Z"])
        self.assertEqual(result, 1)
        sleep.assert_called_once()
        self.assertIn("아직 없음", self.output.getvalue())

    def test_parse_ack(self):
        for value in (None, "", "bad", "2026-11-11", "2026-11-11T00:00:00", "2026-99-11T00:00:00Z"):
            self.assertIsNone(G.parse_ack(value))
        self.assertEqual(G.parse_ack("2026-11-11T09:00:01+09:00"), G.parse_ack("2026-11-11T00:00:01Z"))

    def test_on_success_unfence(self):
        self.assertEqual(self.gate.on_check("worker3", unfence=True), 0)
        self.assertEqual(self.manager.patch_cluster_custom_object.call_args.args[4],
                         {"metadata": {"labels": {G.K.LABEL_FENCED: None},
                                       "annotations": {G.K.ANN_FENCED_BY: None, G.K.ANN_FENCED_WORKER: None}}})

    def test_on_success_without_unfence(self):
        self.assertEqual(self.gate.on_check("worker3"), 0)
        self.manager.patch_cluster_custom_object.assert_not_called()

    def test_on_each_missing_resource(self):
        # 후보가 여럿인 것(Deployment 이름 · relocator SA)은 후보가 전부 없을 때만 없음
        resources = [(self.apps.read_namespaced_deployment, set(names)) for _, names in G.DEPLOYMENTS]
        resources += [(self.co.get_cluster_custom_object, {name}) for name in G.CLUSTER_QUEUES]
        resources += [(self.co.get_namespaced_custom_object, {name}) for name in G.LOCAL_QUEUES]
        resources += [(self.core.read_namespaced_config_map, {G.CLUSTER_INFO}),
                      (self.core.read_namespaced_service_account, {name for _, name in G.RELOCATORS}),
                      (self.manager.get_cluster_custom_object, {"worker3"})]
        for method, missing in resources:
            with self.subTest(missing=sorted(missing)):
                original = method.return_value

                def read(*args, **kwargs):
                    name = args[3] if method in (self.co.get_cluster_custom_object,
                                                 self.manager.get_cluster_custom_object) else (
                        args[4] if method == self.co.get_namespaced_custom_object else args[0])
                    if name in missing:
                        raise client.ApiException(status=404)
                    return original

                method.side_effect = read
                self.assertEqual(self.gate.on_check("worker3", unfence=True), 1)
                self.manager.patch_cluster_custom_object.assert_not_called()
                method.side_effect = None

    def test_on_unready_values(self):
        for kind in ("deployment", "identity", "spare", "active"):
            with self.subTest(kind=kind):
                if kind == "deployment":
                    obj = self.apps.read_namespaced_deployment.return_value.status
                    attr, bad = "available_replicas", 0
                elif kind == "identity":
                    obj = self.core.read_namespaced_config_map.return_value
                    attr, bad = "data", {"CLUSTER_NAME": "worker1"}
                elif kind == "spare":
                    obj = self.core.list_namespaced_pod.return_value
                    attr, bad = "items", []
                else:
                    obj = self.manager.get_cluster_custom_object
                    attr, bad = "return_value", {"metadata": {}}
                original = getattr(obj, attr)
                setattr(obj, attr, bad)
                self.assertEqual(self.gate.on_check("worker3", unfence=True), 1)
                self.manager.patch_cluster_custom_object.assert_not_called()
                setattr(obj, attr, original)

    def test_on_no_spare(self):
        self.core.list_namespaced_pod.return_value = Obj(items=[])
        self.assertEqual(self.gate.on_check("worker3"), 1)
        self.core.list_namespaced_pod.reset_mock()
        self.assertEqual(self.gate.on_check("worker3", no_spare=True), 0)
        self.core.list_namespaced_pod.assert_not_called()

    def test_on_deployment_alternative_names(self):
        # EKS Helm 차트는 jobset-controller, kind upstream manifest는 jobset-controller-manager (GPU-236)
        for present in ("jobset-controller", "jobset-controller-manager"):
            with self.subTest(present=present):
                def read(name, ns, **kwargs):
                    if ns == "jobset-system" and name != present:
                        raise client.ApiException(status=404)
                    return Obj(status=Obj(available_replicas=1))
                self.apps.read_namespaced_deployment.side_effect = read
                self.assertEqual(self.gate.on_check("worker3"), 0)

    def test_on_unready_alternative_deployment(self):
        def read(name, ns, **kwargs):
            if name == "jobset-controller-manager":
                raise client.ApiException(status=404)
            return Obj(status=Obj(available_replicas=0 if name == "jobset-controller" else 1))
        self.apps.read_namespaced_deployment.side_effect = read
        self.assertEqual(self.gate.on_check("worker3", unfence=True), 1)
        self.assertIn("준비 안 됨: Deployment jobset-system/jobset-controller", self.output.getvalue())
        self.manager.patch_cluster_custom_object.assert_not_called()

    def test_on_relocator_namespace_candidates(self):
        # EKS는 IaC가 kueue-system/relocator를 만든다 (GPU-236)
        for present in ("kueue-system", "platform"):
            with self.subTest(present=present):
                def read(name, ns, **kwargs):
                    if ns != present:
                        raise client.ApiException(status=404)
                    return Obj()
                self.core.read_namespaced_service_account.side_effect = read
                self.assertEqual(self.gate.on_check("worker3"), 0)

    def node(self, ready="True", unschedulable=None, taints=("spare",)):
        keys = {"spare": G.K.TAINT_SPARE, "faulty": G.K.TAINT_FAULTY, "gpu": "nvidia.com/gpu"}
        return Obj(spec=Obj(unschedulable=unschedulable, taints=[Obj(key=keys[t]) for t in taints]),
                   status=Obj(conditions=[Obj(type="Ready", status=ready)]))

    def test_on_spare_node_without_placeholder(self):
        # EKS: 자리 표시 Pod 없이 예비 taint 노드그룹 (ADR-25)
        self.core.list_namespaced_pod.return_value = Obj(items=[])
        self.core.list_node.return_value = Obj(items=[self.node(taints=("gpu",)), self.node(taints=("gpu", "spare"))])
        self.assertEqual(self.gate.on_check("worker3", unfence=True), 0)
        self.assertEqual(self.core.list_node.call_args.kwargs["label_selector"], G.K.GPU_NODE_SELECTOR)

    def test_on_spare_node_not_usable(self):
        self.core.list_namespaced_pod.return_value = Obj(items=[])
        for case, node in (("not ready", self.node(ready="False")), ("cordoned", self.node(unschedulable=True)),
                           ("faulty", self.node(taints=("spare", "faulty"))), ("no spare taint", self.node(taints=("gpu",)))):
            with self.subTest(case=case):
                self.core.list_node.return_value = Obj(items=[node])
                self.assertEqual(self.gate.on_check("worker3", unfence=True), 1)
                self.manager.patch_cluster_custom_object.assert_not_called()

    def test_main_connection_and_missing_mkc(self):
        for error in (G.config.ConfigException("bad context"), client.ApiException(status=404),
                      client.ApiException(status=403), G.HTTPError("offline")):
            with self.subTest(error=error), patch.object(G.config, "new_client_from_config", side_effect=error), \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(G.main(["off-check", "worker3"]), 2)

    def test_main_context_options_and_defaults(self):
        for argv, expected in ((["on-check", "worker3"], ["kind-manager", "kind-worker3"]),
                               (["--manager-context", "mgr", "on-check", "worker3", "--worker-context", "wrk"],
                                ["mgr", "wrk"])):
            with patch.object(G.config, "new_client_from_config") as connect, \
                    patch.object(G.Gate, "on_check", return_value=0):
                self.assertEqual(G.main(argv), 0)
                self.assertEqual([c.kwargs["context"] for c in connect.call_args_list], expected)


if __name__ == "__main__":
    unittest.main()
