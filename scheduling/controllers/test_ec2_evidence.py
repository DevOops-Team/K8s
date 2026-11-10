"""python -m unittest -v test_ec2_evidence.py  (AWS 없이 가짜 EC2 클라이언트로)
워커 API가 끊겼을 때 EC2 인스턴스 상태를 옛 실행 종료 증거로 쓰는 규칙."""
import unittest
from types import SimpleNamespace
from unittest import mock

import ec2_evidence as E
import relocator

GOOD = {"eks:eks-cluster-name": "devoops-eks-a", "karpenter.sh/nodepool": "training"}


class FakeEc2:
    """instances: {id: state} 또는 {id: (state, tags)}. 태그 필터·instance-id 필터를 실제처럼 적용"""

    def __init__(self, instances, fail=None):
        self.inst = {i: (v if isinstance(v, tuple) else (v, dict(GOOD))) for i, v in instances.items()}
        self.fail, self.terminated, self.filters = fail, [], []

    def get_paginator(self, _):
        return self

    def paginate(self, Filters):
        if self.fail:
            raise self.fail
        self.filters.append(Filters)

        def match(i, tags):
            for f in Filters:
                if f["Name"] == "instance-id" and i not in f["Values"]:
                    return False
                if f["Name"].startswith("tag:") and tags.get(f["Name"][4:]) not in f["Values"]:
                    return False
            return True
        inst = [{"InstanceId": i, "State": {"Name": s}, "Tags": [{"Key": k, "Value": v} for k, v in t.items()]}
                for i, (s, t) in self.inst.items() if match(i, t)]
        return [{"Reservations": [{"Instances": inst}]}]

    def set(self, i, state):
        self.inst[i] = (state, self.inst[i][1])

    def terminate_instances(self, InstanceIds):
        self.terminated += InstanceIds


def ev(instances, **kw):
    return E.Ec2Evidence(FakeEc2(instances, **kw), {"worker1": "devoops-eks-a"}, ["training", "spare"], terminate=True)


class CheckTests(unittest.TestCase):
    def test_provider_id(self):
        self.assertEqual(E.instance_id("aws:///us-east-2a/i-0abc"), "i-0abc")
        self.assertIsNone(E.instance_id("kind://docker/kind/node"))
        self.assertIsNone(E.instance_id(None))

    def test_whole_pool_dead_is_evidence(self):
        self.assertTrue(ev({"i-1": "terminated", "i-2": "stopped"}).check("worker1")[0])

    def test_any_alive_or_shutting_down_is_not_evidence(self):
        self.assertFalse(ev({"i-1": "terminated", "i-9": "pending"}).check("worker1")[0])
        self.assertFalse(ev({"i-1": "shutting-down"}).check("worker1")[0])

    def test_no_instances_found_is_not_evidence(self):
        self.assertFalse(ev({}).check("worker1")[0])
        other = {"eks:eks-cluster-name": "other", "karpenter.sh/nodepool": "training"}
        self.assertFalse(ev({"i-1": ("running", other)}).check("worker1")[0])

    def test_ec2_error_is_not_evidence(self):
        self.assertFalse(ev({}, fail=RuntimeError("AccessDenied")).check("worker1")[0])

    def test_filters_use_karpenter_tags_and_cluster_mapping(self):
        e = ev({"i-1": "terminated"})
        e.check("worker1")
        f = {x["Name"]: x["Values"] for x in e.ec2.filters[0]}
        self.assertEqual(f["tag:eks:eks-cluster-name"], ["devoops-eks-a"])
        self.assertEqual(f["tag:karpenter.sh/nodepool"], ["training", "spare"])

    def test_terminate_only_recorded_live_instances(self):
        e = ev({"i-1": "running", "i-2": "terminated", "i-3": "running"})
        self.assertEqual(e.terminate("worker1", ["i-1", "i-2"]), ["i-1"])
        self.assertEqual(e.ec2.terminated, ["i-1"])

    def test_terminate_looks_up_by_id_only_and_refuses_wrong_tags(self):
        bad = {"eks:eks-cluster-name": "devoops-eks-a", "karpenter.sh/nodepool": "system"}
        e = ev({"i-1": "running", "i-2": ("running", bad)})
        with self.assertRaises(PermissionError):
            e.terminate("worker1", ["i-1", "i-2"])
        self.assertEqual(e.ec2.terminated, [])
        self.assertEqual([f["Name"] for f in e.ec2.filters[0]], ["instance-id"])

    def test_terminate_never_cluster_wide_or_in_describe_mode(self):
        self.assertEqual(ev({"i-1": "running"}).terminate("worker1", []), [])
        e = ev({"i-1": "running"})
        e.can_terminate = False
        self.assertEqual(e.terminate("worker1", ["i-1"]), [])

    def test_from_env_default_off(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertIsNone(E.from_env())


class StopPathTests(unittest.TestCase):
    """relocator.ec2_stop_evidence: 워커에 닿지 않을 때만, 증거는 클러스터 전체 기준"""

    def ctx(self, ec2, **st):
        saved = {}
        st = {"sourceCluster": "worker1", "isolatedAt": "t", **st}
        return SimpleNamespace(ec2=ec2, st=st, save=lambda **kw: (saved.update(kw), st.update(kw))), saved

    def test_off_keeps_unreachable(self):
        x, _ = self.ctx(None)
        self.assertFalse(relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))[0])

    def test_dead_pool_confirms_stop(self):
        x, _ = self.ctx(ev({"i-1": "terminated"}), oldNodes=[{"node": "n1", "instanceId": "i-1"}])
        ok, why = relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))
        self.assertTrue(ok)
        self.assertIn("모두 꺼짐", why)

    def test_recorded_nodes_dead_but_other_node_alive_is_not_evidence(self):
        # 망 분리 중 JobSet이 Pod를 기록에 없는 i-2에 다시 띄웠을 수 있음
        x, _ = self.ctx(ev({"i-1": "terminated", "i-2": "running"}), oldNodes=[{"node": "n1", "instanceId": "i-1"}])
        with mock.patch.object(relocator.kube, "since", return_value=0):
            self.assertFalse(relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))[0])

    def test_terminates_once_after_wait_then_confirms_when_pool_dead(self):
        e = ev({"i-1": "running"})
        x, saved = self.ctx(e, oldNodes=[{"node": "n1", "instanceId": "i-1"}])
        with mock.patch.object(relocator.kube, "since", return_value=relocator.STOP_WAIT_SECONDS + 1):
            self.assertFalse(relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))[0])
            self.assertEqual(e.ec2.terminated, ["i-1"])
            self.assertIn("ec2TerminatedAt", saved)
            relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))
            self.assertEqual(e.ec2.terminated, ["i-1"])   # 두 번 안 내림
        e.ec2.set("i-1", "terminated")
        self.assertTrue(relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))[0])

    def test_terminate_refusal_is_recorded_and_not_retried(self):
        bad = {"eks:eks-cluster-name": "devoops-eks-a", "karpenter.sh/nodepool": "system"}
        e = ev({"i-1": ("running", bad)})
        x, saved = self.ctx(e, oldNodes=[{"node": "n1", "instanceId": "i-1"}])
        with mock.patch.object(relocator.kube, "since", return_value=relocator.STOP_WAIT_SECONDS + 1):
            self.assertFalse(relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))[0])
        self.assertIn("ec2TerminateError", saved)
        self.assertEqual(e.ec2.terminated, [])

    def test_no_terminate_before_wait(self):
        e = ev({"i-1": "running"})
        x, _ = self.ctx(e, oldNodes=[{"node": "n1", "instanceId": "i-1"}])
        with mock.patch.object(relocator.kube, "since", return_value=0):
            relocator.ec2_stop_evidence(x, relocator.Unreachable("끊김"))
        self.assertEqual(e.ec2.terminated, [])

    def test_stop_old_uses_ec2_only_when_unreachable(self):
        calls = []
        with mock.patch.object(relocator, "ec2_stop_evidence", side_effect=lambda x, w: calls.append(w) or (False, w)):
            for why in ("eks-a에 Pod 2개 남아 있음", relocator.Unreachable("eks-a 접속 안 됨")):
                x = SimpleNamespace(
                    co=mock.Mock(), mgr=None, name="rr", st={"isolatedAt": "t", "phase": "Executing"},
                    rr={"metadata": {"creationTimestamp": "t", "annotations": {}}},
                    job=lambda: ("training", "job-j1-g0001"), save=lambda **kw: None)
                with mock.patch.object(relocator.kube, "workload_for_jobset", return_value=None), \
                        mock.patch.object(relocator, "old_gone_on_worker", return_value=(False, why)), \
                        mock.patch.object(relocator.kube, "since", return_value=0):
                    relocator.stop_old(x)
        self.assertEqual(calls, ["eks-a 접속 안 됨"])


if __name__ == "__main__":
    unittest.main()
