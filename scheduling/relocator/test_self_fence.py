"""python -m unittest -v test_self_fence.py  (쿠버네티스·AWS 없이 돈다)
워커 API에 닿지 않을 때, 복구 컨트롤러가 펜싱 표 self_fence_*를 읽고 RR 주석으로 넘긴 확인을
그 워커의 옛 실행 종료 증거로 쓰는 규칙 (ADR-49, GPU-255). relocator는 펜싱 표를 읽지 않는다 (ADR-50)."""
import copy
import json
import unittest
from types import SimpleNamespace
from unittest import mock

import keys as K
import relocator

N = 2
ORIG = {"kind": "JobSet", "metadata": {"name": "job-j1-g0001", "namespace": "training"},
        "spec": {"failurePolicy": {"maxRestarts": 0},
                 "replicatedJobs": [{"name": "workers", "replicas": 1, "template": {"spec": {
                     "completionMode": "Indexed", "parallelism": N, "completions": N, "backoffLimit": 0,
                     "podReplacementPolicy": "Failed",
                     "template": {"spec": {"restartPolicy": "Never", "containers": [{"name": "trainer"}]}}}}}]}}
GOOD = {"requestId": "evt-1", "generation": 1, "cluster": "eks-a", "worldSize": N, "ranks": [0, 1],
        "pods": ["0:uid-a", "1:uid-b"], "recordedAt": "2026-11-25T01:00:07Z"}
WHY = relocator.Unreachable("eks-a 접속 안 됨 (MaxRetryError)")


def rr(record=GOOD, orig=ORIG):
    ann = {} if record is None else {K.ANN_SELF_FENCE: record if isinstance(record, str) else json.dumps(record)}
    return {"metadata": {"name": "rr-1", "namespace": "training", "annotations": ann,
                         "creationTimestamp": "2026-11-25T01:00:00Z", "uid": "u"},
            "spec": {"requestId": "evt-1", "jobRef": "training/job-j1-g0001", "action": "evacuateCluster",
                     "toGeneration": 2, "jobSet": copy.deepcopy(orig)},
            "status": {"sourceCluster": "eks-a"}}


def ctx(record=GOOD, orig=ORIG):
    with mock.patch.object(relocator.kube, "kueue"):
        return relocator.Ctx(None, None, rr(record, orig))


def judge(record=GOOD, orig=ORIG, src="eks-a", on=True):
    with mock.patch.object(relocator, "SELF_FENCE_EVIDENCE", on):
        return relocator.self_fence_stop_evidence(ctx(record, orig), WHY, src)


def changed(**kw):
    return {**GOOD, **kw}


def orig_with(**job):
    o = copy.deepcopy(ORIG)
    o["spec"]["replicatedJobs"][0]["template"]["spec"].update(job)
    return o


class Judge(unittest.TestCase):
    def test_all_pods_of_old_generation_accepted(self):
        ok, why = judge()
        self.assertTrue(ok)
        self.assertIn("Pod 0~1 전원 종료", why)
        self.assertIn("eks-a 접속 안 됨", why)

    def test_off_by_default(self):
        self.assertFalse(relocator.SELF_FENCE_EVIDENCE)
        self.assertEqual(judge(on=False), (False, WHY))

    def test_missing_or_broken_annotation(self):
        for rec in (None, "not-json", "[1, 2]"):
            self.assertFalse(judge(rec)[0], rec)

    def test_identity_must_match_request_generation_and_worker(self):
        for rec in (changed(requestId="evt-other"), changed(generation=2), changed(generation="1"),
                    changed(cluster="eks-b")):
            self.assertFalse(judge(rec)[0], rec)
        self.assertFalse(judge(src="eks-b")[0])   # 다른 워커의 확인으로 이 워커를 대신하지 않음

    def test_world_size_must_match_original(self):
        self.assertFalse(judge(changed(worldSize=4))[0])
        self.assertFalse(judge(changed(worldSize="2"))[0])

    def test_ranks_must_be_exactly_all(self):
        for ranks in ([0], [0, 1, 2], [0, 0, 1], [0, "1"], None, [1, 2]):
            self.assertFalse(judge(changed(ranks=ranks))[0], ranks)

    def test_pods_need_one_uid_per_index(self):
        for pods in (None, [], ["0:uid-a"], ["0:uid-a", "1:uid-b", "1:uid-c"], ["0:uid-a", "x:uid-b"],
                     ["0:uid-a", "1:"], ["0:uid-a", "2:uid-b"], ["0:uid-a", 1]):
            self.assertFalse(judge(changed(pods=pods))[0], pods)
        # 같은 번호 같은 UID가 두 번 = 같은 Pod의 중복 보고라 하나로 본다
        self.assertTrue(judge(changed(pods=["0:uid-a", "1:uid-b", "1:uid-b"]))[0])

    def test_recorded_at_required(self):
        self.assertFalse(judge(changed(recordedAt=""))[0])
        rec = dict(GOOD)
        rec.pop("recordedAt")
        self.assertFalse(judge(rec)[0])

    def test_original_must_not_recreate_pods(self):
        bad = [orig_with(podReplacementPolicy="TerminatingOrFailed"), orig_with(backoffLimit=3),
               orig_with(completionMode="NonIndexed"), orig_with(completions=3)]
        o = copy.deepcopy(ORIG)
        o["spec"]["failurePolicy"]["maxRestarts"] = 1
        bad.append(o)
        o = copy.deepcopy(ORIG)
        o["spec"]["replicatedJobs"][0]["replicas"] = 2
        bad.append(o)
        o = copy.deepcopy(ORIG)
        o["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["restartPolicy"] = "OnFailure"
        bad.append(o)
        for o in bad:
            ok, why = judge(orig=o)
            self.assertFalse(ok)
            self.assertIn("자기 펜싱 확인 안 씀", why)


class StopPath(unittest.TestCase):
    """old_gone_on_worker: 닿지 않는 워커만 자기 펜싱 확인 -> 안 되면 EC2 증거"""

    def gone(self, results, sf_ok, ec2_ok=False, **st):
        sf_calls, ec2_calls = [], []
        x = SimpleNamespace(st={"sourceCluster": "eks-a", **st})
        with mock.patch.object(relocator, "gone_on", side_effect=lambda _x, src, *_: results[src]), \
                mock.patch.object(relocator, "self_fence_stop_evidence",
                                  side_effect=lambda _x, why, src: sf_calls.append(src) or (sf_ok.get(src, False), why)), \
                mock.patch.object(relocator, "ec2_stop_evidence",
                                  side_effect=lambda _x, why, src: ec2_calls.append(src) or (ec2_ok, why)):
            ok, _ = relocator.old_gone_on_worker(x, "training", "job-j1-g0001")
        return ok, x, sf_calls, ec2_calls

    def test_reachable_worker_with_pods_ignores_self_fence(self):
        ok, _, sf, ec2 = self.gone({"eks-a": (False, "eks-a에 Pod 2개 남아 있음")}, {"eks-a": True})
        self.assertFalse(ok)
        self.assertEqual((sf, ec2), ([], []))

    def test_unreachable_worker_with_self_fence(self):
        ok, x, sf, ec2 = self.gone({"eks-a": (False, WHY)}, {"eks-a": True})
        self.assertTrue(ok)
        self.assertEqual((sf, ec2), (["eks-a"], []))
        self.assertEqual(x.stop_evidence[0]["kind"], "self_fence")

    def test_falls_back_to_ec2(self):
        ok, x, sf, ec2 = self.gone({"eks-a": (False, WHY)}, {}, ec2_ok=True)
        self.assertTrue(ok)
        self.assertEqual((sf, ec2), (["eks-a"], ["eks-a"]))
        self.assertEqual(x.stop_evidence[0]["kind"], "ec2")

    def test_no_evidence_keeps_waiting(self):
        self.assertFalse(self.gone({"eks-a": (False, WHY)}, {})[0])

    def test_other_worker_still_checked(self):
        res = {"eks-a": (False, WHY), "eks-b": (False, "eks-b에 Pod 1개 남아 있음")}
        self.assertFalse(self.gone(res, {"eks-a": True}, seenClusters=["eks-b"])[0])

    def test_stop_kind(self):
        self.assertEqual(relocator.stop_kind([{"kind": "automatic"}, {"kind": "self_fence"}]), "self_fence")
        self.assertEqual(relocator.stop_kind([{"kind": "self_fence"}, {"kind": "ec2"}]), "ec2")
        self.assertEqual(relocator.stop_kind([{"kind": "automatic"}]), "automatic")


if __name__ == "__main__":
    unittest.main()
