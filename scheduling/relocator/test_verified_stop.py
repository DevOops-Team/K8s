"""python -m unittest -v test_verified_stop.py  (쿠버네티스·AWS 없이 돈다)
API는 살아 있는데 옛 JobSet·Workload·Pod가 남는 장애(노드그룹 0대, NotReady 노드)에서
복구 컨트롤러가 검증한 옛 세대 전체 종료 증거(devoops/old-run-stop-verified, GPU-304)를 받아
Held에서도 OLD_RUN_STOPPED로 넘어가는지 본다 (GPU-297). relocator는 서명·EC2를 다시 보지 않고 값을 그대로 옮긴다."""
import copy
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import relocator
from test_relocator_flow import ACK, FakeCO, elapsed, on_worker, original

K = relocator.K
NOW = 1795651200
CREATED = datetime.fromtimestamp(NOW - 60, timezone.utc).isoformat()
NODES = [{"name": "n1"}, {"name": "n2"}]


def proof(**kw):
    """컨트롤러 VerifiedStopInput이 고정해 주석으로 붙이는 증거 모양 (stop_evidence.py)"""
    p = {"generation": 1, "namespace": "training", "jobset_name": "job-abc-g0001", "clusters": ["eks-a"],
         "kind": "manual", "checks": [{"cluster": "eks-a", "kind": "manual", "detail": "recreation disabled"}],
         "confirmed_at": datetime.fromtimestamp(NOW, timezone.utc).isoformat(), "confirmed_by": "kms:key",
         "verificationId": "ab" * 32, "requestUid": "rr-uid", "requestId": "r1", "jobId": "abc", "toGeneration": 2,
         "oldJobsetUid": "old-uid", "participants": [], "recreationBlocked": True,
         "blockEvidence": "recreation disabled", "approval": {}, "ec2Observations": []}
    p.update(kw)
    return p


def rr(value=None, **status):
    ann = {} if value is None else {K.ANN_STOP_VERIFIED: value if isinstance(value, str) else json.dumps(value)}
    return {"metadata": {"name": "rr1", "namespace": "training", "uid": "rr-uid", "creationTimestamp": CREATED,
                         "resourceVersion": "7", "annotations": ann},
            "spec": {"requestId": "r1", "jobRef": "training/job-abc-g0001", "action": "evacuateCluster",
                     "excludeClusters": ["eks-a"], "toGeneration": 2, "jobSet": original(),
                     "sourceCluster": "eks-a", "nodes": copy.deepcopy(NODES)},
            "status": {"phase": "Held", "sourceCluster": "eks-a", "seenClusters": ["eks-a"],
                       "isolatedAt": CREATED, **status}}


def ctx(value=None, **status):
    x = relocator.Ctx(None, None, rr(value, **status))
    x.co = FakeCO()
    x.co.delete_namespaced_custom_object = mock.Mock()
    return x


def run_stop(x, mgr_cluster=None, worker=(False, "eks-a에 Pod 2개 남아 있음")):
    """마감이 지나 Held, 워커에는 옛 객체가 남아 있는 상황 (노드그룹 0대)"""
    with elapsed(relocator.STOP_HOLD_SECONDS + 1), on_worker(mgr_cluster), \
            mock.patch.object(relocator.time, "time", return_value=NOW), \
            mock.patch.object(relocator, "old_gone_on_worker", return_value=worker) as checked:
        relocator.stop_old(x)
    return checked


class Judge(unittest.TestCase):
    def judge(self, value, **status):
        return relocator.verified_stop_evidence(ctx(value, **status))

    def test_valid_evidence_accepted_verbatim(self):
        rec, why = self.judge(proof())
        self.assertEqual(rec, proof())
        self.assertIn("manual", why)

    def test_missing_or_broken(self):
        for value in (None, "not-json", "[1]"):
            self.assertIsNone(self.judge(value)[0], value)

    def test_identity_must_match_this_request(self):
        for kw in ({"requestId": "r2"}, {"requestUid": "other"}, {"jobId": "xyz"}, {"generation": 2},
                   {"generation": "1"}, {"toGeneration": 3}, {"jobset_name": "job-abc-g0002"},
                   {"namespace": "default"}):
            self.assertIsNone(self.judge(proof(**kw))[0], kw)

    def test_must_be_controller_verified_and_block_recreation(self):
        for kw in ({"kind": "automatic"}, {"kind": "self_fence"}, {"verificationId": ""},
                   {"recreationBlocked": False}, {"confirmed_at": None}):
            self.assertIsNone(self.judge(proof(**kw))[0], kw)
        p = proof()
        del p["verificationId"]
        self.assertIsNone(self.judge(p)[0])

    def test_must_cover_every_cluster_with_a_possible_copy(self):
        rec, why = self.judge(proof(), seenClusters=["eks-a", "eks-b"])
        self.assertIsNone(rec)
        self.assertIn("eks-b", why)
        both = proof(clusters=["eks-a", "eks-b"], checks=[
            {"cluster": "eks-a", "kind": "manual", "detail": "d"}, {"cluster": "eks-b", "kind": "manual", "detail": "d"}])
        self.assertIsNotNone(self.judge(both, seenClusters=["eks-a", "eks-b"])[0])

    def test_checks_must_cover_clusters_with_same_kind(self):
        for checks in ([], [{"cluster": "eks-a", "kind": "ec2", "detail": "d"}],
                       [{"cluster": "eks-b", "kind": "manual", "detail": "d"}],
                       [{"cluster": "eks-a", "kind": "manual", "detail": ""}], None):
            self.assertIsNone(self.judge(proof(checks=checks))[0], checks)
        for clusters in ([], ["eks-a", "eks-a"], [""], "eks-a"):
            self.assertIsNone(self.judge(proof(clusters=clusters))[0], clusters)


class StopPath(unittest.TestCase):
    def test_held_with_lingering_objects_advances_on_verified_evidence(self):
        x = ctx(proof())
        checked = run_stop(x)
        self.assertEqual(x.co.status["phase"], "Executing")
        self.assertTrue(x.co.status["stoppedAt"])
        p = x.co.status["recoveryProgress"]
        self.assertEqual((p["phase"], p["requestId"], p["fromGeneration"], p["toGeneration"]),
                         ("OLD_RUN_STOPPED", "r1", 1, 2))
        self.assertEqual(p["oldRunStopEvidence"], proof())   # 컨트롤러가 완전 일치를 대조한다
        checked.assert_not_called()   # 워커 객체 잔존과 무관

    def test_manager_workload_must_be_gone_first(self):
        x = ctx(proof())
        run_stop(x, mgr_cluster="eks-a")
        self.assertNotIn("recoveryProgress", x.co.status or {})
        self.assertNotIn("stoppedAt", x.st)

    def test_invalid_evidence_keeps_held_and_shows_reason(self):
        x = ctx(proof(requestId="r2"), phase="WaitingForStop")
        run_stop(x)
        self.assertEqual(x.co.status["phase"], "Held")
        self.assertNotIn("recoveryProgress", x.co.status)
        self.assertIn("requestId", x.co.status["message"])

    def test_without_annotation_behaviour_unchanged(self):
        x = ctx(None, phase="WaitingForStop")
        run_stop(x)
        self.assertEqual(x.co.status["phase"], "Held")
        self.assertNotIn("컨트롤러 종료 증거 없음", x.co.status["message"])

    def test_verified_evidence_preferred_over_human_annotation(self):
        x = ctx(proof())
        x.rr["metadata"]["annotations"][K.ANN_STOP_CONFIRMED] = "true"
        run_stop(x)
        self.assertEqual(x.co.status["recoveryProgress"]["oldRunStopEvidence"], proof())

    def test_resubmit_keeps_evidence_unchanged(self):
        x = ctx(proof())
        run_stop(x)
        x.st = dict(x.co.status)
        x.rr["metadata"]["annotations"].update(ACK)
        relocator.resubmit(x)
        p = x.co.status["recoveryProgress"]
        self.assertEqual(p["phase"], "NEW_RUN_SUBMITTED")
        self.assertEqual(p["oldRunStopEvidence"], proof())


RC = Path(__file__).resolve().parents[2] / "recovery-controller"
try:
    sys.path[:0] = [str(RC), str(RC / "tests")]
    from integrations.relocation_status import RelocationStatusInput   # noqa: E402
    from integrations.stop_evidence import ANNOTATION, VerifiedStopInput   # noqa: E402
    import test_stop_evidence as rc   # noqa: E402  컨트롤러 테스트의 서명·KMS·저장소 대역
except Exception as e:  # noqa: BLE001
    RelocationStatusInput = None
    WHY = f"recovery-controller import 실패: {e}"
else:
    WHY = ""


@unittest.skipIf(RelocationStatusInput is None, WHY)
class ControllerRoundTrip(unittest.TestCase):
    """컨트롤러가 실제로 만든 주석 -> relocator status -> 컨트롤러 status 검사 통과"""

    def setUp(self):
        self.store = rc.MemoryStore()
        kms = mock.Mock()
        kms.verify.side_effect = rc.kms_verify
        self.verifier = VerifiedStopInput(self.store, lambda region: kms, lambda region: mock.Mock(), rc.config(),
                                          clock=lambda: NOW, approval_source=lambda event: rc.signed(rc.document()))

    def project(self, x):
        req = copy.deepcopy(x.rr)
        req["status"] = copy.deepcopy(x.co.status)
        return RelocationStatusInput(self.store, "training", lambda: NOW, verified_stop_input=self.verifier)(rc.EVENT, req)

    def test_annotation_name_matches(self):
        self.assertEqual(ANNOTATION, K.ANN_STOP_VERIFIED)

    def test_controller_annotation_accepted_end_to_end(self):
        x = ctx()
        value = self.verifier(rc.EVENT, x.rr)
        self.assertIsNotNone(value, self.store.record.get("stop_evidence_error"))
        x.rr["metadata"]["annotations"][K.ANN_STOP_VERIFIED] = value
        run_stop(x)
        self.assertEqual(x.co.status["recoveryProgress"]["phase"], "OLD_RUN_STOPPED")
        self.assertTrue(self.project(x))
        self.assertEqual(self.store.writes, ["OLD_RUN_STOPPED"])

        x.st = dict(x.co.status)
        x.rr["status"] = dict(x.co.status)
        x.rr["metadata"]["annotations"].update(ACK)
        relocator.resubmit(x)
        self.assertTrue(self.project(x))
        self.assertEqual(self.store.writes[-1], "NEW_RUN_SUBMITTED")   # 앞 단계 기록은 컨트롤러가 멱등으로 다시 함
        self.assertEqual(self.store.fence["recovery_phase"], "NEW_RUN_SUBMITTED")

    def test_tampered_status_rejected_by_controller(self):
        x = ctx()
        x.rr["metadata"]["annotations"][K.ANN_STOP_VERIFIED] = self.verifier(rc.EVENT, x.rr)
        run_stop(x)
        x.co.status["recoveryProgress"]["oldRunStopEvidence"]["detail"] = "changed"
        with self.assertRaises(rc.RecoveryBlocked):
            self.project(x)


if __name__ == "__main__":
    unittest.main()
