"""python -m unittest -v test_relocator_flow.py  (kubernetes 패키지만 필요, 클러스터 없이 돈다)
재배치 실행기 단계 함수를 가짜 API로 돌려 본다. 입력은 RR spec, 결과는 status뿐 (ADR-50)."""
import copy
import unittest
from unittest import mock

from kubernetes import client

import relocator

K = relocator.K
R = relocator.R


class FakeCO:
    """CustomObjectsApi 중 실행기가 쓰는 것만"""

    def __init__(self, delete_error=None, existing=None):
        self.created, self.deleted, self.cluster_patches, self.status = [], [], [], None
        self.delete_error = delete_error
        self.existing = existing   # get_namespaced_custom_object가 돌려줄 JobSet (None이면 404)

    def patch_namespaced_custom_object_status(self, g, v, ns, plural, name, body):
        self.status = copy.deepcopy(body["status"])

    def create_namespaced_custom_object(self, g, v, ns, plural, body):
        if self.existing is not None:
            raise client.ApiException(status=409)
        made = copy.deepcopy(body)
        made["metadata"]["uid"] = "uid-new"
        self.created.append(made)
        return made

    def get_namespaced_custom_object(self, g, v, ns, plural, name):
        if self.existing is None:
            raise client.ApiException(status=404)
        return copy.deepcopy(self.existing)

    def delete_namespaced_custom_object(self, g, v, ns, plural, name, **kw):
        if self.delete_error:
            raise self.delete_error
        body = kw.get("body")
        self.deleted.append((ns, name, body.preconditions.uid if body else None))

    def patch_cluster_custom_object(self, *a):
        self.cluster_patches.append(a)


def original():
    return {"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet",
            "metadata": {"name": "job-abc-g0001", "labels": {}, "annotations": {}}, "spec": {}}


def rr(action="moveToSpare", annotations=None, spec=None, **status):
    s = {"requestId": "r1", "jobRef": "training/job-abc-g0001", "action": action,
         "excludeNodes": ["gpu-node-1"], "toGeneration": 2, "jobSet": original()}
    s.update(spec or {})
    return {"metadata": {"name": "rr1", "namespace": "training", "uid": "u1",
                         "creationTimestamp": "2026-11-06T00:00:00Z", "annotations": dict(annotations or {})},
            "spec": s, "status": status}


def ctx(co, action="moveToSpare", annotations=None, spec=None, **status):
    x = relocator.Ctx(None, None, rr(action, annotations, spec, **status))
    x.co = co
    return x


ACK = {K.ANN_STOP_RECORDED: "true"}
EVIDENCE = {"generation": 1, "namespace": "training", "jobset_name": "job-abc-g0001", "clusters": ["worker1"],
            "kind": "automatic", "checks": [{"cluster": "worker1", "kind": "automatic", "detail": "gone"}],
            "confirmed_at": "2026-11-06T00:00:30Z"}


def stopped(**extra):
    """옛 실행 종료까지 끝난 status"""
    return dict(sourceCluster="worker1", stoppedAt="2026-11-06T00:00:30Z",
                recoveryProgress={"schemaVersion": 1, "requestId": "r1", "jobId": "abc", "fromGeneration": 1,
                                  "toGeneration": 2, "phase": "OLD_RUN_STOPPED",
                                  "oldRunStopEvidence": copy.deepcopy(EVIDENCE)}, **extra)


class Spec(unittest.TestCase):
    def test_to_generation_must_be_old_plus_one(self):
        for bad in (3, 1, "2", None):
            with self.subTest(bad=bad), self.assertRaises(PermissionError):
                ctx(FakeCO(), spec={"toGeneration": bad}).to_gen()

    def test_missing_original_rejected(self):
        for bad in (None, {"kind": "Job"}, "x"):
            with self.subTest(bad=bad), self.assertRaises(PermissionError):
                ctx(FakeCO(), spec={"jobSet": bad}).original()


class Resubmit(unittest.TestCase):
    def test_waits_for_controller_stop_record(self):
        co = FakeCO()
        relocator.resubmit(ctx(co, **stopped()))
        self.assertEqual(co.created, [])
        self.assertIn("확인 대기", co.status["message"])

    def test_ok_reports_new_run_in_status(self):
        co = FakeCO()
        relocator.resubmit(ctx(co, annotations=ACK, **stopped()))
        self.assertEqual(co.created[0]["metadata"]["name"], "job-abc-g0002")
        p = co.status["recoveryProgress"]
        self.assertEqual(p["phase"], "NEW_RUN_SUBMITTED")
        self.assertEqual(p["newJobSet"], {"name": "job-abc-g0002", "uid": "uid-new"})
        self.assertEqual(p["oldRunStopEvidence"], EVIDENCE)   # 증거는 그대로
        self.assertEqual(co.status["newJob"], "training/job-abc-g0002")
        ann = co.created[0]["metadata"]["annotations"]
        self.assertEqual(ann[K.ANN_PREFERRED], "worker1")
        self.assertEqual(ann[K.ANN_RECOVERY_ID], "r1")
        self.assertNotIn(K.ANN_TARGET, ann)

    def test_evacuate_excludes_source_without_preference(self):
        co = FakeCO()
        relocator.resubmit(ctx(co, action="evacuateCluster", annotations=ACK, **stopped()))
        ann = co.created[0]["metadata"]["annotations"]
        self.assertNotIn(K.ANN_PREFERRED, ann)
        self.assertIn("worker1", ann[K.ANN_EXCLUDE].split(','))

    def test_409_same_recovery_is_adopted(self):
        co = FakeCO(existing={"metadata": {"name": "job-abc-g0002", "uid": "uid-old",
                                           "annotations": {K.ANN_RECOVERY_ID: "r1"}}})
        relocator.resubmit(ctx(co, annotations=ACK, **stopped()))
        self.assertEqual(co.status["recoveryProgress"]["newJobSet"]["uid"], "uid-old")

    def test_409_other_recovery_rejected(self):
        co = FakeCO(existing={"metadata": {"name": "job-abc-g0002", "uid": "uid-x",
                                           "annotations": {K.ANN_RECOVERY_ID: "other"}}})
        with self.assertRaises(PermissionError):
            relocator.resubmit(ctx(co, annotations=ACK, **stopped()))

    def test_original_from_other_namespace_rejected(self):
        orig = original()
        orig["metadata"]["namespace"] = "other"
        with self.assertRaises(PermissionError):
            relocator.build_new(ctx(FakeCO(), spec={"jobSet": orig}, sourceCluster="worker1"))

    def test_original_of_other_job_rejected(self):
        orig = original()
        orig["metadata"]["labels"][K.LABEL_JOB_ID] = "zzz"
        with self.assertRaises(PermissionError):
            relocator.build_new(ctx(FakeCO(), spec={"jobSet": orig}, sourceCluster="worker1"))


    def test_action_reaches_render(self):
        """restartInPlace 요청만 재제출본에 예비 노드 회피 선호가 붙는다 (GPU-280)"""
        for action, want in (("restartInPlace", True), ("moveToSpare", False), ("evacuateCluster", False)):
            with self.subTest(action=action):
                orig = original()
                orig["spec"] = {"replicatedJobs": [{"name": "workers", "template": {"spec": {"template": {
                    "spec": {"containers": [{"name": "trainer"}]}}}}}]}
                new = relocator.build_new(ctx(FakeCO(), action=action, spec={"jobSet": orig},
                                              sourceCluster="worker1"))
                for rj in new["spec"]["replicatedJobs"]:
                    spec = rj["template"]["spec"]["template"]["spec"]
                    terms = ((spec.get("affinity") or {}).get("nodeAffinity") or {}).get(
                        "preferredDuringSchedulingIgnoredDuringExecution") or []
                    self.assertEqual(R.AVOID_SPARE_TERM in terms, want)


class Cancel(unittest.TestCase):
    def run_step(self, co, **status):
        x = ctx(co, annotations={K.ANN_CANCEL: "superseded", **ACK}, **status)
        relocator.step(x, [x.rr])
        return co

    def test_withdraws_own_new_jobset(self):
        co = self.run_step(FakeCO(existing={"metadata": {"name": "job-abc-g0002", "uid": "uid-new",
                                                         "annotations": {K.ANN_RECOVERY_ID: "r1"}}}), **stopped())
        self.assertEqual(co.deleted, [("training", "job-abc-g0002", "uid-new")])
        self.assertEqual(co.status["phase"], "Cancelled")
        self.assertEqual(co.created, [])

    def test_does_not_touch_other_recovery_jobset(self):
        co = self.run_step(FakeCO(existing={"metadata": {"name": "job-abc-g0002", "uid": "uid-x",
                                                         "annotations": {K.ANN_RECOVERY_ID: "other"}}}), **stopped())
        self.assertEqual(co.deleted, [])
        self.assertEqual(co.status["phase"], "Cancelled")

    def test_before_accept_does_nothing_but_cancel(self):
        co = self.run_step(FakeCO())
        self.assertEqual((co.deleted, co.cluster_patches, co.created), ([], [], []))
        self.assertEqual(co.status["phase"], "Cancelled")

    @mock.patch.object(relocator.time, "sleep")
    def test_withdraw_failure_retries_next_loop(self, _sleep):
        co = FakeCO(delete_error=client.ApiException(status=500),
                    existing={"metadata": {"name": "job-abc-g0002", "uid": "uid-new",
                                           "annotations": {K.ANN_RECOVERY_ID: "r1"}}})
        self.run_step(co, **stopped())
        self.assertNotEqual(co.status.get("phase"), "Cancelled")
        self.assertIn("철회 실패", co.status["message"])

    def test_done_request_with_cancel_is_revisited(self):
        done = rr(annotations={K.ANN_CANCEL: "lost"}, phase="Done")
        self.assertTrue(relocator.wanted(done))
        self.assertFalse(relocator.wanted(rr(phase="Done")))
        self.assertFalse(relocator.wanted(rr(annotations={K.ANN_CANCEL: "x"}, phase="Cancelled")))
        self.assertFalse(relocator.wanted(rr(annotations={K.ANN_CANCEL: "x"}, phase="Duplicate")))


def on_worker(cluster):
    """관리 클러스터 Workload가 cluster에 보내져 있는 것처럼 (None = Workload 없음)"""
    wl = None if cluster is None else {"status": {"clusterName": cluster}}
    return mock.patch.object(relocator.kube, "workload_for_jobset", return_value=wl)


class Accept(unittest.TestCase):
    def test_accepts_from_spec_without_store(self):
        co = FakeCO()
        x = ctx(co)
        with on_worker("worker1"):
            self.assertTrue(relocator.accept(x, [x.rr]))
        self.assertEqual(co.status["phase"], "Accepted")
        self.assertEqual((co.status["fromGeneration"], co.status["toGeneration"]), (1, 2))

    def test_bad_to_generation_is_rejected(self):
        x = ctx(FakeCO(), spec={"toGeneration": 5})
        with on_worker("worker1"), self.assertRaises(PermissionError):
            relocator.accept(x, [x.rr])

    def test_missing_workload_fails(self):
        co = FakeCO()
        x = ctx(co)
        with on_worker(None):
            self.assertFalse(relocator.accept(x, [x.rr]))
        self.assertEqual(co.status["phase"], "Failed")

    def test_duplicate_check_ignores_other_namespaces(self):
        co = FakeCO()
        x = ctx(co)
        intruder = rr()
        intruder["metadata"].update(namespace="other", uid="u0", creationTimestamp="2026-11-05T00:00:00Z")
        with on_worker("worker1"):
            self.assertTrue(relocator.accept(x, [intruder, x.rr]))
        self.assertEqual(co.status["phase"], "Accepted")


class Isolate(unittest.TestCase):
    def test_evacuate_without_source_skips_fencing(self):
        co = FakeCO()
        with on_worker(""):
            relocator.isolate(ctx(co, action="evacuateCluster", sourceCluster=""))
        self.assertEqual(co.cluster_patches, [])
        self.assertEqual(co.status["phase"], "Executing")
        self.assertIn("isolatedAt", co.status)

    def test_evacuate_with_source_fences(self):
        co = FakeCO()
        with on_worker("worker2"):
            relocator.isolate(ctx(co, action="evacuateCluster", sourceCluster="worker2"))
        self.assertEqual(co.cluster_patches[0][2:], ("multikueueclusters", "worker2",
                                                     {"metadata": {"labels": {"devoops/fenced": "true"}}}))

    def test_tampered_status_source_is_ignored(self):
        # status를 worker3으로 바꿔도 Workload가 worker2에 있으면 worker2를 펜싱
        co = FakeCO()
        with on_worker("worker2"):
            relocator.isolate(ctx(co, action="evacuateCluster", sourceCluster="worker3"))
        self.assertEqual([p[3] for p in co.cluster_patches], ["worker2"])
        self.assertEqual(co.status["sourceCluster"], "worker2")

    def test_status_source_without_workload_does_not_fence(self):
        co = FakeCO()
        with on_worker(None):
            relocator.isolate(ctx(co, action="evacuateCluster", sourceCluster="worker3"))
        self.assertEqual(co.cluster_patches, [])


def elapsed(seconds):
    """kube.since가 항상 seconds를 돌려주게 (요청 생성·격리·재제출 뒤 지난 시간)"""
    return mock.patch.object(relocator.kube, "since", return_value=seconds)


class StopOld(unittest.TestCase):
    """옛 실행 종료 확인: 결과는 status.recoveryProgress로만. 마감(spec.stopDeadline) 뒤에는 자동 확인으로 진행하지 않는다"""

    def run_stop(self, seconds, gone, confirmed=False, phase="Executing", spec=None):
        co = FakeCO()
        co.delete_namespaced_custom_object = mock.Mock()
        ann = {K.ANN_STOP_CONFIRMED: "true"} if confirmed else None
        x = ctx(co, annotations=ann, spec=spec, phase=phase, sourceCluster="worker1",
                isolatedAt="2026-11-06T00:00:01Z")
        with elapsed(seconds), on_worker(None), \
                mock.patch.object(relocator, "old_gone_on_worker", return_value=(gone, "why")):
            relocator.stop_old(x)
        return co

    def test_110_seconds_still_allows_graceful_stop(self):
        co = self.run_stop(110, gone=False)
        self.assertIsNone(co.status)

    def test_stop_evidence_is_reported_in_status(self):
        co = self.run_stop(30, gone=True)
        p = co.status["recoveryProgress"]
        self.assertEqual((p["schemaVersion"], p["requestId"], p["jobId"], p["fromGeneration"], p["toGeneration"],
                          p["phase"]), (1, "r1", "abc", 1, 2, "OLD_RUN_STOPPED"))
        self.assertNotIn("newJobSet", p)
        ev = p["oldRunStopEvidence"]
        self.assertEqual(ev["generation"], 1)
        self.assertEqual(ev["jobset_name"], "job-abc-g0001")
        self.assertEqual(ev["namespace"], "training")
        self.assertEqual(ev["clusters"], ["worker1"])
        self.assertEqual(ev["kind"], "automatic")
        self.assertEqual(ev["confirmed_at"], co.status["stoppedAt"])
        self.assertEqual(ev["confirmed_by"], "relocator")
        self.assertTrue(ev["recorded_by"])

    def test_mixed_api_and_ec2_evidence(self):
        co = FakeCO()
        co.delete_namespaced_custom_object = mock.Mock()
        x = ctx(co, sourceCluster="worker1", seenClusters=["worker2"], isolatedAt="t")
        with elapsed(30), on_worker(None), mock.patch.object(
                relocator, "gone_on", side_effect=[(True, "absent"), (False, relocator.Unreachable("offline"))]), \
                mock.patch.object(relocator, "ec2_stop_evidence", return_value=(True, "pool stopped")):
            relocator.stop_old(x)
        ev = co.status["recoveryProgress"]["oldRunStopEvidence"]
        self.assertEqual(ev["kind"], "ec2")
        self.assertEqual(ev["clusters"], ["worker1", "worker2"])
        self.assertEqual(ev["checks"], [
            {"cluster": "worker1", "kind": "automatic", "detail": "absent"},
            {"cluster": "worker2", "kind": "ec2", "detail": "pool stopped"}])

    def test_spec_deadline_controls_stop_transition(self):
        for deadline, expected in ((2100, "Executing"), (2000, "Held")):
            with self.subTest(deadline=deadline):
                co = FakeCO()
                co.delete_namespaced_custom_object = mock.Mock()
                x = ctx(co, spec={"stopDeadline": deadline}, sourceCluster="worker1", isolatedAt="t")
                with elapsed(9999), on_worker(None), mock.patch.object(relocator.time, "time", return_value=2000), \
                        mock.patch.object(relocator, "old_gone_on_worker", return_value=(True, "absent")):
                    relocator.stop_old(x)
                self.assertEqual(co.status["phase"], expected)
                self.assertEqual("recoveryProgress" in co.status, expected == "Executing")

    def test_manual_evidence_does_not_claim_known_human_identity(self):
        co = self.run_stop(1000, gone=False, confirmed=True)
        ev = co.status["recoveryProgress"]["oldRunStopEvidence"]
        self.assertEqual(ev["kind"], "manual")
        self.assertEqual(ev["confirmed_by"], "unknown (RR annotation)")
        self.assertEqual(ev["checks"], [])

    def test_not_gone_after_wait_is_waiting(self):
        co = self.run_stop(relocator.STOP_WAIT_SECONDS + 1, gone=False)
        self.assertEqual(co.status["phase"], "WaitingForStop")
        self.assertNotIn("recoveryProgress", co.status)

    def test_gone_after_hold_does_not_advance(self):
        co = self.run_stop(relocator.STOP_HOLD_SECONDS + 1, gone=True, phase="WaitingForStop")
        self.assertEqual(co.status["phase"], "Held")
        self.assertNotIn("stoppedAt", co.status)

    def test_confirm_annotation_resumes_after_hold(self):
        co = self.run_stop(relocator.STOP_HOLD_SECONDS + 1, gone=False, confirmed=True, phase="Held")
        self.assertEqual(co.status["phase"], "Executing")
        self.assertEqual(co.status["recoveryProgress"]["phase"], "OLD_RUN_STOPPED")


class StopDeadline(unittest.TestCase):
    def held(self, deadline, now=2000, rr_age=10):
        spec = {} if deadline is None else {"stopDeadline": deadline}
        x = ctx(FakeCO(), spec=spec)
        with mock.patch.object(relocator.time, "time", return_value=now), elapsed(rr_age):
            return relocator.stop_held(x)

    def test_deadline_overrides_old_rr(self):
        self.assertFalse(self.held(2100, rr_age=9999))
        self.assertTrue(self.held(2000))

    def test_rfc3339_and_epoch_strings(self):
        self.assertTrue(self.held("2000"))
        self.assertTrue(self.held("1970-01-01T00:33:20Z"))
        self.assertFalse(self.held("1970-01-01T00:35:00+00:00"))

    def test_missing_deadline_falls_back_to_rr_age(self):
        self.assertFalse(self.held(None, rr_age=899))
        self.assertTrue(self.held("", rr_age=901))

    def test_invalid_deadline_does_not_fall_back_and_resume(self):
        for bad in ("invalid", "2026-11-06T00:00:00"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.held(bad)


class WatchAdmit(unittest.TestCase):
    """입장 대기 시간을 넘겨도 실패로 끝내지 않는다 (복구 판단은 복구 컨트롤러)"""

    def run_watch(self, seconds, wl, jobset_404=False, phase="Executing"):
        co = FakeCO()
        co.get_namespaced_custom_object = (mock.Mock(side_effect=client.ApiException(status=404)) if jobset_404
                                           else mock.Mock(return_value={}))
        x = ctx(co, phase=phase, newJob="training/job-abc-g0002", resubmittedAt="2026-11-06T00:01:00Z")
        with elapsed(seconds), mock.patch.object(relocator.kube, "workload_for_jobset", return_value=wl):
            relocator.watch_admit(x)
        return co

    def test_timeout_marks_delayed_not_failed(self):
        co = self.run_watch(relocator.ADMIT_TIMEOUT_SECONDS + 1, {"status": {}})
        self.assertEqual(co.status["phase"], "AdmitDelayed")

    def test_late_admission_is_done(self):
        wl = {"status": {"clusterName": "worker1", "conditions": [{"type": "Admitted", "status": "True"}]}}
        co = self.run_watch(relocator.ADMIT_TIMEOUT_SECONDS * 2, wl, phase="AdmitDelayed")
        self.assertEqual(co.status["phase"], "Done")
        self.assertEqual(co.status["targetCluster"], "worker1")

    def test_vanished_jobset_fails(self):
        co = self.run_watch(relocator.ADMIT_TIMEOUT_SECONDS + 1, None, jobset_404=True)
        self.assertEqual(co.status["phase"], "Failed")


class Scope(unittest.TestCase):
    def test_request_outside_training_namespace_rejected(self):
        r = rr()
        r["metadata"]["namespace"] = "other"
        r["spec"]["jobRef"] = "other/job-abc-g0001"
        with self.assertRaises(PermissionError):
            relocator.Ctx(None, None, r).job()


if __name__ == "__main__":
    unittest.main()
