"""python -m unittest -v test_relocator_flow.py  (kubernetes 패키지만 필요, 클러스터 없이 돈다)
재배치 실행기 단계 함수를 가짜 API·저장소로 돌려 본다: 재제출 철회, 워커 미정 evacuate."""
import copy
import unittest
from unittest import mock

from kubernetes import client

import relocator


class FakeCO:
    """CustomObjectsApi 중 실행기가 쓰는 것만"""

    def __init__(self, delete_error=None):
        self.created, self.deleted, self.cluster_patches, self.status = [], [], [], None
        self.delete_error = delete_error

    def patch_namespaced_custom_object_status(self, g, v, ns, plural, name, body):
        self.status = copy.deepcopy(body["status"])

    def create_namespaced_custom_object(self, g, v, ns, plural, body):
        made = copy.deepcopy(body)
        made["metadata"]["uid"] = "uid-new"
        self.created.append(made)
        return made

    def delete_namespaced_custom_object(self, g, v, ns, plural, name, **kw):
        if self.delete_error:
            raise self.delete_error
        self.deleted.append((ns, name, kw["body"].preconditions.uid))

    def patch_cluster_custom_object(self, *a):
        self.cluster_patches.append(a)


class FakeStore:
    def __init__(self, rec, steal_on_set=False, set_error=None):
        self.rec, self.steal_on_set, self.set_error = rec, steal_on_set, set_error

    def get_fencing(self, job_id):
        return dict(self.rec)

    def get_jobset(self, job_id):
        return {"metadata": {"name": f"job-{job_id}-g0001", "labels": {}, "annotations": {}}, "spec": {}}

    def set_phase(self, job_id, rid, to_gen, allowed_from, new_phase, extra=None):
        if self.set_error:
            raise self.set_error
        if self.steal_on_set:   # 조건부 갱신 직전에 다른 복구가 기록을 가져감
            self.rec.update(recovery_id="other", recovery_phase="GENERATION_ADVANCED")
            return False
        self.rec["recovery_phase"] = new_phase
        return True


def rr(action="moveToSpare", **status):
    return {"metadata": {"name": "rr1", "namespace": "training", "uid": "u1",
                         "creationTimestamp": "2026-11-06T00:00:00Z"},
            "spec": {"requestId": "r1", "jobRef": "training/job-abc-g0001", "action": action,
                     "excludeNodes": ["gpu-node-1"]},
            "status": status}


def fencing(phase="OLD_RUN_STOPPED"):
    return {"recovery_id": "r1", "recovery_phase": phase, "recovery_from_gen": 1, "recovery_to_gen": 2}


def ctx(store, co, action="moveToSpare", **status):
    x = relocator.Ctx(None, None, store, rr(action, **status))
    x.co = co
    return x


class Resubmit(unittest.TestCase):
    def test_ok_records_new_run(self):
        co, store = FakeCO(), FakeStore(fencing())
        relocator.resubmit(ctx(store, co, sourceCluster="worker1"))
        self.assertEqual(store.rec["recovery_phase"], "NEW_RUN_SUBMITTED")
        self.assertEqual(co.created[0]["metadata"]["name"], "job-abc-g0002")
        self.assertEqual(co.deleted, [])

    def test_lost_fencing_withdraws_created_jobset(self):
        co, store = FakeCO(), FakeStore(fencing(), steal_on_set=True)
        with self.assertRaises(PermissionError) as e:
            relocator.resubmit(ctx(store, co, sourceCluster="worker1"))
        self.assertIn("철회함", str(e.exception))
        self.assertEqual(co.deleted, [("training", "job-abc-g0002", "uid-new")])   # 내가 만든 uid만

    def test_transient_store_error_keeps_jobset(self):
        # 끊김은 다음 루프에서 409 -> 같은 복구 확인 -> 다시 기록. 지우면 안 됨
        co, store = FakeCO(), FakeStore(fencing(), set_error=TimeoutError())
        with self.assertRaises(TimeoutError):
            relocator.resubmit(ctx(store, co, sourceCluster="worker1"))
        self.assertEqual(co.deleted, [])

    def test_withdraw_already_gone_or_replaced_is_ok(self):
        for status in (404, 409):
            co = FakeCO(delete_error=client.ApiException(status=status))
            store = FakeStore(fencing(), steal_on_set=True)
            with self.assertRaises(PermissionError) as e:
                relocator.resubmit(ctx(store, co, sourceCluster="worker1"))
            self.assertIn("철회함", str(e.exception))

    @mock.patch.object(relocator.time, "sleep")
    def test_withdraw_failure_says_manual(self, _sleep):
        co = FakeCO(delete_error=client.ApiException(status=500))
        store = FakeStore(fencing(), steal_on_set=True)
        with self.assertRaises(PermissionError) as e:
            relocator.resubmit(ctx(store, co, sourceCluster="worker1"))
        self.assertIn("손으로 지울 것", str(e.exception))


def on_worker(cluster):
    """관리 클러스터 Workload가 cluster에 보내져 있는 것처럼 (None = Workload 없음)"""
    wl = None if cluster is None else {"status": {"clusterName": cluster}}
    return mock.patch.object(relocator.kube, "workload_for_jobset", return_value=wl)


class Isolate(unittest.TestCase):
    def test_evacuate_without_source_skips_fencing(self):
        co = FakeCO()
        x = ctx(FakeStore(fencing("GENERATION_ADVANCED")), co, action="evacuateCluster", sourceCluster="")
        with on_worker(""):
            relocator.isolate(x)
        self.assertEqual(co.cluster_patches, [])
        self.assertEqual(co.status["phase"], "Executing")
        self.assertIn("isolatedAt", co.status)

    def test_evacuate_with_source_fences(self):
        co = FakeCO()
        with on_worker("worker2"):
            relocator.isolate(ctx(FakeStore(fencing("GENERATION_ADVANCED")), co, action="evacuateCluster",
                                  sourceCluster="worker2"))
        self.assertEqual(co.cluster_patches[0][2:], ("multikueueclusters", "worker2",
                                                     {"metadata": {"labels": {"devoops/fenced": "true"}}}))

    def test_tampered_status_source_is_ignored(self):
        # status를 worker3으로 바꿔도 Workload가 worker2에 있으면 worker2를 펜싱
        co = FakeCO()
        with on_worker("worker2"):
            relocator.isolate(ctx(FakeStore(fencing("GENERATION_ADVANCED")), co, action="evacuateCluster",
                                  sourceCluster="worker3"))
        self.assertEqual([p[3] for p in co.cluster_patches], ["worker2"])
        self.assertEqual(co.status["sourceCluster"], "worker2")

    def test_status_source_without_workload_does_not_fence(self):
        co = FakeCO()
        with on_worker(None):
            relocator.isolate(ctx(FakeStore(fencing("GENERATION_ADVANCED")), co, action="evacuateCluster",
                                  sourceCluster="worker3"))
        self.assertEqual(co.cluster_patches, [])

    def test_lost_fencing_blocks_isolation(self):
        co = FakeCO()
        store = FakeStore(dict(fencing("GENERATION_ADVANCED"), recovery_id="other"))
        with on_worker("worker2"), self.assertRaises(PermissionError):
            relocator.isolate(ctx(store, co, action="evacuateCluster", sourceCluster="worker2"))
        self.assertEqual(co.cluster_patches, [])


def elapsed(seconds):
    """kube.since가 항상 seconds를 돌려주게 (요청 생성·격리·재제출 뒤 지난 시간)"""
    return mock.patch.object(relocator.kube, "since", return_value=seconds)


class StopOld(unittest.TestCase):
    """옛 실행 종료 확인: 복구 컨트롤러 HOLD(15분) 뒤에는 자동 확인으로 재제출하지 않는다"""

    def run_stop(self, seconds, gone, confirmed=False, phase="Executing"):
        co, store = FakeCO(), FakeStore(fencing("GENERATION_ADVANCED"))
        co.delete_namespaced_custom_object = mock.Mock()
        x = ctx(store, co, phase=phase, sourceCluster="worker1", isolatedAt="2026-11-06T00:00:01Z")
        if confirmed:
            x.rr["metadata"]["annotations"] = {"devoops/old-execution-stopped": "true"}
        with elapsed(seconds), on_worker(None),                 mock.patch.object(relocator, "old_gone_on_worker", return_value=(gone, "why")):
            relocator.stop_old(x)
        return co, store

    def test_gone_before_hold_advances(self):
        co, store = self.run_stop(30, gone=True)
        self.assertEqual(store.rec["recovery_phase"], "OLD_RUN_STOPPED")
        self.assertIn("stoppedAt", co.status)

    def test_not_gone_after_wait_is_waiting(self):
        co, store = self.run_stop(120, gone=False)
        self.assertEqual(co.status["phase"], "WaitingForStop")
        self.assertEqual(store.rec["recovery_phase"], "GENERATION_ADVANCED")

    def test_gone_after_hold_does_not_advance(self):
        co, store = self.run_stop(relocator.STOP_HOLD_SECONDS + 1, gone=True, phase="WaitingForStop")
        self.assertEqual(co.status["phase"], "Held")
        self.assertEqual(store.rec["recovery_phase"], "GENERATION_ADVANCED")
        self.assertNotIn("stoppedAt", co.status)

    def test_confirm_annotation_resumes_after_hold(self):
        co, store = self.run_stop(relocator.STOP_HOLD_SECONDS + 1, gone=False, confirmed=True, phase="Held")
        self.assertEqual(store.rec["recovery_phase"], "OLD_RUN_STOPPED")
        self.assertEqual(co.status["phase"], "Executing")


class WatchAdmit(unittest.TestCase):
    """입장 대기 시간을 넘겨도 실패로 끝내지 않는다 (복구 판단은 복구 컨트롤러)"""

    def run_watch(self, seconds, wl, jobset_404=False, phase="Executing"):
        co = FakeCO()
        co.get_namespaced_custom_object = (mock.Mock(side_effect=client.ApiException(status=404)) if jobset_404
                                           else mock.Mock(return_value={}))
        x = ctx(FakeStore(fencing("NEW_RUN_SUBMITTED")), co, phase=phase,
                newJob="training/job-abc-g0002", resubmittedAt="2026-11-06T00:01:00Z")
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
        x = relocator.Ctx(None, None, FakeStore(fencing()), r)
        with self.assertRaises(PermissionError):
            x.job()

    def test_duplicate_check_ignores_other_namespaces(self):
        co, store = FakeCO(), FakeStore(fencing("GENERATION_ADVANCED"))
        co.get_namespaced_custom_object = mock.Mock(side_effect=client.ApiException(status=404))
        x = ctx(store, co)
        intruder = rr()
        intruder["metadata"].update(namespace="other", uid="u0", creationTimestamp="2026-11-05T00:00:00Z")
        with on_worker("worker1"):
            self.assertTrue(relocator.accept(x, [intruder, x.rr]))
        self.assertEqual(co.status["phase"], "Accepted")

    def test_original_from_other_namespace_rejected(self):
        store = FakeStore(fencing())
        orig = store.get_jobset("abc")
        orig["metadata"]["namespace"] = "other"
        store.get_jobset = lambda job_id: orig
        with self.assertRaises(PermissionError):
            relocator.build_new(ctx(store, FakeCO(), sourceCluster="worker1"))


if __name__ == "__main__":
    unittest.main()
