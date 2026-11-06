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


class Isolate(unittest.TestCase):
    def test_evacuate_without_source_skips_fencing(self):
        co = FakeCO()
        x = ctx(FakeStore(fencing("GENERATION_ADVANCED")), co, action="evacuateCluster", sourceCluster="")
        relocator.isolate(x)
        self.assertEqual(co.cluster_patches, [])
        self.assertEqual(co.status["phase"], "Executing")
        self.assertIn("isolatedAt", co.status)

    def test_evacuate_with_source_fences(self):
        co = FakeCO()
        relocator.isolate(ctx(FakeStore(fencing("GENERATION_ADVANCED")), co, action="evacuateCluster",
                              sourceCluster="worker2"))
        self.assertEqual(co.cluster_patches[0][2:], ("multikueueclusters", "worker2",
                                                     {"metadata": {"labels": {"devoops/fenced": "true"}}}))


if __name__ == "__main__":
    unittest.main()
