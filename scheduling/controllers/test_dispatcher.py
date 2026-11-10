"""python -m unittest -v test_dispatcher.py  (쿠버네티스 없이 돈다: 조회·patch를 가짜로 바꿈)"""
import unittest
from unittest import mock

from kubernetes import client

import dispatcher as D
import keys as K
from placement import Cluster


def wl(name, gpus, created, *, priority=100, reserved=None, nominated=None):
    ann = {K.ANN_RESERVED: reserved} if reserved else {}
    return {"metadata": {"name": name, "namespace": "training", "creationTimestamp": created, "annotations": ann,
                         "ownerReferences": [{"kind": "JobSet", "name": name}]},
            "spec": {"priority": priority, "podSets": [{"count": gpus, "template": {"spec": {"containers": [
                {"resources": {"requests": {K.GPU: 1}}}]}}}]},
            "status": {"conditions": [{"type": "QuotaReserved", "status": "True",
                                       "lastTransitionTime": "2026-11-10T00:00:00Z"}],
                       **({"nominatedClusterNames": nominated} if nominated else {})}}


def cl(name, free, total=4, **kw):
    return Cluster(name=name, quota_free=free, node_free=free, quota_total=total, node_total=total, **kw)


class Reconcile(unittest.TestCase):
    def run_loop(self, clusters, wls, owner=None, holds=None):
        self.nominated, self.reserved = {}, {}
        owner = owner or (lambda _api, _w: {})
        with mock.patch.object(D, "cluster_state", return_value=clusters), \
             mock.patch.object(D, "relocation_holds", return_value=holds or {}), \
             mock.patch.object(D.kube, "list_workloads", return_value=wls), \
             mock.patch.object(D.kube, "owner_annotations", side_effect=owner), \
             mock.patch.object(D, "patch_nomination",
                               side_effect=lambda _m, w, c, _t: self.nominated.__setitem__(w["metadata"]["name"], c)), \
             mock.patch.object(D, "set_reserved",
                               side_effect=lambda _m, w, c: self.reserved.__setitem__(w["metadata"]["name"], c)), \
             mock.patch.object(D, "touch_nomination"):
            D.reconcile(None, None)

    def test_big_job_reserves_and_small_jobs_go_elsewhere(self):
        # 11/10 compare 상황: big(4장)은 들어갈 곳이 없고, 뒤에 온 1장 잡이 빈 칸을 메우려 함
        cs = [cl("worker1", 2), cl("worker2", 1), cl("worker3", 1)]
        self.run_loop(cs, [wl("big", 4, "T1"), wl("s1", 1, "T2"), wl("s2", 1, "T3"), wl("s3", 1, "T4")])
        self.assertEqual(self.nominated, {"s1": "worker2", "s2": "worker3"})   # s3: 찜한 worker1에는 안 감
        # s3도 들어갈 곳이 없으니 남은 워커를 찜한다 (크기와 상관없이 같은 규칙, 먼저 온 순)
        self.assertEqual(self.reserved, {"big": "worker1", "s3": "worker2"})

    def test_older_small_job_goes_first(self):
        # 찜은 뒤 순서 잡만 막는다. 먼저 온 잡은 그대로 빈 패킹
        cs = [cl("worker1", 1), cl("worker2", 0), cl("worker3", 0)]
        self.run_loop(cs, [wl("old", 1, "T1"), wl("big", 4, "T2")])
        self.assertEqual(self.nominated, {"old": "worker1"})
        self.assertEqual(self.reserved, {"big": "worker1"})

    def test_reservation_is_sticky_and_not_rewritten(self):
        cs = [cl("worker1", 2), cl("worker2", 3), cl("worker3", 0)]
        self.run_loop(cs, [wl("big", 4, "T1", reserved="worker1"), wl("s1", 1, "T2")])
        self.assertEqual(self.reserved, {})                      # 그대로라 다시 안 적음
        self.assertEqual(self.nominated, {"s1": "worker2"})

    def test_reserved_job_nominated_when_any_worker_fits(self):
        cs = [cl("worker1", 3), cl("worker2", 4), cl("worker3", 0)]
        self.run_loop(cs, [wl("big", 4, "T1", reserved="worker1"), wl("s1", 1, "T2")])
        self.assertEqual(self.nominated, {"big": "worker2", "s1": "worker1"})   # 찜이 풀려 worker1도 열림

    def test_recovery_ignores_training_reservation(self):
        cs = [cl("worker1", 2), cl("worker2", 0), cl("worker3", 0)]
        self.run_loop(cs, [wl("big", 4, "T1", reserved="worker1"),
                           wl("rec", 1, "T2", priority=K.RECOVERY_PRIORITY_VALUE)])
        self.assertEqual(self.nominated, {"rec": "worker1"})

    def test_training_respects_recovery_reservation(self):
        cs = [cl("worker1", 2), cl("worker2", 0), cl("worker3", 0)]
        self.run_loop(cs, [wl("rec", 4, "T2", priority=K.RECOVERY_PRIORITY_VALUE), wl("s1", 1, "T1")])
        self.assertEqual(self.reserved["rec"], "worker1")
        self.assertEqual(self.nominated, {})

    def test_one_reservation_per_worker(self):
        cs = [cl("worker1", 2), cl("worker2", 1), cl("worker3", 0)]
        self.run_loop(cs, [wl("big1", 4, "T1"), wl("big2", 4, "T2"), wl("s1", 1, "T3")])
        self.assertEqual((self.reserved["big1"], self.reserved["big2"]), ("worker1", "worker2"))
        self.assertEqual(self.nominated, {})                     # s1: worker1·2는 찜, worker3은 빈 자리 없음

    def test_reservation_moves_off_fenced_worker(self):
        cs = [cl("worker1", 2, fenced=True), cl("worker2", 1), cl("worker3", 0)]
        self.run_loop(cs, [wl("big", 4, "T1", reserved="worker1")])
        self.assertEqual(self.reserved, {"big": "worker2"})

    def test_one_failing_job_does_not_stop_the_rest(self):
        def owner(_api, w):
            if w["metadata"]["name"] == "bad":
                raise client.ApiException(status=500, reason="boom")
            return {}
        cs = [cl("worker1", 2), cl("worker2", 0), cl("worker3", 0)]
        with self.assertLogs("dispatcher", level="ERROR"):
            self.run_loop(cs, [wl("bad", 1, "T1"), wl("ok", 1, "T2")], owner)
        self.assertEqual(self.nominated, {"ok": "worker1"})

    def test_failing_job_keeps_its_reservation(self):
        def owner(_api, w):
            if w["metadata"]["name"] == "big":
                raise client.ApiException(status=500, reason="boom")
            return {}
        cs = [cl("worker1", 2), cl("worker2", 1), cl("worker3", 0)]
        with self.assertLogs("dispatcher", level="ERROR"):
            self.run_loop(cs, [wl("big", 4, "T1", reserved="worker1"), wl("s1", 1, "T2")], owner)
        self.assertEqual(self.nominated, {"s1": "worker2"})

    def test_owner_lookup_failure_skips_instead_of_ignoring_exclude(self):
        # 조회 실패를 빈 어노테이션으로 넘기면 exclude한 worker1로 보내게 됨
        def owner(_api, _w):
            raise client.ApiException(status=503, reason="unavailable")
        with self.assertLogs("dispatcher", level="ERROR"):
            self.run_loop([cl("worker1", 4)], [wl("rec", 1, "T1", priority=K.RECOVERY_PRIORITY_VALUE)], owner)
        self.assertEqual(self.nominated, {})

    def test_relocation_hold_keeps_freed_seats_for_new_job(self):
        # 옛 잡이 내려가 worker1에 4장이 비었고, 쿼터를 쥔 학습 잡 wait-c가 기다리는 중 (시나리오 11)
        cs = [cl("worker1", 4), cl("worker2", 0), cl("worker3", 0)]
        self.run_loop(cs, [wl("wait-c", 4, "T1")], holds={"worker1": "job-a-g0002"})
        self.assertEqual(self.nominated, {})
        # 재제출 잡이 쿼터를 받으면 그 잡만 지켜 둔 클러스터로 들어간다
        cs = [cl("worker1", 4), cl("worker2", 0), cl("worker3", 0)]
        self.run_loop(cs, [wl("wait-c", 4, "T1"), wl("job-a-g0002", 4, "T2", priority=K.RECOVERY_PRIORITY_VALUE)],
                      holds={"worker1": "job-a-g0002"})
        self.assertEqual(self.nominated, {"job-a-g0002": "worker1"})

    def test_relocation_hold_blocks_other_recovery_jobs_too(self):
        cs = [cl("worker1", 4), cl("worker2", 1), cl("worker3", 0)]
        self.run_loop(cs, [wl("job-b-g0002", 1, "T1", priority=K.RECOVERY_PRIORITY_VALUE)],
                      holds={"worker1": "job-a-g0002"})
        self.assertEqual(self.nominated, {"job-b-g0002": "worker2"})


def rr(action="moveToSpare", phase="Executing", **st):
    status = {"phase": phase, "sourceCluster": "worker1", "jobId": "a", "toGeneration": 2,
              "isolatedAt": D.kube.now_rfc3339(), **st}
    return {"spec": {"action": action}, "status": {k: v for k, v in status.items() if v is not None}}


class RelocationHolds(unittest.TestCase):
    def holds(self, rrs, wls=()):
        co = mock.Mock()
        co.list_namespaced_custom_object.return_value = {"items": rrs}
        with mock.patch.object(D.kube, "kueue", return_value=co):
            return D.relocation_holds(None, list(wls))

    def test_in_place_actions_hold_source(self):
        self.assertEqual(self.holds([rr()]), {"worker1": "job-a-g0002"})
        self.assertEqual(self.holds([rr("restartInPlace")]), {"worker1": "job-a-g0002"})

    def test_leaving_actions_and_finished_requests_do_not_hold(self):
        self.assertEqual(self.holds([rr("moveToOtherCluster"), rr("evacuateCluster")]), {})
        self.assertEqual(self.holds([rr(phase="Done"), rr(phase="Failed"), rr(phase="Duplicate")]), {})

    def test_not_before_isolation_or_without_source(self):
        self.assertEqual(self.holds([rr(phase="Accepted", isolatedAt=None)]), {})
        self.assertEqual(self.holds([rr(sourceCluster="")]), {})

    def test_released_when_new_job_is_placed(self):
        self.assertEqual(self.holds([rr()], [wl("job-a-g0002", 4, "T1", nominated=["worker1"])]), {})
        self.assertEqual(self.holds([rr()], [wl("job-a-g0002", 4, "T1")]), {"worker1": "job-a-g0002"})   # 대기 중

    def test_expires(self):
        self.assertEqual(self.holds([rr(isolatedAt="2020-01-01T00:00:00Z")]), {})
        # 옛 실행 종료가 늦었으면 종료 시각부터 다시 센다
        self.assertEqual(self.holds([rr(isolatedAt="2020-01-01T00:00:00Z", stoppedAt=D.kube.now_rfc3339())]),
                         {"worker1": "job-a-g0002"})

    def test_lookup_failure_means_no_holds(self):
        co = mock.Mock()
        co.list_namespaced_custom_object.side_effect = client.ApiException(status=403)
        with mock.patch.object(D.kube, "kueue", return_value=co), self.assertLogs("dispatcher", level="WARNING"):
            self.assertEqual(D.relocation_holds(None, []), {})




class FillCapacity(unittest.TestCase):
    """예비 노드(devoops/spare taint)의 GPU는 자리 표시 Pod가 있든 없든 복구 잡 몫 (11/10 정본 I4)"""

    @staticmethod
    def node(name, *, spare=False, gpus=1, ready=True, cordon=False):
        from types import SimpleNamespace as N
        taints = [N(key=K.TAINT_SPARE)] if spare else []
        return N(metadata=N(name=name), spec=N(unschedulable=cordon, taints=taints),
                 status=N(allocatable={K.GPU: str(gpus)}, conditions=[N(type="Ready", status="True" if ready else "False")]))

    @staticmethod
    def pod(node, gpus=1, placeholder=False):
        from types import SimpleNamespace as N
        ct = N(resources=N(requests={K.GPU: str(gpus)}))
        return N(spec=N(node_name=node, containers=[ct],
                        priority_class_name=K.PLACEHOLDER_PRIORITY_CLASS if placeholder else None))

    def fill(self, nodes, pods):
        from types import SimpleNamespace as N
        core = mock.Mock()
        core.list_pod_for_all_namespaces.return_value = N(items=pods)
        core.list_node.return_value = N(items=nodes)
        kq = mock.Mock()
        kq.list_cluster_custom_object.return_value = {"items": [
            {"spec": {"resourceGroups": [{"flavors": [{"resources": [{"name": K.GPU, "nominalQuota": "4"}]}]}]},
             "status": {}},
            {"spec": {"resourceGroups": [{"flavors": [{"resources": [{"name": K.GPU, "nominalQuota": "1",
                                                                       "lendingLimit": "0"}]}]}]}, "status": {}}]}
        c = Cluster(name="w", active=True)
        with mock.patch.object(D.client, "CoreV1Api", return_value=core), mock.patch.object(D.kube, "kueue", return_value=kq):
            D.fill_capacity(c, object())
        return c

    def test_spare_without_placeholder_is_recovery_only(self):
        nodes = [self.node(f"g{i}") for i in range(4)] + [self.node("s", spare=True)]
        c = self.fill(nodes, [])
        self.assertEqual((c.node_free, c.spare_free, c.node_total), (4, 1, 4))
        self.assertEqual(c.free_for(False), 4)
        self.assertEqual(c.free_for(True), 5)

    def test_spare_with_placeholder_and_busy_spare(self):
        nodes = [self.node("g0"), self.node("s1", spare=True), self.node("s2", spare=True)]
        c = self.fill(nodes, [self.pod("s1", placeholder=True), self.pod("s2")])   # s2는 복구 잡이 쓰는 중
        self.assertEqual((c.node_free, c.spare_free), (1, 1))

    def test_cordoned_spare_not_counted(self):
        c = self.fill([self.node("s", spare=True, cordon=True)], [])
        self.assertEqual(c.spare_free, 0)


if __name__ == "__main__":
    unittest.main()
