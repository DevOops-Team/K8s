"""python -m unittest -v test_dispatcher.py  (쿠버네티스 없이 돈다: 조회·patch를 가짜로 바꿈)"""
import unittest
from unittest import mock

from kubernetes import client

import dispatcher as D
import keys as K
from placement import Cluster

# 쿼터 예약은 기본 켜짐이지만 여기서는 지명·찜 동작만 본다 (쿼터 예약은 test_quota_hold.py)
_quota_hold_off = mock.patch.object(D.quota_hold, "ENABLED", False)


def setUpModule():
    _quota_hold_off.start()


def tearDownModule():
    _quota_hold_off.stop()


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


class RecoveryFallback(unittest.TestCase):
    run_loop = Reconcile.run_loop
    def annotations(self, _api, workload):
        if workload['metadata']['name'].startswith('rec'):
            return {K.ANN_RECOVERY_ID: 'r1', K.ANN_PREFERRED: 'eks-a'}
        return {}

    def recovery(self, name='rec', nominated=None):
        return wl(name, 4, 'T1', priority=K.RECOVERY_PRIORITY_VALUE, nominated=nominated)

    def test_source_wins_even_when_another_cluster_has_tighter_fit(self):
        self.run_loop([cl('eks-a', 5, total=5), cl('eks-b', 4)], [self.recovery()], self.annotations)
        self.assertEqual(self.nominated, {'rec': 'eks-a'})

    def test_source_uses_training_plus_spare(self):
        self.run_loop([cl('eks-a', 3, spare_free=1, recovery_free=1), cl('eks-b', 4)],
                      [self.recovery()], self.annotations)
        self.assertEqual(self.nominated, {'rec': 'eks-a'})

    def test_insufficient_source_falls_back_and_releases_its_hold(self):
        self.run_loop([cl('eks-a', 3), cl('eks-b', 4)],
                      [self.recovery(), wl('small', 1, 'T2')], self.annotations,
                      holds={'eks-a': 'rec'})
        self.assertEqual(self.nominated, {'rec': 'eks-b', 'small': 'eks-a'})

    def test_no_cluster_fits_waits_at_source_without_partial_nomination(self):
        self.run_loop([cl('eks-a', 3), cl('eks-b', 3)], [self.recovery()], self.annotations)
        self.assertEqual(self.nominated, {})
        self.assertEqual(self.reserved, {'rec': 'eks-a'})

    def test_unreserved_destination_precedes_training_reservation(self):
        self.run_loop([cl('eks-a', 3), cl('eks-b', 4), cl('eks-c', 4)],
                      [wl('heavy', 4, 'T0', reserved='eks-b'), self.recovery()], self.annotations)
        self.assertEqual(self.nominated['rec'], 'eks-c')

    def test_only_reserved_destination_is_used_and_audited(self):
        with self.assertLogs('dispatcher', level='WARNING') as logs:
            self.run_loop([cl('eks-a', 3), cl('eks-b', 4)],
                          [wl('heavy', 4, 'T0', reserved='eks-b'), self.recovery()], self.annotations)
        self.assertEqual(self.nominated['rec'], 'eks-b')
        self.assertTrue(any('heavy' in line and 'rec' in line and 'eks-b' in line for line in logs.output))

    def test_fenced_inactive_and_excluded_destinations_are_not_used(self):
        for changes in ({'fenced': True}, {'active': False}, {}):
            def annotations(api, w):
                return dict(self.annotations(api, w), **{K.ANN_EXCLUDE: 'eks-b' if not changes else ''})
            with self.subTest(changes=changes):
                self.run_loop([cl('eks-a', 3), cl('eks-b', 4, **changes)], [self.recovery()], annotations)
                self.assertEqual(self.nominated, {})

    def test_explicit_target_stays_hard_for_normal_and_recovery_jobs(self):
        for priority in (100, K.RECOVERY_PRIORITY_VALUE):
            self.run_loop([cl('eks-a', 3), cl('eks-b', 4)], [wl('pinned', 4, 'T1', priority=priority)],
                          lambda _a, _w: {K.ANN_TARGET: 'eks-a', K.ANN_PREFERRED: 'eks-a', K.ANN_RECOVERY_ID: 'r'})
            self.assertEqual(self.nominated, {})

    def test_failed_nomination_can_be_replaced_after_existing_retry_interval(self):
        self.run_loop([cl('eks-a', 3), cl('eks-b', 4)], [self.recovery(nominated=['eks-a'])], self.annotations)
        self.assertEqual(self.nominated, {'rec': 'eks-b'})

    def test_same_loop_does_not_double_book_fallback(self):
        self.run_loop([cl('eks-a', 3), cl('eks-b', 4)], [self.recovery('rec1'), self.recovery('rec2')], self.annotations)
        self.assertEqual(self.nominated, {'rec1': 'eks-b'})


class FencedAck(unittest.TestCase):
    """GPU-125: 끝까지 돈 루프만, fenced로 본 클러스터에만 그 루프 시작 시각을 남긴다"""

    def test_marks_only_fenced_clusters_with_loop_start(self):
        co = mock.Mock()
        with mock.patch.object(D.kube, "kueue", return_value=co):
            D.ack_fenced(None, [cl("eks-a", 0, fenced=True), cl("eks-b", 4)], "2026-11-11T05:00:00Z")
        co.patch_cluster_custom_object.assert_called_once_with(
            K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", "eks-a",
            {"metadata": {"annotations": {K.ANN_FENCED_ACK: "2026-11-11T05:00:00Z"}}})

    def test_patch_failure_does_not_raise(self):
        co = mock.Mock()
        co.patch_cluster_custom_object.side_effect = client.ApiException(status=403)
        with mock.patch.object(D.kube, "kueue", return_value=co):
            D.ack_fenced(None, [cl("eks-a", 0, fenced=True), cl("eks-b", 0, fenced=True)], "t")
        self.assertEqual(co.patch_cluster_custom_object.call_count, 2)

    def test_reconcile_returns_what_it_saw_even_when_all_disconnected(self):
        cs = [cl("eks-a", 0, fenced=True, active=False)]
        with mock.patch.object(D, "cluster_state", return_value=cs), \
                mock.patch.object(D.kube, "list_workloads", return_value=[]):
            self.assertIs(D.reconcile(None, None), cs)

    def test_failed_loop_leaves_no_ack(self):
        class Stop(Exception):
            pass
        elector = mock.Mock()
        elector.is_leader.return_value = True
        with mock.patch.object(D.kube, "manager_client"), mock.patch.object(D.kube, "Workers"), \
             mock.patch.object(D.leader, "Elector", return_value=elector), \
             mock.patch.object(D, "reconcile", side_effect=RuntimeError("boom")), \
             mock.patch.object(D, "ack_fenced") as ack, \
             mock.patch.object(D.time, "sleep", side_effect=Stop):
            with self.assertRaises(Stop):
                D.main()
        ack.assert_not_called()


def rr(action="moveToSpare", phase="Executing", **st):
    status = {"phase": phase, "sourceCluster": "worker1", "jobId": "a", "toGeneration": 2,
              "isolatedAt": D.kube.now_rfc3339(), **st}
    return {"spec": {"action": action}, "status": {k: v for k, v in status.items() if v is not None}}


class RelocationHolds(unittest.TestCase):
    def holds(self, rrs, wls=(), clusters=None):
        co = mock.Mock()
        co.list_namespaced_custom_object.return_value = {"items": rrs}
        with mock.patch.object(D.kube, "kueue", return_value=co):
            return D.relocation_holds(None, list(wls), clusters)

    def test_submitted_job_releases_insufficient_source_hold(self):
        self.assertEqual(self.holds([rr()], [wl('job-a-g0002', 4, 'T1')], [cl('worker1', 3)]), {})
        self.assertEqual(self.holds([rr()], [wl('job-a-g0002', 4, 'T1')], [cl('worker1', 4)]),
                         {'worker1': 'job-a-g0002'})

    def test_in_place_actions_hold_source(self):
        self.assertEqual(self.holds([rr()]), {"worker1": "job-a-g0002"})
        self.assertEqual(self.holds([rr("restartInPlace")]), {"worker1": "job-a-g0002"})

    def test_leaving_actions_and_finished_requests_do_not_hold(self):
        self.assertEqual(self.holds([rr("evacuateCluster")]), {})
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

    @staticmethod
    def pending(gpus=1, *, spare_toleration=False, placeholder=False):
        from types import SimpleNamespace as N
        p = FillCapacity.pod(None, gpus, placeholder)
        p.spec.tolerations = [N(key=K.TAINT_SPARE)] if spare_toleration else None
        return p

    def test_unscheduled_gpu_pods_take_normal_nodes(self):
        """GPU-260: 입장했지만 노드를 아직 못 잡은 Pod도 자리를 쓴다 (blockAdmission 끈 뒤 이중 배정 방지)"""
        nodes = [self.node(f"g{i}") for i in range(4)] + [self.node("s", spare=True)]
        c = self.fill(nodes, [self.pod("g0"), self.pending(), self.pending()])
        self.assertEqual((c.node_free, c.spare_free, c.node_total), (1, 1, 4))

    def test_unscheduled_recovery_pods_use_spare_first(self):
        nodes = [self.node(f"g{i}") for i in range(4)] + [self.node("s", spare=True)]
        c = self.fill(nodes, [self.pending(spare_toleration=True), self.pending(spare_toleration=True)])
        self.assertEqual((c.node_free, c.spare_free), (3, 0))

    def test_unscheduled_ignores_cpu_and_placeholder_pods(self):
        nodes = [self.node(f"g{i}") for i in range(2)]
        c = self.fill(nodes, [self.pending(0), self.pending(placeholder=True)])
        self.assertEqual(c.node_free, 2)

    def test_unscheduled_never_below_zero(self):
        c = self.fill([self.node("g0")], [self.pending(), self.pending()])
        self.assertEqual(c.node_free, 0)


class PlacementRecord(unittest.TestCase):
    """실제 배치 기록 devoops/placement (GPU-268): 전원 Running일 때만 쓰고, 지우지 않는다"""

    JS = "job-j1-g0002"

    @staticmethod
    def pod(node, *, jobset="job-j1-g0002", ns="training", phase="Running", gpus=1, placeholder=False, deleting=False):
        from types import SimpleNamespace as N
        ct = N(resources=N(requests={K.GPU: str(gpus)}))
        return N(metadata=N(namespace=ns, labels={"jobset.sigs.k8s.io/jobset-name": jobset},
                            deletion_timestamp="x" if deleting else None),
                 status=N(phase=phase),
                 spec=N(node_name=node, containers=[ct],
                        priority_class_name=K.PLACEHOLDER_PRIORITY_CLASS if placeholder else None))

    def workload(self, gpus=2, cluster="eks-a", old=None, finished=False, name=None):
        w = wl("wl-1", gpus, "2026-11-24T00:00:00Z")
        w["metadata"]["ownerReferences"] = [{"kind": "JobSet", "name": name or self.JS}]
        w["status"]["clusterName"] = cluster
        if finished:
            w["status"]["conditions"].append({"type": "Finished", "status": "True"})
        if old is not None:
            w["metadata"]["annotations"][K.ANN_PLACEMENT] = old if isinstance(old, str) else D.json.dumps(old)
        return w

    def cluster(self, pods, name="eks-a", at=1000, active=True):
        c = Cluster(name=name, active=active)
        c.observed_at = at
        D.collect_running(pods, c.running)
        return c

    def record(self, wls, clusters, fail=False):
        written = []

        def fake(_m, w, rec):
            if fail:
                raise RuntimeError("patch 실패")
            written.append((w["metadata"]["name"], rec))
        with mock.patch.object(D, "set_placement", side_effect=fake):
            D.record_placements(None, wls, clusters)
        return written

    def test_all_running_is_recorded(self):
        out = self.record([self.workload()], [self.cluster([self.pod("n2"), self.pod("n1")])])
        self.assertEqual(out, [("wl-1", {"version": 1, "jobset": self.JS, "generation": 2, "cluster": "eks-a",
                                          "nodes": ["n1", "n2"], "observed_at": 1000})])

    def test_partial_pods_do_not_overwrite_last_snapshot(self):
        old = {"version": 1, "jobset": self.JS, "generation": 2, "cluster": "eks-a", "nodes": ["n1", "n2"],
               "observed_at": 900}
        pods = [self.pod("n1"), self.pod("n2", phase="Pending")]
        self.assertEqual(self.record([self.workload(old=old)], [self.cluster(pods)]), [])

    def test_not_scheduled_deleting_placeholder_and_cpu_pods_are_ignored(self):
        pods = [self.pod("n1"), self.pod(None), self.pod("n3", deleting=True),
                self.pod("n4", placeholder=True), self.pod("n5", gpus=0)]
        self.assertEqual(self.record([self.workload()], [self.cluster(pods)]), [])

    def test_same_content_refreshed_only_after_interval(self):
        old = {"version": 1, "jobset": self.JS, "generation": 2, "cluster": "eks-a", "nodes": ["n1", "n2"],
               "observed_at": 1000 - D.PLACEMENT_REFRESH_SECONDS + 1}
        pods = [self.pod("n1"), self.pod("n2")]
        self.assertEqual(self.record([self.workload(old=old)], [self.cluster(pods)]), [])
        old["observed_at"] = 1000 - D.PLACEMENT_REFRESH_SECONDS
        self.assertEqual(len(self.record([self.workload(old=old)], [self.cluster(pods)])), 1)

    def test_changed_nodes_written_immediately(self):
        old = {"version": 1, "jobset": self.JS, "generation": 2, "cluster": "eks-a", "nodes": ["n1", "n9"],
               "observed_at": 999}
        out = self.record([self.workload(old=old)], [self.cluster([self.pod("n1"), self.pod("n2")])])
        self.assertEqual(out[0][1]["nodes"], ["n1", "n2"])

    def test_older_observation_never_replaces_newer(self):
        old = {"version": 1, "jobset": self.JS, "generation": 2, "cluster": "eks-a", "nodes": ["n1", "n9"],
               "observed_at": 2000}
        self.assertEqual(self.record([self.workload(old=old)], [self.cluster([self.pod("n1"), self.pod("n2")])]), [])

    def test_broken_old_annotation_is_replaced(self):
        out = self.record([self.workload(old="{not json")], [self.cluster([self.pod("n1"), self.pod("n2")])])
        self.assertEqual(len(out), 1)

    def test_inactive_or_unread_worker_is_skipped(self):
        pods = [self.pod("n1"), self.pod("n2")]
        self.assertEqual(self.record([self.workload()], [self.cluster(pods, active=False)]), [])
        self.assertEqual(self.record([self.workload()], [self.cluster(pods, at=None)]), [])

    def test_pods_of_other_cluster_namespace_or_jobset_do_not_count(self):
        pods = [self.pod("n1"), self.pod("n2", jobset="job-j1-g0001"), self.pod("n3", ns="other")]
        self.assertEqual(self.record([self.workload()], [self.cluster(pods)]), [])
        self.assertEqual(self.record([self.workload(cluster="eks-b")],
                                     [self.cluster([self.pod("n1"), self.pod("n2")])]), [])

    def test_duplicate_nodes_or_count_mismatch_skipped(self):
        self.assertEqual(self.record([self.workload()], [self.cluster([self.pod("n1"), self.pod("n1")])]), [])
        self.assertEqual(self.record([self.workload()],
                                     [self.cluster([self.pod("n1"), self.pod("n2"), self.pod("n3")])]), [])

    def test_finished_or_unnamed_generation_skipped(self):
        pods = [self.pod("n1"), self.pod("n2")]
        self.assertEqual(self.record([self.workload(finished=True)], [self.cluster(pods)]), [])
        self.assertEqual(self.record([self.workload(name="free-name")],
                                     [self.cluster([self.pod("n1", jobset="free-name"),
                                                    self.pod("n2", jobset="free-name")])]), [])

    def test_patch_failure_does_not_raise(self):
        self.assertEqual(self.record([self.workload()], [self.cluster([self.pod("n1"), self.pod("n2")])],
                                     fail=True), [])

    def test_record_failure_does_not_stop_nomination(self):
        nominated = {}
        with mock.patch.object(D, "cluster_state", return_value=[cl("w1", 4)]), \
             mock.patch.object(D, "relocation_holds", return_value={}), \
             mock.patch.object(D.kube, "list_workloads", return_value=[wl("a", 2, "2026-11-10T00:00:00Z")]), \
             mock.patch.object(D.kube, "owner_annotations", return_value={}), \
             mock.patch.object(D, "record_placements", side_effect=RuntimeError("boom")), \
             mock.patch.object(D, "patch_nomination",
                               side_effect=lambda _m, w, c, _t: nominated.__setitem__(w["metadata"]["name"], c)):
            D.reconcile(None, None)
        self.assertEqual(nominated, {"a": "w1"})

    def test_fill_capacity_collects_running_from_same_pod_list(self):
        from types import SimpleNamespace as N
        core = mock.Mock()
        core.list_pod_for_all_namespaces.return_value = N(items=[self.pod("g0"), self.pod("g1")])
        core.list_node.return_value = N(items=[])
        kq = mock.Mock()
        kq.list_cluster_custom_object.return_value = {"items": []}
        c = Cluster(name="eks-a")
        c.running[("old", "x")] = ["stale"]
        with mock.patch.object(D.client, "CoreV1Api", return_value=core), \
             mock.patch.object(D.kube, "kueue", return_value=kq), mock.patch.object(D.time, "time", return_value=1234.9):
            D.fill_capacity(c, object())
        self.assertEqual(c.running, {("training", self.JS): ["g0", "g1"]})
        self.assertEqual(c.observed_at, 1234)
        core.list_pod_for_all_namespaces.assert_called_once()


class RecoveryCapacity(unittest.TestCase):
    """복구 자리 조회 API (ADR-37 조건 4): 원래 클러스터 우선, 없으면 다른 워커"""

    REQ = {"job_id": "j1", "generation": 1, "cluster": "w1", "node": "g0", "gpu_count": 2}

    @staticmethod
    def mkc(name, active=True, fenced=False):
        return {"metadata": {"name": name, "labels": {K.LABEL_FENCED: "true"} if fenced else {}},
                "status": {"conditions": [{"type": "Active", "status": "True" if active else "False"}]}}

    def ask(self, free, mkcs=None, exclude="", apis=None, failing=()):
        """free: 클러스터 이름 → (node_free, spare_free)"""
        mkcs = mkcs or [self.mkc(n) for n in free]
        by_name = {m["metadata"]["name"]: m for m in mkcs}
        kq = mock.Mock()
        kq.get_cluster_custom_object.side_effect = lambda *a, **k: by_name[a[3]]
        kq.list_cluster_custom_object.return_value = {"items": mkcs}
        workers = mock.Mock()
        workers.get.return_value = apis if apis is not None else {n: n for n in free}

        def fill(c, api, **_):
            if api in failing:
                raise RuntimeError("down")
            c.node_free, c.spare_free = free[api]

        workload = {"metadata": {"namespace": "training", "ownerReferences": [{"kind": "JobSet", "name": "x"}]},
                    "status": {"clusterName": "w1"}}
        with mock.patch.object(D.kube, "workload_for_jobset", return_value=workload), \
                mock.patch.object(D.kube, "kueue", return_value=kq), \
                mock.patch.object(D.kube, "owner_annotations", return_value={K.ANN_EXCLUDE: exclude}), \
                mock.patch.object(D, "fill_node_capacity", side_effect=fill):
            return D.recovery_capacity(object(), workers, dict(self.REQ))

    def test_source_spare_first(self):
        r = self.ask({"w1": (1, 1), "w2": (4, 1)})
        self.assertEqual((r["available"], r["cluster"]), (True, "w1"))

    def test_source_without_spare_falls_back_to_other_worker(self):
        r = self.ask({"w1": (4, 0), "w2": (1, 1)})
        self.assertEqual((r["available"], r["cluster"]), (True, "w2"))
        self.assertEqual((r["spare_free"], r["normal_free_after_stop"]), (0, 4))   # 원래 클러스터 값

    def test_source_too_small_falls_back(self):
        r = self.ask({"w1": (0, 1), "w2": (2, 0)})
        self.assertEqual(r["cluster"], "w2")

    def test_other_worker_skipped_when_fenced_inactive_excluded_or_failing(self):
        free = {"w1": (0, 0), "w2": (4, 1), "w3": (4, 1), "w4": (4, 1), "w5": (4, 1)}
        mkcs = [self.mkc("w1"), self.mkc("w2", fenced=True), self.mkc("w3", active=False),
                self.mkc("w4"), self.mkc("w5")]
        r = self.ask(free, mkcs=mkcs, exclude="w4", failing=("w5",))
        self.assertEqual((r["available"], r["cluster"]), (False, None))

    def test_no_room_anywhere(self):
        r = self.ask({"w1": (1, 0), "w2": (1, 0)})
        self.assertFalse(r["available"])

    def test_source_unusable_is_error(self):
        with self.assertRaises(RuntimeError):
            self.ask({"w1": (4, 1)}, mkcs=[self.mkc("w1", fenced=True)])



class UnplacedWarning(unittest.TestCase):
    """갈 곳 없는 잡 경고 (GPU-237): 2026-11-20 워커 둘 다 펜싱인데 로그가 없어 68분 동안 원인을 못 봤다"""

    def setUp(self):
        D._unplaced.clear()
        self.addCleanup(D._unplaced.clear)

    def run_loop(self, clusters, wls):
        with mock.patch.object(D, "cluster_state", return_value=clusters), \
             mock.patch.object(D, "relocation_holds", return_value={}), \
             mock.patch.object(D.kube, "list_workloads", return_value=wls), \
             mock.patch.object(D.kube, "owner_annotations", return_value={}), \
             mock.patch.object(D, "patch_nomination"), mock.patch.object(D, "set_reserved"), \
             mock.patch.object(D, "touch_nomination"), \
             self.assertNoLogs(D.log, "ERROR"):
            with mock.patch.object(D.log, "warning") as warning:
                D.reconcile(None, None)
        return [c for c in warning.call_args_list if "찜할 워커도 없음 -> 대기" in c.args[0]]

    def fenced_all(self):
        return [cl("eks-a", 4, fenced=True), cl("eks-b", 4, fenced=True)]

    def test_all_fenced_warns_once_per_job_with_reason(self):
        calls = self.run_loop(self.fenced_all(), [wl("j1", 2, "T1"), wl("j2", 4, "T2")])
        self.assertEqual([c.args[1] for c in calls], ["j1", "j2"])
        self.assertEqual(calls[0].args[-1], "eks-a 펜싱, eks-b 펜싱")

    def test_same_reason_not_repeated_every_loop(self):
        self.run_loop(self.fenced_all(), [wl("j1", 2, "T1")])
        self.assertEqual(self.run_loop(self.fenced_all(), [wl("j1", 2, "T1")]), [])

    def test_repeats_after_interval_or_when_reason_changes(self):
        self.run_loop(self.fenced_all(), [wl("j1", 2, "T1")])
        D._unplaced["j1"] = (D._unplaced["j1"][0], 0)   # 간격이 지난 것처럼
        self.assertEqual(len(self.run_loop(self.fenced_all(), [wl("j1", 2, "T1")])), 1)
        changed = [cl("eks-a", 4, active=False), cl("eks-b", 4, fenced=True)]
        calls = self.run_loop(changed, [wl("j1", 2, "T1")])
        self.assertEqual(calls[0].args[-1], "eks-a 끊김, eks-b 펜싱")

    def test_no_warning_when_nominated_or_reserved(self):
        # 들어갈 곳이 있으면 지명, 자리만 없으면 찜 -> 둘 다 이 경고 대상이 아님
        self.assertEqual(self.run_loop([cl("eks-a", 4), cl("eks-b", 0)], [wl("j1", 2, "T1"), wl("big", 4, "T2")]), [])
        self.assertEqual(D._unplaced, {})

    def test_forgets_jobs_that_stop_waiting(self):
        self.run_loop(self.fenced_all(), [wl("j1", 2, "T1")])
        self.run_loop(self.fenced_all(), [])
        self.assertEqual(D._unplaced, {})

    def test_reason_lists_capacity_and_exclusion(self):
        cs = [cl("w1", 0, total=1), cl("w2", 4, fenced=True), cl("w3", 2)]
        self.assertEqual(D.unplaced_reason(cs, 4, False, {"w3"}, None), "w1 GPU 부족, w2 펜싱, w3 exclude·다른 잡의 찜")
        self.assertEqual(D.unplaced_reason(cs, 4, False, set(), "w9"), "target w9 없음")


if __name__ == "__main__":
    unittest.main()
