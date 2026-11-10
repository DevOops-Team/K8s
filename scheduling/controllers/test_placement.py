"""python -m unittest -v test_placement.py  (쿠버네티스 없이 돈다)"""
import unittest

from placement import Cluster, choose, reserve


def c(name, quota, nodes, **kw):
    return Cluster(name=name, quota_free=quota, node_free=nodes, **kw)


class Choose(unittest.TestCase):
    def test_binpack_fills_fullest_first(self):
        cs = [c("worker1", 1, 1), c("worker2", 4, 4), c("worker3", 2, 2)]
        self.assertEqual(choose(1, cs), "worker1")

    def test_tie_by_name(self):
        cs = [c("worker2", 4, 4), c("worker1", 4, 4)]
        self.assertEqual(choose(1, cs), "worker1")

    def test_quota_ok_but_nodes_cordoned(self):
        # 쿼터는 4인데 cordon으로 노드 3대뿐 -> 4장 잡은 다른 곳
        cs = [c("worker1", 4, 3), c("worker2", 4, 4)]
        self.assertEqual(choose(4, cs), "worker2")

    def test_nodes_ok_but_quota_short(self):
        cs = [c("worker1", 3, 4), c("worker2", 4, 4)]
        self.assertEqual(choose(4, cs), "worker2")

    def test_fenced_inactive_excluded(self):
        cs = [c("worker1", 4, 4, fenced=True), c("worker2", 4, 4, active=False), c("worker3", 4, 4)]
        self.assertEqual(choose(1, cs), "worker3")
        self.assertEqual(choose(1, cs, exclude={"worker3"}), None)

    def test_target(self):
        cs = [c("worker1", 1, 1), c("worker2", 4, 4)]
        self.assertEqual(choose(1, cs, target="worker2"), "worker2")

    def test_spare_only_for_recovery(self):
        cs = [c("worker1", 4, 3, spare_free=1)]
        self.assertIsNone(choose(4, cs))
        self.assertEqual(choose(4, cs, recovery=True), "worker1")

    def test_recovery_quota_only_for_recovery(self):
        # 학습 몫 다 씀(0), 복구 몫 1, 노드 1대 빔
        cs = [c("worker1", 0, 1, recovery_free=1)]
        self.assertIsNone(choose(1, cs))
        self.assertEqual(choose(1, cs, recovery=True), "worker1")

    def test_recovery_borrows_training_quota(self):
        # 4장 복구 잡 = 복구 몫 1 + 학습 몫 3 빌림
        cs = [c("worker1", 3, 3, recovery_free=1, spare_free=1)]
        self.assertEqual(choose(4, cs, recovery=True), "worker1")
        self.assertIsNone(choose(4, cs))


class Place(unittest.TestCase):
    def test_recovery_takes_own_quota_and_spare_first(self):
        x = c("worker1", 3, 3, recovery_free=1, spare_free=1)
        x.place(1, recovery=True)
        self.assertEqual((x.quota_free, x.recovery_free, x.node_free, x.spare_free), (3, 0, 3, 0))
        x.place(2, recovery=True)
        self.assertEqual((x.quota_free, x.node_free), (1, 1))

    def test_recovery_leftover_spare_stays_spare(self):
        # 복구 잡이 일반 노드를 써도 예비 자리는 예비로 남음 -> 학습 잡에 빈자리로 안 보임
        x = c("worker1", 4, 2, recovery_free=0, spare_free=1)
        x.place(1, recovery=True)                       # 예비부터 씀
        self.assertEqual((x.node_free, x.spare_free), (2, 0))
        y = c("worker1", 4, 2, spare_free=1)
        self.assertIsNone(choose(3, [y]))               # 학습 3장 잡: 일반 노드 2대뿐 -> 안 됨

    def test_training_never_touches_spare(self):
        x = c("worker1", 4, 2, spare_free=1)
        x.place(2, recovery=False)
        self.assertEqual((x.node_free, x.spare_free, x.quota_free), (0, 1, 2))

    def test_same_loop_double_booking(self):
        # 같은 루프에서 두 잡이 같은 빈자리를 보지 않게
        cs = [c("worker1", 4, 4)]
        self.assertEqual(choose(3, cs), "worker1")
        cs[0].place(3, recovery=False)
        self.assertIsNone(choose(3, cs))
        self.assertEqual(choose(1, cs), "worker1")


class Reserve(unittest.TestCase):
    def test_picks_most_free(self):
        cs = [c("worker1", 1, 1), c("worker2", 3, 3), c("worker3", 2, 2)]
        self.assertEqual(reserve(4, cs), "worker2")

    def test_free_is_min_of_quota_and_nodes(self):
        # worker1은 쿼터 3이지만 노드는 1대만 빔 -> 실제 빈 자리는 worker2가 더 많음
        cs = [c("worker1", 3, 1), c("worker2", 2, 2)]
        self.assertEqual(reserve(4, cs), "worker2")

    def test_pinned_stays_even_if_another_gets_emptier(self):
        # 루프마다 옮기면 옛 워커 빈 칸에 작은 잡이 다시 들어감 -> 한 번 찜하면 그대로
        cs = [c("worker1", 2, 2), c("worker2", 3, 3)]
        self.assertEqual(reserve(4, cs, pinned="worker1"), "worker1")

    def test_pinned_dropped_when_unusable(self):
        cs = [c("worker1", 2, 2, fenced=True), c("worker2", 1, 1)]
        self.assertEqual(reserve(4, cs, pinned="worker1"), "worker2")
        cs = [c("worker1", 2, 2, active=False), c("worker2", 1, 1)]
        self.assertEqual(reserve(4, cs, pinned="worker1"), "worker2")
        cs = [c("worker1", 2, 2), c("worker2", 1, 1)]
        self.assertEqual(reserve(4, cs, pinned="worker1", exclude={"worker1"}), "worker2")   # 앞 잡이 찜함

    def test_never_fits_is_skipped(self):
        # worker1은 cordon으로 노드 3대뿐 -> 다 비어도 4장 잡은 못 들어감
        cs = [c("worker1", 3, 3, quota_total=4, node_total=3), c("worker2", 0, 0, quota_total=4, node_total=4)]
        self.assertEqual(reserve(4, cs), "worker2")
        self.assertEqual(reserve(4, cs, pinned="worker1"), "worker2")
        self.assertEqual(reserve(3, cs), "worker1")

    def test_recovery_counts_spare_and_recovery_quota(self):
        x = c("worker1", 0, 0, quota_total=4, node_total=3, recovery_total=1, spare_free=1, recovery_free=1)
        self.assertIsNone(reserve(4, [x]))
        self.assertEqual(reserve(4, [x], recovery=True), "worker1")

    def test_target_and_none(self):
        cs = [c("worker1", 0, 0), c("worker2", 3, 3)]
        self.assertEqual(reserve(4, cs, target="worker1"), "worker1")
        self.assertIsNone(reserve(4, cs, exclude={"worker1", "worker2"}))


if __name__ == "__main__":
    unittest.main()
