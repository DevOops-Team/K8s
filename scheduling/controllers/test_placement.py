"""python -m unittest -v test_placement.py  (쿠버네티스 없이 돈다)"""
import unittest

from placement import Cluster, choose


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


if __name__ == "__main__":
    unittest.main()
