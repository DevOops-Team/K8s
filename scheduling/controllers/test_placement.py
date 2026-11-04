"""python -m unittest -v test_placement.py  (쿠버네티스 없이 돈다)"""
import unittest

from placement import Cluster, choose, fits, reserve_for_starving


def c(name, quota, nodes, **kw):
    return Cluster(name=name, quota_free=quota, node_free=nodes, **kw)


class Fits(unittest.TestCase):
    def test_gang_needs_all_pods(self):
        self.assertTrue(fits([1, 1, 1, 1], [1, 1, 1, 1]))
        self.assertFalse(fits([1, 1, 1, 1], [1, 1, 1]))      # 노드 3대 -> 4 Pod 잡은 안 됨

    def test_bigger_pods_first(self):
        self.assertTrue(fits([2, 1], [1, 2]))
        self.assertFalse(fits([2, 2], [1, 1, 1, 1]))          # 합은 되지만 노드가 쪼개짐


class Choose(unittest.TestCase):
    def test_binpack_fills_fullest_first(self):
        cs = [c("worker1", 1, [1]), c("worker2", 4, [1, 1, 1, 1]), c("worker3", 2, [1, 1])]
        self.assertEqual(choose([1], cs), "worker1")

    def test_tie_by_name(self):
        cs = [c("worker2", 4, [1] * 4), c("worker1", 4, [1] * 4)]
        self.assertEqual(choose([1], cs), "worker1")

    def test_quota_ok_but_nodes_cordoned(self):
        cs = [c("worker1", 4, [1, 1, 1]), c("worker2", 4, [1] * 4)]
        self.assertEqual(choose([1] * 4, cs), "worker2")

    def test_fenced_inactive_excluded(self):
        cs = [c("worker1", 4, [1] * 4, fenced=True), c("worker2", 4, [1] * 4, active=False),
              c("worker3", 4, [1] * 4)]
        self.assertEqual(choose([1], cs), "worker3")
        self.assertEqual(choose([1], cs, exclude={"worker3"}), None)

    def test_target(self):
        cs = [c("worker1", 1, [1]), c("worker2", 4, [1] * 4)]
        self.assertEqual(choose([1], cs, target="worker2"), "worker2")

    def test_spare_only_for_recovery(self):
        cs = [c("worker1", 4, [1, 1, 1], spare_free=[1])]
        self.assertIsNone(choose([1] * 4, cs))
        self.assertEqual(choose([1] * 4, cs, recovery=True), "worker1")

    def test_blocked_cluster_skipped(self):
        cs = [c("worker1", 2, [1, 1]), c("worker2", 4, [1] * 4)]
        self.assertEqual(choose([1], cs, blocked={"worker1"}), "worker2")


class Starving(unittest.TestCase):
    def test_reserve_emptiest(self):
        cs = [c("worker1", 1, [1]), c("worker2", 3, [1, 1, 1]), c("worker3", 0, [])]
        self.assertEqual(reserve_for_starving([1] * 4, cs), "worker2")


if __name__ == "__main__":
    unittest.main()
