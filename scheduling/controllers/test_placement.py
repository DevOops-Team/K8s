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

    def test_recovery_quota_only_for_recovery(self):
        # 학습 몫 다 씀(0), 복구 몫 1, 노드 1대 빔
        cs = [c("worker1", 0, [1], recovery_free=1)]
        self.assertIsNone(choose([1], cs))
        self.assertEqual(choose([1], cs, recovery=True), "worker1")

    def test_recovery_borrows_training_quota(self):
        # 4장 복구 잡 = 복구 몫 1 + 학습 몫 3 빌림
        cs = [c("worker1", 3, [1, 1, 1], recovery_free=1, spare_free=[1])]
        self.assertEqual(choose([1] * 4, cs, recovery=True), "worker1")
        self.assertIsNone(choose([1] * 4, cs))

    def test_use_takes_recovery_first(self):
        x = c("worker1", 4, [], recovery_free=1)
        x.use(3, recovery=True)
        self.assertEqual((x.recovery_free, x.quota_free), (0, 2))
        x.use(1, recovery=False)
        self.assertEqual(x.quota_free, 1)

    def test_blocked_cluster_skipped(self):
        cs = [c("worker1", 2, [1, 1]), c("worker2", 4, [1] * 4)]
        self.assertEqual(choose([1], cs, blocked={"worker1"}), "worker2")


class Place(unittest.TestCase):
    def test_recovery_leftover_spare_stays_spare(self):
        # 복구 1장 잡이 예비 노드를 씀 -> 남은 일반 노드 3대만 학습 잡에 보임 (예비 자리가 섞이면 안 됨)
        x = c("worker1", 3, [1, 1, 1], recovery_free=1, spare_free=[1])
        x.place([1], recovery=True)
        self.assertEqual((x.node_free, x.spare_free), ([1, 1, 1], []))
        self.assertEqual((x.quota_free, x.recovery_free), (3, 0))

    def test_recovery_does_not_turn_unused_spare_into_free(self):
        # 일반 노드에 2장짜리 Pod 하나가 들어가도 예비 자리는 예비로 남음
        x = c("worker1", 4, [2, 1], recovery_free=1, spare_free=[1])
        x.place([2], recovery=True)
        self.assertEqual((sorted(x.node_free), x.spare_free), ([1], [1]))
        self.assertIsNone(choose([1, 1], [x]))           # 학습 2 Pod 잡: 일반 노드 1대뿐 -> 안 됨

    def test_training_never_touches_spare(self):
        x = c("worker1", 4, [1, 1], spare_free=[1])
        x.place([1, 1], recovery=False)
        self.assertEqual((x.node_free, x.spare_free, x.quota_free), ([], [1], 2))


class Starving(unittest.TestCase):
    def test_reserve_emptiest(self):
        cs = [c("worker1", 1, [1]), c("worker2", 3, [1, 1, 1]), c("worker3", 0, [])]
        self.assertEqual(reserve_for_starving([1] * 4, cs), "worker2")

    def test_reserve_respects_target(self):
        cs = [c("worker1", 1, [1]), c("worker2", 3, [1, 1, 1])]
        self.assertEqual(reserve_for_starving([1] * 4, cs, target="worker1"), "worker1")

    def test_reserve_respects_exclude_and_fencing(self):
        cs = [c("worker1", 1, [1]), c("worker2", 3, [1, 1, 1]), c("worker3", 4, [1] * 4, fenced=True)]
        self.assertEqual(reserve_for_starving([1] * 4, cs, exclude={"worker2"}), "worker1")
        self.assertIsNone(reserve_for_starving([1] * 4, cs, exclude={"worker1", "worker2"}))

    def test_reserve_sticks_to_prev(self):
        # 지난 루프에 worker1을 비워 두기 시작함 -> worker2가 잠깐 더 비어 보여도 바꾸지 않음
        cs = [c("worker1", 2, [1, 1]), c("worker2", 3, [1, 1, 1])]
        self.assertEqual(reserve_for_starving([1] * 4, cs, prev="worker1"), "worker1")
        cs[0].fenced = True                                # 그 사이 펜싱되면 다른 곳으로
        self.assertEqual(reserve_for_starving([1] * 4, cs, prev="worker1"), "worker2")

    def test_reserve_counts_spare_for_recovery(self):
        cs = [c("worker1", 2, [1, 1], recovery_free=1, spare_free=[1]), c("worker2", 3, [1, 1, 1])]
        self.assertEqual(reserve_for_starving([1] * 4, cs), "worker2")                  # 학습: 2 vs 3
        self.assertEqual(reserve_for_starving([1] * 4, cs, recovery=True), "worker1")   # 복구: 3 vs 3 -> 이름순


if __name__ == "__main__":
    unittest.main()
