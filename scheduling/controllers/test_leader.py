"""leader.Elector: 셋 중 하나만 리더, 리더가 죽으면 넘어감, 내려놓으면 바로 넘어감.
python -m unittest -v test_leader.py
"""
import copy
import unittest
from unittest import mock

from kubernetes import client

import leader


class FakeCoord:
    """Lease 하나를 담는 가짜 API. replace는 resourceVersion이 다르면 409."""

    def __init__(self):
        self.lease, self.rv, self.down = None, 0, False

    def _check(self):
        if self.down:
            raise OSError("api down")

    def read_namespaced_lease(self, name, ns, **_):
        self._check()
        if self.lease is None:
            raise client.ApiException(status=404)
        return copy.deepcopy(self.lease)

    def create_namespaced_lease(self, ns, body, **_):
        self._check()
        if self.lease is not None:
            raise client.ApiException(status=409)
        self.rv += 1
        body.metadata.resource_version = str(self.rv)
        self.lease = copy.deepcopy(body)

    def replace_namespaced_lease(self, name, ns, body, **_):
        self._check()
        if body.metadata.resource_version != self.lease.metadata.resource_version:
            raise client.ApiException(status=409)
        self.rv += 1
        body.metadata.resource_version = str(self.rv)
        self.lease = copy.deepcopy(body)


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def make(coord, clock, ident):
    with mock.patch.object(leader.client, "CoordinationV1Api", return_value=coord):
        return leader.Elector(None, "dispatcher", identity=ident, clock=clock, start=False)


class TestElector(unittest.TestCase):
    def setUp(self):
        self.coord, self.clock = FakeCoord(), Clock()
        self.a, self.b, self.c = (make(self.coord, self.clock, n) for n in "abc")

    def leaders(self):
        return [e.identity for e in (self.a, self.b, self.c) if e.leading]

    def test_only_one_leader(self):
        for e in (self.a, self.b, self.c):
            e.step()
        self.assertEqual(self.leaders(), ["a"])
        for _ in range(5):                       # 갱신이 이어지면 계속 a
            self.clock.t += leader.RENEW_SECONDS
            for e in (self.a, self.b, self.c):
                e.step()
        self.assertEqual(self.leaders(), ["a"])

    def test_dead_leader_taken_over_after_lease(self):
        for e in (self.a, self.b, self.c):
            e.step()
        # a가 죽음 (더 이상 step 안 함)
        self.clock.t += leader.LEASE_SECONDS - 1
        self.b.step(); self.c.step()
        self.assertFalse(self.b.leading or self.c.leading)   # 아직 기간 안
        self.clock.t += 2
        self.b.step(); self.c.step()
        self.assertTrue(self.b.leading)
        self.assertFalse(self.c.leading)                     # b가 이미 새로 갱신함
        self.assertEqual(self.coord.lease.spec.lease_transitions, 1)

    def test_release_hands_over_immediately(self):
        self.a.step(); self.b.step()
        self.a.release()
        self.b.step()
        self.assertTrue(self.b.leading)

    def test_conflict_loses(self):
        self.a.step()
        stale = self.coord.read_namespaced_lease("x", "y")
        self.a.step()                            # rv 올라감
        with self.assertRaises(client.ApiException) as cm:
            self.coord.replace_namespaced_lease("x", "y", stale)
        self.assertEqual(cm.exception.status, 409)

    def test_leader_exits_when_taken_over(self):
        self.a.step()
        self.coord.lease.spec.holder_identity = "z"          # 누가 가져감
        with mock.patch.object(leader.os, "_exit", side_effect=SystemExit) as ex:
            with self.assertRaises(SystemExit):
                self.a.step()
        ex.assert_called_once_with(1)

    def test_api_blip_within_lease_keeps_leader(self):
        self.a.step()
        self.coord.down = True
        self.clock.t += leader.RENEW_SECONDS
        self.a.step()
        self.assertTrue(self.a.leading)
        self.clock.t += leader.LEASE_SECONDS
        with mock.patch.object(leader.os, "_exit", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                self.a.step()

    def test_is_leader_false_after_stale_renew(self):
        self.a.step()
        self.assertTrue(self.a.is_leader())
        self.clock.t += leader.LEASE_SECONDS     # 갱신 스레드가 멈춘 상황
        self.assertTrue(self.a.leading)
        self.assertFalse(self.a.is_leader())


if __name__ == "__main__":
    unittest.main()
