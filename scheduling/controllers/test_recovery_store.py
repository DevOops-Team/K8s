"""python -m unittest -v test_recovery_store.py — kind용 ConfigMap 저장소의 조건부 갱신 (쿠버네티스 없이)"""
import copy
import json
import unittest
from unittest import mock

from kubernetes import client

import recovery_store


class FakeCore:
    def __init__(self, cms):
        self.cms, self.rv = cms, 0

    def read_namespaced_config_map(self, name, ns):
        if name not in self.cms:
            raise client.ApiException(status=404)
        return copy.deepcopy(self.cms[name])

    def replace_namespaced_config_map(self, name, ns, body):
        if body.metadata.resource_version != self.cms[name].metadata.resource_version:
            raise client.ApiException(status=409)
        self.rv += 1
        body.metadata.resource_version = f"v{self.rv}"
        self.cms[name] = copy.deepcopy(body)


def cm(name, data):
    return client.V1ConfigMap(metadata=client.V1ObjectMeta(name=name, resource_version="v0"), data=data)


class ConfigMapStoreTest(unittest.TestCase):
    def setUp(self):
        self.core = FakeCore({
            "recovery-fencing-a1": cm("recovery-fencing-a1", {
                "generation": "2", "owner": "", "recovery_id": "r1", "recovery_phase": "GENERATION_ADVANCED",
                "recovery_from_gen": "1", "recovery_to_gen": "2"}),
            "recovery-code-a1": cm("recovery-code-a1", {"jobset.json": '{"metadata": {"name": "job-a1-g0001"}}'}),
        })
        with mock.patch.object(recovery_store.client, "CoreV1Api", return_value=self.core):
            self.store = recovery_store.ConfigMapStore(None, "platform")

    def test_read(self):
        rec = self.store.get_fencing("a1")
        self.assertEqual((rec["generation"], rec["recovery_to_gen"], rec["owner"]), (2, 2, None))
        self.assertEqual(self.store.get_jobset("a1")["metadata"]["name"], "job-a1-g0001")
        self.assertIsNone(self.store.get_fencing("nope"))

    def test_phase_moves_only_under_condition(self):
        ok = self.store.set_phase("a1", "r1", 2, ("GENERATION_ADVANCED", "OLD_RUN_STOPPED"), "OLD_RUN_STOPPED")
        self.assertTrue(ok)
        self.assertEqual(self.store.get_fencing("a1")["recovery_phase"], "OLD_RUN_STOPPED")
        # 다른 복구 / 다른 세대 / 허용 안 된 단계에서는 안 바뀜
        self.assertFalse(self.store.set_phase("a1", "r2", 2, ("OLD_RUN_STOPPED",), "NEW_RUN_SUBMITTED"))
        self.assertFalse(self.store.set_phase("a1", "r1", 3, ("OLD_RUN_STOPPED",), "NEW_RUN_SUBMITTED"))
        self.assertFalse(self.store.set_phase("a1", "r1", 2, ("GENERATION_ADVANCED",), "NEW_RUN_SUBMITTED"))
        self.assertTrue(self.store.set_phase("a1", "r1", 2, ("OLD_RUN_STOPPED",), "NEW_RUN_SUBMITTED",
                                             {"new_jobset_name": "job-a1-g0002", "new_jobset_uid": "u"}))
        rec = self.store.get_fencing("a1")
        self.assertEqual((rec["recovery_phase"], rec["new_jobset_name"]), ("NEW_RUN_SUBMITTED", "job-a1-g0002"))
        self.assertIn("recovery_updated_at", rec)

    def test_concurrent_change_is_conflict(self):
        real = self.core.read_namespaced_config_map

        def stale(name, ns):   # 읽은 뒤 다른 주체가 바꾼 상황
            out = real(name, ns)
            self.core.cms[name].metadata.resource_version = "changed"
            return out
        self.core.read_namespaced_config_map = stale
        with self.assertRaises(recovery_store.Conflict):
            self.store.set_phase("a1", "r1", 2, ("GENERATION_ADVANCED",), "OLD_RUN_STOPPED")

    def test_stop_evidence_round_trip_and_conflict(self):
        evidence = json.dumps({"generation": 1, "kind": "ec2", "checks": [{"cluster": "eks-a"}]})
        extra = {"old_run_stop_evidence": evidence}
        self.assertFalse(self.store.set_phase("a1", "other", 2, ("GENERATION_ADVANCED",), "OLD_RUN_STOPPED", extra))
        self.assertNotIn("old_run_stop_evidence", self.store.get_fencing("a1"))
        self.assertTrue(self.store.set_phase("a1", "r1", 2, ("GENERATION_ADVANCED",), "OLD_RUN_STOPPED", extra))
        self.assertEqual(self.store.get_fencing("a1")["old_run_stop_evidence"], evidence)


if __name__ == "__main__":
    unittest.main()
