"""Quota starvation, cleanup and optimistic concurrency without a cluster."""
import copy
import unittest
from unittest import mock

from kubernetes import client

import keys as K
import quota_hold as Q


def workload(name, size=1, queue="light", created="02", reserved=False, active=True, **kw):
    return {"metadata": {"name": name, "namespace": "training", "uid": name + "-uid", "resourceVersion": "1",
                         "creationTimestamp": created, "annotations": {},
                         "ownerReferences": [{"kind": "JobSet", "name": name}]},
            "spec": {"queueName": "queue-" + queue, "active": active, "priority": 1000 if queue == "recovery" else 100,
                     "podSets": [{"count": size, "template": {"spec": {"containers": [
                         {"resources": {"requests": {K.GPU: 1}}}]}}}]},
            "status": {"conditions": [{"type": "QuotaReserved", "status": "True" if reserved else "False",
                                       "reason": "QuotaReserved" if reserved else "WaitingForQuota",
                                       "lastTransitionTime": "2026-11-22T00:00:00Z"}]}, **kw}


def clusterqueue(name, nominal, used):
    return {"metadata": {"name": name},
            "spec": {"cohortName": "training", "queueingStrategy": "BestEffortFIFO" if name == "light" else "StrictFIFO",
                     "resourceGroups": [{"flavors": [{"name": "gpu", "resources": [
                         {"name": K.GPU, "nominalQuota": nominal, **({"lendingLimit": 0} if name == "recovery" else {})}]}]}]},
            "status": {"conditions": [{"type": "Active", "status": "True"}],
                       "flavorsReservation": [{"name": "gpu", "resources": [{"name": K.GPU, "total": used}]}]}}


class QuotaHold(unittest.TestCase):
    def setUp(self):
        self.co = mock.Mock()
        self.queues = [clusterqueue("light", 2, 1), clusterqueue("heavy", 6, 6), clusterqueue("recovery", 4, 0)]
        self.co.list_cluster_custom_object.side_effect = self.list_objects
        self.co.patch_namespaced_custom_object.side_effect = self.patch
        self.objects = {}
        self.manual = set()
        self.conflict = None
        self.patch_fail = None
        self.patches = []
        for patcher in (mock.patch.object(Q, "ENABLED", True), mock.patch.object(Q, "LIGHT_N", 2),
                        mock.patch.object(Q, "RECOVERY_N", 1), mock.patch.object(Q.kube, "kueue", return_value=self.co),
                        mock.patch.object(Q.kube, "owner_annotations", side_effect=lambda _m, w: {
                            K.ANN_MANUAL: "true"} if w["metadata"]["name"] in self.manual else {})):
            patcher.start()
            self.addCleanup(patcher.stop)

    def list_objects(self, _g, _v, plural, **kw):
        if plural == "clusterqueues":
            return {"items": copy.deepcopy(self.queues)}
        return {"items": [{"metadata": {"name": "queue-" + q, "namespace": "training"}, "spec": {"clusterQueue": q}}
                          for q in ("light", "heavy", "recovery")]}

    def patch(self, _g, _v, ns, plural, name, body, **kw):
        w = self.objects[name]
        if name == self.patch_fail:
            raise client.ApiException(status=503)
        if body.get("spec", {}).get("active") is False and name == self.conflict:
            # Kueue admitted it between list and patch: API rejects the old RV.
            w["status"]["conditions"][0]["status"] = "True"
            w["metadata"]["resourceVersion"] = "999"
        if body["metadata"]["resourceVersion"] != w["metadata"]["resourceVersion"]:
            raise client.ApiException(status=409)
        self.patches.append((name, copy.deepcopy(body)))
        w["spec"].update(body.get("spec", {}))
        for k, v in body["metadata"]["annotations"].items():
            if v is None:
                w["metadata"]["annotations"].pop(k, None)
            else:
                w["metadata"]["annotations"][k] = v
        w["metadata"]["resourceVersion"] = str(int(w["metadata"]["resourceVersion"]) + 1)
        return copy.deepcopy(w)

    def loop(self, *ws):
        if ws:
            self.objects = {w["metadata"]["name"]: copy.deepcopy(w) for w in ws}
        Q.reconcile(None, copy.deepcopy(list(self.objects.values())))

    def base(self):
        return [workload("big", 2), workload("overtake1", created="03", reserved=True),
                workload("overtake2", created="04", reserved=True), workload("next", created="05")]

    def held(self, name="next"):
        return self.objects[name]["metadata"]["annotations"].get(K.ANN_HELD_FOR)

    def test_light_threshold_and_release_after_quota(self):
        self.loop(*self.base())
        self.assertEqual(self.held(), "training/big")
        self.assertFalse(self.objects["next"]["spec"]["active"])
        self.objects["big"]["status"]["conditions"][0]["status"] = "True"
        self.loop()
        self.assertIsNone(self.held())
        self.assertTrue(self.objects["next"]["spec"]["active"])

    def test_counts_unique_admissions_across_loops_and_restart(self):
        ws = self.base()
        self.loop(ws[0], ws[1], ws[3])
        self.loop()
        self.assertIsNone(self.held())
        self.objects["overtake2"] = ws[2]
        self.loop()
        self.assertEqual(self.held(), "training/big")
        # A fresh process has no in-memory state; API annotations are sufficient.
        patches = len(self.patches)
        self.loop()
        self.assertEqual(len(self.patches), patches)

    def test_older_training_work_is_not_held_for_light(self):
        self.loop(*self.base(), workload("older", created="01"), workload("heavy", 3, "heavy", "06"))
        self.assertIsNone(self.held("older"))
        self.assertIsNone(self.held("heavy"))

    def test_recovery_precedes_light_and_holds_all_training_waiters(self):
        self.queues[0] = clusterqueue("light", 2, 2)
        self.queues[2] = clusterqueue("recovery", 4, 1)
        ws = self.base()
        self.loop(*ws, workload("rec", 4, "recovery", "01"), workload("heavy", 3, "heavy", "00"))
        self.assertEqual(self.held(), "training/rec")
        self.assertEqual(self.held("big"), "training/rec")
        self.assertEqual(self.held("heavy"), "training/rec")
        self.assertTrue(self.objects["rec"]["spec"]["active"])

    def test_recovery_three_bypasses_not_three_polls(self):
        self.queues[0] = clusterqueue("light", 2, 2)
        self.queues[2] = clusterqueue("recovery", 4, 1)
        ws = [workload("rec", 4, "recovery", "01"), workload("next", created="06"),
              workload("b1", created="02", reserved=True), workload("b2", created="03", reserved=True)]
        with mock.patch.object(Q, "RECOVERY_N", 3):
            self.loop(*ws)
            for _ in range(3):
                self.loop()
            self.assertIsNone(self.held())
            self.objects["b3"] = workload("b3", created="04", reserved=True)
            self.loop()
        self.assertEqual(self.held(), "training/rec")

    def test_admission_status_without_quota_condition_is_also_protected(self):
        protected = workload("admitting", created="06")
        protected["status"]["admission"] = {"clusterQueue": "light"}
        self.loop(*self.base(), protected)
        self.assertIsNone(self.held("admitting"))

    def test_malformed_counter_releases_owned_holds_without_deactivating_more_jobs(self):
        self.loop(*self.base())
        self.objects["big"]["metadata"]["annotations"][K.ANN_QUOTA_BYPASSES] = "broken"
        self.loop()
        self.assertIsNone(self.held())

    def test_same_named_candidate_in_other_namespace_does_not_steal_hold(self):
        self.loop(*self.base())
        # A new owner with the same name must not revive an orphaned hold.
        self.objects["big"]["metadata"].update(namespace="elsewhere", uid="elsewhere-big")
        self.loop()
        self.assertIsNone(self.held())

    def test_quota_holders_and_external_inactive_and_manual_are_untouched(self):
        self.manual.add("manual")
        self.loop(*self.base(), workload("inactive", active=False), workload("manual", created="06"))
        self.assertIsNone(self.held("inactive"))
        self.assertFalse(self.objects["inactive"]["spec"]["active"])
        self.assertIsNone(self.held("manual"))
        self.assertTrue(self.objects["overtake1"]["spec"]["active"])

    def test_conflict_never_deactivates_newly_admitted_job(self):
        self.conflict = "next"
        self.loop(*self.base())
        self.assertTrue(self.objects["next"]["spec"]["active"])
        self.assertIsNone(self.held())

    def test_disable_releases_owned_holds_but_not_external_inactive(self):
        self.loop(*self.base(), workload("inactive", active=False))
        with mock.patch.object(Q, "ENABLED", False):
            self.loop()
        self.assertIsNone(self.held())
        self.assertTrue(self.objects["next"]["spec"]["active"])
        self.assertFalse(self.objects["inactive"]["spec"]["active"])

    def test_deleted_target_releases_and_same_name_new_uid_does_not_inherit(self):
        self.loop(*self.base())
        self.objects["big"]["metadata"]["uid"] = "replacement"
        self.objects["big"]["metadata"]["annotations"] = {}
        self.objects.pop("overtake1")
        self.objects.pop("overtake2")
        self.loop()
        self.assertIsNone(self.held())
        self.objects.pop("big")
        self.loop()
        self.assertTrue(self.objects["next"]["spec"]["active"])

    def test_releases_when_waiter_deleted_finished_inactive_or_manual(self):
        for change in ("delete", "finish", "inactive", "manual"):
            with self.subTest(change=change):
                self.manual.clear()
                self.loop(*self.base())
                if change == "delete":
                    self.objects.pop("big")
                elif change == "finish":
                    self.objects["big"]["status"]["conditions"].append({"type": "Finished", "status": "True"})
                elif change == "inactive":
                    self.objects["big"]["spec"]["active"] = False
                else:
                    self.manual.add("big")
                self.loop()
                self.assertIsNone(self.held())

    def test_can_borrow_now_does_not_hold(self):
        self.queues[1] = clusterqueue("heavy", 6, 4)
        self.loop(*self.base())
        self.assertIsNone(self.held())

    def test_quota_ready_incumbent_stays_held_until_kueue_admits_it(self):
        self.loop(*self.base())
        self.queues[0] = clusterqueue("light", 2, 0)
        self.loop()
        self.assertEqual(self.held(), "training/big")

    def test_non_quota_failure_and_impossible_size_do_not_hold(self):
        ws = self.base()
        ws[0]["status"]["conditions"][0]["reason"] = "NoMatchingFlavor"
        self.loop(*ws)
        self.assertIsNone(self.held())
        ws[0] = workload("big", 3)
        self.loop(*ws)
        self.assertIsNone(self.held())

    def test_oversize_recovery_does_not_block_training(self):
        self.queues[0] = clusterqueue("light", 2, 2)
        self.queues[2] = clusterqueue("recovery", 4, 1)
        self.loop(workload("rec", 8, "recovery", "01"), workload("next", created="06"),
                  workload("b1", created="02", reserved=True))
        self.assertIsNone(self.held())

    def test_waiting_episode_resets_counter(self):
        ws = self.base()
        self.loop(ws[0], ws[1], ws[3])
        self.objects.pop("overtake1")
        self.objects["big"]["status"]["conditions"][0]["lastTransitionTime"] = "2026-11-22T01:00:00Z"
        self.objects["overtake2"] = ws[2]
        self.loop()
        self.assertIsNone(self.held())

    def test_previous_episode_admissions_are_not_counted_again(self):
        self.loop(*self.base())
        self.objects["big"]["status"]["conditions"][0]["lastTransitionTime"] = "2026-11-22T01:00:00Z"
        self.loop()
        self.assertIsNone(self.held())

    def test_queue_read_error_creates_no_new_hold_and_orphan_cleanup_still_works(self):
        self.loop(*self.base())
        self.objects.pop("big")
        self.co.list_cluster_custom_object.side_effect = client.ApiException(status=503)
        with self.assertRaises(client.ApiException):
            self.loop()
        self.assertIsNone(self.held())

    def test_release_failure_is_retryable(self):
        self.loop(*self.base())
        self.objects.pop("big")
        self.patch_fail = "next"
        with self.assertRaises(client.ApiException):
            self.loop()
        self.patch_fail = None
        self.loop()
        self.assertIsNone(self.held())

    def test_disabled_baseline_does_not_read_queues(self):
        with mock.patch.object(Q, "ENABLED", False):
            self.loop(*self.base())
        self.co.list_cluster_custom_object.assert_not_called()

    def test_same_namespace_name_in_another_namespace_is_independent(self):
        ws = self.base()
        ws[-1]["metadata"]["namespace"] = "elsewhere"
        self.loop(*ws)
        self.assertIsNone(self.held())

    def test_all_worker_down_still_releases_orphan(self):
        import dispatcher as D
        self.loop(*self.base())
        self.objects.pop("big")
        with mock.patch.object(D.kube, "list_workloads", return_value=copy.deepcopy(list(self.objects.values()))), \
                mock.patch.object(D, "cluster_state", return_value=[]), \
                mock.patch.object(D, "relocation_holds", return_value={}):
            D.reconcile(None, None)
        self.assertIsNone(self.held())

    def test_available_accounts_for_peer_borrowing_and_private_quota(self):
        qs = {w["metadata"]["name"]: Q.gpu_quota(w) for w in self.queues}
        qs["heavy"]["used"] = 7
        self.assertEqual(Q.available(qs["light"], qs), 0)
        qs["heavy"]["used"] = 3
        qs["light"].update(used=3, borrow=2)
        self.assertEqual(Q.available(qs["light"], qs), 1)


if __name__ == "__main__":
    unittest.main()
