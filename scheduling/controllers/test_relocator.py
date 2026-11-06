"""python -m unittest -v test_relocator.py  (쿠버네티스·AWS 없이 돈다)
재제출본이 JobSet 템플릿 6절 값만 바뀌는지, 어디로 갈지, 펜싱 기록으로 시작해도 되는지만 본다."""
import copy
import unittest

import recovery_render as R

ORIG = {   # JobSet 템플릿 합의안 4절을 줄인 것 (백엔드가 저장한 1세대 제출본)
    "apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet",
    "metadata": {"name": "job-3f9a1c2e-g0001", "namespace": "training",
                 "labels": {"kueue.x-k8s.io/queue-name": "queue-heavy", "kueue.x-k8s.io/priority-class": "training",
                            "devoops/job-id": "3f9a1c2e", "devoops/generation": "1"},
                 "annotations": {"devoops/strategy": "fsdp", "devoops/world-size": "4"}},
    "spec": {"failurePolicy": {"maxRestarts": 0},
             "network": {"enableDNSHostnames": True, "subdomain": "job-3f9a1c2e-g0001"},
             "replicatedJobs": [{"name": "workers", "replicas": 1, "template": {"spec": {
                 "completions": 4, "parallelism": 4, "template": {
                     "metadata": {"labels": {"devoops/gpu-group": "train", "devoops/job-id": "3f9a1c2e",
                                             "devoops/generation": "1"}},
                     "spec": {
                         "initContainers": [
                             {"name": "ckpt-init", "env": [{"name": "JOB_ID", "value": "3f9a1c2e"},
                                                           {"name": "GENERATION", "value": "1"}]},
                             {"name": "ckpt-sidecar", "restartPolicy": "Always",
                              "env": [{"name": "GENERATION", "value": "1"},
                                      {"name": "RANK", "valueFrom": {"fieldRef": {"fieldPath": "x"}}}]}],
                         "containers": [{"name": "trainer", "command": ["sh", "-c"], "args": [
                             "exec torchrun --nnodes=4 --node-rank=\"$JOB_COMPLETION_INDEX\" "
                             "--master-addr=job-3f9a1c2e-g0001-workers-0-0.job-3f9a1c2e-g0001 "
                             "--master-port=29500 /opt/train/train.py"],
                             "env": [{"name": "GENERATION", "value": "1"}]}]}}}}}]}}


def env(ct, name):
    return next(e["value"] for e in ct["env"] if e["name"] == name)


class Render(unittest.TestCase):
    def setUp(self):
        self.orig = copy.deepcopy(ORIG)
        self.new = R.render(self.orig, "3f9a1c2e", 2, "recovery-7c21", None, ["worker1"])

    def test_names(self):
        self.assertEqual(self.new["metadata"]["name"], "job-3f9a1c2e-g0002")
        self.assertEqual(self.new["spec"]["network"]["subdomain"], "job-3f9a1c2e-g0002")

    def test_labels_and_annotations(self):
        m = self.new["metadata"]
        self.assertEqual(m["labels"]["kueue.x-k8s.io/queue-name"], "queue-recovery")
        self.assertEqual(m["labels"]["kueue.x-k8s.io/priority-class"], "recovery")
        self.assertEqual(m["labels"]["devoops/generation"], "2")
        self.assertEqual(m["annotations"]["devoops/recovery-id"], "recovery-7c21")
        self.assertEqual(m["annotations"]["devoops/exclude-clusters"], "worker1")
        self.assertNotIn("devoops/target-cluster", m["annotations"])

    def test_pod_template(self):
        pod = self.new["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]
        self.assertEqual(pod["metadata"]["labels"]["devoops/generation"], "2")
        spec = pod["spec"]
        for ct in spec["initContainers"] + spec["containers"]:
            self.assertEqual(env(ct, "GENERATION"), "2", ct["name"])
        arg = spec["containers"][0]["args"][0]
        self.assertIn("--master-addr=job-3f9a1c2e-g0002-workers-0-0.job-3f9a1c2e-g0002 ", arg)
        self.assertNotIn("g0001", arg)

    def test_nothing_else_changes(self):
        """6절 목록 밖은 그대로: 바꾼 값을 원래대로 되돌리면 원본과 같아야 함"""
        back = copy.deepcopy(self.new)
        m = back["metadata"]
        m["name"] = ORIG["metadata"]["name"]
        m["labels"] = ORIG["metadata"]["labels"]
        m["annotations"] = ORIG["metadata"]["annotations"]
        back["spec"]["network"]["subdomain"] = "job-3f9a1c2e-g0001"
        pod = back["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]
        pod["metadata"]["labels"]["devoops/generation"] = "1"
        for ct in pod["spec"]["initContainers"] + pod["spec"]["containers"]:
            for e in ct["env"]:
                if e["name"] == "GENERATION":
                    e["value"] = "1"
        pod["spec"]["containers"][0]["args"] = ORIG["spec"]["replicatedJobs"][0]["template"]["spec"]["template"][
            "spec"]["containers"][0]["args"]
        self.assertEqual(back, ORIG)

    def test_original_untouched(self):
        self.assertEqual(self.orig, ORIG)

    def test_target_replaces_old_hints(self):
        orig = copy.deepcopy(ORIG)
        orig["metadata"]["annotations"]["devoops/exclude-clusters"] = "worker3"
        new = R.render(orig, "3f9a1c2e", 2, "r", "worker2", [])
        self.assertEqual(new["metadata"]["annotations"]["devoops/target-cluster"], "worker2")
        self.assertNotIn("devoops/exclude-clusters", new["metadata"]["annotations"])


class Names(unittest.TestCase):
    def test_roundtrip(self):
        self.assertEqual(R.jobset_name("3f9a1c2e", 12), "job-3f9a1c2e-g0012")
        self.assertEqual(R.parse_name("job-3f9a1c2e-g0012"), ("3f9a1c2e", 12))
        self.assertEqual(R.parse_name("job-train-a-g0001"), ("train-a", 1))
        self.assertEqual(R.parse_name("train-a"), (None, None))


class PlacementHints(unittest.TestCase):
    hints = staticmethod(R.placement_hints)

    def test_restart_in_place_pins_source(self):
        self.assertEqual(self.hints("restartInPlace", "worker2", None, None), ("worker2", []))

    def test_move_to_spare_pins_source(self):
        self.assertEqual(self.hints("moveToSpare", "worker2", "worker3", None), ("worker2", ["worker3"]))

    def test_pinned_source_never_excluded(self):
        self.assertEqual(self.hints("moveToSpare", "worker2", "worker2", None), ("worker2", []))

    def test_move_to_other_cluster_leaves_source(self):   # 예비 소진·SDC: 펜싱 없이 원래 클러스터만 뺌
        self.assertEqual(self.hints("moveToOtherCluster", "worker2", None, None), (None, ["worker2"]))

    def test_evacuate_leaves_source_and_keeps_old_excludes(self):
        self.assertEqual(self.hints("evacuateCluster", "worker2", "worker1", ["worker3"]),
                         (None, ["worker1", "worker2", "worker3"]))

    def test_not_dispatched_yet(self):
        self.assertEqual(self.hints("moveToOtherCluster", "", None, None), (None, []))
        self.assertEqual(self.hints("restartInPlace", "", None, None), (None, []))


class Fencing(unittest.TestCase):
    rec = {"recovery_id": "r1", "recovery_phase": "GENERATION_ADVANCED", "recovery_from_gen": 1, "recovery_to_gen": 2}

    def check(self, **kw):
        return R.check_fencing({**self.rec, **kw}, "r1", 1)

    def test_go(self):
        self.assertEqual(self.check(), ("go", "GENERATION_ADVANCED", 2))

    def test_resume_after_restart(self):
        self.assertEqual(self.check(recovery_phase="NEW_RUN_SUBMITTED")[0], "go")

    def test_wait_before_generation_advanced(self):
        self.assertEqual(self.check(recovery_phase="CLAIMED")[0], "wait")
        self.assertEqual(R.check_fencing(None, "r1", 1)[0], "wait")
        self.assertEqual(self.check(recovery_id="other")[0], "wait")

    def test_done_when_recovery_controller_moved_on(self):
        self.assertEqual(self.check(recovery_phase="RESUMING")[0], "done")

    def test_wrong_old_generation_fails(self):   # 요청이 가리키는 잡이 이번 복구의 옛 세대가 아님
        self.assertEqual(R.check_fencing(self.rec, "r1", 2)[0], "fail")


if __name__ == "__main__":
    unittest.main()
