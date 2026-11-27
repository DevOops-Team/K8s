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
        orig_tols = ORIG["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"].get("tolerations")
        pod["spec"]["tolerations"] = [t for t in pod["spec"]["tolerations"] if t != R.SPARE_TOLERATION]
        if orig_tols is None and not pod["spec"]["tolerations"]:
            del pod["spec"]["tolerations"]
        self.assertEqual(pod["spec"].pop("affinity"),
                         {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [R.PREFER_NON_SPARE]}})
        self.assertEqual(back, ORIG)

    def test_spare_toleration_added_once(self):
        """복구 잡만 예비 노드(devoops/spare taint)에 갈 수 있다. 두 번 렌더해도 하나만"""
        pod = self.new["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]
        self.assertEqual(pod["tolerations"].count(R.SPARE_TOLERATION), 1)
        again = R.render(self.new, "3f9a1c2e", 3, "r2", None, [])
        pod2 = again["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]
        self.assertEqual(sum(t.get("key") == "devoops/spare" for t in pod2["tolerations"]), 1)

    def test_original_untouched(self):
        self.assertEqual(self.orig, ORIG)

    def test_master_address_formats_preserve_unrelated_arguments(self):
        old = "job-3f9a1c2e-g0001"
        new = "job-3f9a1c2e-g0002"
        address = f"{old}-workers-0-0.{old}"
        updated = f"{new}-workers-0-0.{new}"
        cases = [
            (["--master-addr", address], ["--master-addr", updated]),
            ([f"--master-addr={address}"], [f"--master-addr={updated}"]),
            ([f"exec torchrun --master-addr {address} --log-dir=/{old}"],
             [f"exec torchrun --master-addr {updated} --log-dir=/{old}"]),
            ([f'--master-addr="{address}" --log-dir=/{old}'],
             [f'--master-addr="{updated}" --log-dir=/{old}']),
            (["--master-addr-extra", address], ["--master-addr-extra", updated]),
        ]
        for field in ("command", "args"):
            for values, expected in cases:
                with self.subTest(field=field, values=values):
                    orig = copy.deepcopy(ORIG)
                    trainer = orig["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
                    trainer.clear()
                    trainer.update(name="trainer", **{field: values})
                    snapshot = copy.deepcopy(orig)
                    rendered = R.render(orig, "3f9a1c2e", 2, "r", None, [])
                    actual = rendered["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
                    self.assertEqual(actual[field], expected)
                    self.assertEqual(orig, snapshot)

    def test_wait_line_address_follows_new_generation(self):
        """getent hosts 대기 줄처럼 --master-addr 밖의 Pod 주소도 새 세대로 (11/25 eks-a 실측 형태)"""
        old = "job-df6827fc-g0001"
        new = "job-df6827fc-g0002"
        script = (f'until getent hosts {old}-workers-0-0.{old} >/dev/null; do '
                  f'echo "waiting for {old}-workers-0-0.{old}"; sleep 2; done; '
                  f'exec torchrun --master-addr={old}-workers-0-0.{old}.training.svc.cluster.local '
                  f'--log-dir=/{old} --tag x{old}-workers-0-0.{old}')
        expected = (f'until getent hosts {new}-workers-0-0.{new} >/dev/null; do '
                    f'echo "waiting for {new}-workers-0-0.{new}"; sleep 2; done; '
                    f'exec torchrun --master-addr={new}-workers-0-0.{new}.training.svc.cluster.local '
                    f'--log-dir=/{old} --tag x{old}-workers-0-0.{old}')
        orig = copy.deepcopy(ORIG)
        trainer = orig["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
        trainer.update(command=["sh", "-c"], args=[script],
                       env=[{"name": "GENERATION", "value": "1"},
                            {"name": "MASTER_ADDR", "value": f"{old}-workers-0-0.{old}"},
                            {"name": "LOG_DIR", "value": f"/ckpt/{old}"},
                            {"name": "OTHER_JOB", "value": "job-aaaa0000-g0001-workers-0-0.job-aaaa0000-g0001"}])
        rendered = R.render(orig, "df6827fc", 2, "r", None, [])
        actual = rendered["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(actual["command"], ["sh", "-c"])
        self.assertEqual(actual["args"], [expected])
        self.assertEqual({e["name"]: e["value"] for e in actual["env"]}, {
            "GENERATION": "2", "MASTER_ADDR": f"{new}-workers-0-0.{new}", "LOG_DIR": f"/ckpt/{old}",
            "OTHER_JOB": "job-aaaa0000-g0001-workers-0-0.job-aaaa0000-g0001"})

    def test_address_needs_matching_subdomain(self):
        """앞뒤 세대가 다른 문자열은 Pod 주소가 아니므로 그대로"""
        odd = "job-3f9a1c2e-g0001-workers-0-0.job-3f9a1c2e-g0005"
        orig = copy.deepcopy(ORIG)
        trainer = orig["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
        trainer.update(args=[f"echo {odd}"])
        rendered = R.render(orig, "3f9a1c2e", 2, "r", None, [])
        actual = rendered["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(actual["args"], [f"echo {odd}"])

    def test_master_address_split_across_command_and_args(self):
        orig = copy.deepcopy(ORIG)
        trainer = orig["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
        trainer.update(command=["torchrun", "--master-addr"],
                       args=["job-3f9a1c2e-g0001-workers-0-0.job-3f9a1c2e-g0001"])
        rendered = R.render(orig, "3f9a1c2e", 2, "r", None, [])
        actual = rendered["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(actual["command"], trainer["command"])
        self.assertEqual(actual["args"], ["job-3f9a1c2e-g0002-workers-0-0.job-3f9a1c2e-g0002"])

    def test_target_replaces_old_hints(self):
        orig = copy.deepcopy(ORIG)
        orig["metadata"]["annotations"]["devoops/exclude-clusters"] = "worker3"
        new = R.render(orig, "3f9a1c2e", 2, "r", "worker2", [])
        self.assertEqual(new["metadata"]["annotations"]["devoops/target-cluster"], "worker2")
        self.assertNotIn("devoops/exclude-clusters", new["metadata"]["annotations"])


def pod_spec(js):
    return js["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]


def preferred_terms(spec):
    return ((spec.get("affinity") or {}).get("nodeAffinity") or {}).get(
        "preferredDuringSchedulingIgnoredDuringExecution")


ONLY_PREFER = {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [R.PREFER_NON_SPARE]}}


def required_terms(spec):
    return (((spec.get("affinity") or {}).get("nodeAffinity") or {}).get(
        "requiredDuringSchedulingIgnoredDuringExecution") or {}).get("nodeSelectorTerms")


PIN = {"key": "kubernetes.io/hostname", "operator": "In", "values": ["node-1", "node-2"]}


class PinNodes(unittest.TestCase):
    """restartInPlace 재제출본은 옛 노드에만 뜬다 (GPU-280)"""

    def test_pins_old_nodes(self):
        spec = pod_spec(R.render(ORIG, "3f9a1c2e", 2, "r", "worker1", [], pin_nodes=["node-2", "node-1"]))
        self.assertEqual(required_terms(spec), [{"matchExpressions": [PIN]}])
        self.assertIn(R.SPARE_TOLERATION, spec["tolerations"])
        # 고정이 있으면 비예비 선호를 넣지 않는다 (옛 노드가 예비여도 그 노드로)
        self.assertIsNone(preferred_terms(spec))

    def test_no_pin_prefers_non_spare_only(self):
        """고정 없는 재제출(노드 장애 restartInPlace·moveToSpare·evacuateCluster)은 예비 허용 + 비예비 선호 (GPU-306)"""
        spec = pod_spec(R.render(ORIG, "3f9a1c2e", 2, "r", None, []))
        self.assertEqual(spec["affinity"], ONLY_PREFER)
        self.assertIn(R.SPARE_TOLERATION, spec["tolerations"])

    def test_prefer_non_spare_added_once_and_dropped_when_pinned(self):
        once = R.render(ORIG, "3f9a1c2e", 2, "r", None, [])
        twice = R.render(once, "3f9a1c2e", 3, "r2", None, [])
        self.assertEqual(preferred_terms(pod_spec(twice)), [R.PREFER_NON_SPARE])
        pinned = pod_spec(R.render(twice, "3f9a1c2e", 4, "r3", "worker1", [], pin_nodes=["node-1", "node-2"]))
        self.assertIsNone(preferred_terms(pinned))
        self.assertEqual(required_terms(pinned), [{"matchExpressions": [PIN]}])

    def test_and_with_original_terms_and_keep_other_affinity(self):
        orig = copy.deepcopy(ORIG)
        gpu = {"key": "devoops/gpu", "operator": "In", "values": ["true"]}
        zone = {"key": "zone", "operator": "In", "values": ["a"]}
        pod_spec(orig)["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": [gpu]}, {"matchExpressions": [zone]}]},
            "preferredDuringSchedulingIgnoredDuringExecution": [{"weight": 1, "preference": {}}]},
            "podAntiAffinity": {"x": 1}}
        once = R.render(orig, "3f9a1c2e", 2, "r", "worker1", [], pin_nodes=["node-1", "node-2"])
        twice = R.render(once, "3f9a1c2e", 3, "r2", "worker1", [], pin_nodes=["node-1", "node-2"])
        spec = pod_spec(twice)
        self.assertEqual(required_terms(spec), [{"matchExpressions": [gpu, PIN]}, {"matchExpressions": [zone, PIN]}])
        self.assertEqual(spec["affinity"]["podAntiAffinity"], {"x": 1})
        self.assertEqual(preferred_terms(spec), [{"weight": 1, "preference": {}}])
        # 다음 세대가 다른 action(고정 없음)이면 hostname 고정만 빠지고 원래 조건은 남는다. 비예비 선호가 더해진다
        moved = pod_spec(R.render(twice, "3f9a1c2e", 4, "r3", None, []))
        self.assertEqual(required_terms(moved), [{"matchExpressions": [gpu]}, {"matchExpressions": [zone]}])
        self.assertEqual(preferred_terms(moved), [{"weight": 1, "preference": {}}, R.PREFER_NON_SPARE])
        self.assertEqual(pod_spec(orig)["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][
            "nodeSelectorTerms"], [{"matchExpressions": [gpu]}, {"matchExpressions": [zone]}])

    def test_strip_pin_only_term(self):
        pinned = R.render(ORIG, "3f9a1c2e", 2, "r", "worker1", [], pin_nodes=["node-1"])
        self.assertEqual(pod_spec(R.render(pinned, "3f9a1c2e", 3, "r2", None, []))["affinity"], ONLY_PREFER)

    def test_pod_count(self):
        js = {"spec": {"replicatedJobs": [{"replicas": 2, "template": {"spec": {"parallelism": 2}}},
                                          {"template": {"spec": {}}}]}}
        self.assertEqual(R.pod_count(js), 5)
        self.assertEqual(R.pod_count(ORIG), 4)   # replicas 1 x parallelism 4


class Names(unittest.TestCase):
    def test_roundtrip(self):
        self.assertEqual(R.jobset_name("3f9a1c2e", 12), "job-3f9a1c2e-g0012")
        self.assertEqual(R.parse_name("job-3f9a1c2e-g0012"), ("3f9a1c2e", 12))
        self.assertEqual(R.parse_name("job-train-a-g0001"), ("train-a", 1))
        self.assertEqual(R.parse_name("train-a"), (None, None))


class PlacementHints(unittest.TestCase):
    hints = staticmethod(R.placement_hints)

    def test_restart_in_place_does_not_hard_pin_source(self):
        self.assertEqual(self.hints("restartInPlace", "worker2", None, None), (None, []))

    def test_move_to_spare_does_not_hard_pin_source(self):
        self.assertEqual(self.hints("moveToSpare", "worker2", "worker3", None), (None, ["worker3"]))

    def test_explicit_exclusions_are_preserved(self):
        self.assertEqual(self.hints("moveToSpare", "worker2", "worker2", None), (None, ["worker2"]))


    def test_evacuate_leaves_source_and_keeps_old_excludes(self):
        self.assertEqual(self.hints("evacuateCluster", "worker2", "worker1", ["worker3"]),
                         (None, ["worker1", "worker2", "worker3"]))

    def test_not_dispatched_yet(self):
        self.assertEqual(self.hints("restartInPlace", "", None, None), (None, []))


if __name__ == "__main__":
    unittest.main()
