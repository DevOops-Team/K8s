"""투입기 실행 스크립트: exp_id 필수·전달, 같은 실험 중복 방지 (외부 접속 없이)"""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import launch


class ApiException(Exception):
    def __init__(self, status):
        self.status = status


class BuildTest(unittest.TestCase):
    def test_exp_id_required(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            launch.main(["s.csv", "--image", "img"])

    def test_exp_id_goes_to_submitter_args(self):
        job = launch.build_job("exp-0001", "img:1.0.0", "scenario.csv", ["--policy", "ours"])
        c = job["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(job["metadata"]["name"], "submitter-exp-0001")
        self.assertEqual(c["args"][c["args"].index("--exp-id") + 1], "exp-0001")
        self.assertEqual(c["args"][-2:], ["--policy", "ours"])
        env = {e["name"]: e["value"] for e in c["env"]}
        self.assertEqual(env["SUBMITTER_IMAGE"], "img:1.0.0")
        self.assertEqual(job["spec"]["backoffLimit"], 0)

    def test_bad_exp_id_and_reserved_args(self):
        for bad in ("Exp_1", "-x", "a" * (launch.MAX_EXP_ID + 1)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                launch.check_exp_id(bad)
        for extra in (["--exp-id", "other"], ["--exp-id=other"], ["--kind"]):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                launch.check_passthrough(extra)

    def test_dry_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            scenario = Path(tmp) / "fault-xid79.csv"
            scenario.write_text("0, 1, 100, fsdp\n", encoding="utf-8")
            out = io.StringIO()
            with redirect_stdout(out):
                code = launch.main([str(scenario), "--exp-id", "exp-7", "--image", "img", "--dry-run",
                                    "--", "--policy", "ours"])
        self.assertEqual(code, 0)
        cm, job = json.loads(out.getvalue())
        self.assertEqual(cm["data"]["scenario.csv"], "0, 1, 100, fsdp\n")
        args = job["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertEqual(args[args.index("--scenario-name") + 1], "fault-xid79.csv")


class LaunchTest(unittest.TestCase):
    def setUp(self):
        self.core, self.batch = Mock(), Mock()
        self.batch.create_namespaced_job.return_value = Mock(metadata=Mock(uid="u-1"))

    def test_creates_and_owns_configmap(self):
        name = launch.launch(self.core, self.batch, "exp-1", {"cm": 1}, {"job": 1}, ApiException)
        self.assertEqual(name, "submitter-exp-1")
        patch = self.core.patch_namespaced_config_map.call_args.args[2]
        self.assertEqual(patch["metadata"]["ownerReferences"][0]["uid"], "u-1")

    def test_refuses_same_exp_id(self):
        self.core.create_namespaced_config_map.side_effect = ApiException(409)
        with self.assertRaises(launch.Exists):
            launch.launch(self.core, self.batch, "exp-1", {}, {}, ApiException)
        self.batch.create_namespaced_job.assert_not_called()

    def test_job_exists_rolls_back_configmap(self):
        self.batch.create_namespaced_job.side_effect = ApiException(409)
        with self.assertRaises(launch.Exists):
            launch.launch(self.core, self.batch, "exp-1", {}, {}, ApiException)
        self.core.delete_namespaced_config_map.assert_called_once_with("submitter-exp-1", launch.NAMESPACE)


if __name__ == "__main__":
    unittest.main()
