"""잡 투입기: 실험 ID 전달, 실험 사건, Kafka 전달 확인, 잡 종료 판단 (외부 접속 없이)"""
from contextlib import redirect_stdout
from email import policy
from email.parser import BytesParser
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import submit


class FakeProducer:
    def __init__(self, err=None, deliver=True):
        self.sent, self.err, self.deliver, self._cb = [], err, deliver, None

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, json.loads(value)))
        self._cb = on_delivery

    def flush(self, timeout):
        if self.deliver:
            self._cb(self.err, None)
            return 0
        return 1


def make_inputs(tmp):
    """작은 모델 폴더(하위 폴더·숨김 폴더 포함) · 데이터 · 학습 코드. main()에 넘길 인자를 돌려준다"""
    tmp = Path(tmp)
    (tmp / "gpt2" / "sub").mkdir(parents=True)
    (tmp / "gpt2" / ".cache").mkdir()
    (tmp / "gpt2" / "config.json").write_text('{"model_type": "gpt2"}', encoding="utf-8")
    (tmp / "gpt2" / "sub" / "weights.bin").write_bytes(b"\x00\x01" * 1000)
    (tmp / "gpt2" / ".cache" / "lock").write_text("x", encoding="utf-8")
    (tmp / "train.jsonl").write_text('{"text": "hi"}\n', encoding="utf-8")
    (tmp / "train.py").write_text("print('train')\n", encoding="utf-8")
    return ["--model-dir", str(tmp / "gpt2"), "--data", str(tmp / "train.jsonl"), "--train-py", str(tmp / "train.py")]


def parse_multipart(request):
    """보낸 요청 본문을 다시 읽어 [(파트 이름, 내용 bytes)]"""
    body = b"".join(request.data)
    assert len(body) == int(request.get_header("Content-length")), "Content-Length가 본문 길이와 다름"
    msg = BytesParser(policy=policy.HTTP).parsebytes(
        b"Content-Type: " + request.get_header("Content-type").encode() + b"\r\n\r\n" + body)
    return [(part.get_param("name", header="content-disposition"), part.get_payload(decode=True))
            for part in msg.iter_parts()]


def js(job_id, gen, state=None, cond=None):
    st = {}
    if state:
        st["terminalState"] = state
    if cond:
        st["conditions"] = [{"type": cond, "status": "True"}]
    return {"metadata": {"name": f"job-{job_id}-g{gen:04d}", "labels": {"devoops/job-id": job_id}}, "status": st}


class EventTest(unittest.TestCase):
    def test_envelope(self):
        ev = submit.experiment_event("experiment.started", "exp-1",
                                     {"name": "x", "policy": "ours", "fault_type": None, "spare_enabled": False})
        self.assertEqual(ev["schema_version"], 1)
        self.assertEqual(ev["source"], "submitter@onprem")
        self.assertTrue(ev["occurred_at"].endswith("Z"))
        self.assertEqual(len(ev["occurred_at"].split(".")[1]), 4)   # 밀리초 3자리 + Z
        # None만 빠지고 False는 남는다
        self.assertEqual(ev["payload"], {"exp_id": "exp-1", "name": "x", "policy": "ours", "spare_enabled": False})

    def test_ended_only_exp_id(self):
        self.assertEqual(submit.experiment_event("experiment.ended", "exp-1")["payload"], {"exp_id": "exp-1"})

    def test_send_key_and_topic(self):
        p = FakeProducer()
        out = io.StringIO()
        submit.record_experiment(submit.KafkaEvents(topic="job.lifecycle", producer=p), out, "experiment.ended", "exp-9")
        topic, key, value = p.sent[0]
        self.assertEqual((topic, key, value["type"]), ("job.lifecycle", b"exp-9", "experiment.ended"))
        self.assertIn("experiment.ended exp_id=exp-9", out.getvalue())

    def test_send_fails_loudly(self):
        for p in (FakeProducer(err="broker down"), FakeProducer(deliver=False)):
            with self.assertRaises(RuntimeError):
                submit.KafkaEvents(producer=p).send(submit.experiment_event("experiment.started", "e"))

    def test_no_kafka_file_only(self):
        out = io.StringIO()
        submit.record_experiment(None, out, "experiment.started", "e")
        self.assertIn("Kafka 안 보냄", out.getvalue())


class SubmissionTest(unittest.TestCase):
    def test_cli_propagates_exp_id_to_every_job_and_event(self):
        for options, expected in ((["--exp-id", "exp-f-on-r1"], "exp-f-on-r1"),
                                  ([], "exp-test-run")):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as tmp:
                scenario, results = Path(tmp) / "scenario.csv", Path(tmp) / "results.csv"
                scenario.write_text("0, 2, 600, fsdp\n0, 1, 300, fsdp\n", encoding="utf-8")
                files = make_inputs(tmp)
                requests = []
                producer = FakeProducer()

                def respond(request, timeout):
                    requests.append((request, parse_multipart(request)))
                    response = io.StringIO(json.dumps({"id": f"j{len(requests)}"}))
                    response.status = 201
                    return response

                with patch.object(submit.urllib.request, "urlopen", side_effect=respond), \
                     patch.object(submit, "KafkaEvents", return_value=submit.KafkaEvents(producer=producer)), \
                     patch.object(submit, "jobset_lister", return_value=lambda: [
                         js("j1", 1, "Completed"), js("j2", 1, "Completed")]), \
                     redirect_stdout(io.StringIO()):
                    code = submit.main([str(scenario), "--prefix", "test-run", "--out", str(results), *files,
                                        *options])

                self.assertEqual(code, 0)
                self.assertEqual(len(requests), 2)
                for (request, parts), steps in zip(requests, (600, 300)):
                    self.assertEqual(request.full_url, f"{submit.BACKEND_URL}/api/jobs")
                    self.assertEqual(request.get_method(), "POST")
                    self.assertEqual(parts[0][0], "spec")
                    spec = json.loads(parts[0][1])
                    self.assertEqual(spec["exp_id"], expected)
                    self.assertEqual(spec["hyperparameters"], {"max_steps": steps})   # 행마다 CSV step 수
                    self.assertNotIn("model", spec)   # 백엔드 spec은 모르는 키를 거부
                    self.assertNotIn("dataset", spec)
                events = [value for _, _, value in producer.sent]
                self.assertEqual([event["type"] for event in events],
                                 ["experiment.started", "experiment.ended"])
                self.assertEqual([event["payload"]["exp_id"] for event in events], [expected, expected])

    def test_job_mode_records_image_and_scenario_name(self):
        """launch.py가 넘기는 인자: 결과에 이미지, experiment.started에 원래 시나리오 이름, 로그에 결과 파일"""
        with tempfile.TemporaryDirectory() as tmp:
            scenario, results = Path(tmp) / "scenario.csv", Path(tmp) / "results.csv"
            scenario.write_text("0, 1, 100, fsdp\n", encoding="utf-8")
            files = make_inputs(tmp)
            producer, out = FakeProducer(), io.StringIO()
            response = io.StringIO(json.dumps({"id": "j1"}))
            response.status = 201
            with patch.object(submit.urllib.request, "urlopen", return_value=response), \
                 patch.object(submit, "KafkaEvents", return_value=submit.KafkaEvents(producer=producer)), \
                 patch.object(submit, "jobset_lister", return_value=lambda: [js("j1", 1, "Completed")]), \
                 patch.object(submit, "IMAGE", "ghcr.io/x/submitter:1.0.0"), redirect_stdout(out):
                code = submit.main([str(scenario), "--exp-id", "exp-9", "--scenario-name", "fault-xid79.csv",
                                    "--out", str(results), "--print-results", *files])
            self.assertEqual(code, 0)
            text = results.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("# submitter image=ghcr.io/x/submitter:1.0.0 exp_id=exp-9 "))
        started = producer.sent[0][2]["payload"]
        self.assertEqual((started["scenario"], started["name"]), ("fault-xid79.csv", "fault-xid79"))
        self.assertIn(text, out.getvalue())

    def test_multipart_parts(self):
        """파트 순서·이름이 백엔드 형식대로: spec 먼저, model/<폴더>/<경로> (숨김 파일 뺌), data/, train/"""
        with tempfile.TemporaryDirectory() as tmp:
            args = make_inputs(tmp)
            inputs = submit.Inputs(args[1], [args[3]], args[5])
            sent = []

            def respond(request, timeout):
                sent.append(parse_multipart(request))
                r = io.StringIO(json.dumps({"id": "j1", "jobsetName": "job-j1-g0001", "queue": "queue-light"}))
                r.status = 201
                return r
            with patch.object(submit.urllib.request, "urlopen", side_effect=respond):
                row = {"gpus": 1, "steps": 200, "strategy": "fsdp"}
                result = submit.BackendSubmitter(inputs)(3, row, "x1117", "exp-1")
        self.assertEqual(result, ("j1", "job-j1-g0001", "queue-light", "201"))
        names = [n for n, _ in sent[0]]
        self.assertEqual(names, ["spec", "model/gpt2/config.json", "model/gpt2/sub/weights.bin",
                                 "data/train.jsonl", "train/train.py"])
        self.assertEqual(dict(sent[0])["model/gpt2/sub/weights.bin"], b"\x00\x01" * 1000)
        self.assertEqual(json.loads(sent[0][0][1]), {"name": "x1117-0003", "strategy": "fsdp", "gpus": 1,
                                                    "hyperparameters": {"max_steps": 200}, "exp_id": "exp-1"})

    def test_fixed_dataset_cli_sends_spec_only(self):
        """--dataset: 파일 인자 없이 시나리오 전체를 spec.dataset만으로 제출하고 experiment.ended까지 (Backend GPU-200)"""
        with tempfile.TemporaryDirectory() as tmp:
            scenario, results = Path(tmp) / "scenario.csv", Path(tmp) / "results.csv"
            scenario.write_text("0, 4, 600, fsdp\n0, 1, 300, ddp\n", encoding="utf-8")
            sent, producer = [], FakeProducer()

            def respond(request, timeout):
                sent.append(parse_multipart(request))
                r = io.StringIO(json.dumps({"id": f"j{len(sent)}"}))
                r.status = 201
                return r
            with patch.object(submit.urllib.request, "urlopen", side_effect=respond), \
                 patch.object(submit, "KafkaEvents", return_value=submit.KafkaEvents(producer=producer)), \
                 patch.object(submit, "jobset_lister", return_value=lambda: [
                     js("j1", 1, "Completed"), js("j2", 1, "Completed")]), \
                 redirect_stdout(io.StringIO()):
                code = submit.main([str(scenario), "--exp-id", "exp-ds-1", "--out", str(results),
                                    "--dataset", "smollm2-wikitext2:1"])
        self.assertEqual(code, 0)
        self.assertEqual([[n for n, _ in parts] for parts in sent], [["spec"], ["spec"]])   # 첫 잡부터 파일 파트 없음
        specs = [json.loads(parts[0][1]) for parts in sent]
        self.assertEqual([sp["dataset"] for sp in specs], [{"id": "smollm2-wikitext2", "version": 1}] * 2)
        self.assertEqual([sp["hyperparameters"]["max_steps"] for sp in specs], [600, 300])
        self.assertEqual({sp["exp_id"] for sp in specs}, {"exp-ds-1"})
        self.assertEqual([v["type"] for _, _, v in producer.sent], ["experiment.started", "experiment.ended"])

    def test_parse_dataset(self):
        self.assertEqual(submit.parse_dataset("smollm2-wikitext2:1"), {"id": "smollm2-wikitext2", "version": 1})
        self.assertEqual(submit.parse_dataset("a-b:12"), {"id": "a-b", "version": 12})
        # Backend가 거절하는 것: '-' 없음(학습 ID 모양) · 대문자 · 끝이 '-' · version 0·문자·빠짐 · ':' 없음
        for bad in ("smollm2:1", "Smol-lm:1", "smol-:1", "smol-lm:0", "smol-lm:v1", "smol-lm:", "smol-lm"):
            with self.subTest(bad=bad), self.assertRaises(submit.argparse.ArgumentTypeError):
                submit.parse_dataset(bad)

    def test_fixed_dataset_excludes_file_args(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = make_inputs(tmp)
            scenario = Path(tmp) / "scenario.csv"
            scenario.write_text("0, 1, 100, fsdp\n", encoding="utf-8")
            for extra in (["--dataset", "smollm2-wikitext2:1", *args],              # 파일과 같이 줌
                          ["--dataset", "smollm2-wikitext2:1", "--data", args[3]],  # 파일 하나만 같이 줘도
                          ["--dataset", "smollm2:1"]):                               # 형식 틀림
                with self.subTest(extra=extra), self.assertRaises(SystemExit), redirect_stdout(io.StringIO()), \
                     patch("sys.stderr", io.StringIO()):
                    submit.main([str(scenario), "--no-kafka", "--no-wait", *extra])
        with self.assertRaises(ValueError):
            submit.BackendSubmitter()

    def test_upload_every_job_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = make_inputs(tmp)
            inputs, sent = submit.Inputs(args[1], [args[3]], args[5]), []

            def respond(request, timeout):
                sent.append(parse_multipart(request))
                r = io.StringIO(json.dumps({"id": f"j{len(sent)}"}))
                r.status = 201
                return r
            with patch.object(submit.urllib.request, "urlopen", side_effect=respond):
                s = submit.BackendSubmitter(inputs)
                for i in (1, 2):
                    s(i, {"gpus": 1, "steps": 10, "strategy": "fsdp"}, "p", "e")
        self.assertEqual([len(parts) for parts in sent], [5, 5])
        self.assertNotIn("dataset", json.loads(sent[1][0][1]))

    def test_rejection_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = make_inputs(tmp)
            inputs = submit.Inputs(args[1], [args[3]], args[5])
            err = submit.urllib.error.HTTPError("u", 413, "too large", {},
                                                io.BytesIO(b'{"error":"model upload is over the limit"}'))
            with patch.object(submit.urllib.request, "urlopen", side_effect=err):
                result = submit.BackendSubmitter(inputs)(1, {"gpus": 1, "steps": 1, "strategy": "fsdp"}, "p", "e")
        self.assertEqual(result[:3], ("", "", ""))
        self.assertTrue(result[3].startswith("413 ") and "over the limit" in result[3])

    def test_inputs_validation_and_required_args(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = make_inputs(tmp)
            (Path(tmp) / "gpt2" / "config.json").unlink()
            with self.assertRaises(ValueError):
                submit.Inputs(args[1], [args[3]], args[5])
            scenario = Path(tmp) / "scenario.csv"
            scenario.write_text("0, 1, 100, fsdp\n", encoding="utf-8")
            for extra in ([], args):   # 파일 인자 없이 백엔드 모드 / config.json 없는 모델 폴더
                with self.subTest(extra=extra), self.assertRaises(SystemExit), redirect_stdout(io.StringIO()), \
                     patch("sys.stderr", io.StringIO()):
                    submit.main([str(scenario), "--no-kafka", "--no-wait", *extra])

    def test_kind_preserves_exp_id_in_jobset_and_recovery_source(self):
        kind = submit.KindSubmitter.__new__(submit.KindSubmitter)
        kind.client = Mock()
        kind.co, kind.core = Mock(), Mock()
        row = {"gpus": 2, "steps": 600, "strategy": "fsdp", "duration": 720}
        result = kind(1, row, "test-run", "exp-kind-r1")
        self.assertEqual(result[-1], "201")
        jobset = kind.co.create_namespaced_custom_object.call_args.args[-1]
        self.assertEqual(jobset["metadata"]["annotations"]["devoops/exp-id"], "exp-kind-r1")
        source_data = kind.client.V1ConfigMap.call_args_list[0].kwargs["data"]
        source = json.loads(source_data["jobset.json"])
        self.assertEqual(source["metadata"]["annotations"]["devoops/exp-id"], "exp-kind-r1")


class DoneTest(unittest.TestCase):
    def test_pending(self):
        jobsets = [js("a", 1, "Completed"),
                   js("b", 1, "Failed"), js("b", 2),          # 재배치된 새 세대가 아직 돎
                   js("c", 1, cond="Completed")]
        self.assertEqual(submit.pending_jobs(jobsets, ["a", "b", "c", "d"]), ["b", "d"])   # d: 아직 없음(재배치 틈)

    def test_wait_done_and_timeout(self):
        polls = iter([[js("a", 1)], [js("a", 1, "Completed")]])
        self.assertEqual(submit.wait_done(lambda: next(polls), ["a"], timeout=100, sleep=lambda s: None), [])
        t = iter(range(0, 1000, 10))
        left = submit.wait_done(lambda: [js("a", 1)], ["a"], timeout=30, sleep=lambda s: None, clock=lambda: next(t))
        self.assertEqual(left, ["a"])

    def test_wait_survives_api_error(self):
        calls = iter([RuntimeError("API down"), [js("a", 1, "Completed")]])

        def lister():
            x = next(calls)
            if isinstance(x, Exception):
                raise x
            return x
        self.assertEqual(submit.wait_done(lister, ["a"], timeout=100, sleep=lambda s: None), [])

    def test_nothing_submitted(self):
        self.assertEqual(submit.wait_done(lambda: 1 / 0, [], timeout=1), [])


if __name__ == "__main__":
    unittest.main()
