"""잡 투입기: 실험 ID 전달, 실험 사건, Kafka 전달 확인, 잡 종료 판단 (외부 접속 없이)"""
from contextlib import redirect_stdout
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
                requests = []
                producer = FakeProducer()

                def respond(request, timeout):
                    requests.append(request)
                    response = io.StringIO(json.dumps({"id": f"j{len(requests)}"}))
                    response.status = 201
                    return response

                with patch.object(submit.urllib.request, "urlopen", side_effect=respond), \
                     patch.object(submit, "KafkaEvents", return_value=submit.KafkaEvents(producer=producer)), \
                     patch.object(submit, "jobset_lister", return_value=lambda: [
                         js("j1", 1, "Completed"), js("j2", 1, "Completed")]), \
                     redirect_stdout(io.StringIO()):
                    code = submit.main([str(scenario), "--prefix", "test-run", "--out", str(results), *options])

                self.assertEqual(code, 0)
                self.assertEqual(len(requests), 2)
                for request in requests:
                    self.assertEqual(request.full_url, f"{submit.BACKEND_URL}/api/jobs")
                    self.assertEqual(request.get_method(), "POST")
                    self.assertEqual(json.loads(request.data)["exp_id"], expected)
                events = [value for _, _, value in producer.sent]
                self.assertEqual([event["type"] for event in events],
                                 ["experiment.started", "experiment.ended"])
                self.assertEqual([event["payload"]["exp_id"] for event in events], [expected, expected])

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
