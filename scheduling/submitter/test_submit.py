"""잡 투입기: 실험 사건 모양, Kafka 전달 확인, 잡 종료 판단 (Kafka·클러스터 없이)"""
import io
import json
import unittest

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
