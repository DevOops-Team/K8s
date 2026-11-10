"""잡 투입기 — 시나리오 파일을 읽어 정해진 시각에 학습 작업을 제출한다 (데이터 -> 쿠버네티스 이관, 11/06 수락).
실험용 부하 생성기: 실제 사용자와 같은 백엔드 API로 넣되, 쓰는 건 실험을 돌리는 팀원.

시나리오 파일 (데이터 파트 김현수가 trace를 가공해 줌). 한 줄 = 작업 하나, '#' 뒤는 주석:
    제출 시각(초, 실험 시작 기준), GPU 수, step 수, 방식[, 실행 시간(초)]
    0, 4, 400, fsdp
    30, 1, 200, fsdp, 180
실행 시간은 선택 칸: kind(KWOK 가짜 GPU)에서는 step을 못 돌리므로 이 시간 뒤 Pod를 끝내 흉내 낸다.

    python submit.py scenario.csv --policy ours         # 백엔드 POST /api/jobs (BACKEND_URL)
    python submit.py scenario.csv --kind --no-kafka     # kind: 백엔드 없이 같은 규칙으로 직접 제출, Kafka 없이
    python submit.py scenario.csv --speed 10            # 시각을 1/10로 줄여서
결과: results-<시각>.csv (작업ID · JobSet 이름 · 큐 · 제출 시각 · 응답). 실험 시작·종료 사건도 같이 찍는다.

실험 시작·종료 기록 (김현수 11/09): DB에 직접 쓰지 않고 Kafka job.lifecycle에 사건만 올린다.
  - 첫 제출 전 experiment.started (payload: exp_id + 실험 조건 칸, 모르는 칸은 뺌)
  - 모든 잡이 끝난 걸 확인한 뒤 experiment.ended (payload: exp_id)
  - 데이터 수집기가 읽어 experiments.started_at·ended_at을 채운다. 봉투 시각 occurred_at(UTC 밀리초) 기준 (ADR-15)
  - 파티션 키 = exp_id, acks=all. 조건 칸 이름은 데이터 DB experiments 열과 같음 (Confluence 12615682)
잡이 끝났는지는 관리 클러스터 training의 JobSet(라벨 devoops/job-id)으로 본다. 재배치로 세대가 바뀌어도
같은 job-id의 JobSet이 하나 이상 있고 전부 Completed·Failed면 끝. 재배치 중 잠깐 0개일 때는 계속 기다린다.
기다리다 시간이 다 되면 experiment.ended를 보내지 않는다 (끝 시각이 틀리면 지표가 틀려서).

아직 안 정해진 것:
  - step 수·실행 시간을 백엔드에 넘길 칸이 API에 없음 (max_steps는 사용자에게 안 엶, 서비스 API 명세 3절) -> 노찬경과 정할 것.
    지금은 hyperparameters에 넣지 않고 결과 파일에만 남긴다.
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

BACKEND_URL = os.getenv("BACKEND_URL", "http://backend.platform.svc:8000")
MODEL = os.getenv("MODEL", "gpt2")
DATASET = {"id": os.getenv("DATASET_ID", "ds-demo"), "version": int(os.getenv("DATASET_VERSION", "1"))}
STORE_NS = "scheduling-local-store"   # kind: 복구 원본·펜싱 기록 ConfigMap 자리 (controllers/recovery_store.py)
HEAVY_MIN_GPUS, MAX_JOB_GPUS = 3, 4   # 백엔드 공용 함수와 같은 규칙 (스케줄링 문서): 3장↑ heavy, 5장↑ 거부
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "172.30.0.40:9092")   # 온프레미스 Kafka VM, 인증 없음
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "job.lifecycle")
EVENT_SOURCE = "submitter@onprem"
LABEL_JOB_ID = "devoops/job-id"       # controllers/keys.py와 같음 (백엔드가 최초 제출 때 붙임)
TRAINING_NS = "training"


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def read_scenario(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.split("#", 1)[0].strip()
            if not line or line.lower().startswith("submit"):
                continue
            cols = [c.strip() for c in line.split(",")]
            if len(cols) not in (4, 5):
                raise ValueError(f"{path}:{n}: 칸이 {len(cols)}개 (4 또는 5개여야 함): {line}")
            rows.append({"at": float(cols[0]), "gpus": int(cols[1]), "steps": int(cols[2]), "strategy": cols[3],
                         "duration": int(cols[4]) if len(cols) == 5 and cols[4] else None})
    return sorted(rows, key=lambda r: r["at"])


def queue_for(gpus):
    if gpus > MAX_JOB_GPUS:
        return None
    return "queue-heavy" if gpus >= HEAVY_MIN_GPUS else "queue-light"


# ---------- 제출 방법 두 가지

def submit_backend(i, row, prefix):
    body = {"name": f"{prefix}-{i:04d}", "model": MODEL, "strategy": row["strategy"], "gpus": row["gpus"],
            "dataset": DATASET}
    req = urllib.request.Request(f"{BACKEND_URL}/api/jobs", data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            out = json.load(r)
            return out.get("id", ""), out.get("jobsetName", ""), out.get("queue", ""), str(r.status)
    except urllib.error.HTTPError as e:
        return "", "", "", f"{e.code} {e.read().decode(errors='replace')[:200]}"
    except (urllib.error.URLError, OSError) as e:
        return "", "", "", f"ERR {e}"


class KindSubmitter:
    """kind 전용: 백엔드 대신 1세대 JobSet + 복구 원본·펜싱 기록 ConfigMap (e2e/harness/jobs.py submit과 같은 모양)"""

    def __init__(self, context):
        from kubernetes import client, config   # 백엔드 모드에서는 필요 없게
        self.client = client
        api = config.new_client_from_config(context=context)
        self.co, self.core = client.CustomObjectsApi(api), client.CoreV1Api(api)

    def __call__(self, i, row, prefix):
        q = queue_for(row["gpus"])
        if not q:
            return "", "", "", f"거부: GPU {row['gpus']} > {MAX_JOB_GPUS}"
        job_id = f"{prefix}-{i:04d}"
        name = f"job-{job_id}-g0001"
        gpus, dur = row["gpus"], f"{row['duration'] or 600}s"
        js = {"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet",
              "metadata": {"name": name, "namespace": "training",
                           "labels": {"kueue.x-k8s.io/queue-name": q, "kueue.x-k8s.io/priority-class": "training",
                                      "devoops/job-id": job_id, "devoops/generation": "1"},
                           "annotations": {"devoops/strategy": row["strategy"], "devoops/world-size": str(gpus),
                                           "submitter/steps": str(row["steps"])}},
              "spec": {"failurePolicy": {"maxRestarts": 0},
                       "network": {"enableDNSHostnames": True, "subdomain": name},
                       "replicatedJobs": [{"name": "workers", "replicas": 1, "template": {"spec": {
                           "completionMode": "Indexed", "parallelism": gpus, "completions": gpus, "backoffLimit": 0,
                           "template": {
                               "metadata": {"labels": {"devoops/job-id": job_id, "devoops/generation": "1"},
                                            "annotations": {"pod-complete.stage.kwok.x-k8s.io/delay": dur}},
                               "spec": {"restartPolicy": "Never",
                                        "tolerations": [{"key": "nvidia.com/gpu", "operator": "Exists",
                                                         "effect": "NoSchedule"}],
                                        "containers": [{"name": "trainer", "image": "busybox:1.36",
                                                        "command": ["sh", "-c"],
                                                        "args": [f"exec sleep infinity --master-addr={name}-workers-0-0.{name}"],
                                                        "env": [{"name": "JOB_ID", "value": job_id},
                                                                {"name": "GENERATION", "value": "1"}],
                                                        "resources": {"requests": {"nvidia.com/gpu": 1},
                                                                      "limits": {"nvidia.com/gpu": 1}}}]}}}}}]}}
        V1 = self.client
        for cm_name, data in ((f"recovery-code-{job_id}", {"jobset.json": json.dumps(js)}),
                              (f"recovery-fencing-{job_id}", {"job_id": job_id, "generation": "1", "retry_count": "0"})):
            body = V1.V1ConfigMap(metadata=V1.V1ObjectMeta(name=cm_name), data=data)
            try:
                self.core.create_namespaced_config_map(STORE_NS, body)
            except V1.ApiException as e:
                if e.status != 409:
                    raise
                self.core.replace_namespaced_config_map(cm_name, STORE_NS, body)
        try:
            self.co.create_namespaced_custom_object("jobset.x-k8s.io", "v1alpha2", "training", "jobsets", js)
            return job_id, name, q, "201"
        except V1.ApiException as e:
            return job_id, name, q, f"{e.status} {e.reason}"


# ---------- 실험 시작·종료 사건 (Kafka)

def experiment_event(etype, exp_id, conditions=None):
    """공통 봉투 + payload. 값이 None인 조건 칸은 뺀다 (모르는 값은 빼도 됨)"""
    payload = {"exp_id": exp_id}
    payload.update({k: v for k, v in (conditions or {}).items() if v is not None})
    return {"event_id": str(uuid.uuid4()), "schema_version": 1, "type": etype, "occurred_at": now(),
            "source": EVENT_SOURCE, "payload": payload}


class KafkaEvents:
    def __init__(self, bootstrap=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC, producer=None):
        if producer is None:
            from confluent_kafka import Producer   # --no-kafka면 필요 없게
            producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True,
                                 "client.id": "submitter"})
        self.p, self.topic = producer, topic

    def send(self, event, timeout=15.0):
        """보내고 브로커 확인까지 기다린다. 못 받으면 RuntimeError (기록 없이 실험이 도는 걸 막으려고)"""
        result = {}
        self.p.produce(self.topic, key=event["payload"]["exp_id"].encode(), value=json.dumps(event).encode(),
                       on_delivery=lambda err, msg: result.setdefault("err", err))
        left = self.p.flush(timeout)
        if left or "err" not in result:
            raise RuntimeError(f"Kafka {self.topic}: {timeout}s 안에 전달 확인 못 받음 ({event['type']})")
        if result["err"] is not None:
            raise RuntimeError(f"Kafka {self.topic}: {event['type']} 전달 실패: {result['err']}")


def record_experiment(events, results, etype, exp_id, conditions=None):
    """experiment.started / experiment.ended를 Kafka에 올리고 결과 파일에도 남긴다. events=None이면 파일만"""
    ev = experiment_event(etype, exp_id, conditions)
    if events is not None:
        events.send(ev)
    note = "" if events is not None else " (Kafka 안 보냄)"
    results.write(f"# {etype} exp_id={exp_id} occurred_at={ev['occurred_at']} event_id={ev['event_id']}{note}\n")
    results.flush()
    return ev


# ---------- 잡이 다 끝났는지

def is_terminal(js):
    st = js.get("status") or {}
    if st.get("terminalState") in ("Completed", "Failed"):
        return True
    return any(c.get("type") in ("Completed", "Failed") and c.get("status") == "True"
               for c in st.get("conditions") or [])


def pending_jobs(jobsets, job_ids):
    """아직 안 끝난 job-id. 같은 job-id의 JobSet이 없으면(재배치 중 옛 세대 지움 ~ 새 세대 생성 사이) 안 끝난 것"""
    by_id = {}
    for js in jobsets:
        jid = ((js.get("metadata") or {}).get("labels") or {}).get(LABEL_JOB_ID)
        if jid:
            by_id.setdefault(jid, []).append(js)
    return [j for j in job_ids if not by_id.get(j) or not all(is_terminal(x) for x in by_id[j])]


def jobset_lister(context):
    from kubernetes import client, config
    try:
        config.load_incluster_config()
        api = client.ApiClient()
    except config.ConfigException:
        api = config.new_client_from_config(context=context)
    co = client.CustomObjectsApi(api)
    return lambda: co.list_namespaced_custom_object("jobset.x-k8s.io", "v1alpha2", TRAINING_NS, "jobsets",
                                                    label_selector=LABEL_JOB_ID)["items"]


def wait_done(list_jobsets, job_ids, timeout, poll=15.0, sleep=time.sleep, clock=time.monotonic):
    """다 끝나면 [], 시간이 다 되면 남은 job-id 목록"""
    t0, left, last = clock(), list(job_ids), None
    while left:
        try:
            left = pending_jobs(list_jobsets(), job_ids)
        except Exception as e:   # API 잠깐 끊김은 다음 차례에 다시
            print(f"JobSet 조회 실패 (다시 시도): {e}", flush=True)
        if not left:
            break
        if len(left) != last:
            print(f"끝나길 기다림: {len(left)}/{len(job_ids)}개 남음", flush=True)
            last = len(left)
        if clock() - t0 >= timeout:
            break
        sleep(poll)
    return left


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario")
    ap.add_argument("--kind", action="store_true", help="백엔드 없이 kind에 직접 제출")
    ap.add_argument("--context", default="kind-manager", help="클러스터 밖에서 돌 때 관리 클러스터 kubeconfig context")
    ap.add_argument("--speed", type=float, default=1.0, help="시각을 이 배수만큼 빨리")
    ap.add_argument("--prefix", default=datetime.now().strftime("x%m%d%H%M"), help="작업 이름 앞부분")
    ap.add_argument("--out", default=None)
    g = ap.add_argument_group("실험 기록 (experiment.started payload = experiments 테이블 열)")
    g.add_argument("--exp-id", default=None, help="기본 exp-<prefix>")
    g.add_argument("--name", default=None, help="기본 시나리오 파일 이름")
    g.add_argument("--policy", default=None, help="예: ours / baseline")
    g.add_argument("--fault-type", default=None, help="예: xid79, none")
    g.add_argument("--spare", dest="spare_enabled", action=argparse.BooleanOptionalAction, default=None)
    g.add_argument("--checkpoint-every-steps", type=int, default=None)
    g.add_argument("--no-kafka", action="store_true", help="Kafka에 안 올림 (로컬 kind 등)")
    g.add_argument("--no-wait", action="store_true", help="잡이 끝나길 안 기다림 -> experiment.ended도 안 보냄")
    g.add_argument("--wait-timeout", type=float, default=6 * 3600, help="잡이 끝나길 기다리는 최대 초")
    a = ap.parse_args(argv)

    rows = read_scenario(a.scenario)
    exp_id = a.exp_id or f"exp-{a.prefix}"
    conditions = {"name": a.name or Path(a.scenario).stem, "scenario": a.scenario, "policy": a.policy,
                  "fault_type": a.fault_type, "spare_enabled": a.spare_enabled,
                  "checkpoint_every_steps": a.checkpoint_every_steps}
    events = None if a.no_kafka else KafkaEvents()
    submit = KindSubmitter(a.context) if a.kind else submit_backend
    list_jobsets = None if a.no_wait else jobset_lister(a.context)   # 제출 전에 접속 설정부터 확인
    out_path = a.out or f"results-{datetime.now():%m%d-%H%M%S}.csv"
    job_ids = []
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        record_experiment(events, f, "experiment.started", exp_id, conditions)
        print(f"experiment.started {exp_id}", flush=True)
        t0 = time.monotonic()
        w = csv.writer(f)
        w.writerow(["index", "at_s", "gpus", "steps", "strategy", "duration_s", "job_id", "jobset", "queue",
                    "submitted_at", "result"])
        for i, row in enumerate(rows, 1):
            wait = row["at"] / a.speed - (time.monotonic() - t0)
            if wait > 0:
                time.sleep(wait)
            job_id, jobset, queue, result = submit(i, row, a.prefix)
            if job_id and result.startswith("20"):
                job_ids.append(job_id)
            w.writerow([i, row["at"], row["gpus"], row["steps"], row["strategy"], row["duration"] or "",
                        job_id, jobset, queue, now(), result])
            f.flush()
            print(f"[{time.monotonic() - t0:7.1f}s] #{i} GPU {row['gpus']} -> {jobset or '-'} {queue or ''} {result}",
                  flush=True)
        if a.no_wait:
            print("--no-wait: experiment.ended 안 보냄", flush=True)
        else:
            left = wait_done(list_jobsets, job_ids, a.wait_timeout)
            if left:
                f.write(f"# {a.wait_timeout:.0f}s 안에 안 끝난 잡 {len(left)}개: {' '.join(left)}"
                        " -> experiment.ended 안 보냄\n")
                print(f"{len(left)}개가 {a.wait_timeout:.0f}s 안에 안 끝남 -> experiment.ended 안 보냄", flush=True)
                print(f"결과: {out_path}")
                return 1
            record_experiment(events, f, "experiment.ended", exp_id)
            print(f"experiment.ended {exp_id}", flush=True)
    print(f"결과: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
