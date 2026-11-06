"""잡 투입기 — 시나리오 파일을 읽어 정해진 시각에 학습 작업을 제출한다 (데이터 -> 쿠버네티스 이관, 11/06 수락).

시나리오 파일 (데이터 파트 김현수가 trace를 가공해 줌). 한 줄 = 작업 하나, '#' 뒤는 주석:
    제출 시각(초, 실험 시작 기준), GPU 수, step 수, 방식[, 실행 시간(초)]
    0, 4, 400, fsdp
    30, 1, 200, fsdp, 180
실행 시간은 선택 칸: kind(KWOK 가짜 GPU)에서는 step을 못 돌리므로 이 시간 뒤 Pod를 끝내 흉내 낸다.

    python submit.py scenario.csv                       # 백엔드 POST /api/jobs (BACKEND_URL)
    python submit.py scenario.csv --kind                # kind: 백엔드 없이 같은 규칙으로 직접 제출
    python submit.py scenario.csv --speed 10            # 시각을 1/10로 줄여서
결과: results-<시각>.csv (작업ID · JobSet 이름 · 큐 · 제출 시각 · 응답). 실험 시작·종료 시각도 같이 찍는다.

아직 안 정해진 것:
  - step 수·실행 시간을 백엔드에 넘길 칸이 API에 없음 (max_steps는 사용자에게 안 엶, 서비스 API 명세 3절) -> 노찬경과 정할 것.
    지금은 hyperparameters에 넣지 않고 결과 파일에만 남긴다.
  - experiments 테이블 started_at·ended_at 기록 경로 (김현수 답 대기) -> record_experiment()만 바꾸면 됨
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

BACKEND_URL = os.getenv("BACKEND_URL", "http://backend.platform.svc:8000")
MODEL = os.getenv("MODEL", "gpt2")
DATASET = {"id": os.getenv("DATASET_ID", "ds-demo"), "version": int(os.getenv("DATASET_VERSION", "1"))}
HEAVY_MIN_GPUS, MAX_JOB_GPUS = 3, 4   # 백엔드 공용 함수와 같은 규칙 (스케줄링 문서): 3장↑ heavy, 5장↑ 거부


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
    """kind 전용: 백엔드 대신 1세대 JobSet + 복구 원본·펜싱 기록 ConfigMap (scenarios/lib.sh submit과 같은 모양)"""

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
                self.core.create_namespaced_config_map("platform", body)
            except V1.ApiException as e:
                if e.status != 409:
                    raise
                self.core.replace_namespaced_config_map(cm_name, "platform", body)
        try:
            self.co.create_namespaced_custom_object("jobset.x-k8s.io", "v1alpha2", "training", "jobsets", js)
            return job_id, name, q, "201"
        except V1.ApiException as e:
            return job_id, name, q, f"{e.status} {e.reason}"


def record_experiment(results, started_at, ended_at):
    """experiments.started_at·ended_at 기록. 경로(DB 직접 / API)는 김현수 답을 받고 채운다. 지금은 결과 파일에만"""
    results.write(f"# experiment started_at={started_at} ended_at={ended_at}\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario")
    ap.add_argument("--kind", action="store_true", help="백엔드 없이 kind에 직접 제출")
    ap.add_argument("--context", default="kind-manager")
    ap.add_argument("--speed", type=float, default=1.0, help="시각을 이 배수만큼 빨리")
    ap.add_argument("--prefix", default=datetime.now().strftime("x%m%d%H%M"), help="작업 이름 앞부분")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    rows = read_scenario(a.scenario)
    submit = KindSubmitter(a.context) if a.kind else submit_backend
    out_path = a.out or f"results-{datetime.now():%m%d-%H%M%S}.csv"
    started_at = now()
    t0 = time.monotonic()
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["index", "at_s", "gpus", "steps", "strategy", "duration_s", "job_id", "jobset", "queue",
                    "submitted_at", "result"])
        for i, row in enumerate(rows, 1):
            wait = row["at"] / a.speed - (time.monotonic() - t0)
            if wait > 0:
                time.sleep(wait)
            job_id, jobset, queue, result = submit(i, row, a.prefix)
            w.writerow([i, row["at"], row["gpus"], row["steps"], row["strategy"], row["duration"] or "",
                        job_id, jobset, queue, now(), result])
            f.flush()
            print(f"[{time.monotonic() - t0:7.1f}s] #{i} GPU {row['gpus']} -> {jobset or '-'} {queue or ''} {result}",
                  flush=True)
        record_experiment(f, started_at, now())
    print(f"결과: {out_path}")


if __name__ == "__main__":
    sys.exit(main())
