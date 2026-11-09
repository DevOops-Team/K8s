import time

import pytest

from harness import cluster as C
from harness.env import NS, REQUEST_TIMEOUT, TRAINING_GPUS, WORKERS
from harness.jobs import submit_auto
from harness.verdict import precondition

pytestmark = [pytest.mark.normal, pytest.mark.doc("배치(1) · 갱 스케줄링")]
SIZES = (4, 3, 2, 1, 4, 2, 1)   # 합 17장 > 학습 12장 -> 일부는 관리 클러스터에서 대기해야 함
WATCH = 120


def partial(kube):
    """워커마다 JobSet별 Pod 수와 Running 수가 다른 것 = 반쪽 배치."""
    out = []
    for w in WORKERS:
        total, running = {}, {}
        for p in kube.wcore(w).list_namespaced_pod(NS, _request_timeout=REQUEST_TIMEOUT).items:
            js = (p.metadata.labels or {}).get("jobset.sigs.k8s.io/jobset-name")
            if not js:
                continue
            total[js] = total.get(js, 0) + 1
            running[js] = running.get(js, 0) + (p.status.phase == "Running")
        out += [f"{w} {js} {running[js]}/{n}" for js, n in total.items() if running[js] != n]
    return out


def test_no_partial_placement(kube, check, info):
    """갱 스케줄링: 1~4장 잡을 섞어 넣어도 Pod 일부만 뜬 잡이 없음 (들어간 잡은 전부 Running, 못 들어간 잡은 Pod 0개)"""
    for i, g in enumerate(SIZES, 1):
        submit_auto(kube, f"mix-{i}", g, "20m")
    C.wait_until(lambda: sum(C.running_pods(kube).values()) > 0, 60)
    seen = []
    end = time.monotonic() + WATCH
    while time.monotonic() < end:
        seen += [p for p in partial(kube) if p not in seen]
        time.sleep(10)
    placed = [w for w in C.with_prefix(kube, "mix-") if w.cluster]
    running = sum(C.running_pods(kube).values())
    info["입장"] = f"{len(placed)}/{len(SIZES)}개, Running GPU {running}"
    precondition(len(placed) < len(SIZES), "전부 들어감 -> 자리가 모자란 상황이 안 만들어짐")
    precondition(running > 0, "아무것도 안 들어감")
    check.true(f"{WATCH}s 동안 반쪽 배치 0건 (10s마다 확인)", not seen, ", ".join(seen))
    check.true("쓰는 GPU가 학습 몫 12 이하", running <= TRAINING_GPUS, str(running))
    want = sum(g for i, g in enumerate(SIZES, 1) if any(w.id == f"mix-{i}" for w in placed))
    check.eq("들어간 잡의 GPU 합 = Running Pod 수", running, want)
