"""ADR-41: 실제 Kueue에서 3회 새치기 후 Light/복구 쿼터 예약을 꺼짐/켜짐 비교.

기존 kind/KWOK 하네스의 training 초기화 범위를 사용한다. 큐 설정은 읽기만 한다.
복구 재제출 경로 자체는 recovery/에서 검증하며 여기서는 복구 형태 JobSet으로 쿼터 단계를 재현한다.
"""
import json
import time

import pytest
from kubernetes import client

from harness import cluster as C
from harness import recovery as R
from harness.env import K, NS, PROFILE, REQUEST_TIMEOUT, WORKERS
from harness.jobs import jobset_body, submit
from harness.reset import reset
from harness.verdict import precondition

pytestmark = [pytest.mark.compare, pytest.mark.doc("ADR-41 · GPU-251"),
              pytest.mark.controllers("dispatcher", QUOTA_HOLD_ENABLED="false")]
WAIT = 120


def raw(kube, job):
    return R.workload(kube, C.jsname(job))


def quota(kube, job):
    return R.quota_at(kube, C.jsname(job)) is not None


def held(kube, job):
    return ((raw(kube, job) or {}).get("metadata", {}).get("annotations") or {}).get(K.ANN_HELD_FOR)


def require_wait(pred, text, timeout=WAIT):
    precondition(C.wait_until(pred, timeout, interval=1) is not None, text)


def expect_wait(pred, text, timeout=WAIT):
    assert C.wait_until(pred, timeout, interval=1) is not None, text


def training(kube, job, size, worker, queue):
    submit(kube, job, size, queue, "30m", {K.ANN_TARGET: worker})


def recovery_job(kube, job, size, worker=None):
    # Admin-shaped recovery submission, same priority/queue/toleration as a relocator
    # result. No generation or fencing record is advanced by this quota-only test.
    body = jobset_body(job, size, K.RECOVERY_QUEUE, "30m", {K.ANN_TARGET: worker} if worker else {})
    body["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]["tolerations"].append(
        {"key": K.TAINT_SPARE, "operator": "Equal", "value": "true", "effect": "NoSchedule"})
    kube.co.create_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, NS, "jobsets", body,
                                            _request_timeout=REQUEST_TIMEOUT)


def remove(kube, job):
    kube.co.delete_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, NS, "jobsets", C.jsname(job),
                                            propagation_policy="Background", _request_timeout=REQUEST_TIMEOUT)
    require_wait(lambda: raw(kube, job) is None, f"{job} Workload 삭제 안 됨")


def wait_seen(kube, target, n):
    def enough():
        w = raw(kube, target) or {}
        value = (w.get("metadata", {}).get("annotations") or {}).get(K.ANN_QUOTA_BYPASSES, "{}")
        return len(json.loads(value).get("uids", [])) >= n
    expect_wait(enough, f"{target} 새치기 {n}회 기록 안 됨")


def settings(enabled):
    return {"QUOTA_HOLD_ENABLED": str(enabled).lower(), "QUOTA_HOLD_BYPASS_LIGHT": "3",
            "QUOTA_HOLD_BYPASS_RECOVERY": "3", "INTERVAL_SECONDS": "1"}


def setup_load(kube, rec):
    # Fill Heavy first so Light cannot borrow unused Heavy quota. A deterministic
    # packed layout leaves the Light slots on the last worker in both profiles.
    remaining = PROFILE.heavy
    for i, worker in enumerate(WORKERS):
        size = min(4, remaining)
        if not size:
            break
        job = f"qh-heavy-{i}"
        training(kube, job, size, worker, "queue-heavy")
        require_wait(lambda j=job: bool(R.placed_on(kube, C.jsname(j))), f"{job} 입장 안 됨")
        remaining -= size
    lights = PROFILE.light if rec else PROFILE.light - 1
    for i in range(lights):
        job = f"qh-light-{i}"
        training(kube, job, 1, WORKERS[-1], "queue-light")
        require_wait(lambda j=job: bool(R.placed_on(kube, C.jsname(j))), f"{job} 입장 안 됨")
    if rec:
        recovery_job(kube, "qh-rec-one", 1, WORKERS[0])
        require_wait(lambda: bool(R.placed_on(kube, C.jsname("qh-rec-one"))), "1장 복구 입장 안 됨")


def run_arm(kube, ctl, enabled, rec, check):
    reset(kube)
    ctl.use(("dispatcher",), **settings(enabled))
    setup_load(kube, rec)
    target = "qh-big"
    if rec:
        recovery_job(kube, target, 4)
    else:
        submit(kube, target, 2, "queue-light", "30m")
    require_wait(lambda: raw(kube, target) is not None, "보호할 Workload 안 생김")
    precondition(not quota(kube, target), "큰 잡이 이미 쿼터 받음: 굶주림 재현 안 됨")
    started = time.monotonic()
    slot = "qh-light-0" if rec else None
    for i in range(1, 4):
        job = f"qh-bypass-{i}"
        training(kube, job, 1, WORKERS[-1], "queue-light")
        require_wait(lambda j=job: raw(kube, j) is not None, "후속 잡 Workload 안 생김")
        if slot:
            remove(kube, slot)
        require_wait(lambda j=job: quota(kube, j), f"{i}번째 새치기 재현 안 됨")
        precondition(not quota(kube, target), "새치기 중 큰 잡도 쿼터 받음: 대조 조건 안 만들어짐")
        if enabled:
            wait_seen(kube, target, i)  # Do not delete a bypass before a dispatcher poll observes it.
        slot = job

    follower = "qh-follower"
    training(kube, follower, 1, WORKERS[-1], "queue-light")
    if enabled:
        expect_wait(lambda: bool(held(kube, follower)), "3회 뒤 후속 잡 보류 안 됨")
        check.eq("보류는 대기 잡만", (raw(kube, follower)["spec"]["active"], quota(kube, follower)), (False, False))
        # Restart the leader while the target still cannot fit. The new process
        # must reconstruct holds and counters from API state.
        coord = client.CoordinationV1Api(kube.mgr)
        previous = coord.read_namespaced_lease("scheduling-dispatcher", "platform").spec.holder_identity
        ctl.stop()
        ctl.use(("dispatcher",), **settings(True))
        expect_wait(lambda: (coord.read_namespaced_lease("scheduling-dispatcher", "platform").spec.holder_identity
                             not in (None, "", previous)), "새 디스패처가 Lease를 못 가져감")
        expect_wait(lambda: bool(held(kube, follower)), "리더 재시작 후 보류 유실")
    remove(kube, slot)
    if not enabled:
        require_wait(lambda: quota(kube, follower), "꺼짐에서 4번째 새치기 재현 안 됨")
        precondition(not quota(kube, target), "꺼짐에서도 큰 잡 쿼터 받음: 비교 불가")
        return {"quota_s": None, "bypasses": 4}
    if not rec:
        remove(kube, "qh-light-0")  # Accumulate two slots after later jobs stop refilling one.
    expect_wait(lambda: quota(kube, target), "예약 뒤 필요한 자리가 비어도 큰 잡 쿼터 못 받음")
    quota_s = time.monotonic() - started
    expect_wait(lambda: not held(kube, follower) and raw(kube, follower)["spec"].get("active", True),
                "큰 잡 쿼터 획득 뒤 후속 잡 재활성화 안 됨")
    if rec:
        remove(kube, "qh-heavy-0")  # Quota succeeded; now make room on one worker.
    expect_wait(lambda: bool(R.placed_on(kube, C.jsname(target))), "쿼터 뒤 워커 입장 안 됨")
    check.true("기존 실행 유지", quota(kube, "qh-rec-one") if rec else quota(kube, "qh-heavy-0"))
    return {"quota_s": round(quota_s, 2), "placed_s": round(time.monotonic() - started, 2), "bypasses": 3}


@pytest.mark.parametrize("rec", [False, True], ids=["light-two", "recovery-four"])
def test_three_bypasses_then_quota_hold(kube, ctl, check, info, rec):
    """3회 새치기 뒤 추가 경쟁을 보류하고 쿼터·워커 입장, 리더 재시작·보류 해제 확인"""
    precondition(PROFILE.heavy + PROFILE.light == PROFILE.training and PROFILE.light >= 2,
                 "학습 쿼터 합계와 실제 학습 GPU 수가 같은 프로필 필요")
    for name, value in (("cq-heavy", PROFILE.heavy), ("cq-light", PROFILE.light), ("cq-recovery", 4)):
        precondition(int(C.cq_gpu(kube, name)["nominalQuota"]) == value, f"{name} 쿼터가 테스트 프로필과 다름")
    try:
        info["disabled"] = run_arm(kube, ctl, False, rec, check)
        info["enabled"] = run_arm(kube, ctl, True, rec, check)
    finally:
        # Return to the disabled setting before final test cleanup.
        ctl.use(("dispatcher",), **settings(False))
        reset(kube)
