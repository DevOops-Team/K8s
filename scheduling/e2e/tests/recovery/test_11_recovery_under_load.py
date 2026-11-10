"""학습 12장이 찬 상태 + Heavy 대기 잡에서 4장 잡 moveToSpare (정본 R10, 설계 문제 1).

옛 시나리오 11은 "복구 잡이 wait-c보다 먼저 쿼터를 받으면 OK"로 판정했는데, 정본 R10에서는 그게 기대가 아니다.
옛 잡을 내리는 순간 풀린 Heavy 몫 4는 wait-c가 먼저 받는다 (복구 잡은 그 뒤 OLD_RUN_STOPPED를 거쳐 만들어짐).
R10이 지키는 것은 "복구 잡이 학습 몫을 빌리지 않고 자기 몫 4로 쿼터를 받는다"와 "worker1 자리는 복구 잡 몫"이다.
복구 몫 3과 비교는 compare/test_recovery_quota.py.
"""
import pytest

from harness import recovery as R

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R10 · 3.4 재배치 자리 지키기")]
WAIT = 180


def test_recovery_under_load(kube, check, info):
    """학습 12장 + Heavy 대기 wait-c에서 train-a(worker1) moveToSpare -> 복구 잡이 복구 몫으로 쿼터·worker1 입장, wait-c는 worker1을 못 가져감"""
    quota = R.recovery_quota(kube)
    check.eq("관리 cq-recovery 몫 (R10)", quota, 4)
    r = R.under_load(kube, WAIT)
    info["RR"] = r["rr"]
    info["쿼터 시각"] = f"복구 {r['recovery_quota_at']}, wait-c {r['wait_c_quota_at']}"
    info["wait-c 워커"] = r["wait_c_cluster"] or "-"
    check.true(f"복구 잡이 {WAIT}s 안에 쿼터", r["recovery_quota_at"] is not None)
    check.eq(f"복구 잡이 {WAIT}s 안에 worker1 입장", r["placed"], "worker1")
    check.true("복구 잡 전에 wait-c가 worker1을 차지하지 않음", not r["stolen"], ", ".join(r["stolen"]))
    w = R.workload(kube, r["new"]) or {}
    check.eq("복구 잡 관리 쿼터", ((w.get("status") or {}).get("admission") or {}).get("clusterQueue"), "cq-recovery")
