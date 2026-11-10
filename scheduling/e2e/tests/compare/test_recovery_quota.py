"""관리 cq-recovery 몫 3(B, 예전 = 예비 노드 수) vs 4(A, 정본 R10 = 잡 최대 크기).

상황은 recovery/test_11과 같다: 학습 12장이 차 있고 Heavy 대기 잡 wait-c(4장)가 있을 때 4장 잡 train-a를 moveToSpare.
가설: B에서는 4장 복구 잡이 학습 몫 1을 빌려야 하는데, 옛 잡이 내려놓은 Heavy 몫을 wait-c가 먼저 받아
cohort에 빌릴 몫이 없어서 복구 잡이 쿼터를 못 받는다. A에서는 자기 몫 4로 바로 받는다.
대조군 B에서 복구 잡이 쿼터를 받으면(문제 상황이 안 생기면) INCONCLUSIVE.
"""
import pytest

from harness import recovery as R
from harness.reset import reset
from harness.verdict import precondition

pytestmark = [pytest.mark.compare, pytest.mark.doc("R10 · 설계 문제 1")]
WAIT = 180


def test_recovery_quota_size(kube, ctl, check, info):
    """복구 몫: 3이면 4장 복구 잡이 학습 대기 잡에 밀려 쿼터를 못 받고, 4면 받음"""
    base = R.recovery_quota(kube)
    precondition(base == 4, f"시작 때 cq-recovery 몫이 4가 아님 ({base}) - queues/manager 다시 apply")
    try:
        R.set_recovery_quota(kube, 3)
        b = R.under_load(kube, WAIT)
    finally:
        R.set_recovery_quota(kube, base)
    info["B 몫 3"] = f"쿼터 {b['recovery_quota_at'] or '없음'}, 입장 {b['placed'] or '없음'}, wait-c {b['wait_c_quota_at']}"
    precondition(b["recovery_quota_at"] is None,
                 "대조군(몫 3)에서도 복구 잡이 쿼터를 받음 -> 문제 상황이 안 만들어짐")
    reset(kube)
    a = R.under_load(kube, WAIT)
    info["A 몫 4"] = f"쿼터 {a['recovery_quota_at'] or '없음'}, 입장 {a['placed'] or '없음'}, wait-c {a['wait_c_quota_at']}"
    check.true(f"A(몫 4)에서 복구 잡이 {WAIT}s 안에 쿼터", a["recovery_quota_at"] is not None)
    check.eq(f"A(몫 4)에서 복구 잡이 {WAIT}s 안에 worker1 입장", a["placed"], "worker1")
