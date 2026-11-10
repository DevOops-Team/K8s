"""대기열이 찬 상태에서 복구 잡이 먼저 (정본 R1·R2, 3.4 재배치 자리 지키기).

옛 시나리오 04는 queue-recovery 잡을 손으로 바로 넣어 예비 노드에 올라가길 기다렸는데, 11/10부터 예비 노드에
devoops/spare taint가 있어 toleration 없는 잡은 못 올라간다 (I4). 복구 잡은 relocator 재제출본만 있으므로 진짜 경로로 바꿨다.
"""
import pytest

from harness import cluster as C
from harness import recovery as R
from harness.env import TRAINING_GPUS, K
from harness.jobs import submit_auto
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R1·R2 · 3.4 재배치 자리 지키기")]
WAIT = 150


def test_recovery_first_when_queue_full(kube, check, info):
    """학습 12장 + 대기 2개에서 restartInPlace -> 복구 잡(1000)이 원래 클러스터에 먼저, 대기 학습 잡은 그 클러스터로 안 감"""
    for i in range(1, TRAINING_GPUS + 3):
        submit_auto(kube, f"fill-{i}", 1, "20m")
    C.wait_until(lambda: C.count(kube, "fill-", "placed") >= TRAINING_GPUS, 120)
    precondition(C.count(kube, "fill-", "placed") == TRAINING_GPUS,
                 f"1장 잡 {TRAINING_GPUS}개가 다 안 들어감 ({C.by_cluster(kube, 'fill-')})")
    waiting = [w.id for w in C.with_prefix(kube, "fill-") if not w.cluster]
    precondition(len(waiting) == 2, f"대기 잡이 2개가 아님 ({waiting})")

    victim = "fill-1"
    src = C.wl(kube, victim).cluster
    new = R.relocate(kube, "rr-first", "evt-004", "restartInPlace", victim, reason="software")

    stolen = []   # 복구 잡이 들어가기 전에 대기 학습 잡이 원래 클러스터로 간 것

    def placed():
        if not R.placed_on(kube, new):
            stolen.extend(f"{w.id}->{w.cluster}" for w in C.with_prefix(kube, "fill-")
                          if w.id in waiting and w.cluster == src and f"{w.id}->{w.cluster}" not in stolen)
            return False
        return True
    took = C.wait_until(placed, WAIT)

    st = R.rr_status(kube, "rr-first")
    w = R.workload(kube, new) or {}
    info["RR"] = R.stage_summary(st)
    info["복구 잡 노드"] = ",".join(R.running_nodes(kube, src, new)) or "-"
    check.true(f"복구 잡 {new}이 {WAIT}s 안에 입장", took is not None, st.get("message", ""))
    check.eq("복구 잡이 원래 클러스터로", R.placed_on(kube, new), src)
    check.eq("복구 잡 우선순위", (w.get("spec") or {}).get("priority"), K.RECOVERY_PRIORITY_VALUE)
    check.eq("복구 잡 관리 쿼터", ((w.get("status") or {}).get("admission") or {}).get("clusterQueue"), "cq-recovery")
    check.true("복구 잡 전에 대기 학습 잡이 원래 클러스터를 차지하지 않음", not stolen, ", ".join(stolen))
    check.eq("RR", st.get("phase"), "Done")
