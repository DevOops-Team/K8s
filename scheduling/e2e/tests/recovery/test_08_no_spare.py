"""예비 없음: moveToSpare인데 예비 노드가 없으면 반쪽 배치 대신 자리가 날 때까지 대기 (정본 R5·R8, I2, 3.4 재배치 자리 지키기).

"예비 없음"은 예비 노드 cordon으로, "새 노드 확보"(EKS에선 Karpenter 보충)는 uncordon으로 흉내 낸다.
05(예비 있음)와 resubmitted -> admitted 간격을 비교할 수 있게 기록한다.
"""
import pytest

from harness import cluster as C
from harness import recovery as R
from harness.env import K
from harness.jobs import submit
from harness.situations import wait
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R8 moveToSpare(예비 없음) · R5 · I2")]
SRC = "worker2"
HOLD = 60


def test_move_to_spare_without_spare(kube, check, info):
    """worker2 예비 cordon 상태에서 moveToSpare -> 새 세대가 Pod 하나도 안 뜨고 대기, 예비 uncordon 뒤 입장"""
    spare = R.spare_nodes(kube, SRC)
    precondition(len(spare) == 1, f"{SRC} 예비 노드가 1대가 아님 ({spare})")
    R.set_cordon(kube, SRC, spare[0], True)
    submit(kube, "train-a", 4, "queue-heavy", "30m", {K.ANN_TARGET: SRC})
    old = "job-train-a-g0001"
    precondition(R.wait_placed(kube, old, 120) == SRC, f"train-a가 {SRC}에 안 들어감")
    precondition(R.wait_running(kube, SRC, old, 4) is not None, "train-a Pod 4개가 Running 안 됨")
    bad = R.first_node(kube, SRC, old)

    new = R.relocate(kube, "rr-nospare", "evt-008", "moveToSpare", "train-a", nodes=[bad])
    precondition(C.wait_until(lambda: R.rr_status(kube, "rr-nospare").get("resubmittedAt"), 150) is not None,
                 f"150s 안에 재제출까지 안 감 ({R.rr_status(kube, 'rr-nospare').get('message', '')})")
    wait(HOLD, "학습 노드 3대뿐인 동안")
    check.eq(f"자리 없는 {HOLD}s 동안 지명·입장 없음", R.placed_on(kube, new), None)
    check.eq("새 세대 Pod 0개 (반쪽 배치 없음)", len(R.pods(kube, SRC, new)), 0)
    check.true("RR 아직 안 끝남", R.rr_status(kube, "rr-nospare").get("phase") not in R.FINAL)

    R.set_cordon(kube, SRC, spare[0], False)
    st = R.wait_rr(kube, "rr-nospare", R.FINAL, 150)
    R.wait_running(kube, SRC, new, 4, 60)
    info["RR"] = R.stage_summary(st)
    check.eq("예비 확보 뒤 RR Done", st.get("phase"), "Done")
    check.eq("새 세대 클러스터", R.placed_on(kube, new), SRC)
    check.eq("새 세대 Pod 4개 Running", len(R.running_nodes(kube, SRC, new)), 4)
    R.check_resubmission(check, kube, "train-a", new, "rr-nospare", "evt-008", target=SRC)
