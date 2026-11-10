"""moveToSpare: 고장 노드 격리 -> 같은 클러스터에 새 세대, 빈자리는 예비 노드 (정본 R8, I1·I4·I6)."""
import pytest

from harness import recovery as R
from harness.jobs import submit
from harness.verdict import precondition
from harness.env import K

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R7·R8 moveToSpare · I1·I4·I6")]
SRC = "worker2"
WAIT = 180


def test_move_to_spare(kube, check, info):
    """worker2 4장 잡의 노드 하나 고장 -> 그 노드 cordon+faulty, 새 세대가 worker2 학습 3대 + 예비 1대에, 자리 표시 Pod는 밀려남"""
    R.placeholder(kube, SRC)
    spare = R.spare_nodes(kube, SRC)
    precondition(len(spare) == 1, f"{SRC} 예비 노드가 1대가 아님 ({spare})")
    submit(kube, "train-a", 4, "queue-heavy", "30m", {K.ANN_TARGET: SRC})
    old = "job-train-a-g0001"
    precondition(R.wait_placed(kube, old, 120) == SRC, f"train-a가 {SRC}에 안 들어감")
    precondition(R.wait_running(kube, SRC, old, 4) is not None, "train-a Pod 4개가 Running 안 됨")
    bad = R.first_node(kube, SRC, old)

    with R.DoubleRunWatch(kube, "train-a") as dbl:
        new = R.relocate(kube, "rr-spare", "evt-005", "moveToSpare", "train-a", nodes=[bad])
        st = R.wait_rr(kube, "rr-spare", R.FINAL, WAIT)
        R.wait_running(kube, SRC, new, 4, 60)
    info["RR"] = R.stage_summary(st)
    check.eq(f"RR {WAIT}s 안에 Done", st.get("phase"), "Done")
    check.eq("RR sourceCluster", st.get("sourceCluster"), SRC)
    check.eq("새 세대 클러스터", R.placed_on(kube, new), SRC)
    check.eq(f"고장 노드 {bad} cordon·faulty", R.isolated(R.node(kube, SRC, bad)), (True, True))
    nodes = R.running_nodes(kube, SRC, new)
    info["새 세대 노드"] = ",".join(nodes)
    check.eq("새 세대 Pod 4개 Running", len(nodes), 4)
    check.true("고장 노드에 새 Pod 없음", bad not in nodes, ",".join(nodes))
    check.true(f"예비 노드 {spare[0]}에 새 Pod", spare[0] in nodes, ",".join(nodes))
    check.true("자리 표시 Pod가 밀려남", R.placeholder_phase(kube, SRC) != "Running", R.placeholder_phase(kube, SRC))
    check.true(f"{SRC} 펜싱 안 함", not R.fenced(kube, SRC))
    check.true("두 세대가 동시에 Running인 적 없음 (I1)", not dbl.overlaps, "; ".join(dbl.overlaps[:3]))
    R.check_resubmission(check, kube, "train-a", new, "rr-spare", "evt-005", target=SRC)
