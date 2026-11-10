"""moveToOtherCluster (예비 소진·SDC): 의심 노드만 격리, 클러스터는 펜싱 안 함, 잡은 다른 워커로 (정본 R8, 06과 비교)."""
import pytest

from harness import recovery as R
from harness.env import K
from harness.jobs import submit
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R8 moveToOtherCluster · I1·I7")]
SRC = "worker2"


def test_move_to_other_cluster(kube, check, info):
    """worker2 2장 잡의 노드 SDC 의심 -> 그 노드만 cordon+faulty, worker2 펜싱 없음, 새 세대는 worker1/3, worker2는 새 잡을 계속 받음"""
    submit(kube, "train-d", 2, "queue-light", "30m", {K.ANN_TARGET: SRC})
    old = "job-train-d-g0001"
    precondition(R.wait_placed(kube, old, 120) == SRC, f"train-d가 {SRC}에 안 들어감")
    precondition(R.wait_running(kube, SRC, old, 2) is not None, "train-d Pod 2개가 Running 안 됨")
    bad = R.first_node(kube, SRC, old)

    with R.DoubleRunWatch(kube, "train-d") as dbl:
        new = R.relocate(kube, "rr-other", "evt-010", "moveToOtherCluster", "train-d", nodes=[bad], reason="sdc")
        st = R.wait_rr(kube, "rr-other", R.FINAL, 180)
    info["RR"] = R.stage_summary(st)
    check.eq("RR Done", st.get("phase"), "Done")
    dest = R.placed_on(kube, new)
    check.true("새 세대가 다른 워커로", dest not in (None, SRC), dest)
    check.eq(f"의심 노드 {bad} cordon·faulty", R.isolated(R.node(kube, SRC, bad)), (True, True))
    check.true(f"{SRC} 펜싱 안 함", not R.fenced(kube, SRC))
    check.true("두 세대가 동시에 Running인 적 없음 (I1)", not dbl.overlaps, "; ".join(dbl.overlaps[:3]))
    R.check_resubmission(check, kube, "train-d", new, "rr-other", "evt-010", exclude=SRC)

    submit(kube, "after-other", 1, "queue-light", "20m", {K.ANN_TARGET: SRC})
    got = R.wait_placed(kube, "job-after-other-g0001", 120)
    check.eq(f"펜싱 안 했으니 새 잡이 {SRC}에 입장", got, SRC)
    R.wait_running(kube, SRC, "job-after-other-g0001", 1, 60)
    nodes = R.running_nodes(kube, SRC, "job-after-other-g0001")
    check.true("새 잡이 격리된 노드는 피함", nodes and bad not in nodes, ",".join(nodes))
