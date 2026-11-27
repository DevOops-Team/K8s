"""노드 고정 없는 restartInPlace(노드 장애): 학습 노드에 자리가 있으면 예비 노드를 쓰지 않음 (GPU-306).

복구 컨트롤러는 노드 소실·NotReady면 spec.nodes 없이 restartInPlace를 보낸다. 재제출본은 예비 toleration과
비예비 선호(preferred nodeAffinity devoops/spare NotIn [true])를 갖는다. 고장 노드는 cordon으로 흉내 낸다
(NotReady 노드처럼 새 Pod가 못 감). 학습 노드가 모자랄 때 예비로 가는 경우는 test_05가 본다.
"""
import pytest

from harness import recovery as R
from harness.jobs import submit
from harness.verdict import precondition
from harness.env import K

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R7 restartInPlace 노드 장애 · GPU-306")]
SRC = "worker2"
WAIT = 180


def test_node_failure_prefers_training_nodes(kube, check, info):
    """worker2 2장 잡의 노드 하나 cordon -> nodes 없는 restartInPlace -> 새 세대 2개 모두 학습 노드, 예비는 비어 있음"""
    spare = R.spare_nodes(kube, SRC)
    precondition(len(spare) == 1, f"{SRC} 예비 노드가 1대가 아님 ({spare})")
    submit(kube, "train-p", 2, "queue-light", "30m", {K.ANN_TARGET: SRC})
    old = "job-train-p-g0001"
    precondition(R.wait_placed(kube, old, 120) == SRC, f"train-p가 {SRC}에 안 들어감")
    precondition(R.wait_running(kube, SRC, old, 2) is not None, "train-p Pod 2개가 Running 안 됨")
    bad = R.first_node(kube, SRC, old)
    precondition(bad not in spare, f"옛 잡이 예비 노드 {bad}에 있음")

    R.set_cordon(kube, SRC, bad, True)
    try:
        with R.DoubleRunWatch(kube, "train-p") as dbl:
            new = R.relocate(kube, "rr-prefer", "evt-017", "restartInPlace", "train-p")
            st = R.wait_rr(kube, "rr-prefer", R.FINAL, WAIT)
            R.wait_running(kube, SRC, new, 2, 60)
        info["RR"] = R.stage_summary(st)
        check.eq(f"RR {WAIT}s 안에 Done", st.get("phase"), "Done")
        check.eq("노드 고정 없음", st.get("pinnedNodes") or [], [])
        nodes = R.running_nodes(kube, SRC, new)
        info["새 Pod 노드"] = ",".join(nodes)
        check.eq("새 Pod 2개 Running", len(nodes), 2)
        check.true("예비 노드에 새 Pod 없음", not set(nodes) & set(spare), ",".join(nodes))
        check.true(f"고장 노드 {bad}에 새 Pod 없음", bad not in nodes, ",".join(nodes))
        check.true("재제출본 비예비 선호", R.prefers_non_spare(R.jobset(kube, new)))
        check.true("이중 실행 없음", not dbl.overlaps, str(dbl.overlaps))
    finally:
        R.set_cordon(kube, SRC, bad, False)
