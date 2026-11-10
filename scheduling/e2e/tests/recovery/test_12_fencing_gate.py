"""세대 +1 전에는 relocator가 아무것도 안 함 (정본 R7 "펜싱 기록의 recovery_id와 세대를 확인", I6).

복구 컨트롤러가 RR을 만들었는데 DynamoDB 세대 +1(GENERATION_ADVANCED)이 아직 안 보이면 Pending으로 기다린다.
옛 잡을 내리거나 새 세대를 내면 안 된다. 세대가 올라가면 그때부터 진행.
"""
import pytest

from harness import recovery as R
from harness.jobs import submit_auto
from harness.situations import wait
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R7 · I6")]


def test_waits_for_generation_advanced(kube, check, info):
    """세대 +1 없이 RR -> Pending, 옛 잡 그대로·새 세대 없음 -> 세대 +1 하면 진행해서 Done"""
    submit_auto(kube, "train-e", 1, "30m")
    old = "job-train-e-g0001"
    src = R.wait_placed(kube, old, 90)
    precondition(src, "train-e가 입장 못 함")
    precondition(R.wait_running(kube, src, old, 1) is not None, "train-e Pod가 Running 안 됨")

    R.create_rr(kube, "rr-gate", "evt-012", "restartInPlace", "train-e", 1, reason="software")
    wait(25, "relocator 루프 몇 번")
    st = R.rr_status(kube, "rr-gate")
    info["세대 +1 전"] = f"{st.get('phase')}: {st.get('message', '')}"
    check.eq("세대 +1 전 RR", st.get("phase"), "Pending")
    check.true("세대 +1 전 격리 안 함", not st.get("isolatedAt"))
    check.true("옛 잡 그대로", R.jobset(kube, old) is not None and len(R.pods(kube, src, old, "Running")) == 1)
    check.eq("새 세대 없음", R.jobsets_of(kube, "train-e"), [old])

    R.advance_generation(kube, "train-e", "evt-012")
    st = R.wait_rr(kube, "rr-gate", R.FINAL, 150)
    info["RR"] = R.stage_summary(st)
    check.eq("세대 +1 뒤 RR Done", st.get("phase"), "Done")
    R.check_resubmission(check, kube, "train-e", "job-train-e-g0002", "rr-gate", "evt-012", target=src)
