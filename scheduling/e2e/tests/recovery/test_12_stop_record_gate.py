"""컨트롤러가 OLD_RUN_STOPPED를 펜싱 기록에 남겼다는 확인 전에는 relocator가 새 세대를 내지 않음 (ADR-36 순서, ADR-50).

relocator는 옛 실행 종료를 확인하면 status.recoveryProgress에만 OLD_RUN_STOPPED를 쓴다.
복구 컨트롤러가 그걸 펜싱 기록에 옮기고 RR에 devoops/recovery-stop-recorded=true를 붙여야 재제출한다.
"""
import pytest

from harness import recovery as R
from harness.jobs import submit_auto
from harness.situations import wait
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R7 · I6 · ADR-50")]


def test_waits_for_stop_record(kube, check, info):
    """확인 주석 없이 -> 종료 확인까지만, 새 세대 없음 -> 확인 붙이면 재제출해서 Done"""
    submit_auto(kube, "train-e", 1, "30m")
    old = "job-train-e-g0001"
    src = R.wait_placed(kube, old, 90)
    precondition(src, "train-e가 입장 못 함")
    precondition(R.wait_running(kube, src, old, 1) is not None, "train-e Pod가 Running 안 됨")

    R.HOLD_ACK.add("rr-gate")
    try:
        new = R.relocate(kube, "rr-gate", "evt-012", "restartInPlace", "train-e", reason="software")
        precondition(R.wait_until_status(kube, "rr-gate", "stoppedAt", 120), "옛 실행 종료 확인까지 안 감")
        wait(15, "relocator 루프 몇 번")
        st = R.rr_status(kube, "rr-gate")
        info["확인 전"] = f"{st.get('phase')}: {st.get('message', '')}"
        check.eq("확인 전 status 단계", (st.get("recoveryProgress") or {}).get("phase"), "OLD_RUN_STOPPED")
        check.true("확인 전 재제출 안 함", not st.get("resubmittedAt"))
        check.eq("새 세대 없음", R.jobset(kube, new), None)
        check.eq("흉내 컨트롤러가 펜싱 기록에 옮김", R.fencing(kube, "train-e").get("recovery_phase"), "OLD_RUN_STOPPED")
    finally:
        R.HOLD_ACK.discard("rr-gate")

    R.ack_stop(kube, "rr-gate")
    st = R.wait_rr(kube, "rr-gate", R.FINAL, 150)
    info["RR"] = R.stage_summary(st)
    check.eq("확인 뒤 RR Done", st.get("phase"), "Done")
    R.check_resubmission(check, kube, "train-e", new, "rr-gate", "evt-012", target=src)
