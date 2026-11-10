"""같은 requestId 요청 두 개 -> 한 번만 실행 (정본 R7, 5절 RR status.phase Duplicate, I1)."""
import pytest

from harness import recovery as R
from harness.jobs import submit_auto
from harness.situations import wait
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R7 requestId · I1")]


def test_duplicate_request_runs_once(kube, check, info):
    """같은 requestId(evt-009)로 restartInPlace 두 번 -> 먼저 만든 요청만 Done, 뒤 요청 Duplicate, 새 세대는 g0002 하나"""
    submit_auto(kube, "train-c", 1, "30m")
    precondition(R.wait_placed(kube, "job-train-c-g0001", 90), "train-c가 입장 못 함")

    with R.DoubleRunWatch(kube, "train-c") as dbl:
        new = R.relocate(kube, "rr-dup-a", "evt-009", "restartInPlace", "train-c", reason="software")
        wait(2, "생성 시각이 갈리게")
        again = R.relocate(kube, "rr-dup-b", "evt-009", "restartInPlace", "train-c", reason="software")
        precondition(again == new, f"흉내 복구 컨트롤러가 세대를 두 번 올림 ({new}, {again})")
        a = R.wait_rr(kube, "rr-dup-a", R.FINAL, 150)
        b = R.wait_rr(kube, "rr-dup-b", R.FINAL, 30)
    info["rr-dup-a"], info["rr-dup-b"] = R.stage_summary(a), b.get("message", "")
    check.eq("먼저 만든 요청", a.get("phase"), "Done")
    check.eq("뒤 요청", b.get("phase"), "Duplicate")
    check.eq("남은 JobSet", R.jobsets_of(kube, "train-c"), [new])
    rec = R.fencing(kube, "train-c")
    check.eq("펜싱 기록 세대", rec.get("generation"), "2")
    check.eq("펜싱 기록 retry_count", rec.get("retry_count"), "1")
    check.true("두 세대가 동시에 Running인 적 없음 (I1)", not dbl.overlaps, "; ".join(dbl.overlaps[:3]))
    check.eq("새 세대가 원래 클러스터로", R.placed_on(kube, new), a.get("sourceCluster"))
    R.check_resubmission(check, kube, "train-c", new, "rr-dup-a", "evt-009", target=a.get("sourceCluster"))
