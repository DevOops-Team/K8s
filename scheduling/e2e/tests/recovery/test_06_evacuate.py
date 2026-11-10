"""evacuateCluster + 워커 끊김: 종료 증거가 없으면 재제출하지 않고, 사람 확인 뒤에만 다른 워커로 (정본 R8·R9, I6·I7).

워커가 끊기면 관리 클러스터 Workload는 지워져도 옛 Pod가 살아 있을 수 있다. 그래서 relocator는 워커에서 사라진 걸
볼 때까지(또는 EC2 증거 — kind에선 꺼짐 — 또는 사람 어노테이션) WaitingForStop으로 기다려야 한다.
"""
import pytest

from harness import cluster as C
from harness import network
from harness import recovery as R
from harness.env import K
from harness.jobs import submit, submit_auto
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("R8 evacuateCluster · R9 · I6·I7")]
SRC = "worker2"
STOP_WAIT = 60      # relocator STOP_WAIT_SECONDS 기본값
SLACK = 45


def test_evacuate_cut_off_cluster(kube, check, info):
    """worker2 끊김 + evacuateCluster -> 펜싱, 증거 없으니 WaitingForStop(재제출 안 함) -> 사람 확인 뒤 다른 워커로, 새 잡도 worker2로 안 감"""
    submit(kube, "train-b", 2, "queue-light", "30m", {K.ANN_TARGET: SRC})
    old = "job-train-b-g0001"
    precondition(R.wait_placed(kube, old, 120) == SRC, f"train-b가 {SRC}에 안 들어감")
    precondition(R.wait_running(kube, SRC, old, 2) is not None, "train-b Pod 2개가 Running 안 됨")

    network.cut_off(SRC)
    new = R.relocate(kube, "rr-evac", "evt-006", "evacuateCluster", "train-b", reason="az")
    st = R.wait_rr(kube, "rr-evac", ("WaitingForStop", *R.FINAL), STOP_WAIT + SLACK + 30)
    info["끊긴 동안 RR"] = f"{st.get('phase')}: {st.get('message', '')[:120]}"
    check.eq("증거 없으면 WaitingForStop", st.get("phase"), "WaitingForStop")
    check.true(f"{SRC} 펜싱 라벨", R.fenced(kube, SRC))
    check.true("확인 전 재제출본 없음 (I6)", R.jobset(kube, new) is None)
    check.eq("확인 전 펜싱 기록 단계", R.fencing(kube, "train-b").get("recovery_phase"), "GENERATION_ADVANCED")

    R.confirm_stopped(kube, "rr-evac")
    st = R.wait_rr(kube, "rr-evac", R.FINAL, 180)
    info["RR"] = R.stage_summary(st)
    check.eq("사람 확인 뒤 RR Done", st.get("phase"), "Done")
    dest = R.placed_on(kube, new)
    check.true("새 세대가 다른 워커로", dest not in (None, SRC), dest)
    R.check_resubmission(check, kube, "train-b", new, "rr-evac", "evt-006", exclude=SRC)

    submit_auto(kube, "after-evac", 1, "20m")
    got = R.wait_placed(kube, "job-after-evac-g0001", 90)
    check.true(f"펜싱 뒤 새 잡은 {SRC} 말고 다른 워커로 (I7)", got not in (None, SRC), got)

    # 다시 연결: 펜싱 라벨은 사람이 지우기 전까지 남는다. 끊긴 쪽에 살아 있던 옛 복사본을 MultiKueue가 치우는지는
    # 정본에 정해진 기대가 없어 기록만 한다 (사람이 확인 어노테이션을 잘못 붙이면 이 시간 동안 이중 실행)
    network.reconnect(SRC, kube)
    gone = C.wait_until(lambda: not R.pods(kube, SRC, old), 180, interval=5)
    info["다시 연결 뒤 worker2 옛 복사본"] = "남음 (180s)" if gone is None else f"{gone:.0f}s 뒤 사라짐"
    check.true(f"다시 연결 뒤에도 {SRC} 펜싱 유지", R.fenced(kube, SRC))
