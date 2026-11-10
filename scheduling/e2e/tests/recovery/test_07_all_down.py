"""워커 전부 끊김: 디스패처가 지명을 멈추고, 다시 붙으면 이어서 지명 (정본 3.4 "워커가 전부 끊기면 아무것도 하지 않는다")."""
import pytest

from harness import cluster as C
from harness import network
from harness import recovery as R
from harness.env import WORKERS
from harness.jobs import submit_auto
from harness.situations import wait
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("3.4 전부 끊김 · I7")]


def test_all_workers_cut_off(kube, ctl, check, info):
    """워커 3개 전부 끊김 -> 쿼터 받은 잡도 지명 없음, 디스패처 로그 '전부 끊김' -> 다시 연결하면 입장"""
    for w in WORKERS:
        network.cut_off(w)
    down = C.wait_until(lambda: not any(R.mkc_active(kube, w) for w in WORKERS), 180, interval=5)
    precondition(down is not None, "180s 안에 MultiKueueCluster가 전부 Active=False로 안 바뀜")
    info["Active=False까지"] = f"{down:.0f}s"

    submit_auto(kube, "while-down", 1, "20m")
    wait(30, "디스패처 루프 몇 번")
    w = C.wl(kube, "while-down")
    precondition(w is not None and w.quota, "while-down이 관리 쿼터를 못 받음 (지명 단계까지 못 감)")
    check.eq("끊긴 동안 지명 없음", w.nominated, [])
    check.eq("끊긴 동안 입장 없음", w.cluster, None)
    log = ctl.logfile.read_text() if ctl.logfile else ""
    check.true("디스패처 로그 '전부 끊김'", "전부 끊김" in log)

    for w_ in WORKERS:
        network.reconnect(w_, kube)
    got = R.wait_placed(kube, "job-while-down-g0001", 240)
    check.true("다시 연결 뒤 240s 안에 입장", got is not None)
