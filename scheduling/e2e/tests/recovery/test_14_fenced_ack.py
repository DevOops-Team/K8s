"""worker3 펜싱 뒤 새 루프의 ACK를 받고, 자동 제출 잡이 다른 워커로 가는지 확인한다.

kind worker1~3과 실행 중인 디스패처가 필요하다. 끝나면 펜싱 라벨을 지운다.
"""
from datetime import datetime, timezone
import time

import pytest

from harness import cluster as C
from harness import recovery as R
from harness.env import K, REQUEST_TIMEOUT, WORKERS
from harness.jobs import submit_auto
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("fenced ACK 뒤 새 잡 제외")]
WORKER = "worker3"


def test_fenced_ack(kube, check, info):
    precondition(set(("worker1", "worker2", WORKER)) <= set(WORKERS), "kind worker1~3이 필요함")
    precondition(R.mkc_active(kube, WORKER), "worker3 연결이 Active가 아님")

    def label(value):
        kube.co.patch_cluster_custom_object(
            K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", WORKER,
            {"metadata": {"labels": {K.LABEL_FENCED: value}}}, _request_timeout=REQUEST_TIMEOUT)

    try:
        started = datetime.now(timezone.utc).replace(microsecond=0)
        clock = time.monotonic()
        label("true")

        def acknowledged():
            ack = R.fenced_ack(kube, WORKER)
            return ack is not None and ack > started

        elapsed = C.wait_until(acknowledged, 30, interval=2)
        info["fenced ACK 대기"] = f"{time.monotonic() - clock:.1f}s"
        check.true("30초 안에 fenced-ack > T", elapsed is not None)
        if elapsed is None:
            return
        submit_auto(kube, "after-fenced-ack", 1, "20m")
        placed = R.wait_placed(kube, "job-after-fenced-ack-g0001", 90)
        check.true("ACK 뒤 새 잡은 worker3 외 워커로", placed in ("worker1", "worker2"), placed)
    finally:
        label(None)
