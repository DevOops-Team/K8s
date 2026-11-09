import pytest

from harness import cluster as C
from harness.jobs import Rejected, queue_for, submit_auto

pytestmark = [pytest.mark.normal, pytest.mark.doc("입력 · 잡 투입기 규칙")]


def test_routing(kube, check):
    """큐 정하기: 1·2장 Light, 3·4장 Heavy, 5장 거부. 3장 잡은 관리 cq-heavy로 입장"""
    for gpus, q in ((1, "queue-light"), (2, "queue-light"), (3, "queue-heavy"), (4, "queue-heavy")):
        check.eq(f"{gpus}장 -> {q}", queue_for(gpus), q)
    try:
        queue_for(5)
        check.true("5장 -> 거부", False, "거부 안 됨")
    except Rejected:
        check.true("5장 -> 거부", True)

    submit_auto(kube, "route-3", 3, "5m")
    C.wait_until(lambda: (w := C.wl(kube, "route-3")) is not None and w.quota, 60)
    w = C.wl(kube, "route-3")
    check.eq("3장 잡이 cq-heavy로 입장", w.cq if w else None, "cq-heavy")
