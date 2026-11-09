import time

import pytest

from harness import cluster as C
from harness.jobs import submit_auto
from harness.situations import fill_light

pytestmark = [pytest.mark.normal, pytest.mark.doc("배분(1)")]


def test_heavy_strict_fifo(kube, check):
    """Heavy StrictFIFO: 3장만 남은 상태에서 4장 잡 h4 뒤의 3장 잡 h3은 들어갈 수 있어도 대기"""
    fill_light(kube, 9)
    submit_auto(kube, "h4", 4, "20m")
    time.sleep(3)
    submit_auto(kube, "h3", 3, "20m")
    time.sleep(30)
    h4, h3 = C.wl(kube, "h4"), C.wl(kube, "h3")
    check.true("h4 Workload 있음", h4 is not None)
    check.true("h3 Workload 있음", h3 is not None)
    check.eq("h4 대기 (3장밖에 안 남음)", h4.quota if h4 else None, False)
    check.eq("h3도 대기 (h4가 앞에 있음)", h3.quota if h3 else None, False)
