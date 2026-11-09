import time

import pytest

from harness import cluster as C
from harness.jobs import submit_auto
from harness.verdict import precondition

pytestmark = [pytest.mark.normal, pytest.mark.doc("배치(1)")]


def test_binpack(kube, check):
    """빈 패킹: 빈 워커 3개에 1장 잡 6개 -> worker1 4, worker2 2, worker3 0 (한 워커부터 채움)"""
    precondition(sum(C.running_pods(kube).values()) == 0, f"워커가 비어 있지 않음 {C.running_pods(kube)}")
    for i in range(1, 7):
        submit_auto(kube, f"bin-{i}", 1, "20m")
        time.sleep(3)
    C.wait_until(lambda: C.count(kube, "bin-", "placed") == 6, 90)
    check.eq("워커별 개수", C.by_cluster(kube, "bin-"), {"worker1": 4, "worker2": 2})
