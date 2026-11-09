import time

import pytest

from harness import cluster as C
from harness.env import TRAINING_GPUS
from harness.jobs import submit_auto

pytestmark = [pytest.mark.normal, pytest.mark.doc("배분(1)·(2)")]


def test_light_borrows_all_when_heavy_idle(kube, check):
    """Heavy 대기가 없으면 Light가 남는 몫 전부 사용: 1장 잡 13개 -> 12개 입장, 1개 대기"""
    for i in range(1, TRAINING_GPUS + 2):
        submit_auto(kube, f"fill-{i}", 1, "20m")
    C.wait_until(lambda: C.count(kube, "fill-", "quota") >= TRAINING_GPUS, 90)
    time.sleep(15)   # 13번째가 잘못 들어가는지 조금 더 봄
    check.eq("쿼터 받은 Light 잡", C.count(kube, "fill-", "quota"), TRAINING_GPUS)
    check.eq("cq-light 사용량", C.cq_used(kube, "cq-light"), TRAINING_GPUS)
