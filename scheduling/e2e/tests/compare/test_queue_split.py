"""Heavy/Light 큐 분리(A)와 큐 1개(B, 큰 잡도 queue-light) 비교.

B는 큐 설정을 바꾸지 않는다. 큰 잡이 Light로 가면 Heavy 몫을 빌려야 하므로 KEP-83 막기가 안 걸리고,
Light 혼자 12장을 쓰는 큐 1개(BestEffortFIFO)와 같아진다.
가설: 작은 잡이 계속 오면 B에서는 4장 잡이 작은 잡에 밀려 쿼터를 늦게(또는 못) 받고, A에서는 받는다.
"""
import pytest

from harness import cluster as C
from harness.env import SLACK, TRAINING_GPUS
from harness.experiment import fmt, run_arm
from harness.jobs import submit, submit_auto
from harness.situations import Stream, fill_light, wait
from harness.verdict import precondition

pytestmark = [pytest.mark.compare, pytest.mark.doc("배분(1)·(2)"), pytest.mark.any_controllers]
MAX_WAIT = 600
MARGIN = 60
FEED_SECONDS = 90   # 작은 잡 길이. 찜한 워커에 남은 잡은 늦어도 이 시간 뒤엔 끝난다


def test_queue_split(kube, ctl, check, info):
    """큐 분리: Light가 꽉 찬 상태에서 1장 잡이 계속 오면, 분리했을 때만 4장 잡이 제때 쿼터를 받음"""

    def prepare():
        fill_light(kube, duration=lambda i: 120 + 10 * i)
        s = Stream(kube, "feed", 1, every=10, duration=f"{FEED_SECONDS}s")
        s.start()
        wait(20, "작은 잡 스트림이 돌기 시작")
        precondition(C.cq_used(kube, "cq-light") == TRAINING_GPUS, "big 제출 때 Light가 꽉 차 있지 않음")
        return [s]

    a = run_arm(kube, ctl, "A 분리 (big -> queue-heavy)", prepare=prepare,
                submit=lambda: submit_auto(kube, "big", 4, "20m"), job="big", max_wait=MAX_WAIT)
    b = run_arm(kube, ctl, "B 큐 1개 (big -> queue-light)", prepare=prepare,
                submit=lambda: submit(kube, "big", 4, "queue-light", "20m"), job="big", max_wait=MAX_WAIT)
    info["A 분리"], info["B 큐 1개"] = fmt(a), fmt(b)
    check.true("A에서 big이 쿼터 받음", a.quota_at is not None, a.summary())
    # 쿼터를 받아도 4장이 한 워커에 비어야 들어간다. 디스패처가 워커 하나를 찜해 작은 잡을 다른 곳으로 돌리므로
    # 그 워커에 남은 작은 잡이 끝나면 바로 들어가야 한다 (찜이 없던 11/10에는 142s)
    if a.wait_after_quota is not None:
        info["A 쿼터 뒤 배치"] = f"{a.wait_after_quota:.0f}s"
    check.true(f"A에서 big이 쿼터 뒤 {FEED_SECONDS + SLACK}s 안에 워커 배치",
               a.wait_after_quota is not None and a.wait_after_quota <= FEED_SECONDS + SLACK, a.summary())
    if a.quota_at is not None:
        later = b.quota_at is None or b.quota_at - a.quota_at >= MARGIN
        check.true(f"B가 A보다 {MARGIN}s 이상 늦게 쿼터 (또는 못 받음)", later,
                   f"A {a.quota_at:.0f}s, B {'없음' if b.quota_at is None else f'{b.quota_at:.0f}s'}")
