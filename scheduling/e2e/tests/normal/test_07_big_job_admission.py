import pytest

from harness import cluster as C
from harness.env import TRAINING_GPUS
from harness.jobs import submit_auto
from harness.measure import track
from harness.situations import Stream, fill_light, wait
from harness.verdict import Invalid, precondition

# 큐 설정(KEP-83)만으로 통과해야 하는 회귀 검사. 컨트롤러는 워커 지명(dispatcher)만 함
pytestmark = [pytest.mark.normal, pytest.mark.doc("배분(2)·배치(1) · KEP-83")]
MAX_WAIT = 480


def test_big_job_admitted_while_small_jobs_keep_coming(kube, check, info):
    """작은 잡이 계속 와도 큰 잡 입장: big이 기다리는 동안 Light가 몫 4를 넘어 새로 늘지 않고, big이 쿼터·워커를 받음"""
    # 한꺼번에 끝나면 12장이 동시에 비어 무조건 big이 들어감 -> 10초 간격으로 하나씩 끝나게 (130s~240s)
    fill_light(kube, duration=lambda i: 120 + 10 * i)
    grew = []
    with Stream(kube, "feed", 1, every=10, duration="90s") as feed:
        wait(20, "작은 잡 스트림이 돌기 시작")
        precondition(C.cq_used(kube, "cq-light") == TRAINING_GPUS,
                     f"big 제출 시점에 Light가 꽉 차 있지 않음 ({C.cq_used(kube, 'cq-light')})")
        submit_auto(kube, "big", 4, "20m")
        prev = {"light": C.cq_used(kube, "cq-light")}

        def watch_light(t, w, tr):
            used = C.cq_used(kube, "cq-light")
            if tr.quota_at is None and used > prev["light"] and used > 4:
                grew.append(f"+{t:.0f}s {prev['light']}->{used}")
            prev["light"] = used

        tr = track(kube, "big", MAX_WAIT, on_sample=watch_light)
    if feed.error:
        raise Invalid(f"작은 잡 스트림 제출 실패: {feed.error}")
    info["big"] = tr.summary()
    check.true("big 대기 중 Light가 몫 4를 넘어 늘어난 적 없음", not grew, ", ".join(grew))
    check.true(f"big이 {MAX_WAIT}s 안에 쿼터 받음", tr.quota_at is not None)
    check.true(f"big이 {MAX_WAIT}s 안에 워커 입장", tr.placed_at is not None)
