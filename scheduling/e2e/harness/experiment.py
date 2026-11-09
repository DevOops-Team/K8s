"""비교 실험의 팔(arm) 하나: 초기화 -> 컨트롤러 설정 -> 상황 만들기 -> 큰 잡 제출 -> 입장까지 측정.

같은 상황 함수로 두 팔을 돌리고 한 가지(컨트롤러 설정이나 큐)만 바꿔서 비교한다.
"""
import logging

from .controllers import ALL
from .measure import track
from .reset import reset
from .verdict import Invalid

log = logging.getLogger("e2e")


def run_arm(kube, ctl, label, *, prepare, submit, job, max_wait, controllers=ALL, env=None, on_sample=None):
    """prepare()는 돌리기 시작한 Stream 목록을 돌려준다 (끝나면 멈춤). 반환: Track."""
    log.info("=== %s", label)
    reset(kube)
    ctl.use(controllers, **(env or {}))
    streams = prepare() or []
    try:
        submit()
        tr = track(kube, job, max_wait, on_sample=on_sample)
    finally:
        for s in streams:
            s.stop()
    bad = [s for s in streams if s.error]
    if bad:
        raise Invalid(f"{label}: 잡 스트림 제출 실패 {bad[0].prefix}: {bad[0].error}")
    log.info("  결과 %s: %s", label, tr.summary())
    return tr


def fmt(tr):
    return tr.summary()
