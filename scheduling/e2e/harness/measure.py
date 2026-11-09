"""큰 잡 하나를 따라가며 쿼터 받은 시각·워커 입장 시각·그동안 논 GPU·초를 잰다. 초는 그 잡 Workload가 생긴 때부터."""
import logging
import time
from dataclasses import dataclass, field

from . import cluster as C
from .situations import idle_gpus

log = logging.getLogger("e2e")


@dataclass
class Track:
    job: str
    quota_at: float | None = None
    placed_at: float | None = None
    cluster: str | None = None
    idle_gpu_s: int = 0
    samples: list = field(default_factory=list)   # 콜백이 남긴 값

    @property
    def wait_after_quota(self):
        if self.quota_at is None or self.placed_at is None:
            return None
        return self.placed_at - self.quota_at

    def summary(self):
        f = lambda v: "없음" if v is None else f"{v:.0f}s"  # noqa: E731
        return f"쿼터 {f(self.quota_at)}, 입장 {f(self.placed_at)} ({self.cluster or '-'}), 논 GPU {self.idle_gpu_s} GPU·s"


def track(kube, job, max_wait, interval=5, on_sample=None):
    """job이 워커에 들어가거나 max_wait이 지날 때까지. on_sample(t, wl, tr)은 매 주기 불림 (판정용 기록)."""
    C.wait_until(lambda: C.wl(kube, job) is not None, 30, interval=1)
    tr = Track(job)
    t0 = last = time.monotonic()
    while True:
        now = time.monotonic()
        t = now - t0
        w = C.wl(kube, job)
        if tr.placed_at is None:
            tr.idle_gpu_s += int(idle_gpus(kube) * (now - last))
        last = now
        if on_sample:
            on_sample(t, w, tr)
        if w and w.quota and tr.quota_at is None:
            tr.quota_at = t
            log.info("  +%.0fs %s 쿼터 받음", t, job)
        if w and w.cluster:
            tr.placed_at, tr.cluster = t, w.cluster
            log.info("  +%.0fs %s -> %s", t, job, w.cluster)
            return tr
        if t >= max_wait:
            log.info("  %s: %ds 안에 입장 못 함 (%s)", job, max_wait, tr.summary())
            return tr
        time.sleep(interval)
