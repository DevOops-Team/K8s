"""케이스가 공통으로 쓰는 상황 만들기. 만든 뒤 상황이 정말 만들어졌는지는 각 함수가 확인한다 (아니면 INCONCLUSIVE)."""
import logging
import threading
import time

from . import cluster as C
from .env import TRAINING_GPUS, TRAINING_GPUS_PER_WORKER, WORKERS, K
from .jobs import submit, submit_auto
from .verdict import precondition

log = logging.getLogger("e2e")


def fill_light(kube, n=TRAINING_GPUS, duration=lambda i: "20m", prefix="fill", timeout=90):
    """Light 1장 잡 n개. duration(i)로 끝나는 시각을 정한다 (i는 1부터)."""
    for i in range(1, n + 1):
        submit_auto(kube, f"{prefix}-{i}", 1, duration(i))
    waited = C.wait_until(lambda: C.count(kube, f"{prefix}-", "quota") >= n, timeout)
    precondition(waited is not None, f"{prefix} {n}개가 {timeout}s 안에 쿼터를 다 못 받음 "
                                     f"({C.count(kube, f'{prefix}-', 'quota')}개)")


def fill_fragmented(kube, per_worker=3, base=120, step=10, prefix="frag", timeout=150):
    """워커마다 1장 잡 per_worker개를 target으로 고정 -> 워커마다 (4 - per_worker)장만 빔. 끝나는 순서는 워커를 돌아가며."""
    n = per_worker * len(WORKERS)
    for i in range(1, n + 1):
        w = WORKERS[(i - 1) % len(WORKERS)]
        submit(kube, f"{prefix}-{i}", 1, "queue-light", base + step * i, {K.ANN_TARGET: w})
    waited = C.wait_until(lambda: C.count(kube, f"{prefix}-", "placed") >= n, timeout)
    precondition(waited is not None, f"{prefix} {n}개가 {timeout}s 안에 다 입장 못 함 ({C.by_cluster(kube, prefix + '-')})")


def max_free_on_one_worker(kube):
    """한 워커에 남은 학습 GPU 중 가장 많은 값 (Running Pod 기준)."""
    return max(TRAINING_GPUS_PER_WORKER - n for n in C.running_pods(kube).values())


def idle_gpus(kube):
    return max(0, TRAINING_GPUS - sum(C.running_pods(kube).values()))


class Stream(threading.Thread):
    """같은 크기 잡을 일정 간격으로 계속 넣는다 (잡 투입기 규칙으로 큐 결정). 예: Light 1장 90s짜리를 10초마다."""

    def __init__(self, kube, prefix, gpus, every, duration, count=60):
        super().__init__(daemon=True, name=f"stream-{prefix}")
        self.kube, self.prefix, self.gpus, self.every, self.duration, self.count = kube, prefix, gpus, every, duration, count
        self.submitted = []
        self.error = None
        self._halt = threading.Event()

    def run(self):
        for i in range(1, self.count + 1):
            if self._halt.is_set():
                return
            try:
                submit_auto(self.kube, f"{self.prefix}-{i}", self.gpus, self.duration)
                self.submitted.append(f"{self.prefix}-{i}")
            except Exception as e:  # noqa: BLE001  (INVALID 판정은 케이스가 error를 보고 함)
                self.error = e
                return
            if self._halt.wait(self.every):
                return

    def stop(self):
        self._halt.set()
        self.join(timeout=30)
        return self

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()


def wait(seconds, why=""):
    if why:
        log.info("  %ds 기다림: %s", seconds, why)
    time.sleep(seconds)
