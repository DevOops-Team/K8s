"""레플리카 3개 중 하나만 일하게 하는 리더 선출 (coordination.k8s.io Lease).

    elector = leader.Elector(kube.manager_client(), "dispatcher")   # 갱신 스레드가 바로 시작
    while True:
        if elector.leading:     # 리더일 때만 일함
            reconcile(...)
        time.sleep(INTERVAL)

- 갱신은 따로 도는 스레드가 RENEW_SECONDS마다 한다. 워커가 끊겨 reconcile 한 바퀴가 길어져도 리더를 안 잃음.
- 갱신은 resourceVersion을 실은 replace라서 두 개가 동시에 잡으려 하면 하나는 409로 진다.
- 남의 Lease가 만료됐는지는 상대 시계(renewTime)가 아니라 "내가 본 값이 마지막으로 바뀐 뒤 지난 시간"으로 판단 (노드 시계 차이 무시).
- 리더가 갱신에 실패하거나 남에게 넘어간 걸 보면 프로세스를 바로 끝낸다(os._exit).
  다른 레플리카가 이미 이어받았을 수 있어서, 하던 reconcile이 한 번 더 바꾸는 일을 막으려는 것. 재시작은 Deployment가 함.
- SIGTERM·SIGINT를 받으면 리더였을 때 Lease를 비우고(holderIdentity 없음) 바로 끝낸다.
  핸들러는 메인 스레드에서 돌아 하던 reconcile은 다시 이어지지 않음. 남은 레플리카가 다음 갱신(RENEW_SECONDS) 때 바로 가져감.
- LEADER_ELECTION=0 이면 끔 (혼자 돌릴 때). run-local.sh는 켠 채로 돌려도 됨: 클러스터 안 레플리카와 같은 Lease를 다툼.
"""
import logging
import os
import signal
import socket
import threading
import time
from datetime import datetime, timezone

from kubernetes import client

log = logging.getLogger("leader")

LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "15"))
RENEW_SECONDS = int(os.getenv("RENEW_SECONDS", "5"))
NAMESPACE = os.getenv("CONTROLLER_NAMESPACE", "job-placement")


def _now():
    return datetime.now(timezone.utc)


class Elector:
    def __init__(self, api, name, identity=None, clock=time.monotonic, start=True):
        self.enabled = os.getenv("LEADER_ELECTION", "1") != "0"
        self.coord = client.CoordinationV1Api(api)
        self.lease_name = f"job-placement-{name}"
        self.identity = identity or os.getenv("POD_NAME") or f"{socket.gethostname()}-{os.getpid()}"
        self.clock = clock
        self.leading = not self.enabled
        self.renewed_at = None              # 내가 마지막으로 갱신에 성공한 시각
        self._seen, self._seen_at = None, None
        if self.enabled and start:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._on_signal)
            threading.Thread(target=self._loop, daemon=True).start()

    def _on_signal(self, signum, _frame):
        if self.leading:
            self.release()
        os._exit(0)

    def release(self):
        """내가 잡고 있으면 Lease를 비운다. 실패해도 LEASE_SECONDS 뒤엔 남이 가져감."""
        try:
            lease = self.coord.read_namespaced_lease(self.lease_name, NAMESPACE)
            if lease.spec and lease.spec.holder_identity == self.identity:
                lease.spec.holder_identity = None
                lease.spec.renew_time = _now()
                self.coord.replace_namespaced_lease(self.lease_name, NAMESPACE, lease)
                log.info("Lease %s 내려놓음", self.lease_name)
        except Exception as e:  # noqa: BLE001
            log.warning("Lease %s 내려놓기 실패: %s", self.lease_name, e)
        self.leading = False

    def _loop(self):
        log.info("리더 선출 시작: Lease %s/%s, 나=%s", NAMESPACE, self.lease_name, self.identity)
        while True:
            self.step()
            time.sleep(RENEW_SECONDS)

    def step(self):
        """한 번 잡기·갱신 시도. 리더였는데 실패하면 종료."""
        try:
            ok = self._try()
        except Exception as e:  # noqa: BLE001  API 끊김
            log.warning("Lease %s 조회·갱신 실패: %s", self.lease_name, e)
            ok = False
            if self.leading and self.clock() - self.renewed_at < LEASE_SECONDS:
                return                          # 아직 내 Lease 기간 안: 다음 갱신에서 다시
        if self.leading and not ok:
            log.error("리더 잃음 (%s) -> 종료", self.identity)
            os._exit(1)
        if ok and not self.leading:
            log.info("리더 됨 (%s)", self.identity)
        self.leading = ok

    def _try(self) -> bool:
        try:
            lease = self.coord.read_namespaced_lease(self.lease_name, NAMESPACE)
        except client.ApiException as e:
            if e.status != 404:
                raise
            return self._create()

        spec = lease.spec or client.V1LeaseSpec()
        holder = spec.holder_identity
        seen = (holder, spec.renew_time, spec.lease_transitions)
        now = self.clock()
        if seen != self._seen:
            self._seen, self._seen_at = seen, now

        if self.leading and holder != self.identity:
            return False                              # 내가 리더인 줄 알았는데 남이 가져감
        if holder and holder != self.identity:
            duration = spec.lease_duration_seconds or LEASE_SECONDS
            if now - self._seen_at < duration:
                return False                          # 남이 살아 있음
            log.info("Lease %s: %s 가 %ss 넘게 갱신 안 함 -> 가져옴", self.lease_name, holder, duration)

        t = _now()
        if holder != self.identity:
            spec.acquire_time = t
            spec.lease_transitions = (spec.lease_transitions or 0) + 1
        spec.holder_identity = self.identity
        spec.lease_duration_seconds = LEASE_SECONDS
        spec.renew_time = t
        lease.spec = spec
        try:
            self.coord.replace_namespaced_lease(self.lease_name, NAMESPACE, lease)
        except client.ApiException as e:
            if e.status == 409:                       # 그 사이 남이 먼저 바꿈
                return False
            raise
        self.renewed_at = self.clock()
        return True

    def _create(self) -> bool:
        t = _now()
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(name=self.lease_name, namespace=NAMESPACE),
            spec=client.V1LeaseSpec(holder_identity=self.identity, lease_duration_seconds=LEASE_SECONDS,
                                    acquire_time=t, renew_time=t, lease_transitions=0))
        try:
            self.coord.create_namespaced_lease(NAMESPACE, body)
        except client.ApiException as e:
            if e.status == 409:                       # 동시에 만든 쪽이 이김
                return False
            raise
        self.renewed_at = self.clock()
        return True
