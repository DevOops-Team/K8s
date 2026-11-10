"""케이스가 도는 동안 인프라를 감시한다. 문제가 생기면 기록하고, 그 구간에 걸친 케이스는 INVALID가 된다.

보는 것 (관리 + 워커 클러스터 전부):
  - 띄운 컨트롤러가 죽었는지
  - 각 클러스터 API 서버 /readyz
  - 각 클러스터 컨트롤 플레인(apiserver·etcd·scheduler·controller-manager) 컨테이너가 재시작했는지
    (워커 scheduler가 재시작하면 지명은 맞아도 Pod가 제때 안 떠서 FAIL처럼 보인다)
  - 관리 클러스터 Kueue 웹훅 엔드포인트가 하나라도 Ready인지
케이스가 일부러 끊은 워커(network.DOWN)는 보지 않는다.
"""
import logging
import threading
from datetime import datetime

from kubernetes import client

from . import network
from .env import K

log = logging.getLogger("e2e")
CONTROL_PLANE = ("kube-apiserver", "etcd", "kube-scheduler", "kube-controller-manager")


class Watchdog(threading.Thread):
    def __init__(self, kube, ctl, interval=5):
        super().__init__(daemon=True, name="watchdog")
        self.kube, self.ctl, self.interval = kube, ctl, interval
        self.incidents = []      # (시각, 내용)
        self._halt = threading.Event()
        self._last = {}
        self._restarts = {}      # (클러스터, 구성요소) -> 마지막으로 본 재시작 횟수
        self._was_down = set()
        self.clusters = {"관리": kube.mgr, **kube.workers}

    def run(self):
        while not self._halt.wait(self.interval):
            for what, ok in self._probe():
                if not ok and self._last.get(what, True):
                    self.incidents.append((datetime.now().strftime("%H:%M:%S"), what))
                    log.warning("  [감시] %s", what)
                self._last[what] = ok

    def _probe(self):
        dead = self.ctl.dead()
        yield f"컨트롤러 종료: {','.join(dead)}", not dead
        for name, api in self.clusters.items():
            if name in network.DOWN:   # 케이스가 일부러 끊은 워커
                self._was_down.add(name)
                continue
            if name in self._was_down:   # 다시 붙은 워커: 끊긴 동안의 재시작은 세지 않게 기준을 새로 잡는다
                self._was_down.discard(name)
                self._restarts = {k: v for k, v in self._restarts.items() if k[0] != name}
            try:
                api.call_api("/readyz", "GET", auth_settings=["BearerToken"], response_type="str",
                             _return_http_data_only=True, _request_timeout=5)
                yield f"{name} API 서버 응답 없음", True
            except Exception:  # noqa: BLE001
                yield f"{name} API 서버 응답 없음", False
                continue
            yield from self._restarted(name, api)
        try:
            slices = client.DiscoveryV1Api(self.kube.mgr).list_namespaced_endpoint_slice(
                K.KUEUE_NS, label_selector="kubernetes.io/service-name=kueue-webhook-service", _request_timeout=5).items
            ready = sum(1 for s in slices for e in (s.endpoints or []) if e.conditions and e.conditions.ready)
            yield "Kueue 웹훅 Ready 엔드포인트 0개", ready > 0
        except Exception:  # noqa: BLE001
            yield "Kueue 웹훅 엔드포인트 조회 실패", False

    def _restarted(self, name, api):
        """재시작 횟수가 늘었으면 그 주기 한 번만 문제로 보고 (다음 주기엔 정상으로 돌아옴)."""
        try:
            pods = client.CoreV1Api(api).list_namespaced_pod("kube-system", _request_timeout=5).items
        except Exception:  # noqa: BLE001
            return
        for p in pods:
            comp = next((c for c in CONTROL_PLANE if p.metadata.name.startswith(c + "-")), None)
            if not comp:
                continue
            n = sum(s.restart_count for s in p.status.container_statuses or [])
            key = (name, comp)
            prev = self._restarts.get(key)
            self._restarts[key] = n
            yield f"{name} {comp} 재시작 ({n}회째)", prev is None or n <= prev

    def mark(self):
        """케이스 시작. 이전부터 계속된 문제도 이 케이스에 다시 기록되게 상태를 비운다."""
        self._last = {}
        return len(self.incidents)

    def since(self, mark):
        return self.incidents[mark:]

    def stop(self):
        self._halt.set()
