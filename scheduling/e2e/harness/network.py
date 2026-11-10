"""워커 클러스터 네트워크 끊기·다시 붙이기 (docker network). 장애복구 케이스 06·07만 쓴다.

kind 클러스터 컨트롤 플레인 컨테이너를 kind 네트워크에서 떼면 관리 클러스터(MultiKueue)와 이 PC(컨트롤러·하네스)
양쪽에서 그 워커 API가 안 보인다. 다시 붙일 때는 원래 IP로 붙인다 (IP가 바뀌면 kind 안 apiserver·etcd 주소가 어긋나 깨짐).
원래 IP = API 서버 advertise 주소. 그걸 못 읽을 때만 끊을 때 저장한 IP를 쓴다.

DOWN에 든 워커는 일부러 끊은 것이라 감시(health.py)가 인프라 문제로 세지 않는다.
"""
import logging
import subprocess
import time

from .env import WORKERS

log = logging.getLogger("e2e")
NET = "kind"
DOWN = set()      # 일부러 끊은 워커
_saved_ip = {}


def _docker(*args, check=True):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=60)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} 실패: {r.stderr.strip()}")
    return r.stdout.strip()


def _container(worker):
    return f"{worker}-control-plane"


def _ip_now(worker):
    return _docker("inspect", "-f", "{{with index .NetworkSettings.Networks \"%s\"}}{{.IPAddress}}{{end}}" % NET,
                   _container(worker), check=False)


def _orig_ip(worker):
    out = _docker("exec", _container(worker), "sh", "-c",
                  "grep -o -- '--advertise-address=[^ ]*' /etc/kubernetes/manifests/kube-apiserver.yaml", check=False)
    return out.partition("=")[2].strip() or _saved_ip.get(worker, "")


def connected(worker):
    return bool(_ip_now(worker))


def cut_off(worker):
    DOWN.add(worker)   # 떼기 전에 넣어야 감시가 끊긴 순간을 문제로 안 셈
    _saved_ip[worker] = _ip_now(worker)
    _docker("network", "disconnect", NET, _container(worker))
    log.info("  %s 네트워크 끊음 (IP %s)", worker, _saved_ip[worker])


def api_ready(kube, worker):
    try:
        kube.workers[worker].call_api("/readyz", "GET", auth_settings=["BearerToken"], response_type="str",
                                      _return_http_data_only=True, _request_timeout=5)
        return True
    except Exception:  # noqa: BLE001
        return False


def reconnect(worker, kube=None, timeout=120):
    """다시 붙이고, kube를 주면 이 PC에서 그 워커 API가 다시 보일 때까지 기다린다 (그 전엔 감시 대상에 안 넣음)."""
    if not connected(worker):
        ip = _orig_ip(worker)
        _docker("network", "connect", *(["--ip", ip] if ip else []), NET, _container(worker))
        log.info("  %s 다시 연결 (IP %s)", worker, _ip_now(worker))
    ip, now = _orig_ip(worker), _ip_now(worker)
    if ip and now != ip:
        raise RuntimeError(f"{worker} IP가 {now} (원래 {ip}). 워커를 전부 끊고 다시 붙일 것")
    if kube is not None:
        end = time.monotonic() + timeout
        while not api_ready(kube, worker):
            if time.monotonic() > end:
                raise RuntimeError(f"{worker} 다시 연결 뒤 {timeout}s 동안 API 응답 없음")
            time.sleep(3)
    DOWN.discard(worker)


def reconnect_all(kube=None):
    """끊겨 있던 워커가 있었으면 True (초기화가 MultiKueue Active를 기다리게)."""
    was = [w for w in WORKERS if not connected(w)] or sorted(DOWN)
    for w in WORKERS:
        reconnect(w, kube)
    return bool(was)
