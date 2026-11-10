"""케이스 시작 전 초기화: 끊어 둔 워커 다시 연결, 잡·요청·복구 기록 삭제, 노드·펜싱·자리 표시 Pod 원상복구.
다 지워질 때까지 기다린다.
"""
import logging

from kubernetes import client

from . import network
from .cluster import wait_until
from .env import GPU_SELECTOR, NS, REQUEST_TIMEOUT, STORE_NS, WORKERS, K
from .verdict import Invalid

log = logging.getLogger("e2e")
T = REQUEST_TIMEOUT


def _ignore_404(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except client.ApiException as e:
        if e.status != 404:
            raise


def _all_active(kube):
    mkcs = kube.co.list_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters",
                                              _request_timeout=T)["items"]
    active = {m["metadata"]["name"] for m in mkcs
              if any(c["type"] == "Active" and c["status"] == "True" for c in (m.get("status") or {}).get("conditions") or [])}
    return set(WORKERS) <= active


def reset(kube, timeout=180):
    co, core = kube.co, kube.core
    # 장애복구 케이스가 끊어 둔 워커부터 붙인다 (아래 워커 API 호출이 걸리지 않게)
    try:
        was_down = network.reconnect_all(kube)
    except Exception as e:  # noqa: BLE001
        raise Invalid(f"워커 네트워크 원상복구 실패: {e}") from e
    if was_down and wait_until(lambda: _all_active(kube), 180, interval=5) is None:
        raise Invalid("다시 연결한 뒤 180s 안에 MultiKueueCluster가 전부 Active로 안 돌아옴")
    _ignore_404(co.delete_collection_namespaced_custom_object, K.RR_GROUP, K.RR_VERSION, NS, K.RR_PLURAL,
                _request_timeout=T)
    co.delete_collection_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, NS, "jobsets",
                                                  propagation_policy="Background", _request_timeout=T)
    for cm in core.list_namespaced_config_map(STORE_NS, _request_timeout=T).items:
        if cm.metadata.name.startswith(("recovery-code-", "recovery-fencing-")):
            _ignore_404(core.delete_namespaced_config_map, cm.metadata.name, STORE_NS, _request_timeout=T)
    for w in WORKERS:
        _ignore_404(co.patch_cluster_custom_object, K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", w,
                    {"metadata": {"labels": {K.LABEL_FENCED: None}}}, _request_timeout=T)
        wcore = kube.wcore(w)
        for n in wcore.list_node(label_selector=GPU_SELECTOR, _request_timeout=T).items:
            taints = [t for t in (n.spec.taints or []) if t.key != K.TAINT_FAULTY]
            if n.spec.unschedulable or len(taints) != len(n.spec.taints or []):
                wcore.patch_node(n.metadata.name, {"spec": {"unschedulable": False,
                                                            "taints": [t.to_dict() for t in taints]}},
                                 _request_timeout=T)
        _ignore_404(client.AppsV1Api(kube.workers[w]).delete_namespaced_deployment, "spare-placeholder", NS,
                    _request_timeout=T)

    def clean():
        if kube.co.list_namespaced_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, NS, "workloads",
                                                 _request_timeout=T)["items"]:
            return False
        return all(not kube.wcore(w).list_namespaced_pod(NS, _request_timeout=T).items for w in WORKERS)

    if wait_until(clean, timeout) is None:
        raise Invalid(f"초기화가 {timeout}s 안에 안 끝남 (Workload·워커 Pod가 남아 있음)")
