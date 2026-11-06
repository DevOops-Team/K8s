"""관리 클러스터 / 워커 클러스터 접속과 자주 쓰는 조회.

접속 방식 두 가지:
  - 클러스터 안 (Deployment): 관리 클러스터는 in-cluster, 워커는 MultiKueue가 쓰는 kubeconfig Secret을 그대로 읽음
  - 노트북에서 직접 실행: KUBE_CONTEXT=kind-manager, WORKER_CONTEXTS=worker1=kind-worker1,worker2=kind-worker2,...
"""
import base64
import logging
import os
import time
from datetime import datetime, timezone

import yaml
from kubernetes import client, config

import keys as K

log = logging.getLogger("kube")
TIMEOUT = 5  # 끊긴 워커 때문에 루프가 멈추지 않게


def setup_logging(name):
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format=f"%(asctime)s {name} %(levelname)s %(message)s", datefmt="%H:%M:%S")


def manager_client():
    ctx = os.getenv("KUBE_CONTEXT")
    if ctx:
        return config.new_client_from_config(context=ctx)
    config.load_incluster_config()
    return client.ApiClient()


class Workers:
    """워커 이름 -> ApiClient. 60초마다 다시 만든다 (Secret 교체 반영).

    own_secret: 컨트롤러 전용 kubeconfig Secret 이름 형식 (예: "{name}-relocator-kubeconfig").
                있으면 그걸 쓰고, 없으면(kind) MultiKueue Secret으로 대신한다."""

    def __init__(self, mgr, own_secret=None):
        self.mgr = mgr
        self.own_secret = own_secret
        self._clients, self._at = {}, 0

    def get(self):
        if time.time() - self._at > 60:
            self._clients, self._at = self._load(), time.time()
        return self._clients

    def _load(self):
        out = {}
        ctxs = os.getenv("WORKER_CONTEXTS")
        if ctxs:
            for pair in ctxs.split(","):
                name, ctx = pair.split("=")
                out[name] = config.new_client_from_config(context=ctx)
            return out
        co = client.CustomObjectsApi(self.mgr)
        core = client.CoreV1Api(self.mgr)
        for mkc in co.list_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters")["items"]:
            name = mkc["metadata"]["name"]
            secret = mkc["spec"]["clusterSource"]["kubeConfig"]["location"]
            try:
                data = None
                if self.own_secret:
                    try:
                        data = core.read_namespaced_secret(self.own_secret.format(name=name), K.KUEUE_NS).data["kubeconfig"]
                    except client.ApiException as e:
                        if e.status not in (403, 404):
                            raise
                if data is None:
                    data = core.read_namespaced_secret(secret, K.KUEUE_NS).data["kubeconfig"]
                out[name] = config.new_client_from_config_dict(yaml.safe_load(base64.b64decode(data)))
            except Exception as e:  # noqa: BLE001
                log.warning("워커 %s kubeconfig 못 읽음: %s", name, e)
        return out


# ---------- 공통 조회

def kueue(api):
    return client.CustomObjectsApi(api)


def cond(obj, ctype):
    for c in (obj.get("status") or {}).get("conditions") or []:
        if c["type"] == ctype:
            return c
    return None


def is_true(obj, ctype):
    c = cond(obj, ctype)
    return bool(c and c["status"] == "True")


def since(ts: str) -> float:
    """RFC3339 시각부터 지난 초"""
    t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return time.time() - t.timestamp()


def now_rfc3339():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def pod_gpus(podspec) -> int:
    total = 0
    for ctr in podspec.get("containers", []):
        res = ctr.get("resources") or {}
        q = (res.get("requests") or {}).get(K.GPU) or (res.get("limits") or {}).get(K.GPU) or 0
        total += int(q)
    return total


def workload_pods(wl) -> list:
    """Workload -> Pod별 GPU 목록. 예: 4 Pod x 1 GPU = [1, 1, 1, 1]"""
    out = []
    for ps in wl["spec"]["podSets"]:
        out += [pod_gpus(ps["template"]["spec"])] * int(ps["count"])
    return out


def workload_owner(wl):
    for o in wl["metadata"].get("ownerReferences") or []:
        if o["kind"] in ("JobSet", "Job"):
            return o["kind"], o["name"]
    return None, None


def owner_annotations(api, wl) -> dict:
    kind, name = workload_owner(wl)
    ns = wl["metadata"]["namespace"]
    try:
        if kind == "JobSet":
            obj = kueue(api).get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name)
            return obj["metadata"].get("annotations") or {}
        if kind == "Job":
            return client.BatchV1Api(api).read_namespaced_job(name, ns).metadata.annotations or {}
    except client.ApiException:
        pass
    return {}


def list_workloads(api, ns=None):
    co = kueue(api)
    if ns:
        return co.list_namespaced_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, ns, "workloads")["items"]
    return co.list_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "workloads")["items"]


def workload_for_jobset(api, ns, name):
    for wl in list_workloads(api, ns):
        if workload_owner(wl) == ("JobSet", name):
            return wl
    return None
