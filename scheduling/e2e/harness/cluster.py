"""조회: 관리 클러스터 Workload·ClusterQueue, 워커 Pod. 케이스는 작업ID(job-<ID>-g0001의 ID)로 부른다."""
import re
import time
from dataclasses import dataclass, field

from kubernetes import client

from .env import NS, REQUEST_TIMEOUT, WORKERS, K

JOB_RE = re.compile(r"^job-(.+)-g(\d{4})$")


def jsname(job_id, gen=1):
    return job_id if job_id.startswith("job-") else f"job-{job_id}-g{gen:04d}"


@dataclass
class WL:
    id: str                 # 작업ID
    name: str               # Workload 이름
    quota: bool             # QuotaReserved
    cluster: str | None     # 들어간 워커 (MultiKueue)
    cq: str | None          # 쿼터 받은 관리 ClusterQueue
    nominated: list = field(default_factory=list)
    priority: int = 0


def _ok(obj, cond_type):
    return any(c.get("type") == cond_type and c.get("status") == "True"
               for c in (obj.get("status") or {}).get("conditions") or [])


def workloads(kube):
    items = kube.co.list_namespaced_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, NS, "workloads",
                                                  _request_timeout=REQUEST_TIMEOUT)["items"]
    out = []
    for w in items:
        owner = next(iter(w["metadata"].get("ownerReferences") or []), {}).get("name", "")
        m = JOB_RE.match(owner)
        st = w.get("status") or {}
        out.append(WL(id=m.group(1) if m else owner, name=w["metadata"]["name"], quota=_ok(w, "QuotaReserved"),
                      cluster=st.get("clusterName") or None,
                      cq=(st.get("admission") or {}).get("clusterQueue"),
                      nominated=st.get("nominatedClusterNames") or [],
                      priority=(w.get("spec") or {}).get("priority") or 0))
    return out


def wl(kube, job_id):
    return next((w for w in workloads(kube) if w.id == job_id), None)


def with_prefix(kube, prefix):
    return [w for w in workloads(kube) if w.id.startswith(prefix)]


def count(kube, prefix, what):
    """prefix로 시작하는 작업 중 쿼터 받은(quota) / 워커에 들어간(placed) 개수."""
    ws = with_prefix(kube, prefix)
    return sum(w.quota for w in ws) if what == "quota" else sum(bool(w.cluster) for w in ws)


def by_cluster(kube, prefix):
    out = {}
    for w in with_prefix(kube, prefix):
        if w.cluster:
            out[w.cluster] = out.get(w.cluster, 0) + 1
    return out


def clusterqueue(kube, name):
    return kube.co.get_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", name,
                                             _request_timeout=REQUEST_TIMEOUT)


def cq_gpu(kube, name):
    """ClusterQueue의 GPU 자원 설정 (nominalQuota·borrowingLimit·lendingLimit)."""
    return clusterqueue(kube, name)["spec"]["resourceGroups"][0]["flavors"][0]["resources"][0]


def cq_used(kube, name):
    st = clusterqueue(kube, name).get("status") or {}
    for f in st.get("flavorsReservation") or []:
        for r in f.get("resources") or []:
            if r.get("name") == K.GPU:
                return int(r.get("total", "0"))
    return 0


def priority_class(kube, job_id):
    try:
        js = kube.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, NS, "jobsets", jsname(job_id),
                                                  _request_timeout=REQUEST_TIMEOUT)
    except client.ApiException as e:
        if e.status == 404:
            return None
        raise
    return js["metadata"].get("labels", {}).get(K.LABEL_PRIORITY)


def running_pods(kube):
    """워커별 Running 학습 Pod 수 (= 쓰는 GPU, Pod 하나에 GPU 1장)."""
    out = {}
    for w in WORKERS:
        pods = kube.wcore(w).list_namespaced_pod(NS, field_selector="status.phase=Running",
                                                 _request_timeout=REQUEST_TIMEOUT).items
        out[w] = len(pods)
    return out


def pods_by_job(kube, worker):
    """워커 하나에서 작업ID별 Running Pod 수."""
    out = {}
    for p in kube.wcore(worker).list_namespaced_pod(NS, field_selector="status.phase=Running",
                                                    _request_timeout=REQUEST_TIMEOUT).items:
        jid = (p.metadata.labels or {}).get(K.LABEL_JOB_ID, "?")
        out[jid] = out.get(jid, 0) + 1
    return out


def wait_until(pred, timeout, interval=3):
    """pred()가 참이 될 때까지. 걸린 초를 돌려주고, 시간 안에 안 되면 None."""
    t0 = time.monotonic()
    while True:
        if pred():
            return time.monotonic() - t0
        if time.monotonic() - t0 >= timeout:
            return None
        time.sleep(interval)
