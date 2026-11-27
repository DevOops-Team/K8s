"""장애복구 케이스 공통: 복구 컨트롤러 흉내, 재배치 요청(RR), 워커 쪽 조회, 이중 실행 감시.

정본 4.1(R7) 순서와 ADR-50 계약을 그대로 흉내 낸다.
  복구 컨트롤러 (여기서 흉내)  펜싱 기록 세대 +1 -> 원본을 spec.jobSet에 넣어 training에 RR 생성
  relocator (진짜 컨트롤러)    격리 -> 옛 실행 종료 확인 -> status.recoveryProgress(OLD_RUN_STOPPED)
  복구 컨트롤러 (여기서 흉내)  펜싱 기록에 OLD_RUN_STOPPED -> RR에 devoops/recovery-stop-recorded=true
  relocator                    재제출 -> status.recoveryProgress(NEW_RUN_SUBMITTED + 새 JobSet 이름·UID)
  복구 컨트롤러 (여기서 흉내)  펜싱 기록에 NEW_RUN_SUBMITTED·new_jobset_name/uid
흉내는 RR을 읽을 때마다(rr()) 한 번씩 돈다. relocator는 펜싱 기록·원본 저장소를 보지 않는다.
kind에서 펜싱 기록·원본은 scheduling-local-store의 ConfigMap recovery-fencing-<작업ID>·recovery-code-<작업ID>.
"""
import logging
import json
import subprocess
import threading
import time
from datetime import datetime

from kubernetes import client

from . import cluster as C
from .cluster import jsname
from .env import NS, REQUEST_TIMEOUT, ROOT, STORE_NS, WORKER_CONTEXTS, WORKERS, PROFILE, K
from .verdict import Invalid, precondition

log = logging.getLogger("e2e")
T = REQUEST_TIMEOUT
FINAL = ("Done", "Failed", "Duplicate", "Cancelled")
HOLD_ACK = set()   # 이 이름의 RR에는 흉내 컨트롤러가 OLD_RUN_STOPPED 확인을 붙이지 않음 (ack_stop으로 직접)
PLACEHOLDER = ROOT / "examples" / "spare" / "placeholder.yaml"
JOBSET_LABEL = "jobset.sigs.k8s.io/jobset-name"


# ---------------------------------------------------------------- 펜싱 기록 (DynamoDB 대신 ConfigMap)
def fencing(kube, job_id):
    cm = kube.core.read_namespaced_config_map(f"recovery-fencing-{job_id}", STORE_NS, _request_timeout=T)
    return dict(cm.data or {})


def advance_generation(kube, job_id, recovery_id):
    """복구 컨트롤러 흉내: 세대 +1 (owner 비움, GENERATION_ADVANCED, from/to gen, retry_count +1).
    같은 recovery_id로 다시 부르면 세대를 또 올리지 않는다 (체크포인트 규약 5절). 반환: 옛 세대(RR jobRef에 들어갈 것)"""
    rec = fencing(kube, job_id)
    if rec.get("recovery_id") == recovery_id:
        return int(rec["recovery_from_gen"])
    gen = int(rec.get("generation") or 1)
    kube.core.patch_namespaced_config_map(f"recovery-fencing-{job_id}", STORE_NS, {"data": {
        "generation": str(gen + 1), "owner": "", "recovery_id": recovery_id,
        "recovery_phase": "GENERATION_ADVANCED", "recovery_from_gen": str(gen), "recovery_to_gen": str(gen + 1),
        "retry_count": str(int(rec.get("retry_count") or 0) + 1), "new_jobset_name": "", "new_jobset_uid": ""}},
        _request_timeout=T)
    log.info("  (복구 컨트롤러 흉내) %s 세대 %d -> %d, recovery_id=%s", job_id, gen, gen + 1, recovery_id)
    return gen


def original(kube, job_id):
    """흉내 컨트롤러가 S3 대신 읽는 원본 JobSet (submit이 recovery-code-<id>에 둔 것). 없으면 None"""
    try:
        cm = kube.core.read_namespaced_config_map(f'recovery-code-{job_id}', STORE_NS, _request_timeout=T)
    except client.ApiException as e:
        if e.status == 404:
            return None
        raise
    return json.loads(cm.data['jobset.json'])


def create_rr(kube, name, recovery_id, action, job_id, gen, nodes=None, clusters=None, reason="hardware"):
    spec = {"requestId": recovery_id, "action": action, "jobRef": f"{NS}/{jsname(job_id, gen)}", "reason": reason,
            "toGeneration": gen + 1}
    orig = original(kube, job_id)
    if orig:
        spec["jobSet"] = orig
    deadline = fencing(kube, job_id).get("stop_wait_deadline")
    if deadline:
        spec["stopDeadline"] = deadline
    if nodes:
        spec["excludeNodes"] = list(nodes)
    if clusters:
        spec["excludeClusters"] = list(clusters)
    body = {"apiVersion": f"{K.RR_GROUP}/{K.RR_VERSION}", "kind": "RelocationRequest",
            "metadata": {"name": name, "namespace": NS}, "spec": spec}
    try:
        kube.co.create_namespaced_custom_object(K.RR_GROUP, K.RR_VERSION, NS, K.RR_PLURAL, body, _request_timeout=T)
    except client.ApiException as e:
        if e.status >= 500 or "failed calling webhook" in (e.body or ""):
            raise Invalid(f"RR {name} 생성 실패 (인프라): {e.status}") from e
        raise
    log.info("  RR %s: %s %s (requestId %s%s)", name, action, spec["jobRef"], recovery_id,
             f", 격리 {nodes}" if nodes else "")


def relocate(kube, name, recovery_id, action, job_id, nodes=None, clusters=None, reason="hardware"):
    """세대 +1 -> RR 생성 (복구 컨트롤러가 하는 순서). 반환: 새 세대 JobSet 이름"""
    gen = advance_generation(kube, job_id, recovery_id)
    create_rr(kube, name, recovery_id, action, job_id, gen, nodes, clusters, reason)
    return jsname(job_id, gen + 1)


PHASES = ["GENERATION_ADVANCED", "OLD_RUN_STOPPED", "NEW_RUN_SUBMITTED", "RESUMING", "COMPLETED"]


def project(kube, obj):
    """복구 컨트롤러 흉내: RR status.recoveryProgress -> 펜싱 기록 (같은 recovery_id·세대일 때만, 단계는 앞으로만).
    OLD_RUN_STOPPED를 기록하면 RR에 확인 주석을 붙인다 (HOLD_ACK에 있는 RR은 빼고)"""
    p = (obj.get("status") or {}).get("recoveryProgress")
    if not isinstance(p, dict) or p.get("phase") not in ("OLD_RUN_STOPPED", "NEW_RUN_SUBMITTED"):
        return
    job_id = p.get("jobId")
    rec = fencing(kube, job_id)
    if (rec.get("recovery_id") != obj["spec"]["requestId"]
            or rec.get("recovery_to_gen") != str(p.get("toGeneration"))):
        return   # 진짜 컨트롤러라면 취소를 붙일 상황. 시나리오가 직접 판단
    cur = rec.get("recovery_phase")
    data = {}
    if cur in PHASES and PHASES.index(cur) < PHASES.index("OLD_RUN_STOPPED"):
        data.update(recovery_phase="OLD_RUN_STOPPED",
                    old_run_stop_evidence=json.dumps(p.get("oldRunStopEvidence"), ensure_ascii=False, sort_keys=True))
        cur = "OLD_RUN_STOPPED"
    new = p.get("newJobSet") or {}
    if p["phase"] == "NEW_RUN_SUBMITTED" and cur == "OLD_RUN_STOPPED" and new.get("uid"):
        data.update(recovery_phase="NEW_RUN_SUBMITTED", new_jobset_name=new["name"], new_jobset_uid=new["uid"])
    if data:
        kube.core.patch_namespaced_config_map(f"recovery-fencing-{job_id}", STORE_NS, {"data": data}, _request_timeout=T)
        log.info("  (복구 컨트롤러 흉내) %s 펜싱 기록 %s", job_id, data["recovery_phase"])
    ann = obj["metadata"].get("annotations") or {}
    if obj["metadata"]["name"] not in HOLD_ACK and ann.get(K.ANN_STOP_RECORDED) != "true":
        ack_stop(kube, obj["metadata"]["name"])


def ack_stop(kube, name):
    """복구 컨트롤러 흉내: 펜싱 기록에 OLD_RUN_STOPPED를 남겼다는 확인 -> relocator가 재제출해도 됨"""
    kube.co.patch_namespaced_custom_object(K.RR_GROUP, K.RR_VERSION, NS, K.RR_PLURAL, name,
                                           {"metadata": {"annotations": {K.ANN_STOP_RECORDED: "true"}}},
                                           _request_timeout=T)
    log.info("  (복구 컨트롤러 흉내) RR %s에 %s=true", name, K.ANN_STOP_RECORDED)


def cancel_rr(kube, name, reason="superseded"):
    """복구 컨트롤러 흉내: 권한을 잃었거나 이미 끝난 복구 -> 취소 주석"""
    kube.co.patch_namespaced_custom_object(K.RR_GROUP, K.RR_VERSION, NS, K.RR_PLURAL, name,
                                           {"metadata": {"annotations": {K.ANN_CANCEL: reason}}},
                                           _request_timeout=T)
    log.info("  (복구 컨트롤러 흉내) RR %s에 %s=%s", name, K.ANN_CANCEL, reason)


def rr(kube, name):
    obj = kube.co.get_namespaced_custom_object(K.RR_GROUP, K.RR_VERSION, NS, K.RR_PLURAL, name, _request_timeout=T)
    project(kube, obj)
    return obj


def rr_status(kube, name):
    return rr(kube, name).get("status") or {}


def wait_rr(kube, name, phases, timeout):
    """status.phase가 phases 중 하나가 될 때까지. 반환: status (시간 안에 안 되면 마지막 status, 'timeout' 키 True)"""
    phases = (phases,) if isinstance(phases, str) else tuple(phases)
    st = {}
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        st = rr_status(kube, name)
        if st.get("phase") in phases:
            return st
        time.sleep(3)
    return {**st, "timeout": True}


def confirm_stopped(kube, name):
    """사람이 옛 실행 종료를 확인했다는 어노테이션 (정본 4.3, O3: 지금은 사람 전용)."""
    kube.co.patch_namespaced_custom_object(K.RR_GROUP, K.RR_VERSION, NS, K.RR_PLURAL, name,
                                           {"metadata": {"annotations": {K.ANN_STOP_CONFIRMED: "true"}}},
                                           _request_timeout=T)
    log.info("  RR %s에 %s=true", name, K.ANN_STOP_CONFIRMED)


def stage_seconds(st, a, b):
    """RR status 단계 시각 두 개 사이 초 (없으면 None)."""
    if not st.get(a) or not st.get(b):
        return None
    f = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))  # noqa: E731
    return (f(st[b]) - f(st[a])).total_seconds()


def stage_summary(st):
    order = ["acceptedAt", "isolatedAt", "stoppedAt", "resubmittedAt", "admittedAt"]
    parts = []
    for a, b in zip(order, order[1:]):
        s = stage_seconds(st, a, b)
        parts.append(f"{a[:-2]}->{b[:-2]} {'-' if s is None else f'{s:.0f}s'}")
    return f"{st.get('phase')} ({', '.join(parts)})"


# ---------------------------------------------------------------- 관리 클러스터 JobSet
def jobset(kube, name):
    try:
        return kube.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, NS, "jobsets", name,
                                                    _request_timeout=T)
    except client.ApiException as e:
        if e.status == 404:
            return None
        raise


def jobsets_of(kube, job_id):
    items = kube.co.list_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, NS, "jobsets",
                                                  label_selector=f"{K.LABEL_JOB_ID}={job_id}", _request_timeout=T)["items"]
    return sorted(js["metadata"]["name"] for js in items if not js["metadata"].get("deletionTimestamp"))


def prefers_spare(js):
    """모든 Pod 템플릿에 예비 선호(preferred nodeAffinity devoops/spare In [true])가 있나 (GPU-306)"""
    for rj in js["spec"]["replicatedJobs"]:
        spec = rj["template"]["spec"]["template"]["spec"]
        prefs = ((spec.get("affinity") or {}).get("nodeAffinity") or {}).get(
            "preferredDuringSchedulingIgnoredDuringExecution") or []
        if not any(e.get("key") == K.TAINT_SPARE and e.get("operator") == "In" and "true" in (e.get("values") or [])
                   for p in prefs for e in (p.get("preference") or {}).get("matchExpressions") or []):
            return False
    return True


def tolerates_spare(js):
    for rj in js["spec"]["replicatedJobs"]:
        for t in rj["template"]["spec"]["template"]["spec"].get("tolerations") or []:
            if t.get("key") == K.TAINT_SPARE:
                return True
    return False


def workload(kube, js_name):
    """JobSet 이름 -> 관리 클러스터 Workload 원본 (없으면 None)"""
    items = kube.co.list_namespaced_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, NS, "workloads",
                                                  _request_timeout=T)["items"]
    return next((w for w in items if any(o.get("name") == js_name for o in w["metadata"].get("ownerReferences") or [])),
                None)


def placed_on(kube, js_name):
    w = workload(kube, js_name)
    return ((w or {}).get("status") or {}).get("clusterName")


def quota_at(kube, js_name):
    """관리 클러스터 QuotaReserved=True가 된 시각 (datetime, 없으면 None)"""
    w = workload(kube, js_name)
    for c in ((w or {}).get("status") or {}).get("conditions") or []:
        if c["type"] == "QuotaReserved" and c["status"] == "True":
            return datetime.fromisoformat(c["lastTransitionTime"].replace("Z", "+00:00"))
    return None


def wait_placed(kube, js_name, timeout):
    """워커에 들어갈 때까지. 반환: 워커 이름 또는 None"""
    got = {}

    def ok():
        got["c"] = placed_on(kube, js_name)
        return bool(got["c"])
    C.wait_until(ok, timeout)
    return got.get("c")


# ---------------------------------------------------------------- 워커 쪽
def pods(kube, worker, js_name, phase=None):
    sel = {"label_selector": f"{JOBSET_LABEL}={js_name}"}
    if phase:
        sel["field_selector"] = f"status.phase={phase}"
    return kube.wcore(worker).list_namespaced_pod(NS, _request_timeout=T, **sel).items


def running_nodes(kube, worker, js_name):
    return sorted(p.spec.node_name for p in pods(kube, worker, js_name, "Running"))


def wait_running(kube, worker, js_name, n, timeout=90):
    return C.wait_until(lambda: len(pods(kube, worker, js_name, "Running")) >= n, timeout)


def node(kube, worker, name):
    return kube.wcore(worker).read_node(name, _request_timeout=T)


def isolated(n):
    """(cordon, devoops/faulty taint)"""
    return bool(n.spec.unschedulable), any(t.key == K.TAINT_FAULTY for t in n.spec.taints or [])


def set_cordon(kube, worker, name, on):
    kube.wcore(worker).patch_node(name, {"spec": {"unschedulable": on}}, _request_timeout=T)
    log.info("  %s/%s %s", worker, name, "cordon" if on else "uncordon")


def fenced(kube, worker):
    m = kube.co.get_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", worker,
                                          _request_timeout=T)
    return (m["metadata"].get("labels") or {}).get(K.LABEL_FENCED) == "true"


def fenced_ack(kube, worker):
    """디스패처가 남긴 시간대 있는 ACK를 읽는다. 없거나 잘못됐으면 None이다."""
    m = kube.co.get_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", worker,
                                         _request_timeout=T)
    value = (m.get("metadata", {}).get("annotations") or {}).get(K.ANN_FENCED_ACK)
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp if stamp.tzinfo is not None else None
    except (AttributeError, TypeError, ValueError):
        return None


def mkc_active(kube, worker):
    m = kube.co.get_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", worker,
                                          _request_timeout=T)
    return any(c["type"] == "Active" and c["status"] == "True" for c in (m.get("status") or {}).get("conditions") or [])


def spare_nodes(kube, worker):
    return sorted(n.metadata.name for n in kube.wcore(worker).list_node(label_selector=f"{K.TAINT_SPARE}=true",
                                                                         _request_timeout=T).items)


def placeholder(kube, worker, timeout=60):
    """워커 예비 노드에 자리 표시 Pod (examples/spare/placeholder.yaml). Running이 돼야 상황이 만들어진 것"""
    r = subprocess.run(["kubectl", "--context", WORKER_CONTEXTS[worker], "apply", "-f", str(PLACEHOLDER)],
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise Invalid(f"{worker} 자리 표시 Pod apply 실패: {r.stderr.strip()[:200]}")
    ok = C.wait_until(lambda: placeholder_phase(kube, worker) == "Running", timeout)
    precondition(ok is not None, f"{worker} 자리 표시 Pod가 {timeout}s 안에 Running 안 됨 ({placeholder_phase(kube, worker)})")


def placeholder_phase(kube, worker):
    ps = kube.wcore(worker).list_namespaced_pod(NS, label_selector="app=spare-placeholder", _request_timeout=T).items
    return ps[0].status.phase if ps else None


def first_node(kube, worker, js_name):
    """잡 Pod 하나가 올라간 노드 (고장 노드로 쓸 것)"""
    nodes = running_nodes(kube, worker, js_name)
    precondition(nodes, f"{worker}에 {js_name} Running Pod가 없음")
    return nodes[0]


# ---------------------------------------------------------------- 이중 실행 감시 (I1)
class DoubleRunWatch(threading.Thread):
    """작업 하나의 Running Pod 세대를 닿는 워커 전부에서 계속 본다. 두 세대가 동시에 Running이면 기록 (I1 위반).
    끊긴 워커는 못 보므로, 끊긴 구간이 있는 케이스는 다시 붙인 뒤 따로 확인한다."""

    def __init__(self, kube, job_id, interval=2):
        super().__init__(daemon=True, name=f"double-{job_id}")
        self.kube, self.job_id, self.interval = kube, job_id, interval
        self.overlaps = []
        self._halt = threading.Event()

    def generations(self):
        from . import network
        seen = {}
        for w in WORKERS:
            if w in network.DOWN:
                continue
            try:
                ps = self.kube.wcore(w).list_namespaced_pod(
                    NS, label_selector=f"{K.LABEL_JOB_ID}={self.job_id}", field_selector="status.phase=Running",
                    _request_timeout=5).items
            except Exception:  # noqa: BLE001  잠깐 안 닿음
                continue
            for p in ps:
                g = (p.metadata.labels or {}).get(K.LABEL_GENERATION, "?")
                seen.setdefault(g, set()).add(w)
        return seen

    def run(self):
        while not self._halt.wait(self.interval):
            seen = self.generations()
            if len(seen) > 1:
                self.overlaps.append(f"{datetime.now():%H:%M:%S} " +
                                     ", ".join(f"g{g}@{'/'.join(sorted(ws))}" for g, ws in sorted(seen.items())))

    def stop(self):
        self._halt.set()
        self.join(timeout=10)
        return self

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()


# ---------------------------------------------------------------- 재제출 공통 판정 (R7·I6, 템플릿 6절)
def check_resubmission(check, kube, job_id, new, rr_name, recovery_id, *, target=None, exclude=None, preferred=None):
    """재제출본과 펜싱 기록이 정본 4.1·5절대로인지. target/exclude는 기대하는 디스패처 힌트"""
    st = rr_status(kube, rr_name)
    js = jobset(kube, new)
    check.true(f"재제출본 {new} 있음", js is not None)
    if js is None:
        return
    meta = js["metadata"]
    labels, ann = meta.get("labels") or {}, meta.get("annotations") or {}
    check.eq("재제출본 큐", labels.get(K.LABEL_QUEUE), K.RECOVERY_QUEUE)
    check.eq("재제출본 우선순위", labels.get(K.LABEL_PRIORITY), K.PRIORITY_RECOVERY)
    check.eq("재제출본 recovery-id", ann.get(K.ANN_RECOVERY_ID), recovery_id)
    check.true("재제출본 예비 노드 toleration", tolerates_spare(js))
    pinned = bool(st.get("pinnedNodes"))
    check.eq("재제출본 예비 선호 (노드 고정 없을 때만)", prefers_spare(js), not pinned)
    check.eq("재제출본 target-cluster", ann.get(K.ANN_TARGET), target)
    check.eq("재제출본 원래 클러스터 선호", ann.get(K.ANN_PREFERRED), preferred)
    if exclude is not None:
        check.true(f"재제출본 exclude에 {exclude}", exclude in (ann.get(K.ANN_EXCLUDE) or "").split(","),
                   ann.get(K.ANN_EXCLUDE, ""))
    others = [n for n in jobsets_of(kube, job_id) if n != new]
    check.true("다른 세대 JobSet 없음", not others, ",".join(others))
    rec = fencing(kube, job_id)
    check.true("펜싱 기록 NEW_RUN_SUBMITTED 이후",
               rec.get("recovery_phase") in ("NEW_RUN_SUBMITTED", "RESUMING", "COMPLETED"), rec.get("recovery_phase"))
    check.eq("펜싱 기록 new_jobset_name", rec.get("new_jobset_name"), new)
    check.eq("펜싱 기록 new_jobset_uid = 실제 JobSet", rec.get("new_jobset_uid"), meta.get("uid"))
    gap = stage_seconds(st, "stoppedAt", "resubmittedAt")
    check.true("옛 실행 종료 확인 뒤에 재제출 (I6)", gap is not None and gap >= 0, f"stopped->resubmitted {gap}")


# ---------------------------------------------------------------- 학습 12장이 찬 상태에서 4장 잡 복구 (R10, 케이스 11·compare)
def recovery_quota(kube):
    return int(C.cq_gpu(kube, "cq-recovery")["nominalQuota"])


def set_recovery_quota(kube, n):
    cq = C.clusterqueue(kube, "cq-recovery")
    cq["spec"]["resourceGroups"][0]["flavors"][0]["resources"][0]["nominalQuota"] = str(n)
    kube.co.replace_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", "cq-recovery", cq,
                                          _request_timeout=T)
    log.info("  관리 cq-recovery nominalQuota = %s", n)


def under_load(kube, wait):
    """Heavy 8(train-a w1, train-b w2) + Light 4(lite-1·2 w3)로 학습 12장을 채우고 Heavy 대기 잡 wait-c(4장)를 둔 뒤
    train-a 노드 하나 고장 -> moveToSpare. 반환: 복구 잡이 wait초 안에 쿼터·입장했는지와 그 사이 기록."""
    from .jobs import submit
    worker_names = dict(zip(PROFILE.workers, WORKERS))
    jobs = [(jid, gpus, q, worker_names[w]) for jid, gpus, q, w in PROFILE.load_jobs()]
    for jid, gpus, q, w in jobs:
        submit(kube, jid, gpus, q, "30m", {K.ANN_TARGET: w})
    for jid, _, _, w in jobs:
        precondition(wait_placed(kube, jsname(jid), 150) == w, f"{jid}가 {w}에 안 들어감")
    precondition(wait_running(kube, "worker1", "job-train-a-g0001", 4) is not None, "train-a Pod 4개가 Running 안 됨")
    submit(kube, "wait-c", 4, "queue-heavy", "30m")
    time.sleep(20)
    precondition(quota_at(kube, "job-wait-c-g0001") is None, "wait-c가 벌써 쿼터를 받음 (학습 12장 채우기 실패)")
    bad = first_node(kube, "worker1", "job-train-a-g0001")

    new = relocate(kube, "rr-load", "evt-011", "moveToSpare", "train-a", nodes=[bad])
    stolen = []

    def admitted():
        c = placed_on(kube, new)
        wc = placed_on(kube, "job-wait-c-g0001")
        if not c and wc == "worker1" and not stolen:
            stolen.append("wait-c -> worker1")
        return bool(c)
    took = C.wait_until(admitted, wait, interval=3)
    rq, cq = quota_at(kube, new), quota_at(kube, "job-wait-c-g0001")
    return {"new": new, "placed": placed_on(kube, new), "took": took, "recovery_quota_at": rq, "wait_c_quota_at": cq,
            "wait_c_cluster": placed_on(kube, "job-wait-c-g0001"), "stolen": stolen,
            "rr": stage_summary(rr_status(kube, "rr-load"))}


def wait_until_status(kube, name, key, timeout):
    """RR status에 key가 생길 때까지. 반환: status 또는 None"""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        st = rr_status(kube, name)
        if st.get(key):
            return st
        time.sleep(3)
    return None
