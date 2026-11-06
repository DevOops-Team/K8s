"""재배치 실행기 (GPU-68) — 관리 클러스터에서 돈다. 노드 격리만 워커에 직접 한다.

보는 것:  RelocationRequest (장애복구 파트가 만든다. 지금은 손으로 넣어 흉내 냄)
판단:     action에 따라 정해진 순서대로
바꾸는 것: 워커 노드 cordon·taint / MultiKueueCluster 펜싱 라벨 / 잡 내리기·재제출 / 요청 status

action:
  restartInPlace   같은 클러스터에 다시 띄움
  moveToSpare      고장 노드 cordon·taint -> 같은 클러스터에 복구 우선순위로 재제출 (빈자리는 예비 노드)
  moveToOtherCluster  (예비 소진, SDC) 의심 노드 cordon·taint -> 원래 클러스터를 빼고 재제출.
                   펜싱 안 함: 그 클러스터의 다른 잡·새 잡은 그대로 받는다
  evacuateCluster  (AZ·클러스터 장애) 클러스터 펜싱 -> 원래 클러스터를 빼고 재제출 (디스패처가 다른 곳을 고름)

자리는 정하지 않는다. 재제출한 잡도 디스패처를 다시 거친다.

단계마다 status에 시각을 남기고, 다음 루프에서 비어 있는 단계부터 이어서 한다.
그래서 실행기가 중간에 죽었다 살아나도 끝난 단계를 두 번 하지 않는다.
  acceptedAt -> isolatedAt -> stoppedAt -> resubmittedAt -> admittedAt (Done)
"""
import copy
import json
import os
import time

from kubernetes import client

import keys as K
import kube
import leader

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "5"))
STOP_WAIT_SECONDS = int(os.getenv("STOP_WAIT_SECONDS", "60"))     # 옛 잡이 내려가길 기다리는 최대 시간
ADMIT_TIMEOUT_SECONDS = int(os.getenv("ADMIT_TIMEOUT_SECONDS", "900"))
CONTROLLER_NS = os.getenv("CONTROLLER_NAMESPACE", "job-placement")   # 재제출용 잡 사본 보관

kube.setup_logging("relocator")
log = kube.logging.getLogger("relocator")

FINAL = {"Done", "Failed", "Duplicate"}
KUEUE_PREFIX = "kueue.x-k8s.io/"


class Ctx:
    def __init__(self, mgr, workers, rr):
        self.mgr, self.workers, self.rr = mgr, workers, rr
        self.co = kube.kueue(mgr)
        self.ns = rr["metadata"]["namespace"]
        self.name = rr["metadata"]["name"]
        self.spec = rr["spec"]
        self.st = dict(rr.get("status") or {})

    def save(self, **kw):
        self.st.update(kw)
        self.co.patch_namespaced_custom_object_status(
            K.RR_GROUP, K.RR_VERSION, self.ns, K.RR_PLURAL, self.name, {"status": self.st})

    def job(self):
        # 요청이 있는 네임스페이스의 잡만 다룬다 (다른 네임스페이스 잡을 지우게 만드는 요청 차단)
        ns, _, name = self.spec["jobRef"].partition("/")
        if ns != self.ns:
            raise PermissionError(f"jobRef 네임스페이스 {ns} != 요청 네임스페이스 {self.ns}")
        return ns, name

    def snapshot_name(self):
        return f"rr-snapshot-{self.rr['metadata']['uid']}"


# ---------- 단계

def accept(x: Ctx, all_rrs):
    rid = x.spec["requestId"]
    older = [r for r in all_rrs if r["spec"]["requestId"] == rid and r["metadata"]["uid"] != x.rr["metadata"]["uid"]
             and (r["metadata"]["creationTimestamp"], r["metadata"]["name"]) <
             (x.rr["metadata"]["creationTimestamp"], x.name)]
    if older:
        x.save(phase="Duplicate", message=f"같은 requestId를 {older[0]['metadata']['name']}이(가) 처리", acceptedAt=kube.now_rfc3339())
        log.info("%s: 중복 요청 (%s) -> 무시", x.name, rid)
        return False
    ns, name = x.job()
    wl = kube.workload_for_jobset(x.mgr, ns, name)
    source = (wl or {}).get("status", {}).get("clusterName") or ""
    if not wl:
        x.save(phase="Failed", message=f"잡 {ns}/{name}의 Workload를 못 찾음", acceptedAt=kube.now_rfc3339())
        return False
    x.save(phase="Accepted", sourceCluster=source, acceptedAt=kube.now_rfc3339(), message="")
    log.info("%s: 접수 %s %s/%s (지금 %s)", x.name, x.spec["action"], ns, name, source or "워커 미정")
    return True


def cordon_nodes(x: Ctx, src, nodes):
    api = x.workers.get().get(src)
    if api is None:
        raise RuntimeError(f"워커 {src} 접속 정보 없음")
    core = client.CoreV1Api(api)
    gpu_nodes = {n.metadata.name for n in core.list_node(
        label_selector=K.GPU_NODE_SELECTOR, _request_timeout=kube.TIMEOUT).items}
    for node in nodes:
        if node not in gpu_nodes:   # GPU 노드가 아닌 것(컨트롤 플레인 등)은 격리하지 않음
            raise PermissionError(f"{node}는 {src}의 GPU 노드({K.GPU_NODE_SELECTOR})가 아님")
        n = core.read_node(node, _request_timeout=kube.TIMEOUT)
        taints = [t for t in (n.spec.taints or []) if t.key != K.TAINT_FAULTY]
        taints.append(client.V1Taint(key=K.TAINT_FAULTY, value="true", effect="NoSchedule"))
        core.patch_node(node, {"spec": {"unschedulable": True, "taints": [t.to_dict() for t in taints]}},
                        _request_timeout=kube.TIMEOUT)
        log.info("%s: %s/%s cordon + taint", x.name, src, node)


def isolate(x: Ctx):
    action, src = x.spec["action"], x.st.get("sourceCluster")
    nodes = x.spec.get("excludeNodes") or []
    if action in ("moveToSpare", "moveToOtherCluster") and nodes:
        if not src:
            raise PermissionError(f"잡이 아직 워커에 안 보내져서 {nodes}를 어느 클러스터에서 격리할지 모름")
        cordon_nodes(x, src, nodes)
    elif action == "evacuateCluster":
        x.co.patch_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", src,
                                         {"metadata": {"labels": {K.LABEL_FENCED: "true"}}})
        log.info("%s: %s 펜싱 (새 잡 안 보냄)", x.name, src)
    x.save(phase="Executing", isolatedAt=kube.now_rfc3339())


def old_gone_on_worker(x: Ctx, ns, name):
    """옛 잡이 돌던 워커에서 JobSet·Workload·Pod가 전부 없어졌는지. (멈춤확인, 이유)
    워커에 접속이 안 되면 확인 불가 = 멈췄다고 보지 않는다"""
    src = x.st.get("sourceCluster")
    if not src:
        return True, "워커에 보내지기 전이었음"
    api = x.workers.get().get(src)
    if api is None:
        return False, f"{src} 접속 정보 없음"
    try:
        try:
            kube.kueue(api).get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name,
                                                         _request_timeout=kube.TIMEOUT)
            return False, f"{src}에 JobSet 남아 있음"
        except client.ApiException as e:
            if e.status != 404:
                raise
        wls = kube.kueue(api).list_namespaced_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, ns, "workloads",
                                                            _request_timeout=kube.TIMEOUT)["items"]
        if any(kube.workload_owner(w) == ("JobSet", name) for w in wls):
            return False, f"{src}에 Workload 남아 있음"
        try:
            pods = client.CoreV1Api(api).list_namespaced_pod(
                ns, label_selector=f"jobset.sigs.k8s.io/jobset-name={name}", _request_timeout=kube.TIMEOUT).items
        except client.ApiException as e:
            if e.status != 403:   # MultiKueue SA(multikueue-sa-role)는 Pod 권한이 없을 수 있음 -> JobSet·Workload로 판단
                raise
            return True, f"{src}에서 JobSet·Workload 사라짐 (Pod 조회 권한 없음)"
        if pods:
            return False, f"{src}에 Pod {len(pods)}개 남아 있음"
        return True, f"{src}에서도 사라짐"
    except Exception as e:  # noqa: BLE001  끊김·타임아웃
        return False, f"{src} 접속 안 됨 ({type(e).__name__})"


def stop_old(x: Ctx):
    ns, name = x.job()
    try:
        x.co.delete_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name,
                                             propagation_policy="Background")
        log.info("%s: 옛 잡 %s/%s 삭제 요청", x.name, ns, name)
    except client.ApiException as e:
        if e.status != 404:
            raise
    mgr_gone = kube.workload_for_jobset(x.mgr, ns, name) is None
    worker_gone, why = (old_gone_on_worker(x, ns, name) if mgr_gone else (False, "관리 클러스터 Workload 남아 있음"))
    confirmed = (x.rr["metadata"].get("annotations") or {}).get(K.ANN_STOP_CONFIRMED) == "true"
    if (mgr_gone and worker_gone) or confirmed:
        how = f"옛 Workload 삭제 확인 ({why})" if (mgr_gone and worker_gone) else f"요청자가 {K.ANN_STOP_CONFIRMED}=true로 확인"
        log.info("%s: 옛 실행 멈춤 (%s)", x.name, how)
        x.save(phase="Executing", stoppedAt=kube.now_rfc3339(), message=how)
        return
    # 멈춘 게 확인되지 않으면 재제출하지 않는다 (contracts.md: 종료 확인 전 restart 금지 = 이중 실행 방지)
    # 관리 클러스터 Workload는 워커가 끊겨 있어도 지워진다. 워커 쪽에서 사라진 걸 봐야 멈춘 것
    if kube.since(x.st["isolatedAt"]) > STOP_WAIT_SECONDS and x.st.get("phase") != "WaitingForStop":
        msg = (f"옛 실행 종료를 {STOP_WAIT_SECONDS}s 안에 확인 못 함 ({why}) -> 재제출 보류. 옛 실행이 멈춘 걸 확인했으면 "
               f"이 요청에 {K.ANN_STOP_CONFIRMED}=true 어노테이션을 붙일 것")
        log.warning("%s: %s", x.name, msg)
        x.save(phase="WaitingForStop", message=msg)


def _strip_meta(meta):
    for field in ("annotations", "labels"):
        d = (meta or {}).get(field)
        if d:
            for k in [k for k in d if k.startswith(KUEUE_PREFIX)]:
                del d[k]


def strip_kueue_injected(spec):
    """입장할 때 Kueue가 템플릿에 붙인 것(kueue.x-k8s.io/workload 어노테이션, podset·큐 라벨)을 지운다.
    그대로 두면 새 Workload 입장 때 'conflict for key=kueue.x-k8s.io/workload'로 unsuspend가 실패하고
    새 Workload가 바로 Finished 처리된다 (스냅샷이 실행 중인 잡에서 떠진 것이라 이미 붙어 있음)"""
    for rj in spec.get("replicatedJobs") or []:
        tmpl = rj.get("template") or {}
        _strip_meta(tmpl.get("metadata"))
        _strip_meta(((tmpl.get("spec") or {}).get("template") or {}).get("metadata"))


LEAVE_SOURCE = {"moveToOtherCluster", "evacuateCluster"}   # 원래 클러스터를 떠나는 action


def placement_hints(action, src, old_exclude_csv, extra_exclude):
    """재제출 잡의 (target-cluster, exclude-clusters 목록). 순수 함수 (test_relocator.py)
    restartInPlace·moveToSpare: 원래 클러스터로 고정 / moveToOtherCluster·evacuateCluster: 원래 클러스터를 뺌"""
    exclude = {c for c in (old_exclude_csv or "").split(",") if c} | set(extra_exclude or [])
    if action in LEAVE_SOURCE:
        if src:
            exclude.add(src)
        return None, sorted(exclude)
    return (src or None), sorted(exclude - {src})


def build_new(x: Ctx, old):
    ns, old_name = x.job()
    labels = old["metadata"].get("labels") or {}
    job_id = labels.get(K.LABEL_JOB_ID, old_name)
    gen = int(labels.get(K.LABEL_GENERATION, "0")) + 1
    new = {"apiVersion": f"{K.JOBSET_GROUP}/{K.JOBSET_VERSION}", "kind": "JobSet",
           "metadata": {"name": f"{job_id}-g{gen}", "namespace": ns,
                        "labels": {**{k: v for k, v in labels.items() if not k.startswith("jobset.sigs")},
                                   K.LABEL_JOB_ID: job_id, K.LABEL_GENERATION: str(gen),
                                   K.LABEL_REQUEST_ID: x.spec["requestId"],
                                   K.LABEL_QUEUE: K.RECOVERY_QUEUE, K.LABEL_PRIORITY: K.PRIORITY_RECOVERY},
                        "annotations": dict(old["metadata"].get("annotations") or {})},
           "spec": copy.deepcopy(old["spec"])}
    strip_kueue_injected(new["spec"])
    new["metadata"]["annotations"].pop("kubectl.kubernetes.io/last-applied-configuration", None)
    new["metadata"]["annotations"].pop(K.ANN_MANUAL, None)
    new["spec"]["suspend"] = True
    new["spec"].pop("managedBy", None)   # Kueue 웹훅이 다시 채움
    ann = new["metadata"]["annotations"]
    target, exclude = placement_hints(x.spec["action"], x.st.get("sourceCluster"),
                                      ann.get(K.ANN_EXCLUDE), x.spec.get("excludeClusters"))
    if target:
        ann[K.ANN_TARGET] = target
    elif x.spec["action"] in LEAVE_SOURCE:
        ann.pop(K.ANN_TARGET, None)
    ann.pop(K.ANN_EXCLUDE, None)
    if exclude:
        ann[K.ANN_EXCLUDE] = ",".join(exclude)
    return new


def resubmit(x: Ctx):
    ns, _ = x.job()
    # 옛 잡 원본은 접수 때 컨트롤러 네임스페이스 ConfigMap에 저장해 둔 것을 쓴다
    # (요청 status는 다른 주체도 쓸 수 있어서 거기 두면 재제출 내용을 바꿔치기할 수 있음)
    cm = client.CoreV1Api(x.mgr).read_namespaced_config_map(x.snapshot_name(), CONTROLLER_NS)
    old = json.loads(cm.data["jobset"])
    new = build_new(x, old)
    try:
        x.co.create_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", new)
    except client.ApiException as e:
        if e.status != 409:   # 이미 있음 = 지난 루프에서 만들고 status 저장 전에 죽은 경우
            raise
    x.save(resubmittedAt=kube.now_rfc3339(), newJob=f"{ns}/{new['metadata']['name']}")
    log.info("%s: 재제출 %s (%s, exclude=%s, target=%s)", x.name, new["metadata"]["name"], K.RECOVERY_QUEUE,
             new["metadata"]["annotations"].get(K.ANN_EXCLUDE, "-"), new["metadata"]["annotations"].get(K.ANN_TARGET, "-"))


def watch_admit(x: Ctx):
    ns, _, name = x.st["newJob"].partition("/")
    wl = kube.workload_for_jobset(x.mgr, ns, name)
    cluster = ((wl or {}).get("status") or {}).get("clusterName")
    if wl and kube.is_true(wl, "Finished"):
        c = kube.cond(wl, "Finished") or {}
        x.save(phase="Failed", message=f"재제출 잡 Workload가 바로 끝남: {c.get('reason', '')} {c.get('message', '')}".strip())
        log.warning("%s: %s Workload가 Finished (%s)", x.name, name, c.get("message", ""))
    elif wl and cluster and kube.is_true(wl, "Admitted"):
        x.save(phase="Done", admittedAt=kube.now_rfc3339(), targetCluster=cluster)
        log.info("%s: 완료, %s가 %s에 입장", x.name, name, cluster)
    elif kube.since(x.st["resubmittedAt"]) > ADMIT_TIMEOUT_SECONDS:
        x.save(phase="Failed", message=f"재제출 잡이 {ADMIT_TIMEOUT_SECONDS}s 안에 입장 못 함")


def snapshot(x: Ctx):
    ns, name = x.job()
    old = x.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name)
    snap = {"metadata": {"labels": old["metadata"].get("labels") or {},
                         "annotations": old["metadata"].get("annotations") or {}},
            "spec": old["spec"]}
    body = client.V1ConfigMap(metadata=client.V1ObjectMeta(
        name=x.snapshot_name(), labels={"app": "relocator", K.LABEL_REQUEST_ID: x.spec["requestId"]}),
        data={"jobset": json.dumps(snap)})
    try:
        client.CoreV1Api(x.mgr).create_namespaced_config_map(CONTROLLER_NS, body)
    except client.ApiException as e:
        if e.status != 409:
            raise


def step(x: Ctx, all_rrs):
    st = x.st
    if not st.get("acceptedAt"):
        if not accept(x, all_rrs):
            return
        snapshot(x)
    if not st.get("isolatedAt"):
        return isolate(x)
    if not st.get("stoppedAt"):
        return stop_old(x)
    if not st.get("resubmittedAt"):
        return resubmit(x)
    return watch_admit(x)


def reconcile(mgr, workers):
    co = kube.kueue(mgr)
    rrs = co.list_cluster_custom_object(K.RR_GROUP, K.RR_VERSION, K.RR_PLURAL)["items"]
    for rr in rrs:
        if (rr.get("status") or {}).get("phase") in FINAL:
            continue
        x = Ctx(mgr, workers, rr)
        try:
            step(x, rrs)
        except PermissionError as e:   # 규칙 위반 요청은 재시도하지 않고 끝냄
            log.warning("%s: 거부 - %s", x.name, e)
            x.save(phase="Failed", message=f"거부: {e}")
        except Exception as e:  # noqa: BLE001  다음 루프에서 같은 단계 재시도
            log.exception("%s: 단계 실패, 다음 루프에서 다시", x.name)
            try:
                x.save(message=f"재시도 중: {e}")
            except Exception:  # noqa: BLE001
                pass


def main():
    mgr = kube.manager_client()
    elector = leader.Elector(kube.manager_client(), "relocator")
    workers = kube.Workers(mgr)
    log.info("시작: interval=%ss", INTERVAL)
    while True:
        if not elector.leading:
            time.sleep(1)
            continue
        try:
            reconcile(mgr, workers)
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
