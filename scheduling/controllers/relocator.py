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
                   잡이 아직 워커에 안 보내졌으면 펜싱은 건너뛰고 재제출만

자리는 정하지 않는다. 재제출한 잡도 디스패처를 다시 거친다.

복구 컨트롤러와 나누는 일 (체크포인트 규약 5·8절, JobSet 템플릿 2·6절):
  복구 컨트롤러  판단 -> DynamoDB 세대 +1 (recovery_phase=GENERATION_ADVANCED, recovery_id·from/to gen) -> 이 요청 생성
  재배치 실행기  요청의 requestId = 펜싱 기록 recovery_id 인지 확인
                 -> 격리 -> 옛 실행이 멈춘 걸 확인하면 OLD_RUN_STOPPED
                 -> 복구 원본(S3 code/{job_id}/jobset.json)에서 6절 값만 바꿔 job-{id}-g{새 세대 4자리} 제출
                 -> NEW_RUN_SUBMITTED + new_jobset_name·uid (전부 조건부 갱신)
  RESUMING·COMPLETED는 복구 컨트롤러 몫.

단계마다 요청 status에 시각을 남기고, 다음 루프에서 비어 있는 단계부터 이어서 한다.
펜싱 기록 갱신과 JobSet 생성도 같은 이름·조건으로 다시 해도 결과가 같아서, 실행기가 중간에 죽었다 살아나도 두 번 하지 않는다.
  acceptedAt -> isolatedAt -> stoppedAt -> resubmittedAt -> admittedAt (Done)

시간 값은 복구 컨트롤러 제한 시간(장애 복구 규약 2절)에 맞춘다:
  옛 실행 종료 15분 (STOP_HOLD_SECONDS) -> 넘으면 Held, 확인 어노테이션으로만 재개
  입장 15분 (ADMIT_TIMEOUT_SECONDS)     -> 넘으면 AdmitDelayed, 실패로 끝내지 않음
"""
import os
import time

from kubernetes import client

import keys as K
import kube
import leader
import recovery_render as R
import recovery_store

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "5"))
STOP_WAIT_SECONDS = int(os.getenv("STOP_WAIT_SECONDS", "60"))     # 옛 잡이 이 시간 안에 안 내려가면 WaitingForStop으로 알림
# 복구 컨트롤러는 옛 실행 종료를 15분 기다린 뒤 HOLD한다 (장애 복구 규약 2절). 그 뒤에 실행기가 혼자 재제출하지 않게
# 요청 생성부터 이 시간이 지나면 자동 확인으로는 진행하지 않고 Held로 멈춘다. 확인 어노테이션(수동 재개)만 받는다
STOP_HOLD_SECONDS = int(os.getenv("STOP_HOLD_SECONDS", "900"))
# 재제출 잡이 이 시간 안에 입장 못 하면 AdmitDelayed로 알리고 계속 기다린다 (JobSet은 그대로 둠).
# 복구 성공·실패 판단은 복구 컨트롤러 몫 (복원·학습 진행 확인 30분 뒤 HOLD)
ADMIT_TIMEOUT_SECONDS = int(os.getenv("ADMIT_TIMEOUT_SECONDS", "900"))
FENCING_WAIT_SECONDS = int(os.getenv("FENCING_WAIT_SECONDS", "300"))   # 복구 컨트롤러의 세대 +1을 기다리는 최대 시간
# EKS마다 따로 만든 relocator SA 토큰 kubeconfig (노드 권한을 MultiKueue와 나눔). 없으면 MultiKueue Secret
OWN_SECRET = os.getenv("RELOCATOR_KUBECONFIG_SECRET", "{name}-relocator-kubeconfig")

kube.setup_logging("relocator")
log = kube.logging.getLogger("relocator")

FINAL = {"Done", "Failed", "Duplicate"}


class Ctx:
    def __init__(self, mgr, workers, store, rr):
        self.mgr, self.workers, self.store, self.rr = mgr, workers, store, rr
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
        # 학습 잡 네임스페이스(training)에 있는 요청이, 같은 네임스페이스의 잡만 다룬다.
        # 복구 원본·펜싱 기록은 job_id로만 찾으므로(S3 code/{job_id}, DynamoDB job_id) 다른 네임스페이스에서
        # 같은 job_id를 대면 남의 기록을 쓰게 된다 -> 요청 자체를 training에서만 받는다
        ns, _, name = self.spec["jobRef"].partition("/")
        if self.ns != K.NAMESPACE:
            raise PermissionError(f"요청 네임스페이스 {self.ns} != 학습 잡 네임스페이스 {K.NAMESPACE}")
        if ns != self.ns:
            raise PermissionError(f"jobRef 네임스페이스 {ns} != 요청 네임스페이스 {self.ns}")
        return ns, name

    # status(jobId·toGeneration)는 기록용. 다른 주체도 status를 쓸 수 있으므로 판단에는 쓰지 않고
    # 매번 spec.jobRef(바꿀 수 없음)와 펜싱 기록에서 다시 구한다
    def job_id(self):
        return R.parse_name(self.job()[1])[0]

    def to_gen(self):
        """펜싱 기록을 새로 읽어 이 요청의 새 세대. 진행해도 되는 상태가 아니면 PermissionError"""
        _, old_gen = R.parse_name(self.job()[1])
        verdict, why, to_gen = R.check_fencing(self.store.get_fencing(self.job_id()), self.spec["requestId"], old_gen)
        if verdict != "go":
            raise PermissionError(f"펜싱 기록이 진행 상태가 아님 ({verdict}: {why})")
        return to_gen


# ---------- 단계

def accept(x: Ctx, all_rrs):
    rid = x.spec["requestId"]   # = 펜싱 기록 recovery_id
    older = [r for r in all_rrs if r["spec"]["requestId"] == rid and r["metadata"]["uid"] != x.rr["metadata"]["uid"]
             and r["metadata"]["namespace"] == x.ns   # 다른 네임스페이스 요청이 진짜 요청을 Duplicate로 막지 못하게
             and (r["metadata"]["creationTimestamp"], r["metadata"]["name"]) <
             (x.rr["metadata"]["creationTimestamp"], x.name)]
    if older:
        x.save(phase="Duplicate", message=f"같은 requestId를 {older[0]['metadata']['name']}이(가) 처리", acceptedAt=kube.now_rfc3339())
        log.info("%s: 중복 요청 (%s) -> 무시", x.name, rid)
        return False
    ns, name = x.job()
    job_id, old_gen = R.parse_name(name)
    if not job_id:
        x.save(phase="Failed", message=f"잡 이름 {name}이 job-<작업ID>-g<세대 4자리> 형식이 아님",
               acceptedAt=kube.now_rfc3339())
        return False
    verdict, why, to_gen = R.check_fencing(x.store.get_fencing(job_id), rid, old_gen)
    if verdict == "wait":
        if kube.since(x.rr["metadata"]["creationTimestamp"]) > FENCING_WAIT_SECONDS:
            x.save(phase="Failed", message=f"{FENCING_WAIT_SECONDS}s 동안 세대 +1이 안 보임: {why}",
                   acceptedAt=kube.now_rfc3339())
        elif x.st.get("message") != f"대기: {why}":
            x.save(phase="Pending", message=f"대기: {why}")
            log.info("%s: 대기 - %s", x.name, why)
        return False
    if verdict == "done":
        x.save(phase="Done", message=f"펜싱 기록이 {why} -> 재배치 할 일 없음", acceptedAt=kube.now_rfc3339())
        return False
    if verdict == "fail":
        x.save(phase="Failed", message=f"펜싱 기록: {why}", acceptedAt=kube.now_rfc3339())
        return False
    wl = kube.workload_for_jobset(x.mgr, ns, name)
    source = (wl or {}).get("status", {}).get("clusterName") or ""
    old_exclude = ""
    try:
        old = x.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name)
        old_exclude = (old["metadata"].get("annotations") or {}).get(K.ANN_EXCLUDE, "")
    except client.ApiException as e:
        if e.status != 404:
            raise
    # 옛 잡이 벌써 없으면 원래 클러스터를 모른다. 단계가 OLD_RUN_STOPPED 이후(실행기가 이미 지운 뒤 재시작)일 때만 진행
    if not wl and why == "GENERATION_ADVANCED":
        x.save(phase="Failed", message=f"잡 {ns}/{name}의 Workload를 못 찾음", acceptedAt=kube.now_rfc3339())
        return False
    x.save(phase="Accepted", sourceCluster=source, jobId=job_id, fromGeneration=old_gen, toGeneration=to_gen,
           oldExclude=old_exclude, acceptedAt=kube.now_rfc3339(), message=f"펜싱 기록 {why}")
    log.info("%s: 접수 %s %s/%s (지금 %s, 세대 %s -> %s)", x.name, x.spec["action"], ns, name,
             source or "워커 미정", old_gen, to_gen)
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


def current_source(x: Ctx):
    """옛 잡이 지금 보내져 있는 워커 (관리 클러스터 Workload 기준). Workload가 없거나 아직 안 보내졌으면 빈 문자열"""
    ns, name = x.job()
    wl = kube.workload_for_jobset(x.mgr, ns, name)
    return ((wl or {}).get("status") or {}).get("clusterName") or ""


def isolate(x: Ctx):
    # 노드 cordon·클러스터 펜싱은 되돌리기 어려운 일이라 status(다른 주체도 쓸 수 있음)를 믿지 않는다:
    # 펜싱 기록을 다시 확인하고, 대상 클러스터도 Workload에서 다시 구한다
    x.to_gen()
    action, src = x.spec["action"], current_source(x)
    if src != (x.st.get("sourceCluster") or ""):
        # 접수 뒤 디스패처가 막 보냈거나(빈 값 -> 워커), status가 바뀐 경우. 어느 쪽이든 Workload가 맞다
        log.warning("%s: status의 sourceCluster=%r 대신 Workload 기준 %r 사용", x.name, x.st.get("sourceCluster"), src)
        x.save(sourceCluster=src)
    nodes = x.spec.get("excludeNodes") or []
    if action in ("moveToSpare", "moveToOtherCluster") and nodes:
        if not src:
            raise PermissionError(f"잡이 아직 워커에 안 보내져서 {nodes}를 어느 클러스터에서 격리할지 모름")
        cordon_nodes(x, src, nodes)
    elif action == "evacuateCluster" and not src:
        # 잡이 아직 워커에 안 보내졌음 -> 어느 클러스터를 펜싱할지 모름. 펜싱은 건너뛰고
        # 옛 잡 내리기·재제출만 한다 (재제출본은 spec.excludeClusters를 피해서 감)
        log.warning("%s: 잡이 워커에 보내지기 전이라 펜싱할 클러스터 없음 -> 펜싱 건너뜀", x.name)
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
            if e.status != 403:   # 조회 권한 부족은 종료 증거가 아니다 (체크포인트 규약 8절)
                raise
            return False, f"{src} Pod 종료 확인 불가 (조회 권한 없음)"
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
    held = kube.since(x.rr["metadata"]["creationTimestamp"]) > STOP_HOLD_SECONDS
    if confirmed or (mgr_gone and worker_gone and not held):
        how = f"옛 Workload 삭제 확인 ({why})" if (mgr_gone and worker_gone) else f"요청자가 {K.ANN_STOP_CONFIRMED}=true로 확인"
        advance(x, ("GENERATION_ADVANCED", "OLD_RUN_STOPPED"), "OLD_RUN_STOPPED")
        log.info("%s: 옛 실행 멈춤 (%s) -> OLD_RUN_STOPPED", x.name, how)
        x.save(phase="Executing", stoppedAt=kube.now_rfc3339(), message=how)
        return
    if held:
        # 복구 컨트롤러가 이미 HOLD했을 시간. 뒤늦게 멈춘 게 보여도 혼자 재제출하지 않는다
        if x.st.get("phase") != "Held":
            msg = (f"요청 후 {STOP_HOLD_SECONDS}s 안에 옛 실행 종료를 확인 못 함 ({why}) -> 멈춤 (복구 컨트롤러 HOLD 시간). "
                   f"재개하려면 옛 실행이 멈춘 걸 확인하고 이 요청에 {K.ANN_STOP_CONFIRMED}=true 어노테이션을 붙일 것")
            log.warning("%s: %s", x.name, msg)
            x.save(phase="Held", message=msg)
        return
    # 멈춘 게 확인되지 않으면 재제출하지 않는다 (contracts.md: 종료 확인 전 restart 금지 = 이중 실행 방지)
    # 관리 클러스터 Workload는 워커가 끊겨 있어도 지워진다. 워커 쪽에서 사라진 걸 봐야 멈춘 것
    if kube.since(x.st["isolatedAt"]) > STOP_WAIT_SECONDS and x.st.get("phase") != "WaitingForStop":
        msg = (f"옛 실행 종료를 {STOP_WAIT_SECONDS}s 안에 확인 못 함 ({why}) -> 재제출 보류. 옛 실행이 멈춘 걸 확인했으면 "
               f"이 요청에 {K.ANN_STOP_CONFIRMED}=true 어노테이션을 붙일 것")
        log.warning("%s: %s", x.name, msg)
        x.save(phase="WaitingForStop", message=msg)


PHASE_ORDER = ["GENERATION_ADVANCED", "OLD_RUN_STOPPED", "NEW_RUN_SUBMITTED", "RESUMING", "COMPLETED"]


def advance(x: Ctx, allowed_from, new_phase, extra=None):
    """펜싱 기록 recovery_phase를 new_phase로 (조건부). 이미 그 단계 이후면 그대로 둠"""
    rid = x.spec["requestId"]
    if x.store.set_phase(x.job_id(), rid, x.to_gen(), allowed_from, new_phase, extra):
        return
    rec = x.store.get_fencing(x.job_id()) or {}
    cur = rec.get("recovery_phase")
    if (rec.get("recovery_id") == rid and cur in PHASE_ORDER
            and PHASE_ORDER.index(cur) >= PHASE_ORDER.index(new_phase)):
        return
    raise PermissionError(f"펜싱 기록을 {new_phase}로 못 바꿈 (recovery_id={rec.get('recovery_id')}, "
                          f"phase={cur}, to_gen={rec.get('recovery_to_gen')}) - 다른 복구가 진행 중")


def build_new(x: Ctx):
    target, exclude = R.placement_hints(x.spec["action"], x.st.get("sourceCluster"),
                                        x.st.get("oldExclude"), x.spec.get("excludeClusters"))
    # 만들기 직전에 펜싱 기록을 다시 확인 (to_gen이 매번 새로 읽음)
    orig = x.store.get_jobset(x.job_id())
    # 복구 원본이 이 요청의 잡 것인지 확인 (job_id만으로 찾으므로 네임스페이스·작업 ID를 한 번 더 맞춰 본다)
    ometa = orig.get("metadata") or {}
    if ometa.get("namespace") and ometa["namespace"] != x.ns:
        raise PermissionError(f"복구 원본 네임스페이스 {ometa['namespace']} != 요청 네임스페이스 {x.ns}")
    oid = (ometa.get("labels") or {}).get(K.LABEL_JOB_ID)
    if oid and oid != x.job_id():
        raise PermissionError(f"복구 원본 {K.LABEL_JOB_ID}={oid} != 요청 잡 {x.job_id()}")
    new = R.render(orig, x.job_id(), x.to_gen(), x.spec["requestId"], target, exclude)
    new["metadata"]["namespace"] = x.ns
    return new


def resubmit(x: Ctx):
    ns, _ = x.job()
    new = build_new(x)
    name = new["metadata"]["name"]
    try:
        made = x.co.create_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", new)
    except client.ApiException as e:
        if e.status != 409:
            raise
        # 이미 있음 = 지난 루프에서 만들고 기록 전에 죽은 경우. 같은 복구가 만든 것인지 확인
        made = x.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name)
        if (made["metadata"].get("annotations") or {}).get(K.ANN_RECOVERY_ID) != x.spec["requestId"]:
            raise PermissionError(f"{name}이 이미 있는데 다른 복구가 만든 것") from e
    try:
        advance(x, ("OLD_RUN_STOPPED", "NEW_RUN_SUBMITTED"), "NEW_RUN_SUBMITTED",
                {"new_jobset_name": name, "new_jobset_uid": made["metadata"]["uid"]})
    except PermissionError as e:
        # 만든 사이 펜싱 기록이 다른 복구로 넘어감 -> 이 JobSet은 기록에 없는 실행. 남겨 두면 이중 실행이라 지운다.
        # (끊김 같은 일시 오류는 여기 안 옴: 다음 루프에서 409 -> 같은 복구 확인 -> 다시 기록)
        if withdraw(x, ns, name, made["metadata"]["uid"]):
            raise PermissionError(f"{e} -> 만든 {name} 철회함") from e
        raise PermissionError(f"{e} -> 만든 {ns}/{name} 철회 실패, 손으로 지울 것") from e
    x.save(resubmittedAt=kube.now_rfc3339(), newJob=f"{ns}/{name}")
    ann = new["metadata"]["annotations"]
    log.info("%s: 재제출 %s (%s, exclude=%s, target=%s) -> NEW_RUN_SUBMITTED", x.name, name, K.RECOVERY_QUEUE,
             ann.get(K.ANN_EXCLUDE, "-"), ann.get(K.ANN_TARGET, "-"))


def withdraw(x: Ctx, ns, name, uid):
    """이 요청이 만든 JobSet만 지운다 (uid 전제조건: 그 사이 같은 이름으로 다시 만든 남의 것은 안 지움).
    지웠거나 이미 없으면 True. 요청은 곧 Failed로 끝나 다시 안 오므로 여기서 몇 번 더 시도한다"""
    for i in range(3):
        try:
            x.co.delete_namespaced_custom_object(
                K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name,
                body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=uid),
                                            propagation_policy="Background"))
            log.warning("%s: 기록 못 한 재제출 잡 %s/%s 철회", x.name, ns, name)
            return True
        except client.ApiException as e:
            if e.status in (404, 409):   # 404 이미 없음 / 409 uid가 다름 = 남의 것
                return True
            err = e.status
        except Exception as e:  # noqa: BLE001  끊김
            err = type(e).__name__
        time.sleep(1 + i)
    log.error("%s: 재제출 잡 %s/%s 철회 실패 (%s) - 손으로 지울 것", x.name, ns, name, err)
    return False


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
        if not wl and not jobset_exists(x, ns, name):
            x.save(phase="Failed", message=f"재제출 잡 {ns}/{name}이 입장 전에 사라짐")
            log.warning("%s: 재제출 잡 %s/%s가 사라짐", x.name, ns, name)
        elif x.st.get("phase") != "AdmitDelayed":
            # 늦게라도 입장하면 Done. 실패로 끝내면 늦게 입장해 복구된 잡과 요청 기록이 어긋난다
            msg = f"재제출 잡이 {ADMIT_TIMEOUT_SECONDS}s 안에 입장 못 함 -> 계속 기다림 (JobSet 유지, 복구 판단은 복구 컨트롤러)"
            log.warning("%s: %s", x.name, msg)
            x.save(phase="AdmitDelayed", message=msg)


def jobset_exists(x: Ctx, ns, name):
    try:
        x.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name)
        return True
    except client.ApiException as e:
        if e.status == 404:
            return False
        raise


def step(x: Ctx, all_rrs):
    st = x.st
    if not st.get("acceptedAt"):
        if not accept(x, all_rrs):
            return
    if not st.get("isolatedAt"):
        return isolate(x)
    if not st.get("stoppedAt"):
        return stop_old(x)
    if not st.get("resubmittedAt"):
        return resubmit(x)
    return watch_admit(x)


def reconcile(mgr, workers, store):
    co = kube.kueue(mgr)
    rrs = co.list_cluster_custom_object(K.RR_GROUP, K.RR_VERSION, K.RR_PLURAL)["items"]
    for rr in rrs:
        if (rr.get("status") or {}).get("phase") in FINAL:
            continue
        x = Ctx(mgr, workers, store, rr)
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
    workers = kube.Workers(mgr, own_secret=OWN_SECRET)
    store = recovery_store.from_env(mgr)
    log.info("시작: interval=%ss store=%s", INTERVAL, type(store).__name__)
    while True:
        if not elector.is_leader():
            time.sleep(1)
            continue
        try:
            reconcile(mgr, workers, store)
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
