"""재배치 실행기 (GPU-68) — 관리 클러스터에서 돈다. 노드 격리만 워커에 직접 한다.

보는 것:  RelocationRequest (복구 컨트롤러가 만든다) — spec과 컨트롤러 제어 주석 두 개만
판단:     action에 따라 정해진 순서대로
바꾸는 것: 워커 노드 cordon·taint / MultiKueueCluster 펜싱 라벨 / 잡 내리기·재제출 / 요청 status

action:
  restartInPlace   노드 격리 없이 재제출, 원래 클러스터 우선·부족하면 다른 곳 (ADR-38)
  moveToSpare      고장 노드 cordon·taint -> 복구 우선순위로 재제출, 원래 클러스터 우선·부족하면 다른 곳
                   펜싱 안 함: 그 클러스터의 다른 잡·새 잡은 그대로 받는다
  evacuateCluster  (AZ·클러스터 장애) 클러스터 펜싱 -> 원래 클러스터를 빼고 재제출 (디스패처가 다른 곳을 고름)
                   잡이 아직 워커에 안 보내졌으면 펜싱은 건너뛰고 재제출만

자리는 정하지 않는다. 재제출한 잡도 디스패처를 다시 거친다.

복구 컨트롤러와 나누는 일 (ADR-50. relocator는 S3·DynamoDB에 가지 않는다):
  복구 컨트롤러  판단 -> 펜싱 표 세대 +1 -> S3 원본 읽기·해시 대조 -> 이 요청 생성
                 spec: requestId · jobRef(옛 세대) · action · excludeNodes/Clusters · toGeneration · stopDeadline · jobSet(원본)
  재배치 실행기  격리 -> 옛 실행이 멈춘 걸 확인하면 status.recoveryProgress(OLD_RUN_STOPPED + 종료 증거)
  복구 컨트롤러  status를 보고 펜싱 표에 OLD_RUN_STOPPED 조건부 기록 -> 요청에 devoops/recovery-stop-recorded=true
  재배치 실행기  spec.jobSet에서 6절 값만 바꿔 job-{id}-g{새 세대 4자리} 제출
                 -> status.recoveryProgress(NEW_RUN_SUBMITTED + 관리 클러스터 새 JobSet 이름·UID)
  복구 컨트롤러  펜싱 표에 NEW_RUN_SUBMITTED·new_jobset_name/uid 조건부 기록. RESUMING·COMPLETED도 컨트롤러 몫
  권한을 잃었거나 이미 끝난 복구면 컨트롤러가 devoops/recovery-cancel을 붙인다 -> 실행기가 만든 JobSet을 철회하고 Cancelled

단계마다 요청 status에 시각을 남기고, 다음 루프에서 비어 있는 단계부터 이어서 한다.
JobSet 생성도 같은 이름으로 다시 해도 결과가 같아서(409 -> 같은 복구 확인), 실행기가 중간에 죽었다 살아나도 두 번 하지 않는다.
  acceptedAt -> isolatedAt -> stoppedAt -> resubmittedAt -> admittedAt (Done)

시간 값은 복구 컨트롤러 제한 시간(장애 복구 규약 2절)에 맞춘다:
  옛 실행 종료: spec.stopDeadline(없으면 요청 생성 + 15분) -> 넘으면 Held, 확인 어노테이션으로만 재개
    워커 API에 접속이 안 되면 EC2 인스턴스 상태를 종료 증거로 쓴다 (ec2_evidence.py, 기본 off)
  입장 15분 (ADMIT_TIMEOUT_SECONDS)     -> 넘으면 AdmitDelayed, 실패로 끝내지 않음
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
import socket
import time
from datetime import datetime

from kubernetes import client

import keys as K
import ec2_evidence
import kube
import leader
import recovery_render as R

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "5"))
STOP_WAIT_SECONDS = int(os.getenv("STOP_WAIT_SECONDS", "120"))    # 옛 잡이 이 시간 안에 안 내려가면 WaitingForStop으로 알림
# 복구 컨트롤러는 옛 실행 종료를 15분 기다린 뒤 HOLD한다 (장애 복구 규약 2절). 그 뒤에 실행기가 혼자 재제출하지 않게
# spec.stopDeadline(없으면 RR 생성 + 이 시간)이 지나면 Held. 확인 어노테이션(수동 재개)만 받는다
STOP_HOLD_SECONDS = int(os.getenv("STOP_HOLD_SECONDS", "900"))
# 재제출 잡이 이 시간 안에 입장 못 하면 AdmitDelayed로 알리고 계속 기다린다 (JobSet은 그대로 둠).
# 복구 성공·실패 판단은 복구 컨트롤러 몫 (복원·학습 진행 확인 30분 뒤 HOLD)
ADMIT_TIMEOUT_SECONDS = int(os.getenv("ADMIT_TIMEOUT_SECONDS", "900"))
# EKS마다 따로 만든 relocator SA 토큰 kubeconfig (노드 권한을 MultiKueue와 나눔). 없으면 MultiKueue Secret
OWN_SECRET = os.getenv("RELOCATOR_KUBECONFIG_SECRET", "{name}-relocator-kubeconfig")

kube.setup_logging("relocator")
log = kube.logging.getLogger("relocator")

FINAL = {"Done", "Failed", "Duplicate", "Cancelled"}
PROGRESS_SCHEMA = 1   # status.recoveryProgress 계약 버전 (복구 컨트롤러 relocation_status.py와 맞춤)


class Ctx:
    def __init__(self, mgr, workers, rr, ec2=None):
        self.mgr, self.workers, self.rr = mgr, workers, rr
        self.ec2 = ec2   # ec2_evidence.Ec2Evidence 또는 None (EC2 증거 안 씀)
        self.co = kube.kueue(mgr)
        self.ns = rr["metadata"]["namespace"]
        self.name = rr["metadata"]["name"]
        self.spec = rr["spec"]
        self.st = dict(rr.get("status") or {})

    def save(self, **kw):
        self.st.update(kw)
        self.co.patch_namespaced_custom_object_status(
            K.RR_GROUP, K.RR_VERSION, self.ns, K.RR_PLURAL, self.name, {"status": self.st})

    def ann(self, key):
        return (self.rr["metadata"].get("annotations") or {}).get(key)

    def job(self):
        # 학습 잡 네임스페이스(training)에 있는 요청이, 같은 네임스페이스의 잡만 다룬다
        ns, _, name = self.spec["jobRef"].partition("/")
        if self.ns != K.NAMESPACE:
            raise PermissionError(f"요청 네임스페이스 {self.ns} != 학습 잡 네임스페이스 {K.NAMESPACE}")
        if ns != self.ns:
            raise PermissionError(f"jobRef 네임스페이스 {ns} != 요청 네임스페이스 {self.ns}")
        return ns, name

    # status(jobId·toGeneration)는 기록용. 다른 주체도 status를 쓸 수 있으므로 판단에는 쓰지 않고
    # 매번 spec(만든 뒤 바꿀 수 없음)에서 다시 구한다
    def job_id(self):
        return R.parse_name(self.job()[1])[0]

    def old_gen(self):
        return R.parse_name(self.job()[1])[1]

    def to_gen(self):
        """spec.toGeneration = 옛 세대 + 1 (복구 컨트롤러가 세대 +1 뒤에만 요청을 만든다). 아니면 PermissionError"""
        to_gen, old_gen = self.spec.get("toGeneration"), self.old_gen()
        if type(to_gen) is not int or old_gen is None or to_gen != old_gen + 1:
            raise PermissionError(f"spec.toGeneration={to_gen!r}이 옛 세대 {old_gen} + 1이 아님")
        return to_gen

    def original(self):
        """spec.jobSet = 복구 컨트롤러가 S3에서 읽고 해시를 대조한 원본 JobSet"""
        orig = self.spec.get("jobSet")
        if not isinstance(orig, dict) or orig.get("kind") != "JobSet":
            raise PermissionError("spec.jobSet에 원본 JobSet이 없음")
        return orig


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
    to_gen = x.to_gen()      # 형식이 틀린 요청은 PermissionError -> Failed
    x.original()
    wl = kube.workload_for_jobset(x.mgr, ns, name)
    if not wl:
        x.save(phase="Failed", message=f"잡 {ns}/{name}의 Workload를 못 찾음", acceptedAt=kube.now_rfc3339())
        return False
    source = (wl.get("status") or {}).get("clusterName") or ""
    old_exclude = ""
    try:
        old = x.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name)
        old_exclude = (old["metadata"].get("annotations") or {}).get(K.ANN_EXCLUDE, "")
    except client.ApiException as e:
        if e.status != 404:
            raise
    x.save(phase="Accepted", sourceCluster=source, jobId=job_id, fromGeneration=old_gen, toGeneration=to_gen,
           oldExclude=old_exclude, acceptedAt=kube.now_rfc3339(), message="접수",
           seenClusters=sorted(set(x.st.get("seenClusters") or []) | copy_clusters(wl)),
           **({"gpus": sum(kube.workload_pods(wl))} if wl.get("spec", {}).get("podSets") else {}))
    log.info("%s: 접수 %s %s/%s (지금 %s, 세대 %s -> %s)", x.name, x.spec["action"], ns, name,
             source or "워커 미정", old_gen, to_gen)
    return True


def copy_clusters(wl) -> set:
    """옛 잡의 복사본이 있을 수 있는 워커: 입장한 곳(clusterName) + 디스패처가 지명한 곳(nominatedClusterNames).
    지명만 된 상태에서도 MultiKueue가 그 워커에 복사본을 만들고 Pod가 뜰 수 있다"""
    st = (wl or {}).get("status") or {}
    return {c for c in [st.get("clusterName"), *(st.get("nominatedClusterNames") or [])] if c}


def note_copies(x: Ctx, wl):
    """관리 클러스터 Workload가 보이는 동안 복사본이 있을 수 있는 워커를 status에 모아 둔다.
    Workload가 지워진 뒤에는 어디에 보냈었는지 알 길이 없어서, 종료 확인(old_gone_on_worker)이 볼 곳을 미리 적어 둠"""
    seen = set(x.st.get("seenClusters") or [])
    new = copy_clusters(wl) - seen
    if new:
        log.info("%s: 옛 잡 복사본이 있을 수 있는 워커 추가 %s", x.name, sorted(new))
        x.save(seenClusters=sorted(seen | new))


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


def record_old_nodes(x: Ctx, src):
    """옛 잡 Pod가 있던 노드 [{node, instanceId}] = terminate 모드 강제 종료 대상. 워커 접속이 안 되거나 실패하면 빈 목록"""
    ns, name = x.job()
    try:
        api = (x.workers.get() if x.workers else {}).get(src)
        if api is None:
            return []
        core = client.CoreV1Api(api)
        pods = core.list_namespaced_pod(ns, label_selector=f"jobset.sigs.k8s.io/jobset-name={name}",
                                        _request_timeout=kube.TIMEOUT).items
        out = []
        for node in sorted({p.spec.node_name for p in pods if p.spec.node_name}):
            n = core.read_node(node, _request_timeout=kube.TIMEOUT)
            out.append({"node": node, "instanceId": ec2_evidence.instance_id(n.spec.provider_id)})
        return out
    except Exception as e:  # noqa: BLE001  기록은 보조 정보라 실패해도 격리는 계속
        log.warning("%s: %s 옛 노드 기록 실패 (%s) - EC2 강제 종료 대상 없음", x.name, src, type(e).__name__)
        return []


def current_source(x: Ctx):
    """옛 잡이 지금 보내져 있는 워커 (관리 클러스터 Workload 기준). Workload가 없거나 아직 안 보내졌으면 빈 문자열"""
    ns, name = x.job()
    wl = kube.workload_for_jobset(x.mgr, ns, name)
    return ((wl or {}).get("status") or {}).get("clusterName") or ""


def isolate(x: Ctx):
    # 노드 cordon·클러스터 펜싱은 되돌리기 어려운 일이라 status(다른 주체도 쓸 수 있음)를 믿지 않는다:
    # 대상 클러스터를 Workload에서 다시 구한다. 권한을 잃은 요청은 컨트롤러가 취소 주석으로 멈춘다(step 맨 앞)
    x.to_gen()
    action, src = x.spec["action"], current_source(x)
    if src != (x.st.get("sourceCluster") or ""):
        # 접수 뒤 디스패처가 막 보냈거나(빈 값 -> 워커), status가 바뀐 경우. 어느 쪽이든 Workload가 맞다
        log.warning("%s: status의 sourceCluster=%r 대신 Workload 기준 %r 사용", x.name, x.st.get("sourceCluster"), src)
        x.save(sourceCluster=src)
    if src and "oldNodes" not in x.st:
        # 워커가 아직 살아 있을 때 옛 잡이 돌던 노드·인스턴스를 적어 둔다. 나중에 워커가 끊기면 강제 종료 대상 (증거 아님)
        x.st["oldNodes"] = record_old_nodes(x, src)
    nodes = x.spec.get("excludeNodes") or []
    if action == "moveToSpare" and nodes:
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


class Unreachable(str):
    """워커 API에 닿지 못해서 확인 못 했다는 이유 (EC2 증거로 넘어가도 되는 경우)"""


def old_gone_on_worker(x: Ctx, ns, name):
    """옛 잡의 복사본이 있을 수 있는 모든 워커에서 JobSet·Workload·Pod가 전부 없어졌는지. (멈춤확인, 이유)
    볼 곳 = 입장했던 워커(sourceCluster) + 지명만 됐던 워커(seenClusters). 하나라도 확인 못 하면 멈췄다고 보지 않는다"""
    x.stop_evidence = []  # 이번 조회에서 확인한 워커별 근거
    places = sorted({c for c in [x.st.get("sourceCluster"), *(x.st.get("seenClusters") or [])] if c})
    if not places:
        return True, "워커에 보내지기 전이었음"
    whys = []
    for src in places:
        gone, why = gone_on(x, src, ns, name)
        kind = "automatic"
        if not gone and isinstance(why, Unreachable):   # 닿지 않는 워커는 그 워커의 EC2 증거로만 대신한다
            gone, why = ec2_stop_evidence(x, why, src)
            kind = "ec2"
        if not gone:
            return False, why
        x.stop_evidence.append({"cluster": src, "kind": kind, "detail": str(why)})
        whys.append(why)
    return True, "; ".join(whys)


def gone_on(x: Ctx, src, ns, name):
    """워커 src에서 옛 잡이 사라졌는지. 워커에 접속이 안 되면 확인 불가 = 멈췄다고 보지 않는다"""
    api = x.workers.get().get(src)
    if api is None:
        return False, Unreachable(f"{src} 접속 정보 없음")
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
        return False, Unreachable(f"{src} 접속 안 됨 ({type(e).__name__})")


def _epoch(value):
    """epoch 초(숫자·문자열) 또는 시간대가 있는 RFC3339 -> epoch 초"""
    try:
        return float(value)
    except (TypeError, ValueError):
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("종료 마감 시각에 시간대가 없음")
        return parsed.timestamp()


def stop_held(x: Ctx):
    """spec.stopDeadline(복구 컨트롤러가 정한 종료 마감)이 지났는지. 없으면 요청 생성 + 15분"""
    deadline = x.spec.get("stopDeadline")
    if deadline not in (None, ""):
        return time.time() >= _epoch(deadline)
    return kube.since(x.rr["metadata"]["creationTimestamp"]) > STOP_HOLD_SECONDS


def progress(x: Ctx, phase, evidence, new_jobset=None):
    """status.recoveryProgress: 복구 컨트롤러가 펜싱 표로 옮기는 결과 (relocation_status.py 계약 v1)"""
    out = {"schemaVersion": PROGRESS_SCHEMA, "requestId": x.spec["requestId"], "jobId": x.job_id(),
           "fromGeneration": x.old_gen(), "toGeneration": x.to_gen(), "phase": phase,
           "oldRunStopEvidence": evidence}
    if new_jobset:
        out["newJobSet"] = new_jobset
    return out


def stop_old(x: Ctx):
    ns, name = x.job()
    note_copies(x, kube.workload_for_jobset(x.mgr, ns, name))   # 지우기 전에: 접수 뒤 디스패처가 지명했을 수 있음
    try:
        x.co.delete_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name,
                                             propagation_policy="Background")
        log.info("%s: 옛 잡 %s/%s 삭제 요청", x.name, ns, name)
    except client.ApiException as e:
        if e.status != 404:
            raise
    left = kube.workload_for_jobset(x.mgr, ns, name)
    note_copies(x, left)
    mgr_gone = left is None
    worker_gone, why = (old_gone_on_worker(x, ns, name) if mgr_gone else (False, "관리 클러스터 Workload 남아 있음"))
    confirmed = x.ann(K.ANN_STOP_CONFIRMED) == "true"
    held = stop_held(x)
    if confirmed or (mgr_gone and worker_gone and not held):
        how = f"요청자가 {K.ANN_STOP_CONFIRMED}=true로 확인" if confirmed else f"옛 Workload 삭제 확인 ({why})"
        stopped_at = kube.now_rfc3339()
        checks = [] if confirmed else getattr(x, "stop_evidence", [])
        evidence = {
            "generation": x.old_gen(), "namespace": ns, "jobset_name": name,
            "clusters": sorted({c for c in [x.st.get("sourceCluster"), *(x.st.get("seenClusters") or [])] if c}),
            "kind": "manual" if confirmed else ("ec2" if any(c["kind"] == "ec2" for c in checks) else "automatic"),
            "checks": checks, "confirmed_at": stopped_at,
            "confirmed_by": "unknown (RR annotation)" if confirmed else "relocator",
            "recorded_by": os.getenv("POD_NAME") or f"{socket.gethostname()}-{os.getpid()}",
            "detail": how,
        }
        # 펜싱 표 기록은 복구 컨트롤러가 이 status를 보고 한다. 증거는 이후 바꾸지 않는다 (NEW_RUN_SUBMITTED에도 그대로)
        x.save(phase="Executing", stoppedAt=stopped_at, message=how,
               recoveryProgress=progress(x, "OLD_RUN_STOPPED", evidence))
        log.info("%s: 옛 실행 멈춤 (%s) -> status OLD_RUN_STOPPED", x.name, how)
        return
    if held:
        # 복구 컨트롤러가 이미 HOLD했을 시간. 뒤늦게 멈춘 게 보여도 혼자 재제출하지 않는다
        if x.st.get("phase") != "Held":
            msg = (f"종료 대기 기한 안에 옛 실행 종료를 확인 못 함 ({why}) -> 멈춤 (복구 컨트롤러 HOLD 시간). "
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


def ec2_stop_evidence(x: Ctx, why, src=None):
    """워커 src에 닿지 못할 때 EC2 인스턴스 상태로 종료 확인. (멈춤확인, 이유)
    증거는 그 클러스터 학습·예비 인스턴스 전부 꺼짐뿐 (기록한 옛 노드만 보면 망 분리 중 다른 노드로 다시 뜬 Pod를 놓침).
    src를 안 주면 입장했던 워커(sourceCluster). 지명만 됐던 워커(seenClusters)도 같은 기준으로 본다.
    terminate 모드면 워커가 STOP_WAIT_SECONDS 넘게 안 닿을 때 기록해 둔 옛 노드만 한 번 내린다 (옛 실행을 끊는 용도).
    기록한 옛 노드는 입장했던 워커의 것이라, 내리기는 src가 sourceCluster일 때만"""
    ec2 = getattr(x, "ec2", None)
    src = src or x.st.get("sourceCluster")
    if ec2 is None or not src:
        return False, why
    is_source = src == x.st.get("sourceCluster")
    old_ids = [n["instanceId"] for n in x.st.get("oldNodes") or [] if n.get("instanceId")] if is_source else []
    ok, ev, _ = ec2.check(src)
    if (not ok and old_ids and ec2.can_terminate and not x.st.get("ec2TerminatedAt")
            and kube.since(x.st["isolatedAt"]) > STOP_WAIT_SECONDS):
        try:
            done = ec2.terminate(src, old_ids)
            x.save(ec2TerminatedAt=kube.now_rfc3339(), ec2Terminated=done)
            ev += f" -> 옛 노드 강제 종료 요청 {done}"
        except Exception as e:  # noqa: BLE001  권한 없음·태그 불일치 등. 사람 확인 경로로 남음
            x.save(ec2TerminatedAt=kube.now_rfc3339(), ec2Terminated=[], ec2TerminateError=str(e))
            ev += f" -> 강제 종료 실패 ({type(e).__name__}: {e})"
    return ok, f"{why}; {ev}"


def build_new(x: Ctx):
    target, exclude = R.placement_hints(x.spec["action"], x.st.get("sourceCluster"),
                                        x.st.get("oldExclude"), x.spec.get("excludeClusters"))
    orig = x.original()
    # 복구 원본이 이 요청의 잡 것인지 확인 (컨트롤러가 해시를 대조했지만 네임스페이스·작업 ID를 한 번 더 맞춰 본다)
    ometa = orig.get("metadata") or {}
    if ometa.get("namespace") and ometa["namespace"] != x.ns:
        raise PermissionError(f"복구 원본 네임스페이스 {ometa['namespace']} != 요청 네임스페이스 {x.ns}")
    oid = (ometa.get("labels") or {}).get(K.LABEL_JOB_ID)
    if oid and oid != x.job_id():
        raise PermissionError(f"복구 원본 {K.LABEL_JOB_ID}={oid} != 요청 잡 {x.job_id()}")
    preferred = x.st.get("sourceCluster") if x.spec["action"] in R.PREFER_SOURCE else None
    new = R.render(orig, x.job_id(), x.to_gen(), x.spec["requestId"], target, exclude, preferred=preferred)
    new["metadata"]["namespace"] = x.ns
    return new


def resubmit(x: Ctx):
    # 컨트롤러가 펜싱 표에 OLD_RUN_STOPPED를 기록했다는 확인이 와야 새 세대를 낸다 (ADR-36 순서: 종료 기록 -> 재제출)
    if x.ann(K.ANN_STOP_RECORDED) != "true":
        msg = "복구 컨트롤러의 OLD_RUN_STOPPED 기록 확인 대기"
        if x.st.get("message") != msg:
            x.save(message=msg)
            log.info("%s: %s", x.name, msg)
        return
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
    uid = made["metadata"]["uid"]
    evidence = (x.st.get("recoveryProgress") or {}).get("oldRunStopEvidence")
    if not isinstance(evidence, dict):
        raise RuntimeError("status.recoveryProgress에 종료 증거가 없음")
    x.save(resubmittedAt=kube.now_rfc3339(), newJob=f"{ns}/{name}", newJobUid=uid, message="재제출",
           recoveryProgress=progress(x, "NEW_RUN_SUBMITTED", evidence, {"name": name, "uid": uid}))
    ann = new["metadata"]["annotations"]
    log.info("%s: 재제출 %s (%s, exclude=%s, target=%s) -> status NEW_RUN_SUBMITTED", x.name, name, K.RECOVERY_QUEUE,
             ann.get(K.ANN_EXCLUDE, "-"), ann.get(K.ANN_TARGET, "-"))


def withdraw(x: Ctx, ns, name, uid):
    """이 요청이 만든 JobSet만 지운다 (uid 전제조건: 그 사이 같은 이름으로 다시 만든 남의 것은 안 지움).
    지웠거나 이미 없으면 True"""
    for i in range(3):
        try:
            x.co.delete_namespaced_custom_object(
                K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name,
                body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=uid),
                                            propagation_policy="Background"))
            log.warning("%s: 재제출 잡 %s/%s 철회", x.name, ns, name)
            return True
        except client.ApiException as e:
            if e.status in (404, 409):   # 404 이미 없음 / 409 uid가 다름 = 남의 것
                return True
            err = e.status
        except Exception as e:  # noqa: BLE001  끊김
            err = type(e).__name__
        time.sleep(1 + i)
    log.error("%s: 재제출 잡 %s/%s 철회 실패 (%s) - 다음 루프에서 다시", x.name, ns, name, err)
    return False


def cancel(x: Ctx):
    """복구 컨트롤러의 취소 신호: 이 요청이 만든 새 세대 JobSet을 철회하고 Cancelled.
    status를 쓰기 전에 죽었을 수 있으니 새 세대 이름으로 찾고, 이 요청의 recovery-id가 붙은 것만 지운다.
    철회에 실패하면 Cancelled로 끝내지 않는다 (다음 루프에서 다시)"""
    js = None
    try:
        ns, _ = x.job()
        name = R.jobset_name(x.job_id(), x.to_gen())
    except PermissionError:   # 형식이 틀린 요청은 JobSet을 만들었을 리 없다
        ns = name = None
    if name:
        try:
            js = x.co.get_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name)
        except client.ApiException as e:
            if e.status != 404:
                raise
    withdrawn = ""
    if js and (js["metadata"].get("annotations") or {}).get(K.ANN_RECOVERY_ID) == x.spec["requestId"]:
        if not withdraw(x, ns, name, js["metadata"]["uid"]):
            x.save(message=f"취소: {ns}/{name} 철회 실패, 다음 루프에서 다시")
            return
        withdrawn = f", {ns}/{name} 철회"
    reason = x.ann(K.ANN_CANCEL)
    x.save(phase="Cancelled", cancelledAt=kube.now_rfc3339(), message=f"복구 컨트롤러 취소 ({reason}){withdrawn}")
    log.warning("%s: 취소 (%s)%s", x.name, reason, withdrawn)


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
    action = x.spec.get("action")
    if action not in (*R.PREFER_SOURCE, "evacuateCluster"):
        raise PermissionError(f"지원하지 않는 재배치 action: {action}")
    if x.ann(K.ANN_CANCEL):   # 어느 단계에서든 취소가 먼저
        return cancel(x)
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


def wanted(rr):
    """이번 루프에서 볼 요청: 끝나지 않은 요청 + 끝났어도 취소가 붙은 요청(Done 뒤 권한을 잃은 경우 철회).
    Duplicate·Cancelled는 만든 JobSet이 없거나 이미 철회했으므로 다시 보지 않는다"""
    phase = (rr.get("status") or {}).get("phase")
    if phase not in FINAL:
        return True
    return phase in ("Done", "Failed") and bool((rr["metadata"].get("annotations") or {}).get(K.ANN_CANCEL))


def reconcile(mgr, workers, ec2=None):
    co = kube.kueue(mgr)
    rrs = co.list_cluster_custom_object(K.RR_GROUP, K.RR_VERSION, K.RR_PLURAL)["items"]
    for rr in rrs:
        if not wanted(rr):
            continue
        x = Ctx(mgr, workers, rr, ec2)
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
    ec2 = ec2_evidence.from_env()
    log.info("시작: interval=%ss ec2=%s", INTERVAL,
             "off" if ec2 is None else ("terminate" if ec2.can_terminate else "describe"))
    while True:
        if not elector.is_leader():
            time.sleep(1)
            continue
        try:
            reconcile(mgr, workers, ec2)
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
