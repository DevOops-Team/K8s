"""빈 패킹 디스패처 (GPU-67) — 관리 클러스터에서 돈다.

보는 것:  관리 클러스터에서 쿼터는 잡았는데(QuotaReserved) 아직 워커가 안 정해진 Workload
판단:     워커별 남은 GPU(쿼터 + 노드) 계산 -> 펜싱·끊김·exclude 클러스터 제외 -> 들어가는 곳 중 가장 꽉 찬 곳
바꾸는 것: Workload.status.nominatedClusterNames (Kueue MultiKueue가 그 워커에만 복사본을 만든다)

추가 규칙:
  - 지명했는데 RENOMINATE_SECONDS 안에 워커에서 입장 못 하면 다른 곳으로 다시 지명
  - 워커가 전부 끊기면 아무것도 안 함 (pfSense 장애처럼 "우리 쪽 문제"일 수 있음)
  - 찜: 쿼터는 받았는데 들어갈 워커가 없는 잡에게 워커 하나를 잡아 두고(devoops/reserved-cluster),
    그 워커에는 뒤 순서 학습 잡을 안 보낸다. 복구 잡은 학습 잡의 찜을 무시하고 들어간다.
    관리 Kueue는 워커 합계로 쿼터를 주므로 쿼터를 받아도 한 워커에 자리가 있다는 보장이 없다.
    찜이 없으면 작은 잡이 빈 칸을 계속 메워 큰 잡이 쿼터만 쥐고 기다린다 (11/10 compare: 쿼터 뒤 142s, 상한 없음).
    11/09에 뺀 비워 두기(STARVE_SECONDS)와 달리 기다리는 시간이 없고, 찜한 워커를 Workload에 적어 고정한다.
  - 재배치 자리 지키기: 같은 클러스터로 다시 띄우는 재배치(restartInPlace·moveToSpare)가 진행 중이면
    그 클러스터를 재제출 잡 몫으로 찜한 것처럼 다룬다 (다른 잡은 그 클러스터로 안 보냄).
    재배치 실행기는 옛 잡을 먼저 내리고 새 잡을 넣는데, 그 사이 풀린 자리를 쿼터를 쥐고 기다리던 학습 잡이
    가져가면 복구 잡이 들어갈 곳이 없어진다. 새 잡이 지명되거나 RELOCATION_HOLD_SECONDS가 지나면 푼다.
  - 잡 하나의 실패(조회·patch 오류)는 그 잡만 건너뛴다. 뒤 잡은 계속 본다.
  - 배치 기록: 실행 중인 잡의 Pod가 전부 Running이면 실제 EKS·노드를 Workload devoops/placement에 남긴다.
    지우지 않으므로 장애 직전 배치가 남고, Backend 복구 조회 API가 읽는다 (GPU-268, PLACEMENT.md).
  - fenced ACK: 루프를 끝까지 돌면 그 루프에서 fenced로 본 MultiKueueCluster에 devoops/fenced-ack=<루프 시작 시각>.
    라벨 붙인 시각보다 뒤면, 라벨을 모르던 루프는 다 끝났다는 뜻 (EKS OFF 절차, GPU-125).
"""
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
import time

from kubernetes import client

import keys as K
import kube
import leader
import recovery_render as R
import quota_hold
from placement import Cluster, choose, reserve, eligible

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "5"))
RENOMINATE_SECONDS = int(os.getenv("RENOMINATE_SECONDS", "60"))
# 진행 중인 재배치의 원래 클러스터를 재제출 잡 몫으로 지키는 최대 시간 (격리 시각, 옛 실행 종료 뒤엔 그 시각부터).
# 넘으면 푼다: 옛 실행이 안 멈추거나 복구 잡이 관리 쿼터를 못 받는 동안 클러스터 하나를 계속 막지 않게
RELOCATION_HOLD_SECONDS = int(os.getenv("RELOCATION_HOLD_SECONDS", "300"))
# 지명도 찜도 못 하는 잡의 경고를 같은 이유로 다시 남기는 간격 (매 루프 반복 방지, GPU-237)
UNPLACED_LOG_SECONDS = int(os.getenv("UNPLACED_LOG_SECONDS", "300"))
# 배치 기록(devoops/placement)의 관측 시각을 내용이 같아도 다시 쓰는 간격 (GPU-268).
# 복구 컨트롤러는 관측 시각이 max_age_seconds(기본 300)보다 오래되면 거절한다. PLACEMENT.md의 신선도 절 참고
PLACEMENT_REFRESH_SECONDS = int(os.getenv("PLACEMENT_REFRESH_SECONDS", "60"))
IN_PLACE = {"restartInPlace", "moveToSpare"}   # 재제출 잡이 원래 클러스터로 돌아오는 action
RR_FINAL = {"Done", "Failed", "Duplicate"}

kube.setup_logging("dispatcher")
log = kube.logging.getLogger("dispatcher")
_unplaced = {}   # Workload 이름 -> (이유, 마지막으로 남긴 시각). 갈 곳 없는 잡 경고의 반복 방지


def cluster_state(mgr, workers) -> list:
    out = []
    co = kube.kueue(mgr)
    for mkc in co.list_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters")["items"]:
        name = mkc["metadata"]["name"]
        c = Cluster(name=name,
                    active=kube.is_true(mkc, "Active"),
                    fenced=(mkc["metadata"].get("labels") or {}).get(K.LABEL_FENCED) == "true")
        api = workers.get().get(name)
        if c.active and api is not None:
            try:
                fill_capacity(c, api)
            except client.ApiException as e:
                if e.status in (401, 403):   # 끊김이 아니라 권한 문제. 로그로 분명히 구분
                    log.error("%s 권한 없음 (%s): 워커 kubeconfig 계정에 nodes·pods list, clusterqueues get 권한 필요",
                              name, e.status)
                else:
                    log.warning("%s 용량 조회 실패: %s %s", name, e.status, e.reason)
                c.active = False
            except Exception as e:  # noqa: BLE001  워커 조회 실패 = 이번 루프에선 못 쓰는 클러스터
                log.warning("%s 용량 조회 실패 (끊김?): %s", name, e)
                c.active = False
        out.append(c)
    return out


def fill_capacity(c: Cluster, api):
    c.quota_total, c.recovery_total, c.node_total = 0, 0, 0
    # 1) 쿼터: 워커 ClusterQueue마다 nominalQuota - 예약량
    #    lendingLimit 0인 CQ(복구 전용)는 복구 잡만 쓸 수 있는 몫으로 따로 셈.
    #    복구 CQ가 학습 몫을 빌려 쓰고 있으면(예약 > nominal) 그만큼 학습 몫에서 뺌
    for cq in kube.kueue(api).list_cluster_custom_object(
            K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", _request_timeout=kube.TIMEOUT)["items"]:
        nominal, private = 0, False
        for rg in cq["spec"].get("resourceGroups", []):
            for fl in rg["flavors"]:
                for r in fl["resources"]:
                    if r["name"] == K.GPU:
                        nominal += int(r["nominalQuota"])
                        private = private or str(r.get("lendingLimit", "")) == "0"
        reserved = 0
        for fl in (cq.get("status") or {}).get("flavorsReservation") or []:
            for r in fl["resources"]:
                if r["name"] == K.GPU:
                    reserved += int(r["total"])
        if private:
            c.recovery_total += nominal
            c.recovery_free += max(nominal - reserved, 0)
            c.quota_free -= max(reserved - nominal, 0)   # 빌려 간 학습 몫
        else:
            c.quota_total += nominal
            c.quota_free += nominal - reserved
    fill_node_capacity(c, api, running=c.running)


def fill_node_capacity(c, api, excluded_node=None, released_job=None, running=None):
    # 2) 노드: cordon 안 된 Ready GPU 노드의 allocatable - 올라간 Pod 요청
    #    예비 노드(devoops/spare taint)는 자리 표시 Pod가 있든 없든 전부 복구 잡 몫 (학습 잡 Pod는 taint로 못 감)
    #    일반 노드의 자리 표시 Pod 자리도 복구 잡 몫 (예비 taint 도입 전 구성 호환)
    #    노드를 아직 못 잡은 GPU Pod(입장했지만 nodeName 없음)도 곧 노드 하나를 쓰므로 미리 뺀다 (GPU-260).
    #    blockAdmission을 끄면 워커가 앞 잡 Pod Ready 전에 다음 잡을 받으므로, 빼지 않으면 같은 자리를 두 번 내줄 수 있다.
    #    예비 taint를 견디는 Pod(복구 잡)는 예비 자리부터, 나머지는 일반 노드 자리에서 뺀다. 자리 표시 Pod는 세지 않는다.
    core = client.CoreV1Api(api)
    used, held = {}, {}
    pending_normal, pending_spare = 0, 0
    pods = core.list_pod_for_all_namespaces(
        field_selector="status.phase!=Succeeded,status.phase!=Failed", _request_timeout=kube.TIMEOUT).items
    if running is not None:   # 배치 기록용: 이 Pod 목록을 읽은 시각과 JobSet별 Running 노드 (추가 조회 없음)
        running.clear()
        c.observed_at = int(time.time())
        collect_running(pods, running)
    for p in pods:
        if released_job and getattr(p, 'metadata', None):
            namespace, jobset = released_job
            if (p.metadata.namespace == namespace
                    and (p.metadata.labels or {}).get('jobset.sigs.k8s.io/jobset-name') == jobset):
                continue
        g = sum(int(((ct.resources.requests or {}) if ct.resources else {}).get(K.GPU, 0)) for ct in p.spec.containers)
        if not p.spec.node_name:
            if g and p.spec.priority_class_name != K.PLACEHOLDER_PRIORITY_CLASS:
                if any(getattr(t, "key", None) == K.TAINT_SPARE for t in getattr(p.spec, "tolerations", None) or []):
                    pending_spare += g
                else:
                    pending_normal += g
            continue
        if p.spec.priority_class_name == K.PLACEHOLDER_PRIORITY_CLASS:
            held[p.spec.node_name] = held.get(p.spec.node_name, 0) + g
        else:
            used[p.spec.node_name] = used.get(p.spec.node_name, 0) + g
    for n in core.list_node(label_selector=K.GPU_NODE_SELECTOR, _request_timeout=kube.TIMEOUT).items:
        ready = any(x.type == "Ready" and x.status == "True" for x in n.status.conditions or [])
        taints = n.spec.taints or []
        if (n.metadata.name == excluded_node or n.spec.unschedulable or not ready
                or any(t.key == K.TAINT_FAULTY for t in taints)):
            continue
        alloc = int((n.status.allocatable or {}).get(K.GPU, 0))
        if any(t.key == K.TAINT_SPARE for t in (n.spec.taints or [])):
            c.spare_free += max(alloc - used.get(n.metadata.name, 0), 0)
            continue
        free = alloc - used.get(n.metadata.name, 0) - held.get(n.metadata.name, 0)
        c.node_total = (c.node_total or 0) + max(alloc - held.get(n.metadata.name, 0), 0)
        c.node_free += max(free, 0)
        c.spare_free += held.get(n.metadata.name, 0)
    spare = min(pending_spare, max(c.spare_free, 0))
    c.spare_free -= spare
    c.node_free = max(c.node_free - pending_normal - (pending_spare - spare), 0)


def collect_running(pods, running):
    """(namespace, JobSet 이름) -> Running GPU 학습 Pod가 올라간 노드 목록. 자리 표시·지워지는 Pod는 뺀다"""
    for p in pods:
        meta, status = getattr(p, "metadata", None), getattr(p, "status", None)
        if (meta is None or getattr(status, "phase", None) != "Running" or not p.spec.node_name
                or getattr(meta, "deletion_timestamp", None)
                or p.spec.priority_class_name == K.PLACEHOLDER_PRIORITY_CLASS):
            continue
        jobset = (meta.labels or {}).get("jobset.sigs.k8s.io/jobset-name")
        g = sum(int(((ct.resources.requests or {}) if ct.resources else {}).get(K.GPU, 0)) for ct in p.spec.containers)
        if jobset and g:
            running.setdefault((meta.namespace, jobset), []).append(p.spec.node_name)


def placement_of(wl, c):
    """Workload의 지금 배치 기록. 학습 Pod가 전부 서로 다른 노드에서 Running일 때만, 아니면 None.
    노드당 GPU 1장·Pod당 GPU 1장 구성이라 Running Pod 수 = 노드 수 = 요청 GPU 수여야 한다"""
    st, meta = wl.get("status") or {}, wl["metadata"]
    if st.get("clusterName") != c.name or meta.get("deletionTimestamp") or kube.is_true(wl, "Finished"):
        return None
    kind, jobset = kube.workload_owner(wl)
    if kind != "JobSet":
        return None
    _, generation = R.parse_name(jobset)
    gpus = sum(kube.workload_pods(wl))
    nodes = c.running.get((meta["namespace"], jobset), [])
    if generation is None or not gpus or len(nodes) != gpus or len(set(nodes)) != gpus:
        return None
    return {"version": 1, "jobset": jobset, "generation": generation, "cluster": c.name,
            "nodes": sorted(nodes), "observed_at": c.observed_at}


def old_placement(wl):
    try:
        old = json.loads((wl["metadata"].get("annotations") or {}).get(K.ANN_PLACEMENT) or "null")
    except ValueError:
        return None
    return old if isinstance(old, dict) else None


def record_placements(mgr, all_wls, clusters):
    """실행 중인 잡의 실제 배치(EKS·노드)를 관리 클러스터 Workload에 남긴다 (GPU-268).
    조건이 안 맞으면(Pod 일부 죽음·스케줄 전·워커 조회 실패) 쓰지 않고, 지우지도 않는다 -> 장애 직전 배치가 남는다.
    같은 내용이면 PLACEMENT_REFRESH_SECONDS마다 관측 시각만 갱신. 실패는 그 잡만 건너뛴다 (지명에 영향 없음)"""
    by_name = {c.name: c for c in clusters if c.active and c.observed_at}
    for wl in all_wls:
        c = by_name.get((wl.get("status") or {}).get("clusterName"))
        if c is None:
            continue
        rec = placement_of(wl, c)
        if rec is None:
            continue
        old = old_placement(wl)
        if old is not None:
            same = all(old.get(k) == rec[k] for k in rec if k != "observed_at")
            age = rec["observed_at"] - (old.get("observed_at") if type(old.get("observed_at")) is int else 0)
            if age <= 0 or (same and age < PLACEMENT_REFRESH_SECONDS):
                continue
        try:
            set_placement(mgr, wl, rec)
        except Exception as e:  # noqa: BLE001
            log.warning("%s: 배치 기록 못 씀: %s", wl["metadata"]["name"], e)
            continue
        if old is None or not same:
            log.info("%s: 배치 기록 %s %s (세대 %d)", wl["metadata"]["name"], rec["cluster"], rec["nodes"],
                     rec["generation"])


def set_placement(mgr, wl, rec):
    kube.kueue(mgr).patch_namespaced_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, wl["metadata"]["namespace"], "workloads", wl["metadata"]["name"],
        {"metadata": {"annotations": {K.ANN_PLACEMENT: json.dumps(rec, separators=(",", ":"))}}})


def waiting(wl) -> bool:
    st = wl.get("status") or {}
    return (kube.is_true(wl, "QuotaReserved") and not st.get("clusterName")
            and not wl["metadata"].get("deletionTimestamp")   # 지워지는 중 (재배치 실행기가 옛 잡을 내리는 중일 수 있음)
            and not kube.is_true(wl, "Finished") and wl["spec"].get("active", True))


def is_recovery(wl) -> bool:
    ref = wl["spec"].get("priorityClassRef") or {}
    return ref.get("name") == K.PRIORITY_RECOVERY or (wl["spec"].get("priority") or 0) >= K.RECOVERY_PRIORITY_VALUE


def csv(s):
    return {x.strip() for x in (s or "").split(",") if x.strip()}


def relocation_holds(mgr, all_wls, clusters=None) -> dict:
    """진행 중인 재배치가 지켜야 할 자리: {원래 클러스터: 재제출 JobSet 이름}.
    요청 status는 재배치 실행기만 쓴다 (admission-policy.yaml). 조회가 안 되면 지키기 없이 계속한다"""
    try:
        rrs = kube.kueue(mgr).list_namespaced_custom_object(K.RR_GROUP, K.RR_VERSION, K.NAMESPACE, K.RR_PLURAL)["items"]
    except Exception as e:  # noqa: BLE001  CRD 없음·권한 없음·끊김
        log.warning("재배치 요청 조회 실패 -> 이번 루프는 재배치 자리 지키기 없음: %s", e)
        return {}
    placed = {kube.workload_owner(w)[1] for w in all_wls
              if (w.get("status") or {}).get("clusterName") or (w.get("status") or {}).get("nominatedClusterNames")}
    out = {}
    by_name = {c.name: c for c in clusters or []}
    new_sizes = {kube.workload_owner(w)[1]: sum(kube.workload_pods(w)) for w in all_wls}
    for rr in rrs:
        st, spec = rr.get("status") or {}, rr.get("spec") or {}
        if st.get("phase") in RR_FINAL or spec.get("action") not in IN_PLACE:
            continue
        src, job_id, gen = st.get("sourceCluster"), st.get("jobId"), st.get("toGeneration")
        since_at = st.get("stoppedAt") or st.get("isolatedAt")
        if not (src and job_id and gen and since_at) or kube.since(since_at) > RELOCATION_HOLD_SECONDS:
            continue
        new = R.jobset_name(job_id, int(gen))
        # 제출본이 생긴 뒤에는 부족한 원래 워커를 막지 않는다. 옛 실행 종료 전의 자리 보호는 유지한다.
        if new in new_sizes and src in by_name and by_name[src].free_for(True) < new_sizes[new]:
            continue
        if new not in placed:   # 새 잡이 지명·입장했으면 그 자리는 이미 계산에 들어가 있음
            out[src] = new
    return out


def patch_nomination(mgr, wl, cluster, tried):
    co = kube.kueue(mgr)
    ns, name = wl["metadata"]["namespace"], wl["metadata"]["name"]
    co.patch_namespaced_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, ns, "workloads", name, {
        "metadata": {"annotations": {K.ANN_NOMINATED_AT: str(int(time.time())), K.ANN_TRIED: ",".join(sorted(tried)),
                                     K.ANN_RESERVED: None}}})
    co.patch_namespaced_custom_object_status(K.KUEUE_GROUP, K.KUEUE_VERSION, ns, "workloads", name, {
        "status": {"nominatedClusterNames": [cluster]}})


def touch_nomination(mgr, wl, tried):
    """지명은 그대로 두고 지명 시각만 지금으로 (재지명 확인을 RENOMINATE_SECONDS 뒤로 미룸)"""
    kube.kueue(mgr).patch_namespaced_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, wl["metadata"]["namespace"], "workloads", wl["metadata"]["name"],
        {"metadata": {"annotations": {K.ANN_NOMINATED_AT: str(int(time.time())), K.ANN_TRIED: ",".join(sorted(tried))}}})


def ack_fenced(mgr, clusters, started):
    """이번 루프에서 fenced로 본 클러스터에 '이 루프 시작 시각'을 남긴다 (GPU-125).
    루프는 시작할 때 라벨을 한 번 읽으므로, 라벨을 붙인 뒤 시작해서 끝까지 돈 루프가 있어야
    그 전에 옛 정보로 돌던 루프도 끝났다고 말할 수 있다. 실패해도 다음 루프에서 다시 쓴다"""
    for c in clusters:
        if not c.fenced:
            continue
        try:
            kube.kueue(mgr).patch_cluster_custom_object(
                K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", c.name,
                {"metadata": {"annotations": {K.ANN_FENCED_ACK: started}}})
        except Exception as e:  # noqa: BLE001
            log.warning("%s fenced-ack 못 씀: %s", c.name, e)


def reconcile(mgr, workers):
    """한 바퀴. 끝까지 돌면 이번 루프가 본 클러스터 목록을 돌려준다 (fenced-ack용)"""
    all_wls = kube.list_workloads(mgr)
    try:
        quota_hold.reconcile(mgr, all_wls)
    except Exception:  # 추가 큐 권한·일시적인 조회 실패가 기존 워커 지명까지 막지 않게
        log.exception("쿼터 예약 실패, 다음 루프에서 재시도")
    clusters = cluster_state(mgr, workers)
    if clusters and not any(c.active for c in clusters):
        log.warning("워커 %d개 전부 끊김 -> 지명 중단 (네트워크 쪽 장애일 수 있음)", len(clusters))
        return clusters
    by_name = {c.name: c for c in clusters}
    try:
        record_placements(mgr, all_wls, clusters)
    except Exception:  # noqa: BLE001  배치 기록 문제로 지명을 멈추지 않는다
        log.exception("배치 기록 실패, 지명은 계속")
    wls = [w for w in all_wls if waiting(w)]
    for gone in set(_unplaced) - {w["metadata"]["name"] for w in wls}:   # 더 안 기다리는 잡은 잊는다
        del _unplaced[gone]

    # 이미 지명했지만 아직 워커 쿼터에 안 잡힌 것 = 곧 들어갈 몫. 미리 빼 둔다
    for wl in wls:
        for n in (wl["status"].get("nominatedClusterNames") or []):
            if n in by_name:
                by_name[n].place(sum(kube.workload_pods(wl)), is_recovery(wl))

    # 우선순위 높은 순, 같으면 먼저 온 순
    wls.sort(key=lambda w: (-(w["spec"].get("priority") or 0), w["metadata"]["creationTimestamp"]))

    # 찜한 워커 -> (찜한 잡이 복구 잡인지, 그 잡의 JobSet 이름). 앞 순서 잡의 찜만 뒤 잡을 막는다
    # 진행 중인 재배치의 원래 클러스터는 재제출 잡(복구) 몫으로 미리 찜해 둔다
    reserved = {c: (True, new) for c, new in relocation_holds(mgr, all_wls, clusters).items() if c in by_name}
    training_reservations = {(w['metadata'].get('annotations') or {}).get(K.ANN_RESERVED): kube.workload_owner(w)[1]
                             for w in wls if not is_recovery(w)
                             and (w['metadata'].get('annotations') or {}).get(K.ANN_RESERVED)}
    for wl in wls:
        name = wl["metadata"]["name"]
        try:
            dispatch_one(mgr, wl, clusters, by_name, reserved, training_reservations)
        except Exception as e:  # noqa: BLE001  이 잡만 건너뛰고 뒤 잡은 계속 (한 잡이 계속 실패해도 전체가 안 멈춤)
            if isinstance(e, client.ApiException) and e.status == 404:
                log.info("%s: 보는 사이 사라짐 (404), 건너뜀", name)
            else:
                log.exception("%s: 이번 루프에서 건너뜀", name)
            # 실패한 잡이 찜해 둔 워커는 이번 루프에도 지킨다
            pinned = (wl["metadata"].get("annotations") or {}).get(K.ANN_RESERVED)
            if pinned in by_name:
                reserved.setdefault(pinned, (is_recovery(wl), kube.workload_owner(wl)[1]))
    return clusters


def set_reserved(mgr, wl, cluster):
    """찜한 워커를 Workload에 적는다 (None이면 지움). 리더가 바뀌어도 같은 워커를 이어서 지킨다"""
    kube.kueue(mgr).patch_namespaced_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, wl["metadata"]["namespace"], "workloads", wl["metadata"]["name"],
        {"metadata": {"annotations": {K.ANN_RESERVED: cluster}}})


def quota_wait(wl) -> int:
    """관리 클러스터 쿼터를 받은 뒤 지난 초"""
    c = kube.cond(wl, "QuotaReserved") or {}
    return int(kube.since(c["lastTransitionTime"])) if c.get("lastTransitionTime") else 0


def dispatch_one(mgr, wl, clusters, by_name, reserved, training_reservations=None):
    training_reservations = training_reservations or {}
    name = wl["metadata"]["name"]
    ann = kube.owner_annotations(mgr, wl)   # 조회 실패 = 예외 -> 이 잡은 이번 루프에서 건너뜀 (exclude 없이 보내지 않게)
    if ann.get(K.ANN_MANUAL) == "true":
        return
    meta_ann = wl["metadata"].get("annotations") or {}
    tried = csv(meta_ann.get(K.ANN_TRIED))
    current = wl["status"].get("nominatedClusterNames") or []
    if current:
        at = int(meta_ann.get(K.ANN_NOMINATED_AT, "0"))
        if time.time() - at < RENOMINATE_SECONDS:
            return
        if ann.get(K.ANN_TARGET):
            # 갈 수 있는 곳이 하나뿐 -> 다시 지명할 곳이 없음. 시각만 갱신해서 다음 확인을 미룸
            log.warning("%s: %s에서 %ds 동안 입장 못 함 (target 고정이라 그대로 대기)", name, current, RENOMINATE_SECONDS)
            touch_nomination(mgr, wl, tried)
            return
        tried |= set(current)
        log.info("%s: %s에서 %ds 동안 입장 못 함 -> 다시 지명", name, current, RENOMINATE_SECONDS)
    gpus = sum(kube.workload_pods(wl))
    rec = is_recovery(wl)
    # 앞 순서 잡이 찜한 워커는 뺀다. 복구 잡은 학습 잡의 찜은 무시하고, 다른 복구 잡의 찜만 지킨다.
    # 재배치가 지켜 둔 클러스터는 그 재제출 잡 자신에게는 열려 있다
    owner = kube.workload_owner(wl)[1]
    blocked = {c for c, (by_recovery, holder) in reserved.items() if (by_recovery or not rec) and holder != owner}
    exclude = csv(ann.get(K.ANN_EXCLUDE)) | blocked
    opts = dict(recovery=rec, target=ann.get(K.ANN_TARGET))
    preferred = ann.get(K.ANN_PREFERRED) if rec and ann.get(K.ANN_RECOVERY_ID) and not opts['target'] else None

    def pick_from(avoided):
        if preferred:
            source = choose(gpus, clusters, recovery=True, target=preferred, exclude=avoided)
            if source:
                return source
            # 같은 복구의 원래 자리 보호를 제외하고 학습 찜이 없는 곳부터 본다.
            training_holds = set(training_reservations) | {c for c, (by_recovery, _) in reserved.items() if not by_recovery}
            free = choose(gpus, clusters, recovery=True, exclude=avoided | training_holds)
            if free:
                return free
        return choose(gpus, clusters, exclude=avoided, **opts)
    # 못 들어갔던 곳은 빼고 고른다. 그래도 없으면 지금 곳만 빼고 예전 곳을 다시 시도
    pick = pick_from(exclude | tried)
    if pick is None and tried:
        pick = pick_from(exclude | set(current))
    if pick is None or current == [pick]:
        if current:   # 다시 지명할 곳이 없음 -> 지금 지명 유지, 시각만 갱신 (매 루프 반복 방지)
            log.info("%s: 다른 워커도 자리 없음 -> %s 지명 유지", name, current)
            touch_nomination(mgr, wl, tried)
            return
        # 들어갈 워커 없음 -> 워커 하나를 찜해서 뒤 순서 학습 잡이 그 빈 칸을 메우지 못하게
        pinned = meta_ann.get(K.ANN_RESERVED)
        if preferred:
            source = by_name.get(preferred)
            hold = preferred if source and eligible(source, exclude=exclude) else None
        else:
            hold = reserve(gpus, clusters, exclude=exclude, pinned=pinned, **opts)
        if hold:
            reserved[hold] = (rec, owner)
        if hold != pinned:
            set_reserved(mgr, wl, hold)
            if hold:
                c = by_name[hold]
                log.info("%s (GPU %d%s): 들어갈 워커 없음 -> %s 찜 (지금 빈 GPU %d, 쿼터 뒤 %ds)%s", name, gpus,
                         ", 복구" if rec else "", hold, max(c.free_for(rec), 0), quota_wait(wl),
                         f" [전에 찜: {pinned}]" if pinned else "")
            else:
                log.warning("%s (GPU %d): 찜할 워커도 없음 (펜싱·끊김·exclude·다른 잡의 찜), 대기", name, gpus)
        if hold is None and pinned is None:   # 찜이 막 풀린 루프는 위 경고가 이미 남김
            warn_unplaced(wl, gpus, unplaced_reason(clusters, gpus, rec, exclude, opts["target"]))
        elif hold is not None:
            _unplaced.pop(name, None)
        return
    patch_nomination(mgr, wl, pick, tried)
    _unplaced.pop(name, None)
    for cluster, (_, holder) in list(reserved.items()):
        if holder == owner:
            del reserved[cluster]
    if preferred and pick != preferred:
        log.info("%s: 원래 클러스터 %s 부족 -> %s로 복구 배치 (ADR-38)", name, preferred, pick)
    displaced = training_reservations.get(pick)
    if rec and pick in reserved and not reserved[pick][0]:
        displaced = reserved.pop(pick)[1]
    if rec and displaced:
        log.warning("%s: 복구 잡 %s가 학습 잡 %s의 %s 찜 사용", name, owner, displaced, pick)
    c = by_name[pick]
    c.place(gpus, rec)
    log.info("%s (GPU %d%s) -> %s  [남은 쿼터 학습 %d / 복구 %d, 쿼터 뒤 %ds]", name, gpus, ", 복구" if rec else "",
             pick, c.quota_free, c.recovery_free, quota_wait(wl))


def unplaced_reason(clusters, gpus, rec, exclude, target):
    """잡이 갈 수 있는 워커가 하나도 없는 이유를 워커마다 적는다 (예: "eks-a 펜싱, eks-b 펜싱")"""
    parts = []
    for c in sorted(clusters, key=lambda c: c.name):
        if target and c.name != target:
            continue
        if not c.active:
            why = "끊김"
        elif c.fenced:
            why = "펜싱"
        elif c.name in exclude:
            why = "exclude·다른 잡의 찜"
        elif not c.can_ever_fit(gpus, rec):
            why = "GPU 부족"
        else:
            why = f"빈 GPU {max(c.free_for(rec), 0)}"
        parts.append(f"{c.name} {why}")
    if target and not parts:
        parts.append(f"target {target} 없음")
    return ", ".join(parts) or "워커 없음"


def warn_unplaced(wl, gpus, reason):
    """지명도 찜도 못 한 잡 경고. 이유가 바뀌었거나 UNPLACED_LOG_SECONDS가 지났을 때만 남긴다.
    예전에는 이 경우 로그가 없어서 펜싱이 남은 워커 때문에 잡이 68분 멈춘 걸 알 수 없었다 (2026-11-20)"""
    name = wl["metadata"]["name"]
    last = _unplaced.get(name)
    now = time.time()
    if last and last[0] == reason and now - last[1] < UNPLACED_LOG_SECONDS:
        return
    _unplaced[name] = (reason, now)
    log.warning("%s (GPU %d, 쿼터 뒤 %ds): 보낼 워커도 찜할 워커도 없음 -> 대기 [%s]",
                name, gpus, quota_wait(wl), reason)


def recovery_capacity(mgr, workers, payload):
    job_id, generation = payload.get('job_id'), payload.get('generation')
    cluster, node = payload.get('cluster'), payload.get('node')
    count = payload.get('gpu_count')
    if (not isinstance(job_id, str) or len(job_id) > 230
            or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', job_id)
            or type(generation) is not int or not 1 <= generation <= 999999999
            or not isinstance(cluster, str) or not isinstance(node, str) or not node
            or type(count) is not int or not 1 <= count <= 4):
        raise ValueError('invalid recovery capacity request')
    namespace = os.getenv('RECOVERY_JOB_NAMESPACE', 'training')
    jobset = f'job-{job_id}-g{generation:04d}'
    workload = kube.workload_for_jobset(mgr, namespace, jobset)
    if ((workload or {}).get('status') or {}).get('clusterName') != cluster:
        raise ValueError('current workload placement could not be verified')
    apis = workers.get()
    worker = apis.get(cluster)
    if worker is None:
        raise RuntimeError('worker unavailable')
    co = kube.kueue(mgr)
    mkc = co.get_cluster_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, 'multikueueclusters', cluster, _request_timeout=kube.TIMEOUT)
    if not usable(mkc):
        raise RuntimeError('worker unavailable')
    source = Cluster(cluster)
    fill_node_capacity(source, worker, excluded_node=node, released_job=(namespace, jobset))
    result = {'spare_free': source.spare_free, 'normal_free_after_stop': source.node_free}
    # 1) 원래 클러스터: 예비 1장 이상 + 잡 전체 수용
    if source.spare_free > 0 and source.nodes_for(True) >= count:
        return {'available': True, 'cluster': cluster, **result}
    # 2) 원래 클러스터에 자리가 없으면 다른 워커 (ADR-38과 같은 기준: Active·펜싱 아님·exclude 아님).
    #    exclude 조회 실패는 예외 그대로 → 503 → 복구 컨트롤러 HOLD
    exclude = csv(kube.owner_annotations(mgr, workload).get(K.ANN_EXCLUDE))
    others = co.list_cluster_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, 'multikueueclusters', _request_timeout=kube.TIMEOUT)['items']
    for other in sorted(others, key=lambda m: m['metadata']['name']):
        name = other['metadata']['name']
        api = apis.get(name)
        if name == cluster or name in exclude or api is None or not usable(other):
            continue
        c = Cluster(name)
        try:
            fill_node_capacity(c, api)
        except Exception as e:  # noqa: BLE001  이 워커만 후보에서 뺌
            log.warning('recovery capacity: %s 조회 실패: %s', name, e)
            continue
        if c.nodes_for(True) >= count:
            return {'available': True, 'cluster': name, **result}
    return {'available': False, 'cluster': None, **result}


def usable(mkc):
    return kube.is_true(mkc, 'Active') and (mkc['metadata'].get('labels') or {}).get(K.LABEL_FENCED) != 'true'


def start_capacity_server(port):
    # Separate API clients from the scheduling loop, which can update nominations.
    mgr = kube.manager_client()
    workers = kube.Workers(mgr)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != '/v1/recovery-capacity':
                self.send_error(404)
                return
            self.connection.settimeout(5)
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 4096:
                    raise ValueError('invalid request length')
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ValueError('invalid request body')
                body = json.dumps(recovery_capacity(mgr, workers, payload)).encode()
            except ValueError:
                self.send_error(400)
                return
            except Exception:
                log.exception('recovery capacity unavailable')
                self.send_error(503)
                return
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = HTTPServer(('0.0.0.0', port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def main():
    port = int(os.getenv('DISPATCHER_CAPACITY_PORT', '0'))
    if port:
        start_capacity_server(port)
    mgr = kube.manager_client()
    elector = leader.Elector(kube.manager_client(), "dispatcher")
    workers = kube.Workers(mgr)
    log.info("시작: interval=%ss renominate=%ss", INTERVAL, RENOMINATE_SECONDS)
    log.info("쿼터 예약: enabled=%s bypass_light=%d bypass_recovery=%d", quota_hold.ENABLED,
             quota_hold.LIGHT_N, quota_hold.RECOVERY_N)
    while True:
        if not elector.is_leader():
            time.sleep(1)
            continue
        started = kube.now_rfc3339()   # 라벨을 읽기 전 시각 (초 단위 버림 -> ack > T 비교가 안전한 쪽)
        try:
            clusters = reconcile(mgr, workers)
        except Exception:  # noqa: BLE001  루프가 중간에 끊기면 ack 안 남김
            log.exception("루프 실패")
        else:
            ack_fenced(mgr, clusters, started)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
