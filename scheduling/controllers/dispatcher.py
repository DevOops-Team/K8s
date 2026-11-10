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
"""
import os
import time

from kubernetes import client

import keys as K
import kube
import leader
import recovery_render as R
from placement import Cluster, choose, reserve

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "5"))
RENOMINATE_SECONDS = int(os.getenv("RENOMINATE_SECONDS", "60"))
# 진행 중인 재배치의 원래 클러스터를 재제출 잡 몫으로 지키는 최대 시간 (격리 시각, 옛 실행 종료 뒤엔 그 시각부터).
# 넘으면 푼다: 옛 실행이 안 멈추거나 복구 잡이 관리 쿼터를 못 받는 동안 클러스터 하나를 계속 막지 않게
RELOCATION_HOLD_SECONDS = int(os.getenv("RELOCATION_HOLD_SECONDS", "300"))
IN_PLACE = {"restartInPlace", "moveToSpare"}   # 재제출 잡이 원래 클러스터로 돌아오는 action
RR_FINAL = {"Done", "Failed", "Duplicate"}

kube.setup_logging("dispatcher")
log = kube.logging.getLogger("dispatcher")


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
    # 2) 노드: cordon 안 된 Ready GPU 노드의 allocatable - 올라간 Pod 요청
    #    예비 노드(devoops/spare taint)는 자리 표시 Pod가 있든 없든 전부 복구 잡 몫 (학습 잡 Pod는 taint로 못 감)
    #    일반 노드의 자리 표시 Pod 자리도 복구 잡 몫 (예비 taint 도입 전 구성 호환)
    core = client.CoreV1Api(api)
    used, held = {}, {}
    pods = core.list_pod_for_all_namespaces(
        field_selector="status.phase!=Succeeded,status.phase!=Failed", _request_timeout=kube.TIMEOUT).items
    for p in pods:
        if not p.spec.node_name:
            continue
        g = sum(int(((ct.resources.requests or {}) if ct.resources else {}).get(K.GPU, 0)) for ct in p.spec.containers)
        if p.spec.priority_class_name == K.PLACEHOLDER_PRIORITY_CLASS:
            held[p.spec.node_name] = held.get(p.spec.node_name, 0) + g
        else:
            used[p.spec.node_name] = used.get(p.spec.node_name, 0) + g
    for n in core.list_node(label_selector=K.GPU_NODE_SELECTOR, _request_timeout=kube.TIMEOUT).items:
        ready = any(x.type == "Ready" and x.status == "True" for x in n.status.conditions or [])
        if n.spec.unschedulable or not ready:
            continue
        alloc = int((n.status.allocatable or {}).get(K.GPU, 0))
        if any(t.key == K.TAINT_SPARE for t in (n.spec.taints or [])):
            c.spare_free += max(alloc - used.get(n.metadata.name, 0), 0)
            continue
        free = alloc - used.get(n.metadata.name, 0) - held.get(n.metadata.name, 0)
        c.node_total += max(alloc - held.get(n.metadata.name, 0), 0)
        c.node_free += max(free, 0)
        c.spare_free += held.get(n.metadata.name, 0)


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


def relocation_holds(mgr, all_wls) -> dict:
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
    for rr in rrs:
        st, spec = rr.get("status") or {}, rr.get("spec") or {}
        if st.get("phase") in RR_FINAL or spec.get("action") not in IN_PLACE:
            continue
        src, job_id, gen = st.get("sourceCluster"), st.get("jobId"), st.get("toGeneration")
        since_at = st.get("stoppedAt") or st.get("isolatedAt")
        if not (src and job_id and gen and since_at) or kube.since(since_at) > RELOCATION_HOLD_SECONDS:
            continue
        new = R.jobset_name(job_id, int(gen))
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


def reconcile(mgr, workers):
    clusters = cluster_state(mgr, workers)
    if clusters and not any(c.active for c in clusters):
        log.warning("워커 %d개 전부 끊김 -> 지명 중단 (네트워크 쪽 장애일 수 있음)", len(clusters))
        return
    by_name = {c.name: c for c in clusters}
    all_wls = kube.list_workloads(mgr)
    wls = [w for w in all_wls if waiting(w)]

    # 이미 지명했지만 아직 워커 쿼터에 안 잡힌 것 = 곧 들어갈 몫. 미리 빼 둔다
    for wl in wls:
        for n in (wl["status"].get("nominatedClusterNames") or []):
            if n in by_name:
                by_name[n].place(sum(kube.workload_pods(wl)), is_recovery(wl))

    # 우선순위 높은 순, 같으면 먼저 온 순
    wls.sort(key=lambda w: (-(w["spec"].get("priority") or 0), w["metadata"]["creationTimestamp"]))

    # 찜한 워커 -> (찜한 잡이 복구 잡인지, 그 잡의 JobSet 이름). 앞 순서 잡의 찜만 뒤 잡을 막는다
    # 진행 중인 재배치의 원래 클러스터는 재제출 잡(복구) 몫으로 미리 찜해 둔다
    reserved = {c: (True, new) for c, new in relocation_holds(mgr, all_wls).items() if c in by_name}
    for wl in wls:
        name = wl["metadata"]["name"]
        try:
            dispatch_one(mgr, wl, clusters, by_name, reserved)
        except Exception as e:  # noqa: BLE001  이 잡만 건너뛰고 뒤 잡은 계속 (한 잡이 계속 실패해도 전체가 안 멈춤)
            if isinstance(e, client.ApiException) and e.status == 404:
                log.info("%s: 보는 사이 사라짐 (404), 건너뜀", name)
            else:
                log.exception("%s: 이번 루프에서 건너뜀", name)
            # 실패한 잡이 찜해 둔 워커는 이번 루프에도 지킨다
            pinned = (wl["metadata"].get("annotations") or {}).get(K.ANN_RESERVED)
            if pinned in by_name:
                reserved.setdefault(pinned, (is_recovery(wl), kube.workload_owner(wl)[1]))


def set_reserved(mgr, wl, cluster):
    """찜한 워커를 Workload에 적는다 (None이면 지움). 리더가 바뀌어도 같은 워커를 이어서 지킨다"""
    kube.kueue(mgr).patch_namespaced_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, wl["metadata"]["namespace"], "workloads", wl["metadata"]["name"],
        {"metadata": {"annotations": {K.ANN_RESERVED: cluster}}})


def quota_wait(wl) -> int:
    """관리 클러스터 쿼터를 받은 뒤 지난 초"""
    c = kube.cond(wl, "QuotaReserved") or {}
    return int(kube.since(c["lastTransitionTime"])) if c.get("lastTransitionTime") else 0


def dispatch_one(mgr, wl, clusters, by_name, reserved):
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
    # 못 들어갔던 곳은 빼고 고른다. 그래도 없으면 지금 곳만 빼고 예전 곳을 다시 시도
    pick = choose(gpus, clusters, exclude=exclude | tried, **opts)
    if pick is None and tried:
        pick = choose(gpus, clusters, exclude=exclude | set(current), **opts)
    if pick is None or current == [pick]:
        if current:   # 다시 지명할 곳이 없음 -> 지금 지명 유지, 시각만 갱신 (매 루프 반복 방지)
            log.info("%s: 다른 워커도 자리 없음 -> %s 지명 유지", name, current)
            touch_nomination(mgr, wl, tried)
            return
        # 들어갈 워커 없음 -> 워커 하나를 찜해서 뒤 순서 학습 잡이 그 빈 칸을 메우지 못하게
        pinned = meta_ann.get(K.ANN_RESERVED)
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
        return
    patch_nomination(mgr, wl, pick, tried)
    c = by_name[pick]
    c.place(gpus, rec)
    log.info("%s (GPU %d%s) -> %s  [남은 쿼터 학습 %d / 복구 %d, 쿼터 뒤 %ds]", name, gpus, ", 복구" if rec else "",
             pick, c.quota_free, c.recovery_free, quota_wait(wl))


def main():
    mgr = kube.manager_client()
    elector = leader.Elector(kube.manager_client(), "dispatcher")
    workers = kube.Workers(mgr)
    log.info("시작: interval=%ss renominate=%ss", INTERVAL, RENOMINATE_SECONDS)
    while True:
        if not elector.is_leader():
            time.sleep(1)
            continue
        try:
            reconcile(mgr, workers)
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
