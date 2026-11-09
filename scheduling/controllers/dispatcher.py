"""빈 패킹 디스패처 (GPU-67) — 관리 클러스터에서 돈다.

보는 것:  관리 클러스터에서 쿼터는 잡았는데(QuotaReserved) 아직 워커가 안 정해진 Workload
판단:     워커별 남은 GPU(쿼터 + 노드) 계산 -> 펜싱·끊김·exclude 클러스터 제외 -> 들어가는 곳 중 가장 꽉 찬 곳
바꾸는 것: Workload.status.nominatedClusterNames (Kueue MultiKueue가 그 워커에만 복사본을 만든다)

추가 규칙:
  - 지명했는데 RENOMINATE_SECONDS 안에 워커에서 입장 못 하면 다른 곳으로 다시 지명
  - 워커가 전부 끊기면 아무것도 안 함 (pfSense 장애처럼 "우리 쪽 문제"일 수 있음)

하지 않는 것: 큰 잡 몫으로 워커 비워 두기 (11/09에 뺌)
  큰 잡이 쿼터를 받으면 그 몫만큼은 다른 잡이 못 쓰고, 이 루프는 우선순위·먼저 온 순으로 보므로
  4장 빈 워커가 생기는 순간 큰 잡이 먼저 가져간다. 빈 패킹이 작은 잡을 꽉 찬 워커로 모아 빈 워커를 남긴다.
  e2e 비교(켬/끔)에서 입장 시각 차이가 없었음 (185s vs 192s, 192s vs 189s).
"""
import os
import time

from kubernetes import client

import keys as K
import kube
import leader
from placement import Cluster, choose

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "5"))
RENOMINATE_SECONDS = int(os.getenv("RENOMINATE_SECONDS", "60"))

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
            c.recovery_free += max(nominal - reserved, 0)
            c.quota_free -= max(reserved - nominal, 0)   # 빌려 간 학습 몫
        else:
            c.quota_free += nominal - reserved
    # 2) 노드: cordon 안 된 Ready GPU 노드의 allocatable - 올라간 Pod 요청
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
        free = alloc - used.get(n.metadata.name, 0) - held.get(n.metadata.name, 0)
        c.node_free += max(free, 0)
        c.spare_free += held.get(n.metadata.name, 0)


def waiting(wl) -> bool:
    st = wl.get("status") or {}
    return (kube.is_true(wl, "QuotaReserved") and not st.get("clusterName")
            and not kube.is_true(wl, "Finished") and wl["spec"].get("active", True))


def is_recovery(wl) -> bool:
    ref = wl["spec"].get("priorityClassRef") or {}
    return ref.get("name") == K.PRIORITY_RECOVERY or (wl["spec"].get("priority") or 0) >= K.RECOVERY_PRIORITY_VALUE


def csv(s):
    return {x.strip() for x in (s or "").split(",") if x.strip()}


def patch_nomination(mgr, wl, cluster, tried):
    co = kube.kueue(mgr)
    ns, name = wl["metadata"]["namespace"], wl["metadata"]["name"]
    co.patch_namespaced_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, ns, "workloads", name, {
        "metadata": {"annotations": {K.ANN_NOMINATED_AT: str(int(time.time())), K.ANN_TRIED: ",".join(sorted(tried))}}})
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
    wls = [w for w in kube.list_workloads(mgr) if waiting(w)]

    # 이미 지명했지만 아직 워커 쿼터에 안 잡힌 것 = 곧 들어갈 몫. 미리 빼 둔다
    for wl in wls:
        for n in (wl["status"].get("nominatedClusterNames") or []):
            if n in by_name:
                by_name[n].place(sum(kube.workload_pods(wl)), is_recovery(wl))

    # 우선순위 높은 순, 같으면 먼저 온 순
    wls.sort(key=lambda w: (-(w["spec"].get("priority") or 0), w["metadata"]["creationTimestamp"]))

    for wl in wls:
        name = wl["metadata"]["name"]
        ann = kube.owner_annotations(mgr, wl)
        if ann.get(K.ANN_MANUAL) == "true":
            continue
        meta_ann = wl["metadata"].get("annotations") or {}
        tried = csv(meta_ann.get(K.ANN_TRIED))
        current = wl["status"].get("nominatedClusterNames") or []
        if current:
            at = int(meta_ann.get(K.ANN_NOMINATED_AT, "0"))
            if time.time() - at < RENOMINATE_SECONDS:
                continue
            if ann.get(K.ANN_TARGET):
                # 갈 수 있는 곳이 하나뿐 -> 다시 지명할 곳이 없음. 시각만 갱신해서 다음 확인을 미룸
                log.warning("%s: %s에서 %ds 동안 입장 못 함 (target 고정이라 그대로 대기)", name, current, RENOMINATE_SECONDS)
                touch_nomination(mgr, wl, tried)
                continue
            tried |= set(current)
            log.info("%s: %s에서 %ds 동안 입장 못 함 -> 다시 지명", name, current, RENOMINATE_SECONDS)
        gpus = sum(kube.workload_pods(wl))
        rec = is_recovery(wl)
        exclude = csv(ann.get(K.ANN_EXCLUDE))
        opts = dict(recovery=rec, target=ann.get(K.ANN_TARGET))
        # 못 들어갔던 곳은 빼고 고른다. 그래도 없으면 지금 곳만 빼고 예전 곳을 다시 시도
        pick = choose(gpus, clusters, exclude=exclude | tried, **opts)
        if pick is None and tried:
            pick = choose(gpus, clusters, exclude=exclude | set(current), **opts)
        if pick is None or current == [pick]:
            if current:   # 다시 지명할 곳이 없음 -> 지금 지명 유지, 시각만 갱신 (매 루프 반복 방지)
                log.info("%s: 다른 워커도 자리 없음 -> %s 지명 유지", name, current)
                touch_nomination(mgr, wl, tried)
            else:
                log.debug("%s (GPU %d): 들어갈 워커 없음, 대기", name, gpus)
            continue
        patch_nomination(mgr, wl, pick, tried)
        c = by_name[pick]
        c.place(gpus, rec)
        log.info("%s (GPU %d%s) -> %s  [남은 쿼터 학습 %d / 복구 %d]", name, gpus, ", 복구" if rec else "",
                 pick, c.quota_free, c.recovery_free)


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
