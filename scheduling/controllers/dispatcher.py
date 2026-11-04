"""빈 패킹 디스패처 (GPU-67) — 관리 클러스터에서 돈다.

보는 것:  관리 클러스터에서 쿼터는 잡았는데(QuotaReserved) 아직 워커가 안 정해진 Workload
판단:     워커별 남은 GPU(쿼터 + 노드) 계산 -> 펜싱·끊김·exclude 클러스터 제외 -> 들어가는 곳 중 가장 꽉 찬 곳
바꾸는 것: Workload.status.nominatedClusterNames (Kueue MultiKueue가 그 워커에만 복사본을 만든다)

추가 규칙:
  - 지명했는데 RENOMINATE_SECONDS 안에 워커에서 입장 못 하면 다른 곳으로 다시 지명
  - STARVE_SECONDS 넘게 못 들어간 큰 잡이 있으면, 가장 빈 워커 하나를 그 잡 몫으로 비워 둠 (굶주림 방지)
  - 워커가 전부 끊기면 아무것도 안 함 (pfSense 장애처럼 "우리 쪽 문제"일 수 있음)
"""
import os
import time

from kubernetes import client

import keys as K
import kube
from placement import Cluster, choose, reserve_for_starving, take

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "5"))
RENOMINATE_SECONDS = int(os.getenv("RENOMINATE_SECONDS", "60"))
STARVE_SECONDS = int(os.getenv("STARVE_SECONDS", "120"))

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
        if free > 0:
            c.node_free.append(free)
        if held.get(n.metadata.name):
            c.spare_free.append(held[n.metadata.name])


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


def reconcile(mgr, workers, starving_holder):
    clusters = cluster_state(mgr, workers)
    if clusters and not any(c.active for c in clusters):
        log.warning("워커 %d개 전부 끊김 -> 지명 중단 (네트워크 쪽 장애일 수 있음)", len(clusters))
        return starving_holder
    by_name = {c.name: c for c in clusters}
    wls = [w for w in kube.list_workloads(mgr) if waiting(w)]

    # 이미 지명했지만 아직 워커 쿼터에 안 잡힌 것 = 곧 들어갈 몫. 미리 빼 둔다
    for wl in wls:
        for n in (wl["status"].get("nominatedClusterNames") or []):
            if n in by_name:
                c, pods = by_name[n], kube.workload_pods(wl)
                c.use(sum(pods), is_recovery(wl))
                c.node_free = [f for f in take(pods, c.nodes_for(is_recovery(wl))) if f > 0]

    # 우선순위 높은 순, 같으면 먼저 온 순
    wls.sort(key=lambda w: (-(w["spec"].get("priority") or 0), w["metadata"]["creationTimestamp"]))

    # 굶주림: 가장 오래 기다린 잡이 STARVE_SECONDS를 넘으면 워커 하나를 그 잡 몫으로 잡아 둔다
    blocked, holder = set(), None
    for wl in wls:
        qr = kube.cond(wl, "QuotaReserved")
        if kube.since(qr["lastTransitionTime"]) > STARVE_SECONDS and not wl["status"].get("nominatedClusterNames"):
            holder = wl["metadata"]["uid"]
            r = reserve_for_starving(kube.workload_pods(wl), clusters)
            if r:
                blocked.add(r)
                if starving_holder != (holder, r):
                    log.info("굶주림 방지: %s (GPU %d) 몫으로 %s 비워 둠",
                             wl["metadata"]["name"], sum(kube.workload_pods(wl)), r)
                starving_holder = (holder, r)
            break
    else:
        starving_holder = None

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
            tried |= set(current)
            log.info("%s: %s에서 %ds 동안 입장 못 함 -> 다시 지명", name, current, RENOMINATE_SECONDS)
        pods = kube.workload_pods(wl)
        rec = is_recovery(wl)
        my_block = set() if starving_holder and starving_holder[0] == wl["metadata"]["uid"] else blocked
        exclude = csv(ann.get(K.ANN_EXCLUDE))
        opts = dict(recovery=rec, target=ann.get(K.ANN_TARGET), blocked=my_block)
        # 못 들어갔던 곳은 빼고 고른다. 그래도 없으면 지금 곳만 빼고 예전 곳을 다시 시도
        pick = choose(pods, clusters, exclude=exclude | tried, **opts)
        if pick is None and tried:
            pick = choose(pods, clusters, exclude=exclude | set(current), **opts)
        if pick is None:
            log.debug("%s (GPU %d): 들어갈 워커 없음, 대기", name, sum(pods))
            continue
        if current == [pick]:
            continue
        patch_nomination(mgr, wl, pick, tried)
        c = by_name[pick]
        c.use(sum(pods), rec)
        c.node_free = [f for f in take(pods, c.nodes_for(rec)) if f > 0]
        log.info("%s (GPU %d%s) -> %s  [남은 쿼터 학습 %d / 복구 %d]", name, sum(pods), ", 복구" if rec else "",
                 pick, c.quota_free, c.recovery_free)
    return starving_holder


def main():
    mgr = kube.manager_client()
    workers = kube.Workers(mgr)
    log.info("시작: interval=%ss renominate=%ss starve=%ss", INTERVAL, RENOMINATE_SECONDS, STARVE_SECONDS)
    holder = None
    while True:
        try:
            holder = reconcile(mgr, workers, holder)
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
