"""우선순위 조정 컨트롤러 (GPU-69) — 관리 클러스터에서 돈다.

보는 것:  관리 클러스터의 JobSet과 그 Workload (아직 워커에 안 들어간 것)
판단:     복구 큐 잡인가? 일반 잡이 AGING_SECONDS 넘게 기다렸나?
바꾸는 것: JobSet 라벨 kueue.x-k8s.io/priority-class
           (Kueue가 그 라벨을 보고 Workload 우선순위를 다시 맞춘다. Workload를 직접 고치지 않는 이유:
            Kueue 잡 컨트롤러가 라벨 기준으로 되돌릴 수 있음)

우선순위는 Kueue 큐 안의 줄 순서와 디스패처가 고르는 순서에 쓰인다. 선점은 꺼져 있다.
"""
import os
import time

import keys as K
import kube

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "10"))
AGING_SECONDS = int(os.getenv("AGING_SECONDS", "180"))

kube.setup_logging("priority")
log = kube.logging.getLogger("priority")


def desired(js, wl):
    labels = js["metadata"].get("labels") or {}
    cur = labels.get(K.LABEL_PRIORITY)
    if labels.get(K.LABEL_QUEUE) == K.RECOVERY_QUEUE:
        return K.PRIORITY_RECOVERY
    if cur == K.PRIORITY_RECOVERY:
        return cur  # 일반 큐에 recovery를 단 건 사람이 일부러 한 것. 건드리지 않음
    if wl and not (wl.get("status") or {}).get("clusterName"):
        if kube.since(wl["metadata"]["creationTimestamp"]) > AGING_SECONDS:
            return K.PRIORITY_AGED
    return cur


def reconcile(mgr):
    co = kube.kueue(mgr)
    wls = {}
    for wl in kube.list_workloads(mgr):
        kind, name = kube.workload_owner(wl)
        if kind == "JobSet":
            wls[(wl["metadata"]["namespace"], name)] = wl
    for js in co.list_cluster_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, "jobsets")["items"]:
        ns, name = js["metadata"]["namespace"], js["metadata"]["name"]
        if K.LABEL_QUEUE not in (js["metadata"].get("labels") or {}):
            continue
        cur = (js["metadata"].get("labels") or {}).get(K.LABEL_PRIORITY)
        want = desired(js, wls.get((ns, name)))
        if want and want != cur:
            co.patch_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name,
                                              {"metadata": {"labels": {K.LABEL_PRIORITY: want}}})
            log.info("%s/%s: 우선순위 %s -> %s", ns, name, cur, want)


def main():
    mgr = kube.manager_client()
    log.info("시작: interval=%ss aging=%ss", INTERVAL, AGING_SECONDS)
    while True:
        try:
            reconcile(mgr)
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
