"""배분 조정 컨트롤러 (GPU-69) — 관리 클러스터에서 돈다. (예전 이름: 우선순위 조정 컨트롤러)

설계: Confluence 「평소 학습, 장애복구 시 스케줄링 방식」 배분(2)·배분(3)

할 일: 우선순위 올리기 (배분 3)
  보는 것:  관리 클러스터의 JobSet과 그 Workload (아직 워커에 안 들어간 것)
  판단:     대기 시간 x GPU 수 >= AGING_GPU_SECONDS 인가? (기본 720 = 12 GPU·분 -> 4장 3분, 1장 12분)
  바꾸는 것: JobSet 라벨 kueue.x-k8s.io/priority-class
            (Kueue가 그 라벨을 보고 Workload 우선순위를 다시 맞춘다. Workload를 직접 고치지 않는 이유:
             Kueue 잡 컨트롤러가 라벨 기준으로 되돌릴 수 있음)

하지 않는 것: Light 빌려 쓰기 조절 (배분 2)
  Kueue가 이미 한다. 자기 몫 안에 들어가는 Workload(예: Heavy 몫 8 안의 4장 잡)가 기다리는 동안
  같은 cohort의 다른 큐는 새로 빌리지 못한다(KEP-83). 그래서 Heavy 잡이 기다리면 Light는
  자기 몫 4까지만 새로 받는다. 대조군 실행(11/05)에서 이 컨트롤러 없이도 큰 잡이 들어가는 걸 확인함.

상태는 쿠버네티스 객체(JobSet 라벨)에만 둔다.
"""
import os
import time

import keys as K
import kube
from allocation_rules import aged

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "10"))
AGING_GPU_SECONDS = int(os.getenv("AGING_GPU_SECONDS", "720"))

kube.setup_logging("allocator")
log = kube.logging.getLogger("allocator")


def desired(js, wl):
    labels = js["metadata"].get("labels") or {}
    cur = labels.get(K.LABEL_PRIORITY)
    if labels.get(K.LABEL_QUEUE) == K.RECOVERY_QUEUE:
        return K.PRIORITY_RECOVERY
    if cur in (K.PRIORITY_RECOVERY, K.PRIORITY_AGED):
        return cur  # recovery: 일반 큐에 단 건 사람이 일부러 한 것 / aged: 한 번 올리면 내리지 않음
    if wl and not (wl.get("status") or {}).get("clusterName"):
        if aged(kube.since(wl["metadata"]["creationTimestamp"]), sum(kube.workload_pods(wl)), AGING_GPU_SECONDS):
            return K.PRIORITY_AGED
    return cur


def reconcile_priority(mgr, wls):
    co = kube.kueue(mgr)
    by_owner = {}
    for wl in wls:
        kind, name = kube.workload_owner(wl)
        if kind == "JobSet":
            by_owner[(wl["metadata"]["namespace"], name)] = wl
    for js in co.list_cluster_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, "jobsets")["items"]:
        ns, name = js["metadata"]["namespace"], js["metadata"]["name"]
        if K.LABEL_QUEUE not in (js["metadata"].get("labels") or {}):
            continue
        cur = (js["metadata"].get("labels") or {}).get(K.LABEL_PRIORITY)
        wl = by_owner.get((ns, name))
        want = desired(js, wl)
        if want and want != cur:
            co.patch_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, ns, "jobsets", name,
                                              {"metadata": {"labels": {K.LABEL_PRIORITY: want}}})
            extra = ""
            if wl and want == K.PRIORITY_AGED:
                extra = f" (GPU {sum(kube.workload_pods(wl))}, {int(kube.since(wl['metadata']['creationTimestamp']))}s 대기)"
            log.info("%s/%s: 우선순위 %s -> %s%s", ns, name, cur, want, extra)


def main():
    mgr = kube.manager_client()
    log.info("시작: interval=%ss aging=%s GPU·s", INTERVAL, AGING_GPU_SECONDS)
    while True:
        try:
            reconcile_priority(mgr, kube.list_workloads(mgr))
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
