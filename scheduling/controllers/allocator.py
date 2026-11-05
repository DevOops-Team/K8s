"""배분 조정 컨트롤러 (GPU-69) — 관리 클러스터에서 돈다. (예전 이름: 우선순위 조정 컨트롤러)

설계: Confluence 「평소 학습, 장애복구 시 스케줄링 방식」 배분(2)·배분(3)

할 일 1. 빌려 쓰기 조절 (배분 2)
  보는 것:  Heavy 큐에서 쿼터를 못 받고 기다리는 Workload
  판단:     TIGHTEN_SECONDS 넘게 기다린 Heavy 잡이 있나? / Heavy 대기가 RELEASE_SECONDS 동안 0이었나?
  바꾸는 것: Light ClusterQueue의 borrowingLimit (0 = 자기 몫까지만, 없음 = 남는 몫 전부 빌려 씀)
            실행 중인 Light 잡은 건드리지 않는다(선점 없음). 끝나는 대로 빌렸던 몫이 Heavy로 돌아간다.

할 일 2. 우선순위 올리기 (배분 3)
  보는 것:  관리 클러스터의 JobSet과 그 Workload (아직 워커에 안 들어간 것)
  판단:     대기 시간 x GPU 수 >= AGING_GPU_SECONDS 인가? (기본 720 = 12 GPU·분 -> 4장 3분, 1장 12분)
  바꾸는 것: JobSet 라벨 kueue.x-k8s.io/priority-class
            (Kueue가 그 라벨을 보고 Workload 우선순위를 다시 맞춘다. Workload를 직접 고치지 않는 이유:
             Kueue 잡 컨트롤러가 라벨 기준으로 되돌릴 수 있음)

상태는 쿠버네티스 객체에만 둔다. 메모리에 있는 건 "Heavy 대기가 0이 된 시각" 하나뿐이고,
재시작하면 그 시각부터 다시 센다(풀기가 최대 RELEASE_SECONDS 늦어질 뿐).
"""
import os
import time

import keys as K
import kube
from allocation_rules import aged, want_tight

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "10"))
AGING_GPU_SECONDS = int(os.getenv("AGING_GPU_SECONDS", "720"))
TIGHTEN_SECONDS = int(os.getenv("TIGHTEN_SECONDS", "60"))
RELEASE_SECONDS = int(os.getenv("RELEASE_SECONDS", "300"))

kube.setup_logging("allocator")
log = kube.logging.getLogger("allocator")


# ---------- 빌려 쓰기 조절

def heavy_pending_wait(wls):
    """Heavy 큐에서 쿼터를 못 받은 Workload 중 가장 오래 기다린 시간. 없으면 None"""
    waits = []
    for wl in wls:
        if wl["spec"].get("queueName") != K.HEAVY_QUEUE or not wl["spec"].get("active", True):
            continue
        if kube.is_true(wl, "QuotaReserved") or kube.is_true(wl, "Finished"):
            continue
        waits.append(kube.since(wl["metadata"]["creationTimestamp"]))
    return max(waits) if waits else None


def gpu_resources(cq):
    for rg in cq["spec"].get("resourceGroups", []):
        for fl in rg["flavors"]:
            for r in fl["resources"]:
                if r["name"] == K.GPU:
                    yield r


def is_tight(cq) -> bool:
    return any(str(r.get("borrowingLimit", "")) == "0" for r in gpu_resources(cq))


def set_tight(mgr, cq, tight: bool, why: str):
    for r in gpu_resources(cq):
        if tight:
            r["borrowingLimit"] = "0"
        else:
            r.pop("borrowingLimit", None)
    # resourceGroups는 리스트라 merge patch로 통째로 바뀐다 -> 필드를 빼면 그대로 빠짐
    kube.kueue(mgr).patch_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", K.LIGHT_CQ, {
        "metadata": {"annotations": {K.ANN_BORROW_TIGHTENED_AT: kube.now_rfc3339() if tight else None}},
        "spec": {"resourceGroups": cq["spec"]["resourceGroups"]}})
    log.info("%s 빌려 쓰기 %s (%s)", K.LIGHT_CQ, "조임: borrowingLimit 0" if tight else "풂: borrowingLimit 없음", why)


class Borrow:
    def __init__(self):
        self.idle_since = None   # Heavy 대기가 0이 된 시각

    def reconcile(self, mgr, wls):
        cq = kube.kueue(mgr).get_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", K.LIGHT_CQ)
        tight = is_tight(cq)
        wait = heavy_pending_wait(wls)
        if wait is None:
            self.idle_since = self.idle_since or time.time()
            idle_for = time.time() - self.idle_since
        else:
            self.idle_since, idle_for = None, None
        want = want_tight(tight, wait, idle_for, TIGHTEN_SECONDS, RELEASE_SECONDS)
        if want != tight:
            why = (f"Heavy 잡이 {int(wait)}s 기다림" if want
                   else f"Heavy 대기 0이 {int(idle_for)}s 이어짐")
            set_tight(mgr, cq, want, why)


# ---------- 우선순위 올리기

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
    borrow = Borrow()
    log.info("시작: interval=%ss aging=%s GPU·s tighten=%ss release=%ss heavy=%s light=%s",
             INTERVAL, AGING_GPU_SECONDS, TIGHTEN_SECONDS, RELEASE_SECONDS, K.HEAVY_QUEUE, K.LIGHT_CQ)
    while True:
        try:
            wls = kube.list_workloads(mgr)
            borrow.reconcile(mgr, wls)
            reconcile_priority(mgr, wls)
        except Exception:  # noqa: BLE001
            log.exception("루프 실패")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
