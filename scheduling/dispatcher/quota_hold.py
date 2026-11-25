"""ADR-41 쿼터 예약. 관리 Workload만 변경하며 실행 중인 잡은 비활성화하지 않는다.

프로젝트의 단일 GPU flavor / 같은 cohort의 CQ들을 대상으로 한다. 큐를 수정하거나
Kueue의 입장 판단을 대신하지 않는다. 판별할 수 없는 구성/조회 오류에는 새 보류를 만들지 않는다.
"""
import json
import logging
import os
from datetime import datetime

from kubernetes import client

import keys as K
import kube

log = logging.getLogger("dispatcher.quota_hold")
ENABLED = os.getenv("QUOTA_HOLD_ENABLED", "false").lower() in ("1", "true", "yes")
LIGHT_N = max(1, int(os.getenv("QUOTA_HOLD_BYPASS_LIGHT", "3")))
RECOVERY_N = max(1, int(os.getenv("QUOTA_HOLD_BYPASS_RECOVERY", "3")))


def ref(w):
    return f"{w['metadata']['namespace']}/{w['metadata']['name']}"


def annotations(w):
    return w["metadata"].get("annotations") or {}


def recovery(w):
    return ((w["spec"].get("priorityClassRef") or {}).get("name") == K.PRIORITY_RECOVERY
            or (w["spec"].get("priority") or 0) >= K.RECOVERY_PRIORITY_VALUE)


def unreserved(w):
    st = w.get("status") or {}
    return not (st.get("admission") or st.get("clusterName") or st.get("nominatedClusterNames")
                or kube.is_true(w, "QuotaReserved") or kube.is_true(w, "Admitted"))


def pending(w):
    return (unreserved(w) and w["spec"].get("active", True)
            and not w["metadata"].get("deletionTimestamp") and not kube.is_true(w, "Finished"))


def order(w):
    return w["metadata"]["creationTimestamp"], ref(w)


def patch(co, w, ann, active=None):
    # resourceVersion is required even for reactivation/annotation cleanup. Never retry a
    # conflicting deactivation using fresh data without re-evaluating its quota state.
    meta = w["metadata"]
    body = {"metadata": {"resourceVersion": meta["resourceVersion"], "annotations": ann}}
    if active is not None:
        body["spec"] = {"active": active}
    try:
        result = co.patch_namespaced_custom_object(
            K.KUEUE_GROUP, K.KUEUE_VERSION, meta["namespace"], "workloads", meta["name"], body,
            _request_timeout=kube.TIMEOUT)
    except client.ApiException as e:
        if e.status not in (404, 409):
            raise
        log.info("%s: quota hold patch %s, 다음 루프에서 재평가", ref(w), e.status)
        return False
    w.clear()
    w.update(result)
    return True


def release(co, w):
    ann = {k: None for k in (K.ANN_HELD_FOR, K.ANN_HELD_AT, K.ANN_HELD_UID)}
    # A quota holder must not be deactivated, even if stale hold annotations remain.
    active = True if w["spec"].get("active") is False and unreserved(w) else None
    if patch(co, w, ann, active):
        log.info("%s: 쿼터 보류 해제", ref(w))
        return True
    return False


def gpu_quota(cq):
    rows = [(f["name"], r) for rg in cq["spec"].get("resourceGroups", [])
            for f in rg["flavors"] for r in f["resources"] if r["name"] == K.GPU]
    if len(rows) != 1 or not kube.is_true(cq, "Active") or cq["spec"].get("stopPolicy", "None") != "None":
        return None
    flavor, r = rows[0]
    used = sum(int(x["total"]) for f in (cq.get("status") or {}).get("flavorsReservation", [])
               if f["name"] == flavor for x in f["resources"] if x["name"] == K.GPU)
    return {"name": cq["metadata"]["name"], "flavor": flavor, "nominal": int(r["nominalQuota"]),
            "used": used, "lend": int(r.get("lendingLimit", r["nominalQuota"])),
            "borrow": int(r["borrowingLimit"]) if "borrowingLimit" in r else None,
            "cohort": cq["spec"].get("cohortName") or cq["spec"].get("cohort"),
            "strategy": cq["spec"].get("queueingStrategy", "BestEffortFIFO")}


def peers(q, queues):
    return [p for p in queues.values() if p and p["name"] != q["name"] and q["cohort"]
            and p["cohort"] == q["cohort"] and p["flavor"] == q["flavor"]]


def available(q, queues):
    ps = peers(q, queues)
    # Account for quota already borrowed by any peer; private (lendingLimit=0) quota
    # cannot be borrowed. Respect the target CQ's own borrowingLimit as well.
    pool = sum(min(max(p["nominal"] - p["used"], 0), p["lend"]) for p in ps)
    pool -= sum(max(p["used"] - p["nominal"], 0) for p in ps)
    free = q["nominal"] - q["used"]
    capacity = max(free + pool, 0)
    if q["borrow"] is not None:
        capacity = min(capacity, max(q["nominal"] + q["borrow"] - q["used"], 0))
    return capacity


def quota_reason(w):
    c = kube.cond(w, "QuotaReserved") or {}
    if c.get("status") != "False":
        return False
    if c.get("reason") == "WaitingForQuota":
        return True
    # v0.20 with granular observability disabled still reports Pending. Do not
    # mistake TAS/flavor/admission-check failures for quota starvation.
    msg = c.get("message", "").lower()
    return c.get("reason") == "Pending" and any(s in msg for s in (
        "insufficient unused quota", "insufficient quota", "couldn't assign flavors", "borrowing limit")) and "quota" in msg


def admitted_during(w, episode):
    at = (kube.cond(w, "QuotaReserved") or {}).get("lastTransitionTime")
    if not at or not episode:
        return False
    return datetime.fromisoformat(at.replace("Z", "+00:00")) >= datetime.fromisoformat(episode.replace("Z", "+00:00"))


def reconcile(mgr, workloads):
    """Runs under the dispatcher's Lease, before placement, including when workers are down."""
    held = [w for w in workloads if annotations(w).get(K.ANN_HELD_FOR)]
    if not ENABLED and not held:
        return  # Disabled baseline needs no additional queue permissions.
    co = kube.kueue(mgr)
    by_ref = {ref(w): w for w in workloads}
    # Always clean up obvious orphans first, even if queue reads subsequently fail.
    for w in held:
        a = annotations(w)
        target = by_ref.get(a[K.ANN_HELD_FOR])
        if (not ENABLED or not target or target["metadata"].get("uid") != a.get(K.ANN_HELD_UID)
                or not pending(target) or recovery(w)):
            release(co, w)
    if not ENABLED:
        return
    queues = {q["metadata"]["name"]: gpu_quota(q) for q in co.list_cluster_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", _request_timeout=kube.TIMEOUT)["items"]}
    local = {(q["metadata"]["namespace"], q["metadata"]["name"]): q for q in co.list_cluster_custom_object(
        K.KUEUE_GROUP, K.KUEUE_VERSION, "localqueues", _request_timeout=kube.TIMEOUT)["items"]}

    def queue(w):
        lq = local.get((w["metadata"]["namespace"], w["spec"].get("queueName")))
        if not lq or lq["spec"].get("stopPolicy", "None") != "None":
            return None
        return queues.get(lq["spec"]["clusterQueue"])

    # Cache owner reads for one loop. Failure propagates: never guess manual=false.
    manual = {ref(w): kube.owner_annotations(mgr, w).get(K.ANN_MANUAL) == "true"
              for w in workloads if pending(w) or annotations(w).get(K.ANN_HELD_FOR)}
    candidates = []
    for w in sorted(workloads, key=lambda x: (not recovery(x), order(x))):
        if not pending(w) or manual.get(ref(w)) or not quota_reason(w):
            continue
        q = queue(w)
        if not q:
            continue
        size = sum(kube.workload_pods(w))
        rec = recovery(w)
        if not 1 <= size <= 4:  # Job submitter / project maximum: one worker's training GPUs.
            continue
        if w["spec"].get("queueName") != (K.RECOVERY_QUEUE if rec else "queue-light"):
            continue
        incumbent = any(annotations(h).get(K.ANN_HELD_FOR) == ref(w)
                        and annotations(h).get(K.ANN_HELD_UID) == w["metadata"]["uid"] for h in held)
        if rec:
            # Recovery only needs help when it must borrow, and can eventually fit.
            max_borrow = sum(p["lend"] for p in peers(q, queues))
            if q["borrow"] is not None:
                max_borrow = min(max_borrow, q["borrow"])
            if size <= max(q["nominal"] - q["used"], 0) or size > q["nominal"] + max_borrow:
                continue
        elif q["strategy"] != "BestEffortFIFO" or not 1 < size <= q["nominal"]:
            continue
        # If borrowing can already satisfy a new candidate, no hold is necessary.
        if not incumbent and size <= available(q, queues):
            continue
        threshold = RECOVERY_N if rec else LIGHT_N
        episode = (kube.cond(w, "QuotaReserved") or {}).get("lastTransitionTime")
        try:
            saved = json.loads(annotations(w).get(K.ANN_QUOTA_BYPASSES, "{}"))
            if not isinstance(saved, dict) or not isinstance(saved.get("uids", []), list):
                raise ValueError("invalid quota bypass annotation")
            if not all(isinstance(uid, str) for uid in saved.get("uids", [])):
                raise ValueError("invalid quota bypass UIDs")
        except (ValueError, TypeError):
            log.warning("%s: 잘못된 quota-bypasses 기록, 새 예약 건너뜀", ref(w))
            continue
        seen = set(saved.get("uids", [])) if saved.get("since") == episode else set()
        for other in workloads:
            oq = queue(other)
            same_pool = oq and (oq["name"] == q["name"] or oq in peers(q, queues))
            if (order(other) > order(w) and kube.is_true(other, "QuotaReserved")
                    and admitted_during(other, episode)
                    and not recovery(other) and same_pool and (rec or oq["name"] == q["name"])):
                seen.add(other["metadata"]["uid"])
            if len(seen) >= threshold:
                break
        value = json.dumps({"since": episode, "uids": sorted(seen)[:threshold]}, separators=(",", ":"))
        if value != annotations(w).get(K.ANN_QUOTA_BYPASSES):
            if not patch(co, w, {K.ANN_QUOTA_BYPASSES: value}):
                continue
        if len(seen) >= threshold:
            candidates.append((w, q))
    chosen = candidates[0] if candidates else None

    def should_hold(w):
        if (not chosen or recovery(w) or manual.get(ref(w))
                or w["spec"].get("queueName") not in ("queue-light", "queue-heavy")):
            return False
        target, tq = chosen
        q = queue(w)
        return bool(ref(w) != ref(target) and q and (q["name"] == tq["name"] or q in peers(tq, queues))
                    and (recovery(target) or (q["name"] == tq["name"] and order(w) > order(target))))

    # Release previous ownership before making any new holds. Conflicts retry next loop.
    for w in held:
        a = annotations(w)
        if a.get(K.ANN_HELD_FOR) and (not should_hold(w) or not chosen
                or a[K.ANN_HELD_FOR] != ref(chosen[0]) or a.get(K.ANN_HELD_UID) != chosen[0]["metadata"]["uid"]):
            if not release(co, w):
                return
    if not chosen:
        return
    target, _ = chosen
    for w in workloads:
        if not pending(w) or annotations(w).get(K.ANN_HELD_FOR) or not should_hold(w):
            continue
        if patch(co, w, {K.ANN_HELD_FOR: ref(target), K.ANN_HELD_UID: target["metadata"]["uid"],
                         K.ANN_HELD_AT: kube.now_rfc3339()}, active=False):
            log.info("%s: %s 쿼터 확보까지 대기 차례 보류", ref(w), ref(target))
