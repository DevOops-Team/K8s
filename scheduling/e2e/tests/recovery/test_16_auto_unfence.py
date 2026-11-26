"""EKS OFF 펜싱 자동 해제 (GPU-277).

kind에서는 EKS를 지우고 다시 만들 수 없어서, 워커 ClusterQueue(train-cq·recovery-cq)를 지웠다 다시 만들어
"다시 만든 워커"를 흉내 낸다. 디스패처는 이 ClusterQueue UID로 워커가 바뀐 것을 안다.

- eks_gate fence -> 디스패처가 워커 식별값을 남김 -> 같은 워커인 동안은 펜싱 유지
- ClusterQueue 재생성 -> 라벨·표시가 사람 개입 없이 지워짐 -> 그 워커로 새 잡 지명
- relocator 펜싱(겹친 경우 포함)과 표시 없는 라벨은 재생성 뒤에도 남음
"""
import subprocess
import sys
import time

import pytest
from kubernetes import client

from harness import cluster as C
from harness import recovery as R
from harness.env import K, MGR_CONTEXT, REQUEST_TIMEOUT, ROOT, WORKERS
from harness.jobs import submit_auto
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("EKS OFF 펜싱 자동 해제 · relocator 펜싱 유지")]
WORKER = WORKERS[-1]
QUEUES = ("train-cq", "recovery-cq")
T = REQUEST_TIMEOUT


def mkc(kube):
    return kube.co.get_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", WORKER,
                                             _request_timeout=T)


def patch_mkc(kube, metadata):
    kube.co.patch_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters", WORKER,
                                        {"metadata": metadata}, _request_timeout=T)


def annotations(kube):
    return mkc(kube)["metadata"].get("annotations") or {}


def fence_eks_off():
    r = subprocess.run([sys.executable, str(ROOT / "ops" / "eks_gate.py"), "--manager-context", MGR_CONTEXT,
                        "fence", WORKER], capture_output=True, text=True, timeout=120)
    precondition(r.returncode == 0, f"eks_gate fence 실패 ({r.returncode}): {(r.stdout + r.stderr).strip()[-300:]}")
    return r.stdout


def wait_recorded(kube, check):
    seen = C.wait_until(lambda: annotations(kube).get(K.ANN_FENCED_WORKER), 30, interval=2)
    check.true("디스패처가 워커 식별값 기록", seen is not None)
    return seen is not None


def recreate_queues(kube):
    """워커 ClusterQueue를 지우고 같은 spec으로 다시 만든다 (EKS 재생성 흉내). reset 뒤라 입장한 잡이 없다"""
    co = client.CustomObjectsApi(kube.workers[WORKER])
    saved = []
    for name in QUEUES:
        cq = co.get_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", name, _request_timeout=T)
        meta = cq["metadata"]
        saved.append({"apiVersion": cq["apiVersion"], "kind": cq["kind"], "spec": cq["spec"],
                      "metadata": {k: meta[k] for k in ("name", "labels", "annotations") if meta.get(k)}})
        co.delete_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", name, _request_timeout=T)

    def gone():
        items = co.list_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues",
                                              _request_timeout=T)["items"]
        return not any(q["metadata"]["name"] in QUEUES for q in items)

    precondition(C.wait_until(gone, 60, interval=2) is not None, f"{WORKER} ClusterQueue가 60s 안에 안 지워짐")
    for body in saved:
        co.create_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", body, _request_timeout=T)


def test_eks_off_fence_released_after_recreate(kube, check, info):
    precondition(len(WORKERS) >= 2, "워커 2개 이상이 필요함")
    precondition(R.mkc_active(kube, WORKER), f"{WORKER} 연결이 Active가 아님")
    try:
        out = fence_eks_off()
        check.true("fence가 자동 해제 기준 기록을 알림", "자동 해제 기준 기록됨" in out, out[-200:])
        check.eq("붙인 쪽 표시 eks-off", annotations(kube).get(K.ANN_FENCED_BY), K.FENCED_BY_EKS_OFF)
        if not wait_recorded(kube, check):
            return
        time.sleep(20)   # 디스패처 루프 여러 번
        check.true("같은 워커인 동안 펜싱 유지", R.fenced(kube, WORKER))

        clock = time.monotonic()
        recreate_queues(kube)
        released = C.wait_until(lambda: not R.fenced(kube, WORKER), 90, interval=2)
        info["재생성 -> 자동 해제"] = f"{time.monotonic() - clock:.1f}s"
        check.true("워커 재생성 뒤 90s 안에 라벨 자동 해제", released is not None)
        if released is None:
            return
        ann = annotations(kube)
        check.true("펜싱 표시도 지워짐", K.ANN_FENCED_BY not in ann and K.ANN_FENCED_WORKER not in ann, ann)
        submit_auto(kube, "after-auto-unfence", 1, "20m", {K.ANN_TARGET: WORKER})
        placed = R.wait_placed(kube, "job-after-auto-unfence-g0001", 120)
        check.eq(f"해제 뒤 새 잡이 {WORKER}로 지명", placed, WORKER)
    finally:
        patch_mkc(kube, {"labels": {K.LABEL_FENCED: None},
                         "annotations": {K.ANN_FENCED_BY: None, K.ANN_FENCED_WORKER: None}})


@pytest.mark.parametrize("case", ["overlap", "relocator", "unmarked"])
def test_recovery_fence_kept_after_recreate(kube, check, info, case):
    precondition(R.mkc_active(kube, WORKER), f"{WORKER} 연결이 Active가 아님")
    try:
        if case == "overlap":   # EKS OFF 중에 relocator도 펜싱 (relocator isolate와 같은 patch)
            fence_eks_off()
            if not wait_recorded(kube, check):
                return
            patch_mkc(kube, {"labels": {K.LABEL_FENCED: "true"},
                             "annotations": {K.ANN_FENCED_BY: K.FENCED_BY_RELOCATOR}})
        elif case == "relocator":
            patch_mkc(kube, {"labels": {K.LABEL_FENCED: "true"},
                             "annotations": {K.ANN_FENCED_BY: K.FENCED_BY_RELOCATOR}})
        else:   # 표시 없는 옛 라벨
            patch_mkc(kube, {"labels": {K.LABEL_FENCED: "true"}})
        recreate_queues(kube)
        time.sleep(30)
        check.true(f"{case}: 워커 재생성 뒤에도 펜싱 유지", R.fenced(kube, WORKER))
    finally:
        patch_mkc(kube, {"labels": {K.LABEL_FENCED: None},
                         "annotations": {K.ANN_FENCED_BY: None, K.ANN_FENCED_WORKER: None}})
