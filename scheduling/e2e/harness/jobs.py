"""잡 제출: 백엔드 흉내. JobSet 템플릿 합의안 모양으로 1세대를 내고, 복구 원본·펜싱 기록 ConfigMap도 남긴다.

Pod GPU 수개 x GPU 1장 (노드당 GPU 1개 모양). 학습 대신 KWOK이 duration 뒤 Pod를 끝낸다.
"""
import copy
import json
import logging

from kubernetes import client

from .cluster import jsname
from .env import HEAVY_MIN_GPUS, MAX_JOB_GPUS, NS, REQUEST_TIMEOUT, STORE_NS, K
from .verdict import Invalid

log = logging.getLogger("e2e")


class Rejected(Exception):
    """잡 투입기가 거부하는 크기 (한 클러스터 학습 노드 수 초과)."""


def queue_for(gpus):
    """잡 투입기 규칙: 3장 이상 Heavy, 2장 이하 Light, MAX_JOB_GPUS 초과는 거부."""
    if gpus > MAX_JOB_GPUS:
        raise Rejected(f"GPU {gpus} > {MAX_JOB_GPUS}")
    return "queue-heavy" if gpus >= HEAVY_MIN_GPUS else "queue-light"


def _duration(d):
    return f"{d}s" if isinstance(d, int) else d


def jobset_body(job_id, gpus, queue, duration, annotations=None):
    name = jsname(job_id)
    labels = {K.LABEL_JOB_ID: job_id, K.LABEL_GENERATION: "1"}
    return {
        "apiVersion": f"{K.JOBSET_GROUP}/{K.JOBSET_VERSION}", "kind": "JobSet",
        "metadata": {"name": name, "namespace": NS,
                     "labels": {K.LABEL_QUEUE: queue,
                                K.LABEL_PRIORITY: K.PRIORITY_RECOVERY if queue == K.RECOVERY_QUEUE else K.PRIORITY_TRAINING,
                                **labels},
                     "annotations": {"scenario/gpus": str(gpus), **(annotations or {})}},
        "spec": {
            "failurePolicy": {"maxRestarts": 0},
            "network": {"enableDNSHostnames": True, "subdomain": name},
            "replicatedJobs": [{"name": "workers", "replicas": 1, "template": {"spec": {
                "completionMode": "Indexed", "parallelism": gpus, "completions": gpus, "backoffLimit": 0,
                "template": {
                    "metadata": {"labels": labels,
                                 "annotations": {"pod-complete.stage.kwok.x-k8s.io/delay": _duration(duration)}},
                    "spec": {
                        "restartPolicy": "Never",
                        "tolerations": [{"key": K.GPU, "operator": "Exists", "effect": "NoSchedule"}],
                        "containers": [{
                            "name": "trainer", "image": "busybox:1.36", "command": ["sh", "-c"],
                            "args": [f"exec sleep infinity --master-addr={name}-workers-0-0.{name}"],
                            "env": [{"name": "JOB_ID", "value": job_id}, {"name": "GENERATION", "value": "1"}],
                            "resources": {"requests": {K.GPU: 1}, "limits": {K.GPU: 1}}}]}}}}}]},
    }


def _apply_cm(kube, name, data):
    body = client.V1ConfigMap(metadata=client.V1ObjectMeta(name=name, namespace=STORE_NS), data=data)
    try:
        kube.core.create_namespaced_config_map(STORE_NS, body, _request_timeout=REQUEST_TIMEOUT)
    except client.ApiException as e:
        if e.status != 409:
            raise
        kube.core.replace_namespaced_config_map(name, STORE_NS, body, _request_timeout=REQUEST_TIMEOUT)


def submit(kube, job_id, gpus, queue, duration="20m", annotations=None):
    body = jobset_body(job_id, gpus, queue, duration, annotations)
    _apply_cm(kube, f"recovery-code-{job_id}", {"jobset.json": json.dumps(copy.deepcopy(body))})
    _apply_cm(kube, f"recovery-fencing-{job_id}", {"job_id": job_id, "generation": "1", "retry_count": "0"})
    try:
        kube.co.create_namespaced_custom_object(K.JOBSET_GROUP, K.JOBSET_VERSION, NS, "jobsets", body,
                                                _request_timeout=REQUEST_TIMEOUT)
    except client.ApiException as e:
        if e.status >= 500 or "failed calling webhook" in (e.body or ""):
            raise Invalid(f"{jsname(job_id)} 제출 실패 (인프라): {e.status} {(e.body or '')[:200]}") from e
        raise
    except Exception as e:  # noqa: BLE001  (timeout 등)
        raise Invalid(f"{jsname(job_id)} 제출 실패 (인프라): {e}") from e
    log.info("  제출: %s (GPU %d, %s, %s)", jsname(job_id), gpus, queue, _duration(duration))


def submit_auto(kube, job_id, gpus, duration="20m", annotations=None):
    submit(kube, job_id, gpus, queue_for(gpus), duration, annotations)
