"""Gitops·Argo 담당자가 반영한 예비 Pod의 OFF/ON 결과를 읽어서 확인한다."""
import argparse
import sys
import time

from kubernetes import client, config
from urllib3.exceptions import HTTPError

import eks_gate as G

DEPLOYMENT = "spare-placeholder"


def ready(core, apps, state, cluster):
    """클러스터 이름, Deployment 반영, 실제 Pod 상태가 모두 맞아야 통과한다."""
    try:
        identity = core.read_namespaced_config_map(G.CLUSTER_INFO, G.K.NAMESPACE, _request_timeout=G.REQUEST_TIMEOUT)
        dep = apps.read_namespaced_deployment(DEPLOYMENT, G.K.NAMESPACE, _request_timeout=G.REQUEST_TIMEOUT)
        pods = core.list_namespaced_pod(G.K.NAMESPACE, _request_timeout=G.REQUEST_TIMEOUT).items
    except client.ApiException as e:
        if e.status == 404:
            return False
        raise
    if (identity.data or {}).get("CLUSTER_NAME") != cluster:
        return False
    count = 0 if state == "off" else 1
    if dep.spec.replicas != count or (dep.status.observed_generation or 0) < dep.metadata.generation:
        return False
    placeholders = [p for p in pods if p.spec.priority_class_name == G.K.PLACEHOLDER_PRIORITY_CLASS
                    or (p.metadata.labels or {}).get("app") == DEPLOYMENT]
    if state == "off":
        return not placeholders and (dep.status.replicas or 0) == 0
    return (dep.status.available_replicas or 0) >= 1 and any(
        p.status.phase == "Running" and not p.metadata.deletion_timestamp
        and any(c.type == "Ready" and c.status == "True" for c in p.status.conditions or [])
        for p in placeholders)


def wait_ready(context, state, timeout, cluster):
    end = time.monotonic() + timeout
    with config.new_client_from_config(context=context) as api:
        core, apps = client.CoreV1Api(api), client.AppsV1Api(api)
        while True:
            if ready(core, apps, state, cluster):
                print(f"예비 Pod 확인 통과: {context} {state}")
                return 0
            remaining = end - time.monotonic()
            if remaining <= 0:
                print(f"예비 Pod 확인 시간 초과: {context} {state}. "
                      "클러스터 이름·replicas·rollout·Pod 상태와 Argo sync 결과를 확인한다")
                return 2
            time.sleep(min(2, remaining))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cluster")
    parser.add_argument("state", choices=("on", "off"))
    parser.add_argument("--worker-context", required=True)
    parser.add_argument("--timeout", type=G.positive, default=300)
    args = parser.parse_args(argv)
    try:
        return wait_ready(args.worker_context, args.state, args.timeout, args.cluster)
    except (client.ApiException, config.ConfigException, HTTPError, OSError) as e:
        print(f"예비 Pod 조회 실패: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
