"""EKS 워커 OFF/ON 준비 상태를 확인한다."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
import time

from kubernetes import client, config
from urllib3.exceptions import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
import keys as K

DEPLOYMENTS = (("kueue-system", "kueue-controller-manager"),
               ("jobset-system", "jobset-controller-manager"))
CLUSTER_QUEUES = ("train-cq", "recovery-cq")
LOCAL_QUEUES = ("queue-heavy", "queue-light", "queue-recovery")
CLUSTER_INFO = "devoops-cluster-info"
RELOCATOR = ("platform", "relocator")
MKC = "multikueueclusters"
REQUEST_TIMEOUT = 10
FINAL = ("Done", "Failed", "Duplicate")


def parse_ack(value):
    """시간대가 있는 RFC3339 시각만 받는다. 잘못된 값은 아직 없음으로 본다."""
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


class Gate:
    def __init__(self, manager, worker=None):
        self.manager = manager
        self.worker = worker

    def mkc(self, cluster):
        return self.manager.get_cluster_custom_object(
            K.KUEUE_GROUP, K.KUEUE_VERSION, MKC, cluster, _request_timeout=REQUEST_TIMEOUT)

    def label(self, cluster, value):
        self.manager.patch_cluster_custom_object(
            K.KUEUE_GROUP, K.KUEUE_VERSION, MKC, cluster,
            {"metadata": {"labels": {K.LABEL_FENCED: value}}}, _request_timeout=REQUEST_TIMEOUT)

    def objects(self, plural, group=K.KUEUE_GROUP, version=K.KUEUE_VERSION):
        """페이지가 나뉘어도 빠짐없이 읽는다."""
        token = None
        while True:
            page = self.manager.list_namespaced_custom_object(
                group, version, K.NAMESPACE, plural, _continue=token, _request_timeout=REQUEST_TIMEOUT)
            yield from page.get("items", [])
            token = (page.get("metadata") or {}).get("continue")
            if not token:
                break

    def off_check(self, cluster):
        self.mkc(cluster)
        failures = []
        for w in self.objects("workloads"):
            st, meta = w.get("status") or {}, w["metadata"]
            if condition(st, "Finished"):
                continue
            owner = next((o.get("name") for o in meta.get("ownerReferences") or []
                          if o.get("kind") == "JobSet"), "미정")
            desc = f"{K.NAMESPACE}/{meta['name']} (jobset {owner})"
            for blocked, reason in (
                    (st.get("clusterName") == cluster, "실행 중"),
                    (cluster in (st.get("nominatedClusterNames") or []), "보내는 중"),
                    ((meta.get("annotations") or {}).get(K.ANN_RESERVED) == cluster, "자리 예약")):
                if blocked:
                    failures.append(f"{reason}: {desc}")
        for rr in self.objects(K.RR_PLURAL, K.RR_GROUP, K.RR_VERSION):
            st = rr.get("status") or {}
            name = rr["metadata"]["name"]
            if not st.get("phase"):
                failures.append(f"처리 전 RR: {name} (대상 미정)")
            elif st["phase"] not in FINAL and cluster in (st.get("sourceCluster"), st.get("targetCluster")):
                failures.append(f"처리 중 RR: {name} ({st['phase']})")
        return verdict(failures, f"OFF 가능: {cluster}")

    def fence(self, cluster, timeout):
        started = datetime.now(timezone.utc).replace(microsecond=0)
        print(f"펜싱 기준 시각: {started.isoformat().replace('+00:00', 'Z')}")
        end = time.monotonic() + timeout
        m = self.mkc(cluster)
        if (m.get("metadata", {}).get("labels") or {}).get(K.LABEL_FENCED) != "true":
            self.label(cluster, "true")
        try:
            while True:
                ack = parse_ack((self.mkc(cluster).get("metadata", {}).get("annotations") or {}).get(K.ANN_FENCED_ACK))
                if ack is not None and ack > started:
                    return self.off_check(cluster)
                print(f"fenced-ack: {ack.isoformat() if ack else '아직 없음'}")
                remaining = end - time.monotonic()
                if remaining <= 0:
                    print("디스패처가 안 돌거나 patch 권한이 없음. 디스패처 로그 확인")
                    return 2
                time.sleep(min(2, remaining))
        finally:
            print("라벨은 유지됨. 취소하려면 on-check --unfence가 아니라 "
                  f"kubectl label multikueuecluster {cluster} {K.LABEL_FENCED}-")

    def on_check(self, cluster, no_spare=False, unfence=False):
        co, core, apps = self.worker
        failures = []

        def exists(desc, read, valid=lambda obj: True):
            try:
                obj = read()
            except client.ApiException as e:
                if e.status != 404:
                    raise
                failures.append(f"없음: {desc}")
                return
            if not valid(obj):
                failures.append(f"준비 안 됨: {desc}")

        for ns, name in DEPLOYMENTS:
            exists(f"Deployment {ns}/{name}",
                   lambda: apps.read_namespaced_deployment(name, ns, _request_timeout=REQUEST_TIMEOUT),
                   lambda d: (d.status.available_replicas or 0) >= 1)
        for name in CLUSTER_QUEUES:
            exists(f"ClusterQueue {name}", lambda: co.get_cluster_custom_object(
                K.KUEUE_GROUP, K.KUEUE_VERSION, "clusterqueues", name, _request_timeout=REQUEST_TIMEOUT))
        for name in LOCAL_QUEUES:
            exists(f"LocalQueue {name}", lambda: co.get_namespaced_custom_object(
                K.KUEUE_GROUP, K.KUEUE_VERSION, K.NAMESPACE, "localqueues", name, _request_timeout=REQUEST_TIMEOUT))
        exists(f"ConfigMap {CLUSTER_INFO} CLUSTER_NAME={cluster}",
               lambda: core.read_namespaced_config_map(CLUSTER_INFO, K.NAMESPACE, _request_timeout=REQUEST_TIMEOUT),
               lambda cm: (cm.data or {}).get("CLUSTER_NAME") == cluster)
        if not no_spare:
            ps = core.list_namespaced_pod(K.NAMESPACE, _request_timeout=REQUEST_TIMEOUT).items
            if not any(p.spec.priority_class_name == K.PLACEHOLDER_PRIORITY_CLASS and p.status.phase == "Running"
                       for p in ps):
                failures.append("준비 안 됨: Running 예비 Pod")
        ns, name = RELOCATOR
        exists(f"ServiceAccount {ns}/{name}",
               lambda: core.read_namespaced_service_account(name, ns, _request_timeout=REQUEST_TIMEOUT))
        exists(f"MultiKueueCluster {cluster} Active", lambda: self.mkc(cluster),
               lambda m: condition(m.get("status") or {}, "Active"))
        if failures:
            return verdict(failures, "")
        if unfence:
            self.label(cluster, None)
        print(f"ON 가능: {cluster}")
        return 0


def condition(status, name):
    return any(c.get("type") == name and c.get("status") == "True" for c in status.get("conditions") or [])


def verdict(failures, success):
    for failure in failures:
        print(failure)
    if not failures:
        print(success)
    return int(bool(failures))


def positive(value):
    number = float(value)
    if not 0 < number < float("inf"):
        raise argparse.ArgumentTypeError("양의 유한한 초를 지정한다")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    contexts = argparse.ArgumentParser(add_help=False)
    contexts.add_argument("--manager-context", default=argparse.SUPPRESS)
    contexts.add_argument("--worker-context", default=argparse.SUPPRESS)
    parser.add_argument("--manager-context", default="kind-manager")
    parser.add_argument("--worker-context")
    subs = parser.add_subparsers(dest="command", required=True)
    for command in ("off-check", "fence", "on-check"):
        sub = subs.add_parser(command, parents=[contexts])
        sub.add_argument("cluster")
        if command == "fence":
            sub.add_argument("--ack-timeout", type=positive, default=60)
        if command == "on-check":
            sub.add_argument("--no-spare", action="store_true")
            sub.add_argument("--unfence", action="store_true")
    args = parser.parse_args(argv)
    try:
        with config.new_client_from_config(context=args.manager_context) as manager:
            gate = Gate(client.CustomObjectsApi(manager))
            if args.command == "off-check":
                return gate.off_check(args.cluster)
            if args.command == "fence":
                return gate.fence(args.cluster, args.ack_timeout)
            with config.new_client_from_config(context=args.worker_context or f"kind-{args.cluster}") as worker:
                gate.worker = (client.CustomObjectsApi(worker), client.CoreV1Api(worker), client.AppsV1Api(worker))
                return gate.on_check(args.cluster, args.no_spare, args.unfence)
    except (client.ApiException, config.ConfigException, HTTPError, OSError) as e:
        print(f"접속·API 오류: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
