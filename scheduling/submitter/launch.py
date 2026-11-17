"""잡 투입기를 실험 하나당 Job 하나로 띄운다 (관리 클러스터 platform). 실험할 때마다 손으로 밟던 절차를 한 번에.

    python launch.py scenario.csv --exp-id exp-0001 --image <투입기 이미지> -- --policy ours --fault-type xid79 --spare

하는 일:
  1. 시나리오 CSV를 ConfigMap submitter-<exp_id>로 올린다
  2. Job submitter-<exp_id>를 만든다. 투입기 인자에 --exp-id를 넣으므로 experiment.started·ended와
     모든 POST /api/jobs 요청에 같은 exp_id가 들어간다 (측정이 실험 단위로 묶임)
  3. ConfigMap을 Job에 딸리게 해서 Job이 지워질 때 같이 지워지게 한다
같은 exp_id의 ConfigMap이나 Job이 이미 있으면 띄우지 않는다 (같은 실험이 두 번 돌면 측정이 섞임).

-- 뒤 인자는 submit.py에 그대로 넘긴다 (실험 조건: --policy --fault-type --spare --checkpoint-every-steps
--wait-timeout --speed 등). --exp-id·--out·--scenario-name·--print-results·--kind는 이 스크립트가 정한다.
필요한 것: ServiceAccount submitter (platform)와 training JobSet get·list 권한 (Gitops),
이 스크립트를 실행하는 사람의 platform Job·ConfigMap 생성·삭제 권한.
"""
import argparse
import os
import re
import sys
from pathlib import Path

NAMESPACE = "platform"
SERVICE_ACCOUNT = "submitter"
EXP_ID = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
MAX_EXP_ID = 63 - len("submitter-")   # Job 이름·Pod job-name 라벨 63자
RESERVED = ("--exp-id", "--out", "--scenario-name", "--print-results", "--kind")
ENV_DEFAULTS = {"BACKEND_URL": "http://backend.platform.svc:8000", "KAFKA_BOOTSTRAP": "172.30.0.40:9092",
                "KAFKA_TOPIC": "job.lifecycle", "MODEL": "gpt2", "DATASET_ID": "ds-demo", "DATASET_VERSION": "1"}


def check_exp_id(exp_id):
    if not EXP_ID.match(exp_id) or len(exp_id) > MAX_EXP_ID:
        raise ValueError(f"exp_id {exp_id!r}: 소문자·숫자·'-'만, {MAX_EXP_ID}자 이하 (Job 이름에 들어감)")
    return exp_id


def check_passthrough(extra):
    bad = [x for x in extra if x.split("=", 1)[0] in RESERVED]
    if bad:
        raise ValueError(f"{' '.join(bad)}: launch.py가 정하는 인자라 -- 뒤에 넣지 않는다")
    return extra


def names(exp_id):
    return f"submitter-{exp_id}"


def build_configmap(exp_id, scenario_path):
    return {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": names(exp_id), "namespace": NAMESPACE, "labels": {"devoops/exp-id": exp_id}},
            "data": {"scenario.csv": Path(scenario_path).read_text(encoding="utf-8")}}


def build_job(exp_id, image, scenario_name, extra, env=None):
    env = {**ENV_DEFAULTS, **(env or {}), "SUBMITTER_IMAGE": image}
    args = ["/input/scenario.csv", "--exp-id", exp_id, "--scenario-name", scenario_name,
            "--out", "/tmp/results.csv", "--print-results", *extra]
    meta = {"labels": {"app": "submitter", "devoops/exp-id": exp_id}, "annotations": {"submitter/image": image}}
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": names(exp_id), "namespace": NAMESPACE, **meta},
            "spec": {"backoffLimit": 0, "ttlSecondsAfterFinished": 86400,   # 한 번만 돈다: 실패해도 다시 안 띄움
                     "template": {"metadata": meta, "spec": {
                         "serviceAccountName": SERVICE_ACCOUNT, "restartPolicy": "Never",
                         "securityContext": {"runAsNonRoot": True, "runAsUser": 65532,
                                             "seccompProfile": {"type": "RuntimeDefault"}},
                         "containers": [{
                             "name": "submitter", "image": image, "args": args,
                             "env": [{"name": k, "value": v} for k, v in env.items()],
                             "volumeMounts": [{"name": "input", "mountPath": "/input", "readOnly": True},
                                              {"name": "tmp", "mountPath": "/tmp"}],
                             "resources": {"requests": {"cpu": "50m", "memory": "128Mi"},
                                           "limits": {"memory": "256Mi"}},
                             "securityContext": {"allowPrivilegeEscalation": False,
                                                 "capabilities": {"drop": ["ALL"]}}}],
                         "volumes": [{"name": "input", "configMap": {"name": names(exp_id)}},
                                     {"name": "tmp", "emptyDir": {}}]}}}}


class Exists(Exception):
    pass


def launch(core, batch, exp_id, cm, job, api_exception):
    """ConfigMap -> Job -> ConfigMap을 Job에 딸리게. 같은 이름이 있으면 Exists (만든 ConfigMap은 되돌림)"""
    name = names(exp_id)
    try:
        core.create_namespaced_config_map(NAMESPACE, cm)
    except api_exception as e:
        if e.status == 409:
            raise Exists(f"ConfigMap {NAMESPACE}/{name}이 이미 있음: 같은 exp_id 실험이 돌았거나 도는 중") from e
        raise
    try:
        created = batch.create_namespaced_job(NAMESPACE, job)
    except api_exception as e:
        core.delete_namespaced_config_map(name, NAMESPACE)
        if e.status == 409:
            raise Exists(f"Job {NAMESPACE}/{name}이 이미 있음: 같은 exp_id 실험이 돌았거나 도는 중") from e
        raise
    uid = created["metadata"]["uid"] if isinstance(created, dict) else created.metadata.uid
    core.patch_namespaced_config_map(name, NAMESPACE, {"metadata": {"ownerReferences": [
        {"apiVersion": "batch/v1", "kind": "Job", "name": name, "uid": uid}]}})
    return name


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    extra = []
    if "--" in argv:
        i = argv.index("--")
        argv, extra = argv[:i], argv[i + 1:]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", help="시나리오 CSV (submit.py 형식)")
    ap.add_argument("--exp-id", required=True, help="실험 ID. Job 이름과 모든 사건·제출 요청에 들어감")
    ap.add_argument("--image", default=os.getenv("SUBMITTER_IMAGE"), help="투입기 이미지 (태그 또는 digest)")
    ap.add_argument("--context", default=None, help="관리 클러스터 kubeconfig context (기본: 현재 context)")
    for k in ENV_DEFAULTS:
        ap.add_argument(f"--{k.lower().replace('_', '-')}", dest=k, default=os.getenv(k),
                        help=f"투입기 환경 변수 {k} (기본 {ENV_DEFAULTS[k]})")
    ap.add_argument("--dry-run", action="store_true", help="만들 ConfigMap·Job을 출력만")
    a = ap.parse_args(argv)
    if not a.image:
        ap.error("--image (또는 SUBMITTER_IMAGE)가 필요하다: 어떤 코드로 돌렸는지 결과에 남기려고")
    try:
        exp_id = check_exp_id(a.exp_id)
        extra = check_passthrough(extra)
    except ValueError as e:
        ap.error(str(e))
    cm = build_configmap(exp_id, a.scenario)
    job = build_job(exp_id, a.image, Path(a.scenario).name, extra,
                    {k: getattr(a, k) for k in ENV_DEFAULTS if getattr(a, k)})
    if a.dry_run:
        import json
        print(json.dumps([cm, job], ensure_ascii=False, indent=2))
        return 0

    from kubernetes import client, config
    api = config.new_client_from_config(context=a.context)
    try:
        name = launch(client.CoreV1Api(api), client.BatchV1Api(api), exp_id, cm, job, client.ApiException)
    except Exists as e:
        print(f"띄우지 않음: {e}", file=sys.stderr)
        return 1
    print(f"Job {NAMESPACE}/{name} 띄움 (exp_id={exp_id}, image={a.image})")
    print(f"로그: kubectl -n {NAMESPACE} logs -f job/{name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
