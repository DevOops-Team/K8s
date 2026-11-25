"""재제출 JobSet 만들기 — 순수 함수만 (쿠버네티스·AWS 없이 test_relocator.py로 확인).

원본은 백엔드가 최초 제출 때 저장한 jobset.json (S3 code/{job_id}/jobset.json, ADR-12).
바꾸는 값은 JobSet 템플릿 합의안 6절 목록뿐이고, 나머지는 한 글자도 같게 둔다.

| 경로                                             | 1세대 -> 2세대 |
| metadata.name · spec.network.subdomain            | job-{id}-g0001 -> job-{id}-g0002 |
| label queue-name / priority-class                 | queue-heavy·light / training -> queue-recovery / recovery |
| label devoops/generation (JobSet · Pod 템플릿)    | 1 -> 2 |
| annotation devoops/recovery-id                    | (없음) -> recovery-... |
| trainer args --master-addr                        | job-{id}-g0001-workers-0-0.job-{id}-g0001 -> ...g0002 |
| command·args·env 값 안의 Pod 주소 (getent 대기 줄 등) | job-{id}-g0001-{rj}-{i}-{j}.job-{id}-g0001 -> ...g0002 |
| env GENERATION (모든 컨테이너)                    | 1 -> 2 |
| 배치 힌트 어노테이션                              | recovery-source-cluster(원래 우선) · exclude-clusters |
| Pod 템플릿 tolerations                            | + devoops/spare=true:NoSchedule (예비 노드에 갈 수 있게, 11/10 정본) |

예비 노드 toleration을 Kueue ResourceFlavor가 아니라 여기서 넣는 이유: 4장 복구 잡이 학습 노드 3대 + 예비 1대에
걸쳐 뜨려면 학습 몫을 빌려야 하고(같은 flavor), flavor에 toleration을 두면 그 flavor를 쓰는 학습 잡도 예비에 간다.
학습 잡이 이 toleration을 달고 오는 것은 관리 클러스터 정책(admission-policy.yaml)이 막는다.
"""
import copy
import re

import keys as K

PREFER_SOURCE = {"restartInPlace", "moveToSpare"}
SPARE_TOLERATION = {"key": K.TAINT_SPARE, "operator": "Equal", "value": "true", "effect": "NoSchedule"}
_GEN_NAME = re.compile(r"^job-(?P<id>.+)-g(?P<gen>\d{4})$")


def jobset_name(job_id: str, gen: int) -> str:
    """job-{작업ID}-g{세대 4자리}. 같은 세대는 같은 이름 -> 중복 제출은 AlreadyExists"""
    return f"job-{job_id}-g{gen:04d}"


def parse_name(name: str):
    """job-{id}-g0002 -> ("id", 2). 형식이 아니면 (None, None)"""
    m = _GEN_NAME.match(name or "")
    return (m["id"], int(m["gen"])) if m else (None, None)


def placement_hints(action, src, old_exclude_csv, extra_exclude):
    """재제출 잡의 (target-cluster, exclude-clusters 목록)
    restartInPlace·moveToSpare는 별도 원래 클러스터 선호 힌트로 배치한다 (ADR-38)."""
    exclude = {c for c in (old_exclude_csv or "").split(",") if c} | set(extra_exclude or [])
    if action == "evacuateCluster" and src:
        exclude.add(src)
    return None, sorted(exclude)


def _pod_specs(js):
    for rj in (js.get("spec") or {}).get("replicatedJobs") or []:
        tmpl = ((rj.get("template") or {}).get("spec") or {}).get("template") or {}
        yield tmpl


def _pod_addresses(job_id):
    """옛 세대 Pod 주소 job-{id}-g{n}-{replicatedJob}-{i}-{j}.job-{id}-g{n} (뒤의 .ns.svc… 는 그대로 둔다).
    대기 줄(getent hosts …)처럼 --master-addr 밖에 박힌 주소도 이 형태면 새 세대로 바꾼다.
    로그 경로처럼 주소 형태가 아닌 옛 이름은 건드리지 않는다."""
    jid = re.escape(job_id)
    return re.compile(rf"(?<![\w.-])job-{jid}-g(\d{{4}})-([a-z0-9-]+-\d+-\d+)\.job-{jid}-g\1(?![\w-])")


def _new_addresses(addresses, name, value):
    return addresses.sub(lambda m: f"{name}-{m[2]}.{name}", value)


def _master_addresses(values, old_names, addresses, name):
    """--master-addr 값과 Pod 주소 형태만 새 이름으로 바꾸고, argv·셸 서식은 그대로 둔다."""
    option = re.compile(r"(?<![\w-])(--master-addr(?:=|\s+))([^\s;]+)")
    out = []
    previous = None
    for value in values:
        if previous == "--master-addr":
            updated = old_names.sub(name, value)
        else:
            updated = option.sub(lambda m: m[1] + old_names.sub(name, m[2]), value)
        out.append(_new_addresses(addresses, name, updated))
        previous = value
    return out


def render(orig: dict, job_id: str, gen: int, recovery_id: str, target, exclude, *, preferred=None) -> dict:
    """원본 jobset.json -> gen 세대 재제출본. orig는 바꾸지 않는다"""
    new = copy.deepcopy(orig)
    new.pop("status", None)
    meta = new.setdefault("metadata", {})
    for k in ("uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "selfLink"):
        meta.pop(k, None)
    name = jobset_name(job_id, gen)
    meta["name"] = name

    labels = meta.setdefault("labels", {})
    labels[K.LABEL_QUEUE] = K.RECOVERY_QUEUE
    labels[K.LABEL_PRIORITY] = K.PRIORITY_RECOVERY
    labels[K.LABEL_GENERATION] = str(gen)

    ann = meta.setdefault("annotations", {})
    ann[K.ANN_RECOVERY_ID] = recovery_id
    ann.pop(K.ANN_TARGET, None)
    ann.pop(K.ANN_PREFERRED, None)
    ann.pop(K.ANN_EXCLUDE, None)
    if target:
        ann[K.ANN_TARGET] = target
    if preferred:
        ann[K.ANN_PREFERRED] = preferred
    if exclude:
        ann[K.ANN_EXCLUDE] = ",".join(exclude)

    network = (new.get("spec") or {}).get("network")
    if network and "subdomain" in network:
        network["subdomain"] = name

    old_names = re.compile(rf"job-{re.escape(job_id)}-g\d{{4}}")
    addresses = _pod_addresses(job_id)
    for tmpl in _pod_specs(new):
        tmeta = tmpl.setdefault("metadata", {})
        tmeta.setdefault("labels", {})[K.LABEL_GENERATION] = str(gen)
        spec = tmpl.setdefault("spec", {})
        tols = spec.setdefault("tolerations", [])
        if not any(t.get("key") == K.TAINT_SPARE for t in tols):
            tols.append(dict(SPARE_TOLERATION))
        for ct in (spec.get("initContainers") or []) + (spec.get("containers") or []):
            for e in ct.get("env") or []:
                if e.get("name") == "GENERATION" and "value" in e:
                    e["value"] = str(gen)
                elif isinstance(e.get("value"), str):
                    e["value"] = _new_addresses(addresses, name, e["value"])
            command = ct.get("command") or []
            args = ct.get("args") or []
            values = _master_addresses(command + args, old_names, addresses, name)
            if command:
                ct["command"] = values[:len(command)]
            if args:
                ct["args"] = values[len(command):]
    return new

