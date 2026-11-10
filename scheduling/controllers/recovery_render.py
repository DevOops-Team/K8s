"""재제출 JobSet 만들기 — 순수 함수만 (쿠버네티스·AWS 없이 test_relocator.py로 확인).

원본은 백엔드가 최초 제출 때 저장한 jobset.json (S3 code/{job_id}/jobset.json, ADR-12).
바꾸는 값은 JobSet 템플릿 합의안 6절 목록뿐이고, 나머지는 한 글자도 같게 둔다.

| 경로                                             | 1세대 -> 2세대 |
| metadata.name · spec.network.subdomain            | job-{id}-g0001 -> job-{id}-g0002 |
| label queue-name / priority-class                 | queue-heavy·light / training -> queue-recovery / recovery |
| label devoops/generation (JobSet · Pod 템플릿)    | 1 -> 2 |
| annotation devoops/recovery-id                    | (없음) -> recovery-... |
| trainer args --master-addr                        | job-{id}-g0001-workers-0-0.job-{id}-g0001 -> ...g0002 |
| env GENERATION (모든 컨테이너)                    | 1 -> 2 |
| target / exclude 어노테이션                       | (없음) -> devoops/target-cluster · devoops/exclude-clusters |
| Pod 템플릿 tolerations                            | + devoops/spare=true:NoSchedule (예비 노드에 갈 수 있게, 11/10 정본) |

예비 노드 toleration을 Kueue ResourceFlavor가 아니라 여기서 넣는 이유: 4장 복구 잡이 학습 노드 3대 + 예비 1대에
걸쳐 뜨려면 학습 몫을 빌려야 하고(같은 flavor), flavor에 toleration을 두면 그 flavor를 쓰는 학습 잡도 예비에 간다.
학습 잡이 이 toleration을 달고 오는 것은 관리 클러스터 정책(admission-policy.yaml)이 막는다.
"""
import copy
import re

import keys as K

LEAVE_SOURCE = {"moveToOtherCluster", "evacuateCluster"}   # 원래 클러스터를 떠나는 action
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
    restartInPlace·moveToSpare: 원래 클러스터로 고정 / moveToOtherCluster·evacuateCluster: 원래 클러스터를 뺌"""
    exclude = {c for c in (old_exclude_csv or "").split(",") if c} | set(extra_exclude or [])
    if action in LEAVE_SOURCE:
        if src:
            exclude.add(src)
        return None, sorted(exclude)
    return (src or None), sorted(exclude - {src})


def _pod_specs(js):
    for rj in (js.get("spec") or {}).get("replicatedJobs") or []:
        tmpl = ((rj.get("template") or {}).get("spec") or {}).get("template") or {}
        yield tmpl


def render(orig: dict, job_id: str, gen: int, recovery_id: str, target, exclude) -> dict:
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
    ann.pop(K.ANN_EXCLUDE, None)
    if target:
        ann[K.ANN_TARGET] = target
    if exclude:
        ann[K.ANN_EXCLUDE] = ",".join(exclude)

    network = (new.get("spec") or {}).get("network")
    if network and "subdomain" in network:
        network["subdomain"] = name

    old_names = re.compile(rf"job-{re.escape(job_id)}-g\d{{4}}")
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
            for field in ("command", "args"):
                if ct.get(field):
                    ct[field] = [old_names.sub(name, a) if "--master-addr" in a else a for a in ct[field]]
    return new


def check_fencing(rec, request_id, old_gen):
    """재배치를 시작해도 되는지 펜싱 기록(DynamoDB)으로 판단 -> ("go" | "wait" | "done" | "fail", 이유, 새 세대)
    복구 컨트롤러가 세대 +1(GENERATION_ADVANCED)까지 해 둔 요청만 처리한다 (체크포인트 규약 5·8절)"""
    if not rec:
        return "wait", "펜싱 기록 없음", None
    if rec.get("recovery_id") != request_id:
        return "wait", f"펜싱 기록의 recovery_id={rec.get('recovery_id')} (이 요청 {request_id} 아님)", None
    phase = rec.get("recovery_phase")
    to_gen = rec.get("recovery_to_gen")
    if phase == "CLAIMED":
        return "wait", "복구 컨트롤러가 아직 세대 +1 전 (CLAIMED)", None
    if phase in ("RESUMING", "COMPLETED"):
        return "done", f"이미 {phase}", to_gen
    if phase not in ("GENERATION_ADVANCED", "OLD_RUN_STOPPED", "NEW_RUN_SUBMITTED"):
        return "fail", f"모르는 recovery_phase {phase}", None
    if to_gen is None or rec.get("recovery_from_gen") is None:
        return "fail", "recovery_from_gen·recovery_to_gen 없음", None
    if old_gen is not None and int(rec["recovery_from_gen"]) != old_gen:
        return "fail", f"요청한 잡은 {old_gen}세대인데 펜싱 기록은 {rec['recovery_from_gen']}세대 복구", None
    return "go", phase, int(to_gen)
