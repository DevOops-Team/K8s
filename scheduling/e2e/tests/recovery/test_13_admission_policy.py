"""관리 클러스터 입장 정책 (controllers/deploy/admission-policy.yaml, 정본 3.1·5절·I4).

권한 없는 사용자(e2e-tester, 이 케이스 동안만 training에 JobSet·RR create 권한)로 흉내 내서 정책이 막는지 본다.
전부 dry-run이라 아무것도 안 남는다. 관리자도 예외 없는 것은 예비 노드 toleration 하나뿐이다.
"""
import copy

import pytest
from kubernetes import client, config

from harness.env import MGR_CONTEXT, NS, REQUEST_TIMEOUT, K
from harness.jobs import jobset_body
from harness.verdict import precondition

pytestmark = [pytest.mark.recovery, pytest.mark.doc("3.1 VAP · 5절 · I4"), pytest.mark.any_controllers]
USER, GROUP = "e2e-tester", "e2e:testers"
ROLE = "e2e-admission-tester"
T = REQUEST_TIMEOUT


def _impersonated():
    api = config.new_client_from_config(context=MGR_CONTEXT)
    api.set_default_header("Impersonate-User", USER)
    api.set_default_header("Impersonate-Group", GROUP)
    return client.CustomObjectsApi(api)


def _grant(kube):
    rbac = client.RbacAuthorizationV1Api(kube.mgr)
    role = client.V1Role(metadata=client.V1ObjectMeta(name=ROLE, namespace=NS), rules=[
        client.V1PolicyRule(api_groups=[K.JOBSET_GROUP], resources=["jobsets"], verbs=["create"]),
        client.V1PolicyRule(api_groups=[K.RR_GROUP], resources=[K.RR_PLURAL], verbs=["create"])])
    rb = client.V1RoleBinding(metadata=client.V1ObjectMeta(name=ROLE, namespace=NS),
                              role_ref=client.V1RoleRef(api_group="rbac.authorization.k8s.io", kind="Role", name=ROLE),
                              subjects=[client.RbacV1Subject(kind="Group", name=GROUP,
                                                             api_group="rbac.authorization.k8s.io")])
    for fn, body in ((rbac.create_namespaced_role, role), (rbac.create_namespaced_role_binding, rb)):
        try:
            fn(NS, body, _request_timeout=T)
        except client.ApiException as e:
            if e.status != 409:
                raise


def _revoke(kube):
    rbac = client.RbacAuthorizationV1Api(kube.mgr)
    for fn in (rbac.delete_namespaced_role_binding, rbac.delete_namespaced_role):
        try:
            fn(ROLE, NS, _request_timeout=T)
        except client.ApiException as e:
            if e.status != 404:
                raise


def _try(co, plural, body, group=K.JOBSET_GROUP, version=K.JOBSET_VERSION):
    """dry-run 생성. 반환: (허용됨, 막은 정책 이름 또는 오류 요약)"""
    try:
        co.create_namespaced_custom_object(group, version, NS, plural, body, dry_run="All", _request_timeout=T)
        return True, ""
    except client.ApiException as e:
        text = e.body or ""
        policy = next((p for p in POLICIES if f"'{p}'" in text), None)
        return False, policy or f"{e.status} {text[:160]}"


POLICIES = ("scheduling-relocation-request-writers", "scheduling-recovery-queue",
            "scheduling-dispatch-annotations", "scheduling-spare-toleration")


def _js(name, queue="queue-light", tolerations=(), annotations=None, priority=None):
    b = jobset_body(name, 1, queue, "20m", annotations)
    if priority:
        b["metadata"]["labels"][K.LABEL_PRIORITY] = priority
    spec = b["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]
    spec["tolerations"] += [copy.deepcopy(t) for t in tolerations]
    return b


SPARE_TOL = {"key": K.TAINT_SPARE, "operator": "Equal", "value": "true", "effect": "NoSchedule"}


def test_admission_policy(kube, check, info):
    """권한 없는 사용자는 복구 큐·복구 우선순위·디스패처 어노테이션·RR을 못 만들고, 학습 잡은 관리자도 예비 toleration을 못 닮"""
    admin = kube.co
    _grant(kube)
    try:
        tester = _impersonated()
        ok, why = _try(tester, "jobsets", _js("vap-plain"))
        precondition(ok, f"권한 없는 사용자가 평범한 학습 잡도 못 만듦 (RBAC 준비 실패?): {why}")
        rr = {"apiVersion": f"{K.RR_GROUP}/{K.RR_VERSION}", "kind": "RelocationRequest",
              "metadata": {"name": "vap-rr", "namespace": NS},
              "spec": {"requestId": "vap", "action": "restartInPlace", "jobRef": f"{NS}/job-vap-plain-g0001"}}
        cases = [
            ("사용자: queue-recovery 잡", tester, "jobsets", _js("vap-q", queue=K.RECOVERY_QUEUE),
             "scheduling-recovery-queue"),
            ("사용자: recovery 우선순위 학습 잡", tester, "jobsets", _js("vap-p", priority=K.PRIORITY_RECOVERY),
             "scheduling-recovery-queue"),
            ("사용자: target-cluster 어노테이션", tester, "jobsets", _js("vap-t", annotations={K.ANN_TARGET: "worker1"}),
             "scheduling-dispatch-annotations"),
            ("사용자: manual-dispatch 어노테이션", tester, "jobsets", _js("vap-m", annotations={K.ANN_MANUAL: "true"}),
             "scheduling-dispatch-annotations"),
            ("사용자: RelocationRequest", tester, K.RR_PLURAL, rr, "scheduling-relocation-request-writers"),
            ("관리자: 학습 잡 + 예비 toleration", admin, "jobsets", _js("vap-s", tolerations=[SPARE_TOL]),
             "scheduling-spare-toleration"),
            ("관리자: 학습 잡 + 키 없는 Exists", admin, "jobsets", _js("vap-e", tolerations=[{"operator": "Exists"}]),
             "scheduling-spare-toleration"),
        ]
        for title, co, plural, body, policy in cases:
            g, v = (K.RR_GROUP, K.RR_VERSION) if plural == K.RR_PLURAL else (K.JOBSET_GROUP, K.JOBSET_VERSION)
            ok, why = _try(co, plural, body, g, v)
            check.true(f"{title} -> {policy}가 막음", not ok and why == policy, "허용됨" if ok else why)
        ok, why = _try(admin, "jobsets", _js("vap-r", queue=K.RECOVERY_QUEUE, tolerations=[SPARE_TOL],
                                             priority=K.PRIORITY_RECOVERY))
        check.true("관리자: 재제출본 모양(queue-recovery + 예비 toleration)은 허용", ok, why)
    finally:
        _revoke(kube)
