import pytest

from harness import cluster as C

pytestmark = [pytest.mark.normal, pytest.mark.doc("배분(1)"), pytest.mark.any_controllers]


def test_queue_config(kube, check):
    """큐 설정: 몫 8/4/3, Heavy StrictFIFO, Light 빌려 쓰기 제한 없음, 복구 몫 안 빌려줌, cohort 하나"""
    heavy, light, rec = (C.clusterqueue(kube, n)["spec"] for n in ("cq-heavy", "cq-light", "cq-recovery"))
    gpu = lambda spec: spec["resourceGroups"][0]["flavors"][0]["resources"][0]  # noqa: E731
    check.eq("cq-heavy 몫 8", str(gpu(heavy)["nominalQuota"]), "8")
    check.eq("cq-heavy StrictFIFO", heavy.get("queueingStrategy"), "StrictFIFO")
    check.eq("cq-light 몫 4", str(gpu(light)["nominalQuota"]), "4")
    check.eq("cq-light BestEffortFIFO", light.get("queueingStrategy"), "BestEffortFIFO")
    check.eq("cq-light borrowingLimit 없음", gpu(light).get("borrowingLimit"), None)
    check.eq("cq-recovery 몫 3", str(gpu(rec)["nominalQuota"]), "3")
    check.eq("cq-recovery lendingLimit 0", str(gpu(rec).get("lendingLimit")), "0")
    for name, spec in (("cq-heavy", heavy), ("cq-light", light), ("cq-recovery", rec)):
        check.eq(f"{name} cohort training", spec.get("cohortName") or spec.get("cohort"), "training")
        check.eq(f"{name} 선점 안 함", spec.get("preemption", {}).get("reclaimWithinCohort"), "Never")
