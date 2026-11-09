"""케이스 공통 준비와 판정·결과 기록.

케이스마다: 컨트롤러를 그 케이스 설정대로(@pytest.mark.controllers) -> 초기화 -> 감시 시작 -> 실행.
판정: 예외 종류와 감시 기록으로 PASS / FAIL / INCONCLUSIVE / INVALID를 정하고,
      logs/<시각>/results.json, summary.md에 항목별 체크·측정값과 함께 남긴다.
"""
import json
import logging
import os
import pathlib
from datetime import datetime

import pytest

from harness import cluster as C
from harness.controllers import ALL, Controllers
from harness.env import WORKERS, Kube, K
from harness.health import Watchdog
from harness.reset import reset
from harness.verdict import Checks, Inconclusive, Invalid

log = logging.getLogger("e2e")
HERE = pathlib.Path(__file__).parent
RESULTS = []
MARK = pytest.StashKey[int]()
RECORD = pytest.StashKey[dict]()


# ---------------------------------------------------------------- 세션
@pytest.fixture(scope="session")
def logdir():
    d = pathlib.Path(os.getenv("E2E_LOG") or HERE / "logs" / datetime.now().strftime("%m%d-%H%M%S"))
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture(scope="session")
def kube():
    k = Kube()
    _preflight(k)
    return k


def _preflight(k):
    """클러스터가 검사할 수 있는 상태인지. 아니면 아무 케이스도 돌리지 않는다."""
    try:
        for cq in ("cq-heavy", "cq-light", "cq-recovery"):
            C.clusterqueue(k, cq)
        mkcs = k.co.list_cluster_custom_object(K.KUEUE_GROUP, K.KUEUE_VERSION, "multikueueclusters")["items"]
    except Exception as e:  # noqa: BLE001
        pytest.exit(f"준비 안 됨: 관리 클러스터 조회 실패 ({e}). cluster/setup.sh, scenarios/bootstrap.sh 먼저", 3)
    active = {m["metadata"]["name"] for m in mkcs
              if any(c["type"] == "Active" and c["status"] == "True" for c in m.get("status", {}).get("conditions", []))}
    missing = set(WORKERS) - active
    if missing:
        pytest.exit(f"준비 안 됨: MultiKueue Active 아님 {sorted(missing)}. scenarios/bootstrap.sh 먼저", 3)


@pytest.fixture(scope="session")
def ctl(logdir):
    c = Controllers(logdir)
    yield c
    c.stop()


@pytest.fixture(scope="session")
def watchdog(kube, ctl):
    w = Watchdog(kube, ctl)
    w.start()
    yield w
    w.stop()


# ---------------------------------------------------------------- 케이스
@pytest.fixture(autouse=True)
def _case(request, kube, ctl, watchdog):
    """케이스 준비: 컨트롤러 설정 맞추기 -> 초기화 -> 감시 구간 시작."""
    m = request.node.get_closest_marker("controllers")
    if not request.node.get_closest_marker("any_controllers"):
        ctl.use(m.args if m and m.args else ALL, **(m.kwargs if m else {}))
    log.info("[%s] 초기화", request.node.name)
    reset(kube)
    request.node.stash[MARK] = watchdog.mark()
    yield
    # 끝난 뒤 남은 잡은 다음 케이스 초기화가 지움


@pytest.fixture
def check(request):
    c = Checks()
    request.node.stash[RECORD] = request.node.stash.get(RECORD, {}) | {"checks": c}
    return c


@pytest.fixture
def info(request):
    """측정값 기록 (판정과 무관, 결과에 남음)."""
    d = {}
    request.node.stash[RECORD] = request.node.stash.get(RECORD, {}) | {"info": d}
    return d


@pytest.hookimpl(hookwrapper=True)
def pytest_pyfunc_call(pyfuncitem):
    """케이스 함수가 끝났을 때 체크 항목이 하나라도 틀렸으면 FAIL로."""
    outcome = yield
    chk = pyfuncitem.funcargs.get("check")
    if outcome.excinfo is None and chk is not None and chk.failed:
        outcome.force_exception(AssertionError("틀린 항목: " + "; ".join(f"{n} ({d})" for n, _, d in chk.failed)))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    out = yield
    rep = out.get_result()
    if rep.when != "call" and not (rep.when == "setup" and rep.failed):
        return
    verdict, reason = ("PASS", "") if rep.passed else ("FAIL", "")
    exc = call.excinfo
    if exc is not None:
        if exc.errisinstance(Inconclusive):
            verdict, reason = "INCONCLUSIVE", str(exc.value)
        elif exc.errisinstance(Invalid):
            verdict, reason = "INVALID", str(exc.value)
        else:
            reason = exc.exconly()[:600]
    wd = item.funcargs.get("watchdog")
    incidents = wd.since(item.stash[MARK]) if wd and MARK in item.stash else []
    if incidents and verdict != "INVALID":
        reason = f"인프라 문제가 있던 구간 ({'; '.join(f'{t} {w}' for t, w in incidents)}). 원래 판정 {verdict}: {reason}"
        verdict = "INVALID"
    if verdict in ("INCONCLUSIVE", "INVALID"):
        rep.outcome = "skipped"
        rep.longrepr = (str(item.path), item.location[1] or 0, f"{verdict}: {reason}")
    rec = item.stash.get(RECORD, {})
    chk = rec.get("checks")
    ctl = item.funcargs.get("ctl")
    doc = item.get_closest_marker("doc")
    RESULTS.append({
        "case": item.nodeid, "title": (item.function.__doc__ or item.name).strip().splitlines()[0],
        "doc": doc.args[0] if doc else "", "verdict": verdict, "reason": reason,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in (chk.items if chk else [])],
        "info": rec.get("info", {}), "seconds": round(rep.duration),
        "controllers": list(ctl.names) if ctl else [],
        "controllers_log": ctl.logfile.name if ctl and ctl.logfile else "",
        "incidents": incidents,
    })


def pytest_sessionfinish(session):
    d = os.getenv("E2E_LOG")
    if not d or not RESULTS:
        return
    d = pathlib.Path(d)
    meta = {"finished": datetime.now().isoformat(timespec="seconds")}
    (d / "results.json").write_text(json.dumps({"meta": meta, "results": RESULTS}, ensure_ascii=False, indent=2))
    lines = [f"# e2e 결과 {meta['finished']}", "",
             "| 판정 | 케이스 | 설명 | 항목 | 비고 |", "|---|---|---|---|---|"]
    for r in RESULTS:
        ok = sum(c["ok"] for c in r["checks"])
        note = r["reason"] or ", ".join(f"{k}={v}" for k, v in r["info"].items())
        lines.append(f"| {r['verdict']} | `{r['case'].split('::')[-1]}` | {r['title']} | {ok}/{len(r['checks'])} | "
                     f"{note.replace('|', '/')[:300]} |")
    (d / "summary.md").write_text("\n".join(lines) + "\n")


def pytest_terminal_summary(terminalreporter):
    if not RESULTS:
        return
    tr = terminalreporter
    tr.section("판정")
    for r in RESULTS:
        tr.write_line(f"  {r['verdict']:<12} {r['case'].split('::')[-1]:<40} {r['title']}")
        for c in r["checks"]:
            if not c["ok"]:
                tr.write_line(f"      x {c['name']}: {c['detail']}")
        if r["reason"]:
            tr.write_line(f"      - {r['reason'][:300]}")
        for k, v in r["info"].items():
            tr.write_line(f"      · {k}: {v}")
    counts = {v: sum(r["verdict"] == v for r in RESULTS) for v in ("PASS", "FAIL", "INCONCLUSIVE", "INVALID")}
    tr.write_line("  " + "  ".join(f"{k} {n}" for k, n in counts.items()))
    if os.getenv("E2E_LOG"):
        tr.write_line(f"  결과: {os.getenv('E2E_LOG')}/results.json, summary.md")
