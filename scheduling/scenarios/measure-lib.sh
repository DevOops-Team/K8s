# 측정용 조회·대기 함수. normal-training.sh, control.sh가 lib.sh 다음에 source 한다.

# ---------- 조회
WLQ='
import json, re, sys
from collections import Counter
mode = sys.argv[1]; arg = sys.argv[2] if len(sys.argv) > 2 else ""
items = json.load(sys.stdin)["items"]
def owner(w):   # JobSet 이름 job-<작업ID>-g<세대> -> 작업ID (시나리오는 작업ID로 부름)
    for o in w["metadata"].get("ownerReferences") or []:
        m = re.match(r"^job-(.+)-g[0-9]{4}$", o["name"])
        return m.group(1) if m else o["name"]
    return ""
def ok(w, t):
    return any(c["type"] == t and c["status"] == "True" for c in (w.get("status") or {}).get("conditions") or [])
sel = [w for w in items if (owner(w) == arg if mode == "one" else owner(w).startswith(arg))]
st = lambda w: w.get("status") or {}
if mode == "reserved":
    print(sum(ok(w, "QuotaReserved") for w in sel))
elif mode == "placed":
    print(sum(bool(st(w).get("clusterName")) for w in sel))
elif mode == "by_cluster":
    c = Counter(st(w)["clusterName"] for w in sel if st(w).get("clusterName"))
    print(" ".join(f"{k}={c[k]}" for k in sorted(c)))
elif mode == "one":
    if not sel:
        print("none - -")
    else:
        w = sel[0]
        print(ok(w, "QuotaReserved"), st(w).get("clusterName") or "-", (st(w).get("admission") or {}).get("clusterQueue") or "-")
'
wlq() { kubectl --context $MGR -n $NS get workloads -o json 2>/dev/null | python3 -c "$WLQ" "$@"; }
wl_quota()   { wlq one "$1" | awk '{print $1}'; }
wl_cluster() { wlq one "$1" | awk '{print $2}'; }
wl_cq()      { wlq one "$1" | awk '{print $3}'; }
has_quota()  { [ "$(wl_quota "$1")" = True ]; }
is_placed()  { [ "$(wl_cluster "$1")" != - ] && [ -n "$(wl_cluster "$1")" ]; }
prio_of()    { kubectl --context $MGR -n $NS get jobset "$(jsname "$1")" -o jsonpath='{.metadata.labels.kueue\.x-k8s\.io/priority-class}' 2>/dev/null; }
is_aged()    { [ "$(prio_of "$1")" = training-aged ]; }
cq_get()     { kubectl --context $MGR get clusterqueue "$1" -o jsonpath="$2" 2>/dev/null; }
light_limit(){ cq_get cq-light '{.spec.resourceGroups[0].flavors[0].resources[0].borrowingLimit}'; }
cq_used()    { local v; v=$(cq_get "$1" '{.status.flavorsReservation[0].resources[0].total}'); echo "${v:-0}"; }
reserved_is(){ [ "$(wlq reserved "$1")" = "$2" ]; }

# wait_until 최대초 명령...  -> 성공 시 0, 걸린 초는 $WAITED
wait_until() {
  local max=$1; shift; local t0; t0=$(date +%s)
  while :; do
    if "$@"; then WAITED=$(( $(date +%s) - t0 )); return 0; fi
    if [ $(( $(date +%s) - t0 )) -ge "$max" ]; then WAITED=$max; return 1; fi
    sleep 5
  done
}

FEEDER=""
stop_feeder() { [ -n "$FEEDER" ] && kill "$FEEDER" 2>/dev/null; wait "$FEEDER" 2>/dev/null; FEEDER=""; }

reset_all() {
  stop_feeder
  reset
  for _ in $(seq 1 40); do
    [ "$(kubectl --context $MGR -n $NS get workloads --no-headers 2>/dev/null | wc -l)" -eq 0 ] && break
    sleep 3
  done
}
