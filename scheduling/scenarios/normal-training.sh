#!/usr/bin/env bash
# 평소 학습 검증: Confluence 「평소 학습, 장애복구 시 스케줄링 방식」 v16의 '평소' 표대로 도는지 자동 판정
#
#   ./normal-training.sh              준비 -> 컨트롤러 시작 -> 검사 1~8 전부 (설계 기본값, 약 25분)
#   FAST=1 ./normal-training.sh       시간 값을 줄여서 (약 12분). 판정 기준도 같이 줄어듦
#   ./normal-training.sh 5 7 8        골라서 (8은 7 바로 뒤에 돌아야 함)
#   SKIP_BOOTSTRAP=1                  준비(bootstrap.sh) 건너뛰기
#   USE_RUNNING_CONTROLLERS=1         이미 띄운 컨트롤러 사용 (그쪽 시간 값과 이 스크립트 값이 같아야 함)
#
# 검사 항목                                        문서 위치
#   1 큐 설정 (몫 8/4/3, StrictFIFO, lendingLimit 0)   배분(1)
#   2 큐 정하기 (3장 이상 Heavy, 5장 이상 거부)          입력
#   3 빈 패킹 (1장 6개 -> worker1 4, worker2 2)         배치(1)
#   4 Heavy 대기 없으면 Light가 남는 몫 전부 사용         배분(1)·(2)
#   5 Heavy StrictFIFO (앞 4장이 막히면 뒤 3장도 대기)     배분(1)
#   6 우선순위 = 대기 시간 x GPU 수                     배분(3)
#   7 Heavy 대기 -> Light 조임 -> 작은 잡이 계속 와도 큰 잡 입장   배분(2)·배치(1)
#   8 Heavy 대기 0 이 이어지면 Light 풂                  배분(2)
#
# 결과: scenarios/logs/normal-<시각>/ 아래 run.log, controllers.log. 종료 코드 = FAIL 개수
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
set +e   # 판정 실패로 스크립트가 멈추지 않게 (lib.sh의 -e 해제)

# ---------- 시간 값 (컨트롤러에도 같은 값을 넘김)
if [ "${FAST:-0}" = 1 ]; then
  export TIGHTEN_SECONDS=20 RELEASE_SECONDS=60 AGING_GPU_SECONDS=240 STARVE_SECONDS=60
else
  export TIGHTEN_SECONDS=${TIGHTEN_SECONDS:-60} RELEASE_SECONDS=${RELEASE_SECONDS:-300} \
         AGING_GPU_SECONDS=${AGING_GPU_SECONDS:-720} STARVE_SECONDS=${STARVE_SECONDS:-120}
fi
SLACK=30   # 컨트롤러 루프 간격(10s) + Kueue 반영 지연 여유

LOG="$HERE/logs/normal-$(date +%m%d-%H%M)"
mkdir -p "$LOG"
exec > >(tee "$LOG/run.log") 2>&1

# ---------- 판정 기록
FAILS=0; RESULTS=(); CUR=""
pass() { echo "  [PASS] $*"; RESULTS+=("PASS  $CUR  $*"); }
fail() { echo "  [FAIL] $*"; RESULTS+=("FAIL  $CUR  $*"); FAILS=$((FAILS+1)); }
check() { local what=$1; shift; if "$@"; then pass "$what"; else fail "$what"; fi; }
eq() { [ "$1" = "$2" ] || { echo "    기대 '$2', 실제 '$1'"; return 1; }; }

# ---------- 조회
WLQ='
import json, sys
from collections import Counter
mode = sys.argv[1]; arg = sys.argv[2] if len(sys.argv) > 2 else ""
items = json.load(sys.stdin)["items"]
def owner(w):
    for o in w["metadata"].get("ownerReferences") or []:
        return o["name"]
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
prio_of()    { kubectl --context $MGR -n $NS get jobset "$1" -o jsonpath='{.metadata.labels.kueue\.x-k8s\.io/priority-class}' 2>/dev/null; }
is_aged()    { [ "$(prio_of "$1")" = training-aged ]; }
cq_get()     { kubectl --context $MGR get clusterqueue "$1" -o jsonpath="$2" 2>/dev/null; }
light_limit(){ cq_get cq-light '{.spec.resourceGroups[0].flavors[0].resources[0].borrowingLimit}'; }
is_tight()   { [ "$(light_limit)" = 0 ]; }
is_loose()   { [ -z "$(light_limit)" ]; }
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

# Light 빌려 쓰기를 원상태(필드 없음)로. 컨트롤러는 Heavy 대기가 없으면 다시 조이지 않는다
loosen_light() {
  is_loose && return 0
  kubectl --context $MGR patch clusterqueue cq-light --type=json \
    -p '[{"op":"remove","path":"/spec/resourceGroups/0/flavors/0/resources/0/borrowingLimit"}]' >/dev/null
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
  loosen_light
}

# ======================================================================
t1() {
  say "1. 큐 설정 (배분 1)"
  check "cq-heavy 몫 8"            eq "$(cq_get cq-heavy '{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}')" 8
  check "cq-heavy StrictFIFO"      eq "$(cq_get cq-heavy '{.spec.queueingStrategy}')" StrictFIFO
  check "cq-light 몫 4"            eq "$(cq_get cq-light '{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}')" 4
  check "cq-light BestEffortFIFO"  eq "$(cq_get cq-light '{.spec.queueingStrategy}')" BestEffortFIFO
  check "cq-light 처음엔 borrowingLimit 없음" eq "$(light_limit)" ""
  check "cq-recovery 몫 3"         eq "$(cq_get cq-recovery '{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}')" 3
  check "cq-recovery lendingLimit 0" eq "$(cq_get cq-recovery '{.spec.resourceGroups[0].flavors[0].resources[0].lendingLimit}')" 0
  for cq in cq-heavy cq-light cq-recovery; do
    check "$cq cohort training" eq "$(cq_get $cq '{.spec.cohortName}')" training
  done
}

t2() {
  say "2. 큐 정하기 (입력: 잡 투입기 규칙)"
  check "1장 -> lq-light" eq "$(queue_for 1)" lq-light
  check "2장 -> lq-light" eq "$(queue_for 2)" lq-light
  check "3장 -> lq-heavy" eq "$(queue_for 3)" lq-heavy
  check "4장 -> lq-heavy" eq "$(queue_for 4)" lq-heavy
  queue_for 5 >/dev/null; check "5장 -> 거부" eq "$?" 2
  submit_auto route-3 3 5m
  wait_until 60 has_quota route-3
  check "3장 잡이 관리 cq-heavy로 입장" eq "$(wl_cq route-3)" cq-heavy
}

t3() {
  say "3. 빈 패킹 (배치 1)"
  expect "1장 잡 6개 -> worker1 4, worker2 2, worker3 0"
  for i in 1 2 3 4 5 6; do submit_auto "bin-$i" 1 20m >/dev/null; sleep 3; done
  wait_until 90 eval '[ "$(wlq placed bin-)" = 6 ]'
  check "워커별 개수" eq "$(wlq by_cluster bin-)" "worker1=4 worker2=2"
}

t4() {
  say "4. Heavy 대기 없으면 Light가 남는 몫 전부 사용 (배분 1·2)"
  expect "1장 잡 13개 -> 12개 입장(Light 몫 4 + Heavy 몫 8 빌림), 1개 대기, borrowingLimit 그대로 없음"
  for i in $(seq 1 13); do submit_auto "fill-$i" 1 20m >/dev/null; done
  wait_until 90 reserved_is fill- 12
  sleep 15   # 13번째가 잘못 들어가는지 조금 더 봄
  check "쿼터 받은 Light 잡 12개" eq "$(wlq reserved fill-)" 12
  check "cq-light 사용량 12"     eq "$(cq_used cq-light)" 12
  sleep $((TIGHTEN_SECONDS + 10))
  check "Heavy 대기가 없으니 조이지 않음 (${TIGHTEN_SECONDS}s 뒤에도)" is_loose
}

t5() {
  say "5. Heavy StrictFIFO (배분 1)"
  expect "Light 9장 사용 -> 남은 3장. 4장 잡(h4) 뒤의 3장 잡(h3)은 들어갈 수 있어도 대기"
  for i in $(seq 1 9); do submit_auto "fill-$i" 1 20m >/dev/null; done
  wait_until 90 reserved_is fill- 9
  submit_auto h4 4 20m; sleep 3; submit_auto h3 3 20m
  sleep 30
  check "h4 대기 (3장밖에 안 남음)" eq "$(wl_quota h4)" False
  check "h3도 대기 (h4가 앞에 있으므로)" eq "$(wl_quota h3)" False
}

t6() {
  say "6. 우선순위 = 대기 시간 x GPU 수 (배분 3, 기준 ${AGING_GPU_SECONDS} GPU·s)"
  local t4=$((AGING_GPU_SECONDS / 4)) t2=$((AGING_GPU_SECONDS / 2))
  expect "자리가 꽉 찬 상태에서 4장·2장·1장 잡 동시 제출 -> 4장 ${t4}s, 2장 ${t2}s, 1장 ${AGING_GPU_SECONDS}s에 training-aged"
  for i in $(seq 1 12); do submit_auto "fill-$i" 1 20m >/dev/null; done
  wait_until 90 reserved_is fill- 12
  submit_auto age-4 4 20m; submit_auto age-2 2 20m; submit_auto age-1 1 20m
  if wait_until $((t4 + SLACK)) is_aged age-4; then
    pass "4장 잡 aged (${WAITED}s, 기대 ${t4}s)"
    check "그때 2장 잡은 아직 training" eq "$(prio_of age-2)" training
    check "그때 1장 잡은 아직 training" eq "$(prio_of age-1)" training
  else
    fail "4장 잡이 $((t4 + SLACK))s 안에 aged 안 됨 (지금 $(prio_of age-4))"
  fi
  if wait_until $((t2 - t4 + SLACK)) is_aged age-2; then
    pass "2장 잡 aged"
    check "그때 1장 잡은 아직 training" eq "$(prio_of age-1)" training
  else
    fail "2장 잡이 기대 시각 안에 aged 안 됨 (지금 $(prio_of age-2))"
  fi
}

T7_QUOTA_AT=""   # 8번이 씀: 큰 잡이 쿼터를 받은 시각 = Heavy 대기가 0이 된 시각
t7() {
  say "7. Heavy 대기 -> Light 조임 -> 작은 잡이 계속 와도 큰 잡 입장 (배분 2, 배치 1)"
  expect "Light 12장(150s짜리)으로 꽉 참 + 1장 잡이 10초마다 계속 옴 -> 4장 잡 제출"
  expect "${TIGHTEN_SECONDS}s 뒤 cq-light borrowingLimit 0 -> Light가 자기 몫 4 넘게는 새로 못 받음 -> 큰 잡 쿼터 -> 워커 지명"
  for i in $(seq 1 12); do submit_auto "fill-$i" 1 150s >/dev/null; done
  wait_until 90 reserved_is fill- 12
  ( for i in $(seq 1 60); do submit_auto "feed-$i" 1 90s >/dev/null; sleep 10; done ) &
  FEEDER=$!
  sleep 20
  submit_auto big 4 20m
  local t0 now tight_at="" quota_at="" placed_at="" prev used bad=0 max=480
  t0=$(date +%s); prev=$(cq_used cq-light)
  while :; do
    now=$(( $(date +%s) - t0 ))
    used=$(cq_used cq-light)
    if is_tight; then
      [ -z "$tight_at" ] && { tight_at=$now; echo "  [$(ts)] +${now}s 조임 확인 (Light 사용 $used)"; }
      # 조인 뒤에는 Light가 자기 몫(4)을 넘어서 늘어나면 안 됨
      if [ "$used" -gt "$prev" ] && [ "$used" -gt 4 ]; then
        bad=$((bad + 1)); echo "  [$(ts)] +${now}s Light 사용량 $prev -> $used (몫 4 초과로 늘어남)"
      fi
    fi
    if [ -z "$quota_at" ] && has_quota big; then
      quota_at=$now; T7_QUOTA_AT=$(date +%s); echo "  [$(ts)] +${now}s big 쿼터 받음 (Light 사용 $used)"
    fi
    if [ -n "$quota_at" ] && is_placed big; then
      placed_at=$now; echo "  [$(ts)] +${now}s big -> $(wl_cluster big)"; break
    fi
    [ "$now" -ge "$max" ] && break
    prev=$used; sleep 5
  done
  if [ -n "$tight_at" ] && [ "$tight_at" -le $((TIGHTEN_SECONDS + SLACK)) ]; then
    pass "조임 (${tight_at}s, 기대 ${TIGHTEN_SECONDS}s)"
  else
    fail "조임이 $((TIGHTEN_SECONDS + SLACK))s 안에 안 됨 (${tight_at:-없음})"
  fi
  check "조인 뒤 Light가 몫 4를 넘어 늘어난 적 없음" eq "$bad" 0
  if [ -n "$quota_at" ]; then pass "big 쿼터 받음 (${quota_at}s)"; else fail "big이 ${max}s 안에 쿼터 못 받음"; fi
  if [ -n "$placed_at" ]; then pass "big 워커 입장 (${placed_at}s)"; else fail "big이 ${max}s 안에 워커 지명 못 받음"; fi
  show | grep -E "NAME|big" || true
}

t8() {
  say "8. Heavy 대기 0이 ${RELEASE_SECONDS}s 이어지면 Light 풂 (배분 2)"
  if [ -z "$T7_QUOTA_AT" ] || ! is_tight; then
    fail "7번이 먼저 성공해야 함 (조인 상태에서 시작)"; return
  fi
  stop_feeder
  if wait_until $((RELEASE_SECONDS + SLACK + 30)) is_loose; then
    local idle=$(( $(date +%s) - T7_QUOTA_AT ))
    if [ "$idle" -ge $((RELEASE_SECONDS - 15)) ]; then
      pass "풂 (Heavy 대기 0 된 뒤 ${idle}s, 기대 ${RELEASE_SECONDS}s)"
    else
      fail "너무 일찍 풂 (Heavy 대기 0 된 뒤 ${idle}s, 기대 ${RELEASE_SECONDS}s)"
    fi
  else
    fail "$((RELEASE_SECONDS + SLACK + 30))s 안에 안 풀림"; return
  fi
  expect "다시 Light가 몫 4를 넘어 빌려 씀"
  for i in $(seq 1 6); do submit_auto "after-$i" 1 20m >/dev/null; done
  if wait_until 60 eval '[ "$(cq_used cq-light)" -gt 4 ]'; then
    pass "풀린 뒤 Light 사용량 $(cq_used cq-light) (> 4)"
  else
    fail "풀린 뒤에도 Light 사용량 $(cq_used cq-light)"
  fi
}

# ======================================================================
if [ "${SKIP_BOOTSTRAP:-0}" != 1 ]; then
  bash "$HERE/bootstrap.sh" > "$LOG/bootstrap.log" 2>&1 || { echo "준비 실패 -> $LOG/bootstrap.log"; exit 1; }
  echo "준비 완료 (로그 $LOG/bootstrap.log)"
fi

CTL=""
cleanup() { stop_feeder; [ -n "$CTL" ] && { kill "$CTL" 2>/dev/null; wait "$CTL" 2>/dev/null; }; }
trap cleanup EXIT
if [ "${USE_RUNNING_CONTROLLERS:-0}" != 1 ]; then
  bash "$HERE/../controllers/run-local.sh" > "$LOG/controllers.log" 2>&1 &
  CTL=$!
  sleep 15
  kill -0 "$CTL" 2>/dev/null || { echo "컨트롤러가 바로 죽음:"; tail -30 "$LOG/controllers.log"; exit 1; }
  grep "시작:" "$LOG/controllers.log"
fi
echo "시간 값: tighten=${TIGHTEN_SECONDS}s release=${RELEASE_SECONDS}s aging=${AGING_GPU_SECONDS} GPU·s starve=${STARVE_SECONDS}s"

pick=("$@"); [ ${#pick[@]} -eq 0 ] && pick=(1 2 3 4 5 6 7 8)
for n in "${pick[@]}"; do
  CUR="#$n"
  # 8번은 7번이 남긴 상태(조임, big 입장)에서 이어서 본다
  [ "$n" != 1 ] && [ "$n" != 8 ] && reset_all
  "t$n"
done
reset_all >/dev/null 2>&1

echo; echo "########## 요약"
printf '  %s\n' "${RESULTS[@]}"
echo "  FAIL ${FAILS}개 / 로그: $LOG"
echo "  컨트롤러 판단 근거: grep -E '빌려 쓰기|우선순위|굶주림' $LOG/controllers.log"
exit "$FAILS"
