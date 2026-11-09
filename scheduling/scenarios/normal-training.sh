#!/usr/bin/env bash
# 평소 학습 검증: Confluence 「평소 학습, 장애복구 시 스케줄링 방식」 v16의 '평소' 표대로 도는지 자동 판정
#
#   ./normal-training.sh              준비 -> 컨트롤러 시작 -> 검사 1~7 전부 (설계 기본값, 약 20분)
#   FAST=1 ./normal-training.sh       시간 값을 줄여서 (약 10분). 판정 기준도 같이 줄어듦
#   ./normal-training.sh 5 7          골라서
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
#   7 작은 잡이 계속 와도 큰 잡 입장 (Kueue가 Light 새 빌려 쓰기를 막음, KEP-83)   배분(2)·배치(1)
#     컨트롤러 없이 Kueue 설정만으로 통과해야 함 -> 큐 설정을 바꿨을 때 회귀 검사
#
# 결과: scenarios/logs/normal-<시각>/ 아래 run.log, controllers.log. 종료 코드 = FAIL 개수
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
set +e   # 판정 실패로 스크립트가 멈추지 않게 (lib.sh의 -e 해제)

# ---------- 시간 값 (컨트롤러에도 같은 값을 넘김)
if [ "${FAST:-0}" = 1 ]; then
  export AGING_GPU_SECONDS=240 STARVE_GPU_SECONDS=240
else
  export AGING_GPU_SECONDS=${AGING_GPU_SECONDS:-720} STARVE_GPU_SECONDS=${STARVE_GPU_SECONDS:-480}
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

source "$HERE/measure-lib.sh"   # 조회(wlq, cq_used ...)·wait_until·reset_all

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
  check "1장 -> queue-light" eq "$(queue_for 1)" queue-light
  check "2장 -> queue-light" eq "$(queue_for 2)" queue-light
  check "3장 -> queue-heavy" eq "$(queue_for 3)" queue-heavy
  check "4장 -> queue-heavy" eq "$(queue_for 4)" queue-heavy
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
  expect "1장 잡 13개 -> 12개 입장(Light 몫 4 + Heavy 몫 8 빌림), 1개 대기"
  for i in $(seq 1 13); do submit_auto "fill-$i" 1 20m >/dev/null; done
  wait_until 90 reserved_is fill- 12
  sleep 15   # 13번째가 잘못 들어가는지 조금 더 봄
  check "쿼터 받은 Light 잡 12개" eq "$(wlq reserved fill-)" 12
  check "cq-light 사용량 12"     eq "$(cq_used cq-light)" 12
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

t7() {
  say "7. 작은 잡이 계속 와도 큰 잡 입장 (배분 2, 배치 1)"
  expect "big이 Heavy 몫 8 안에서 기다리는 동안 Kueue가 Light의 새 빌려 쓰기를 막음(KEP-83)"
  expect "-> Light는 몫 4 넘게 새로 못 받음 -> 자리가 모이면 big 쿼터 -> 워커 지명"
  expect "Light 12장으로 꽉 참(10초 간격으로 하나씩 끝남: 130s~240s) + 1장 잡이 10초마다 계속 옴 -> 4장 잡 제출"
  # 한꺼번에 끝나면 12장이 동시에 비어 무조건 big이 들어감 -> 하나씩 끝나게 해야 빌려 쓰기 막기가 보임
  for i in $(seq 1 12); do submit_auto "fill-$i" 1 "$((120 + 10 * i))s" >/dev/null; done
  wait_until 90 reserved_is fill- 12
  ( for i in $(seq 1 60); do submit_auto "feed-$i" 1 90s >/dev/null; sleep 10; done ) &
  FEEDER=$!
  sleep 20
  submit_auto big 4 20m
  wait_until 30 eval '[ "$(wl_quota big)" != none ]'   # Workload가 생긴 뒤부터 잼
  local t0 now quota_at="" placed_at="" prev used heavy idle last bad=0 max=480 idle_gs=0
  t0=$(date +%s); last=$t0; prev=$(cq_used cq-light)
  while :; do
    now=$(( $(date +%s) - t0 ))
    used=$(cq_used cq-light)
    if [ -z "$quota_at" ]; then
      # big이 기다리는 동안: Light가 몫 4를 넘어 늘어나면 안 됨
      if [ "$used" -gt "$prev" ] && [ "$used" -gt 4 ]; then
        bad=$((bad + 1)); echo "  [$(ts)] +${now}s Light 사용량 $prev -> $used (몫 4 초과로 늘어남)"
      fi
      # 노는 GPU·초: 학습 몫 12 - 쓰는 중 (big 자리를 모으는 값)
      heavy=$(cq_used cq-heavy); idle=$((12 - used - heavy)); [ "$idle" -lt 0 ] && idle=0
      idle_gs=$((idle_gs + idle * ($(date +%s) - last)))
      if has_quota big; then
        quota_at=$now; echo "  [$(ts)] +${now}s big 쿼터 받음 (Light 사용 $used)"
      fi
    fi
    last=$(date +%s)
    if [ -n "$quota_at" ] && is_placed big; then
      placed_at=$now; echo "  [$(ts)] +${now}s big -> $(wl_cluster big)"; break
    fi
    [ "$now" -ge "$max" ] && break
    prev=$used; sleep 5
  done
  check "big 대기 중 Light가 몫 4를 넘어 늘어난 적 없음" eq "$bad" 0
  if [ -n "$quota_at" ]; then pass "big 쿼터 받음 (${quota_at}s)"; else fail "big이 ${max}s 안에 쿼터 못 받음"; fi
  if [ -n "$placed_at" ]; then pass "big 워커 입장 (${placed_at}s)"; else fail "big이 ${max}s 안에 워커 지명 못 받음"; fi
  echo "  [정보] big 자리를 모으는 동안 논 GPU: ${idle_gs} GPU·s (약 $((idle_gs / 60)) GPU·분)"
  show | grep -E "NAME|big" || true
}

# ======================================================================
if [ "${SKIP_BOOTSTRAP:-0}" != 1 ]; then
  echo "[$(ts)] 준비 중... 워커 Kueue 재시작 포함 몇 분 걸림 (진행: tail -f $LOG/bootstrap.log)"
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
echo "시간 값: aging=${AGING_GPU_SECONDS} GPU·s starve=${STARVE_GPU_SECONDS} GPU·s"

pick=("$@")
[ ${#pick[@]} -eq 0 ] && pick=(1 2 3 4 5 6 7)
for num in "${pick[@]}"; do
  CUR="#$num"
  if ! declare -F "t$num" >/dev/null; then fail "없는 검사 번호"; continue; fi
  [ "$num" != 1 ] && reset_all
  "t$num"
done
reset_all >/dev/null 2>&1

echo; echo "########## 요약"
printf '  %s\n' "${RESULTS[@]}"
echo "  FAIL ${FAILS}개 / 로그: $LOG"
echo "  컨트롤러 판단 근거: grep -E '우선순위|굶주림' $LOG/controllers.log"
exit "$FAILS"
