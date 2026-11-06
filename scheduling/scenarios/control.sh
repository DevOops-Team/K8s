#!/usr/bin/env bash
# 대조군 실험: 한 가지만 끄거나 바꿔서 두 번 돌리고, 큰 잡의 대기 시간과 논 GPU를 비교한다.
# Claude Docs 「평소 학습 스케줄링 방식 비교」의 '남은 확인' 항목.
#
#   ./control.sh                    세 실험 전부 (설계 기본값, 약 1시간)
#   FAST=1 ./control.sh             시간 값 축소 (aging 240 GPU·s, 비워 두기 60s)
#   ./control.sh queue hold         골라서
#   SKIP_BOOTSTRAP=1                준비(bootstrap.sh) 건너뛰기
#
# 실험          바꾸는 것 (A = 현재 구성 / B = 대조군)                         상황
#   queue       A Heavy/Light 분리 / B 큰 잡도 lq-light에 넣음 (= 큐 1개 12장 BestEffortFIFO)
#                                                                              Light 12장 꽉 참 + 1장 잡이 계속 옴 -> 4장 잡
#   hold        A 디스패처 비워 두기 켬 / B 끔 (STARVE_SECONDS 아주 크게)       워커마다 1장 잡 3개(target 고정)로 조각남 + 1장 잡이 계속 옴 -> 4장 잡
#   aging       A allocator 켬 / B 끔 (dispatcher·relocator만)                Light 꽉 참 + 1장 잡이 계속 옴 -> 2장 잡(Light)
#
# queue B에서 큐 설정은 안 바꾼다. 큰 잡이 Light로 가면 Heavy 몫을 빌려야 하므로 KEP-83 막기가 안 걸리고,
# Light 혼자 12장을 쓰는 큐 1개와 같아진다.
#
# 판정 없음(비교만). 결과: scenarios/logs/control-<시각>/run.log, ctl-<실험>-<A|B>.log
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
set +e
source "$HERE/measure-lib.sh"

if [ "${FAST:-0}" = 1 ]; then
  export AGING_GPU_SECONDS=240 STARVE_SECONDS=60
else
  export AGING_GPU_SECONDS=${AGING_GPU_SECONDS:-720} STARVE_SECONDS=${STARVE_SECONDS:-120}
fi
DEFAULT_STARVE=$STARVE_SECONDS
MAX_WAIT=${MAX_WAIT:-600}   # 큰 잡을 이만큼 기다려도 못 들어가면 "못 들어감"으로 기록

LOG="$HERE/logs/control-$(date +%m%d-%H%M)"
mkdir -p "$LOG"
exec > >(tee "$LOG/run.log") 2>&1
RESULTS=()

# ---------- 컨트롤러 (실험 팔마다 새로 띄움)
CTL=""
start_ctl() {  # 로그이름 [컨트롤러 ...]  (없으면 셋 다)
  bash "$HERE/../controllers/run-local.sh" "${@:2}" > "$LOG/ctl-$1.log" 2>&1 &
  CTL=$!
  sleep 15
  kill -0 "$CTL" 2>/dev/null || { echo "컨트롤러가 바로 죽음:"; tail -30 "$LOG/ctl-$1.log"; exit 1; }
  grep "시작:" "$LOG/ctl-$1.log" | sed 's/^/  /'
}
stop_ctl() { [ -n "$CTL" ] && { kill "$CTL" 2>/dev/null; wait "$CTL" 2>/dev/null; }; CTL=""; }
trap 'stop_feeder; stop_ctl' EXIT

# ---------- 상황 만들기
running_gpus() {  # 워커 전체에서 돌고 있는 학습 Pod 수 = 쓰는 GPU
  local w s=0
  for w in "${WORKERS[@]}"; do
    s=$(( s + $(kubectl --context kind-$w -n $NS get pods --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l) ))
  done
  echo "$s"
}

start_feeder() {  # 1장 90s 잡을 10초마다 (최대 60개)
  ( for i in $(seq 1 60); do submit_auto "feed-$i" 1 90s >/dev/null; sleep 10; done ) &
  FEEDER=$!
}

fill_light_full() {  # Light 1장 잡 12개, 10초 간격으로 하나씩 끝남 (130s~240s)
  for i in $(seq 1 12); do submit_auto "fill-$i" 1 "$((120 + 10 * i))s" >/dev/null; done
  wait_until 90 reserved_is fill- 12 || echo "  경고: fill 12개가 다 쿼터를 못 받음 ($(wlq reserved fill-))"
}

fill_fragmented() {  # 워커마다 1장 잡 3개를 target으로 고정 -> 각 워커 1장씩만 빔. 끝나는 순서는 워커를 돌아가며
  local i w
  for i in $(seq 1 9); do
    w=${WORKERS[$(( (i - 1) % 3 ))]}
    submit "frag-$i" 1 lq-light "$((120 + 10 * i))s" "devoops/target-cluster: $w" >/dev/null
  done
  wait_until 120 eval '[ "$(wlq placed frag-)" = 9 ]' || echo "  경고: frag 9개가 다 입장 못 함 ($(wlq by_cluster frag-))"
  echo "  워커별: $(wlq by_cluster frag-)"
}

# ---------- 측정: 큰 잡의 쿼터·입장 시각, 그동안 논 GPU·초
track() {  # 실험 팔 잡이름
  local exp=$1 arm=$2 name=$3 t0 now last quota_at="" placed_at="" idle idle_gs=0
  wait_until 30 eval '[ "$(wl_quota '"$name"')" != none ]'
  t0=$(date +%s); last=$t0
  while :; do
    now=$(( $(date +%s) - t0 ))
    idle=$(( 12 - $(running_gpus) )); [ "$idle" -lt 0 ] && idle=0
    idle_gs=$(( idle_gs + idle * ($(date +%s) - last) )); last=$(date +%s)
    if [ -z "$quota_at" ] && has_quota "$name"; then
      quota_at=$now; echo "  [$(ts)] +${now}s $name 쿼터 받음"
    fi
    if [ -n "$quota_at" ] && is_placed "$name"; then
      placed_at=$now; echo "  [$(ts)] +${now}s $name -> $(wl_cluster "$name")"; break
    fi
    [ "$now" -ge "$MAX_WAIT" ] && break
    sleep 5
  done
  RESULTS+=("$(printf '%-6s %-30s 쿼터 %6s  입장 %6s  논 GPU %6s GPU·s' \
    "$exp" "$arm" "${quota_at:-없음}" "${placed_at:-없음}" "$idle_gs")")
  echo "  결과: 쿼터 ${quota_at:-${MAX_WAIT}s 안에 못 받음}s, 입장 ${placed_at:-못 함}, 논 GPU ${idle_gs} GPU·s"
}

# ======================================================================
exp_queue() {
  say "queue A: Heavy/Light 분리 (현재)"
  start_ctl queue-A
  fill_light_full; start_feeder; sleep 20
  submit_auto big 4 20m
  track queue "A 분리 (big -> lq-heavy)" big
  stop_ctl; reset_all

  say "queue B: 큐 1개 (big도 lq-light)"
  start_ctl queue-B
  fill_light_full; start_feeder; sleep 20
  submit big 4 lq-light 20m
  track queue "B 큐 1개 (big -> lq-light)" big
  stop_ctl; reset_all
}

exp_hold() {
  say "hold A: 비워 두기 켬 (STARVE_SECONDS=$DEFAULT_STARVE)"
  STARVE_SECONDS=$DEFAULT_STARVE start_ctl hold-A
  fill_fragmented; start_feeder
  submit_auto big 4 20m
  track hold "A 비워 두기 켬" big
  grep "굶주림" "$LOG/ctl-hold-A.log" | sed 's/^/  /'
  stop_ctl; reset_all

  say "hold B: 비워 두기 끔"
  STARVE_SECONDS=100000000 start_ctl hold-B
  fill_fragmented; start_feeder
  submit_auto big 4 20m
  track hold "B 비워 두기 끔" big
  stop_ctl; reset_all
}

exp_aging() {
  say "aging A: allocator 켬 (기준 ${AGING_GPU_SECONDS} GPU·s -> 2장 잡은 $((AGING_GPU_SECONDS / 2))s에 aged)"
  start_ctl aging-A
  fill_light_full; start_feeder; sleep 20
  submit_auto mid 2 20m
  track aging "A allocator 켬" mid
  echo "  mid 우선순위: $(prio_of mid)"
  stop_ctl; reset_all

  say "aging B: allocator 끔"
  start_ctl aging-B dispatcher relocator
  fill_light_full; start_feeder; sleep 20
  submit_auto mid 2 20m
  track aging "B allocator 끔" mid
  stop_ctl; reset_all
}

# ======================================================================
if [ "${SKIP_BOOTSTRAP:-0}" != 1 ]; then
  echo "[$(ts)] 준비 중... (진행: tail -f $LOG/bootstrap.log)"
  bash "$HERE/bootstrap.sh" > "$LOG/bootstrap.log" 2>&1 || { echo "준비 실패 -> $LOG/bootstrap.log"; exit 1; }
fi
echo "시간 값: aging=${AGING_GPU_SECONDS} GPU·s starve=${DEFAULT_STARVE}s 최대 대기=${MAX_WAIT}s"

pick=("$@")
[ ${#pick[@]} -eq 0 ] && pick=(queue hold aging)
reset_all
for e in "${pick[@]}"; do
  if declare -F "exp_$e" >/dev/null; then "exp_$e"; else echo "없는 실험: $e (queue | hold | aging)"; fi
done

echo; echo "########## 비교 (초는 큰 잡 Workload가 생긴 때부터)"
printf '  %s\n' "${RESULTS[@]}"
echo "  로그: $LOG"
