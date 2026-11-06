#!/usr/bin/env bash
# 전부 한 번에: 준비 -> 컨트롤러 3개 백그라운드 실행 -> 시나리오 1~11 -> 컨트롤러 종료
#   ./run-all.sh            전부 (30분 안팎)
#   ./run-all.sh 1 5 9      골라서
#   SKIP_BOOTSTRAP=1 ./run-all.sh   준비 건너뛰기
# 결과: scenarios/logs/<시각>/ 아래 scenario-N.log, controllers.log
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOG="$HERE/logs/$(date +%m%d-%H%M)"
mkdir -p "$LOG"

if [ "${SKIP_BOOTSTRAP:-0}" != 1 ]; then
  bash "$HERE/bootstrap.sh" 2>&1 | tee "$LOG/bootstrap.log" || { echo "준비 실패 -> $LOG/bootstrap.log"; exit 1; }
fi

echo "[컨트롤러 시작] 로그: $LOG/controllers.log"
bash "$HERE/../controllers/run-local.sh" >"$LOG/controllers.log" 2>&1 &
ctl=$!
trap 'kill $ctl 2>/dev/null; wait $ctl 2>/dev/null' EXIT
sleep 15
if ! kill -0 $ctl 2>/dev/null; then echo "컨트롤러가 바로 죽음:"; tail -30 "$LOG/controllers.log"; exit 1; fi
grep "시작:" "$LOG/controllers.log" || true

pick=("$@")
[ ${#pick[@]} -eq 0 ] && pick=(1 2 3 4 5 6 7 8 9 10 11)
results=()
for n in "${pick[@]}"; do
  f=$(ls "$HERE"/$(printf %02d "$n")-*.sh)
  echo; echo "########## 시나리오 $n: $(basename "$f")"
  if bash "$f" 2>&1 | tee "$LOG/scenario-$n.log"; then results+=("$n 끝까지 실행"); else results+=("$n 중간에 멈춤 (로그 확인)"); fi
done

bash -c "source '$HERE/lib.sh'; reset" >/dev/null 2>&1 || true
echo; echo "########## 요약"
printf '  %s\n' "${results[@]}"
echo "  로그: $LOG"
echo "  '끝까지 실행'은 스크립트가 안 멈췄다는 뜻. 통과 여부는 각 로그의 '예상:' 줄과 결과를 비교해서 판단"
