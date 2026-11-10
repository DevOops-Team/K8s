#!/usr/bin/env bash
# 스케줄링 e2e. 클러스터는 cluster/setup.sh, 큐·CRD는 e2e/bootstrap.sh로 먼저 준비.
#   ./run.sh normal              평소 학습 검사 01~07 (PASS/FAIL)
#   ./run.sh recovery            장애복구 검사 04~13 (PASS/FAIL, 06·07은 docker network로 워커를 끊음)
#   ./run.sh compare             비교 실험 (가설 판정, 오래 걸림)
#   ./run.sh all                 전부
#   ./run.sh -k gang             pytest 인자 그대로
# 결과: e2e/logs/<시각>/ results.json, summary.md, pytest.log, controllers-*.log
# 종료 코드: FAIL 또는 INVALID가 있으면 1, 준비 안 됨 3
set -uo pipefail
cd "$(dirname "$0")"
if [ ! -x .venv/bin/pytest ]; then
  python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt || { echo "venv 준비 실패"; exit 3; }
fi
export E2E_LOG="$PWD/logs/$(date +%m%d-%H%M%S)"
mkdir -p "$E2E_LOG"
args=()
case "${1:-normal}" in
  normal|recovery|compare) args=(-m "$1"); shift ;;
  all) shift ;;
esac
.venv/bin/pytest "${args[@]}" "$@" 2>&1 | tee "$E2E_LOG/pytest.log"
rc=${PIPESTATUS[0]}
[ "$rc" = 3 ] && exit 3
[ -f "$E2E_LOG/results.json" ] || exit "$rc"
python3 -c "
import json, sys
r = json.load(open('$E2E_LOG/results.json'))['results']
sys.exit(1 if any(x['verdict'] in ('FAIL', 'INVALID') for x in r) else 0)"
