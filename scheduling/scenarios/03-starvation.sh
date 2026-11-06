#!/usr/bin/env bash
# 3. 큰 잡 굶주림 방지: 4장 잡 1개 + 1장 잡을 계속 -> 일정 시간 안에 4장 잡이 입장
#    디스패처 STARVE_SECONDS(기본 120s)가 지나면 가장 빈 워커를 4장 잡 몫으로 비워 둔다
source "$(dirname "$0")/lib.sh"
reset

say "1장 잡(90초짜리)을 10초마다 넣기 시작"
( for i in $(seq 1 36); do submit "small-$i" 1 queue-light 90s >/dev/null; sleep 10; done ) &
feeder=$!
trap 'kill $feeder 2>/dev/null' EXIT

sleep 40
say "4장 잡 제출"
expect "처음엔 4장 비는 워커가 없어 대기 -> 120s 뒤 디스패처 로그에 '굶주림 방지: big ... 비워 둠' -> 그 워커의 작은 잡이 끝나면(최대 90s) big 입장"
submit big 4 queue-heavy 20m
start=$(date +%s)
if wait_admitted big 400; then
  say "big 입장까지 $(( $(date +%s) - start ))초"
fi
kill $feeder 2>/dev/null || true
show
