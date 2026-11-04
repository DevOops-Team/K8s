#!/usr/bin/env bash
# 4. 복구 잡 우선: 대기열이 찬 상태에서 복구 잡 제출 -> 복구 잡이 맨 앞에서 입장
#    cq-recovery는 lendingLimit 0 이라 복구 몫(2장)은 늘 비어 있다 (선점 없이 바로 쿼터)
source "$(dirname "$0")/lib.sh"
reset

say "1장 잡 12개 제출 -> 10개 입장, 2개 대기 (일반 잡은 최대 10장)"
for i in $(seq 1 12); do submit "fill-$i" 1 lq-light 20m; done
sleep 30
show

say "복구 잡 제출 (lq-recovery)"
expect "fill-11, fill-12보다 먼저 쿼터를 받고 입장. PRIO 1000"
submit rescue 1 lq-recovery 20m
wait_admitted rescue 60 || true
show
