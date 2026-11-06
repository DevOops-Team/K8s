#!/usr/bin/env bash
# 4. 복구 잡 우선: 대기열이 찬 상태에서 복구 잡 제출 -> 복구 잡이 맨 앞에서 입장
#    복구 몫(관리 cq-recovery 3 = 예비 1 x 3, 워커 recovery-cq 1)은 학습 잡에 안 빌려줌 -> 늘 비어 있다
source "$(dirname "$0")/lib.sh"
reset

say "1장 잡 14개 제출 -> 12개 입장, 2개 대기 (일반 잡은 학습 몫 12장까지)"
for i in $(seq 1 14); do submit "fill-$i" 1 queue-light 20m; done
sleep 30
show

say "복구 잡 제출 (queue-recovery)"
expect "fill-13, fill-14보다 먼저 입장. 워커의 예비 노드(gpu-node-5)에 올라감. PRIO 1000"
submit rescue 1 queue-recovery 20m
wait_admitted rescue 60 || true
show
