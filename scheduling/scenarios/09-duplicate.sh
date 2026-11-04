#!/usr/bin/env bash
# 9. 중복 요청: 같은 requestId를 두 번 -> 한 번만 실행
source "$(dirname "$0")/lib.sh"
reset

say "1장 잡 train-c"
submit train-c 1 lq-light 30m
wait_admitted train-c 60

say "같은 requestId(evt-009)로 restartInPlace 요청 2개"
expect "rr-dup-a는 Done, rr-dup-b는 Duplicate, 새 잡은 train-c-g1 하나뿐"
relocate rr-dup-a evt-009 restartInPlace train-c
sleep 1
relocate rr-dup-b evt-009 restartInPlace train-c
watch_for 60 15 show_rr
kubectl --context $MGR -n $NS get jobsets -L devoops/job-id,devoops/generation,devoops/request-id
