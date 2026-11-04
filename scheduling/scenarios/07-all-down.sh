#!/usr/bin/env bash
# 7. 전부 끊김: 워커 3개 모두 끊기 -> 디스패처가 지명을 멈춤 (pfSense 장애 가정)
source "$(dirname "$0")/lib.sh"
reset
trap 'for w in "${WORKERS[@]}"; do reconnect $w; done' EXIT

say "워커 3개 네트워크 끊기"
for w in "${WORKERS[@]}"; do cut_off "$w"; done
expect "MultiKueueCluster Active=False가 되기까지 기다림 (수십 초)"
watch_for 90 15 kubectl --context $MGR get multikueueclusters

say "잡 제출"
expect "NOMINATED 비어 있음, 디스패처 로그 '전부 끊김 -> 지명 중단'"
submit while-down 1 lq-light 20m
sleep 30
show

say "다시 연결"
for w in "${WORKERS[@]}"; do reconnect "$w"; done
expect "Active=True로 돌아온 뒤 while-down이 지명·입장"
wait_admitted while-down 180 || true
show
