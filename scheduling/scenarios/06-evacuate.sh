#!/usr/bin/env bash
# 6. 클러스터 대피: evacuateCluster + 워커 하나 네트워크 끊기 -> 잡이 다른 워커로, 끊긴 워커에는 새 잡이 안 감
source "$(dirname "$0")/lib.sh"
reset
trap 'docker network connect kind worker2-control-plane 2>/dev/null || true' EXIT

say "2장 잡 train-b를 worker2에"
submit train-b 2 lq-heavy 30m '"devoops/target-cluster": "worker2"'
wait_admitted train-b 120

say "worker2 네트워크 끊기 + evacuateCluster 요청"
docker network disconnect kind worker2-control-plane
relocate rr-evac evt-002 evacuateCluster train-b
expect "worker2 펜싱 라벨, 옛 Workload는 60s 안에 안 지워짐(워커가 안 보여서) -> 경고 남기고 진행, train-b-g1이 worker1 또는 worker3에"
watch_for 120 15 show_rr
rr_times rr-evac

say "새 일반 잡 제출 -> worker2로 가면 안 됨"
submit after-evac 1 lq-light 20m
wait_admitted after-evac 60 || true
kubectl --context $MGR get multikueueclusters -L devoops/fenced

say "네트워크 다시 연결 (펜싱 라벨은 사람이 지우기 전까지 유지)"
docker network connect kind worker2-control-plane
expect "다시 연결되면 Kueue가 worker2의 옛 train-b 복사본을 정리하는지 -> 이게 이중 실행 질문의 답"
sleep 60
kubectl --context kind-worker2 -n $NS get jobsets,pods 2>/dev/null || true
