#!/usr/bin/env bash
# 6. 클러스터 대피: evacuateCluster + 워커 하나 네트워크 끊기 -> 잡이 다른 워커로, 끊긴 워커에는 새 잡이 안 감
source "$(dirname "$0")/lib.sh"
reset
trap 'reconnect worker2' EXIT

say "2장 잡 train-b를 worker2에"
submit train-b 2 queue-light 30m '"devoops/target-cluster": "worker2"'
wait_admitted train-b 120

say "worker2 네트워크 끊기 + evacuateCluster 요청"
cut_off worker2
relocate rr-evac evt-002 evacuateCluster train-b
expect "worker2 펜싱 라벨, 옛 Workload가 안 지워짐(워커가 안 보여서) -> 60s 뒤 phase=WaitingForStop, 재제출 보류"
watch_for 90 15 show_rr

say "복구 컨트롤러가 다른 방법으로 옛 실행 종료를 확인했다고 가정 -> 어노테이션"
kubectl --context $MGR -n $NS annotate relocationrequest rr-evac devoops/old-execution-stopped=true --overwrite
expect "job-train-b-g0002이 worker1 또는 worker3에 재제출·입장"
watch_for 60 15 show_rr
rr_times rr-evac

say "새 일반 잡 제출 -> worker2로 가면 안 됨"
submit after-evac 1 queue-light 20m
wait_admitted after-evac 60 || true
kubectl --context $MGR get multikueueclusters -L devoops/fenced

say "네트워크 다시 연결 (펜싱 라벨은 사람이 지우기 전까지 유지)"
reconnect worker2
expect "다시 연결되면 Kueue가 worker2의 옛 train-b 복사본을 정리하는지 -> 이게 이중 실행 질문의 답"
sleep 60
kubectl --context kind-worker2 -n $NS get jobsets,pods 2>/dev/null || true
