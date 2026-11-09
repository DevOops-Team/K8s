#!/usr/bin/env bash
# 8. 예비 off: 5번을 자리 표시 Pod 없이 -> 자리가 날 때까지 대기. 5번과 시각 차이를 비교
#    "예비 노드가 없음"을 gpu-node-5 cordon으로 흉내 내고, 60초 뒤 uncordon = 새 노드 확보(Karpenter 역할)
source "$(dirname "$0")/lib.sh"
reset

say "worker2 gpu-node-5 cordon (예비 없음)"
kubectl --context kind-worker2 cordon gpu-node-5 >/dev/null

say "4장 잡 train-a를 worker2에"
submit train-a 4 queue-heavy 30m '"devoops/target-cluster": "worker2"'
wait_admitted train-a 120
sleep 10
bad=$(kubectl --context kind-worker2 -n $NS get pods -l jobset.sigs.k8s.io/jobset-name=$(jsname train-a) -o jsonpath='{.items[0].spec.nodeName}')

say "$bad 고장 가정 -> moveToSpare 요청"
expect "job-train-a-g0002이 재제출되지만 노드가 3대뿐이라 지명 안 됨 (반쪽 배치 대신 대기)"
relocate rr-nospare evt-003 moveToSpare train-a "$bad"
sleep 60
show_rr
show

say "새 노드 확보 흉내: gpu-node-5 uncordon"
kubectl --context kind-worker2 uncordon gpu-node-5 >/dev/null
wait_admitted "$(jsname train-a 2)" 120 || true
rr_times rr-nospare
expect "resubmitted -> admitted 간격이 5번보다 약 60초 이상 김. 실제로는 이 자리가 노드 기동 시간"
