#!/usr/bin/env bash
# 5. 예비 노드 이전: moveToSpare -> 고장 노드 격리, 자리 표시 Pod가 밀려나고 잡이 예비 노드에
source "$(dirname "$0")/lib.sh"
reset

say "worker2 예비 노드(gpu-node-5)에 자리 표시 Pod"
kubectl --context kind-worker2 apply -f "$ROOT/examples/spare/placeholder.yaml" >/dev/null
sleep 10

say "4장 잡 train-a를 worker2에"
submit train-a 4 queue-heavy 30m '"devoops/target-cluster": "worker2"'
wait_admitted train-a 120
sleep 10
show_nodes
bad=$(kubectl --context kind-worker2 -n $NS get pods -l jobset.sigs.k8s.io/jobset-name=$(jsname train-a) -o jsonpath='{.items[0].spec.nodeName}')

say "$bad 고장 가정 -> moveToSpare 요청"
expect "$bad cordon+taint, train-a 삭제, job-train-a-g0002(복구 우선순위)이 worker2에 재제출, Pod 하나는 gpu-node-5, 자리 표시 Pod는 Pending"
relocate rr-spare evt-001 moveToSpare train-a "$bad"
watch_for 90 15 show_rr
show_nodes
rr_times rr-spare
