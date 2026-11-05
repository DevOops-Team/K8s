#!/usr/bin/env bash
# 10. 다른 클러스터로 이전 (예비 소진·SDC): moveToOtherCluster -> 의심 노드만 격리, 잡은 다른 워커로,
#     worker2는 펜싱하지 않으므로 새 잡을 계속 받는다 (6번 evacuateCluster와 비교)
source "$(dirname "$0")/lib.sh"
reset

say "2장 잡 train-d를 worker2에"
submit train-d 2 lq-light 30m '"devoops/target-cluster": "worker2"'
wait_admitted train-d 120
sleep 10
bad=$(kubectl --context kind-worker2 -n $NS get pods -l jobset.sigs.k8s.io/jobset-name=train-d -o jsonpath='{.items[0].spec.nodeName}')

say "$bad SDC 의심 -> moveToOtherCluster 요청"
expect "$bad cordon + devoops/faulty, worker2 펜싱 라벨 없음, train-d-g1이 worker1 또는 worker3에 입장 (exclude=worker2)"
relocate rr-other evt-010 moveToOtherCluster train-d "$bad"
wait_admitted train-d-g1 180 || true
show_rr
rr_times rr-other
kubectl --context $MGR get multikueueclusters -L devoops/fenced
kubectl --context kind-worker2 get node "$bad" -o jsonpath='{.spec.unschedulable} {.spec.taints}{"\n"}'

say "새 일반 잡을 worker2로 -> 펜싱 안 했으므로 들어가야 함"
submit after-other 1 lq-light 20m '"devoops/target-cluster": "worker2"'
expect "after-other가 worker2에 입장 ($bad는 격리돼 있으니 다른 노드에)"
wait_admitted after-other 120 || true
show
