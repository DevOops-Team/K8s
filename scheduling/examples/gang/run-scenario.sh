#!/usr/bin/env bash
# 갱 스케줄링 시나리오 (kind + KWOK 실습 전용)
#
#   ./run-scenario.sh setup     워커 3개에 kueue/worker 설정(waitForPodsReady) 적용 + 재시작
#   ./run-scenario.sh run       노드 2대 cordon -> 4 GPU JobSet 제출 -> worker2 지명 -> 4분 관찰
#   ./run-scenario.sh cleanup   JobSet 삭제 + uncordon
#
# 예상 (이걸 맞히는지 보는 게 목적):
#   t=0      worker2에서 승인. Pod 3개 Running, 1개 Pending (노드가 3대뿐)
#   t≈2m     worker2 Workload가 PodsReadyTimeout으로 Evicted -> Pod 4개 전부 내려감
#   이후     requeuingStrategy.backoffLimitCount 0 (JobSet 템플릿 5절) -> 다시 큐에 안 넣고 Workload 비활성화
#            (11/06 전에는 backoff 약 60s 뒤 다시 승인되며 반복됐음)
#   ??       관리 클러스터 Workload는 어떻게 되나? 다른 워커로 다시 보내지나? <- 확인할 것
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"   # scheduling/
MGR=kind-manager
WORKERS=(kind-worker1 kind-worker2 kind-worker3)
TARGET=worker2
CORDON_NODES=(gpu-node-1 gpu-node-2)
NS=training
WATCH_SECONDS=${WATCH_SECONDS:-240}

ts() { date +%H:%M:%S; }
log() { echo; echo "[$(ts)] === $*"; }

setup() {
  for ctx in "${WORKERS[@]}"; do
    log "$ctx: kueue/worker 적용"
    kubectl --context "$ctx" apply --server-side --force-conflicts -k "$ROOT/kueue/worker" >/dev/null
    kubectl --context "$ctx" -n platform rollout restart deploy/kueue-controller-manager
    kubectl --context "$ctx" -n platform rollout status deploy/kueue-controller-manager --timeout=180s
    echo "적용된 waitForPodsReady:"
    kubectl --context "$ctx" -n platform get cm kueue-manager-config \
      -o jsonpath='{.data.controller_manager_config\.yaml}' | grep -A3 '^waitForPodsReady'
  done
}

mgr_wl() {
  kubectl --context "$MGR" -n "$NS" get workloads -o name | grep 'jobset-gang-4gpu' | head -1 | cut -d/ -f2
}

snapshot() {
  local wl="$1"
  echo "--- [$(ts)] 관리 클러스터 Workload $wl"
  kubectl --context "$MGR" -n "$NS" get workload "$wl" -o jsonpath='{"clusterName="}{.status.clusterName}{"  nominated="}{.status.nominatedClusterNames}{"\n"}{range .status.conditions[*]}{"  "}{.type}{"="}{.status}{" ("}{.reason}{")\n"}{end}' 2>/dev/null || echo "  (없음)"
  echo "--- worker2 Workload"
  kubectl --context "kind-$TARGET" -n "$NS" get workloads -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.conditions[*]}{"  "}{.type}{"="}{.status}{" ("}{.reason}{")\n"}{end}{"  requeue="}{.status.requeueState}{"\n"}{end}' 2>/dev/null || echo "  (없음)"
  echo "--- worker2 Pod"
  kubectl --context "kind-$TARGET" -n "$NS" get pods -o wide --no-headers 2>/dev/null \
    | awk '{printf "  %-40s %-10s %s\n", $1, $3, $7}' || true
  echo "--- 다른 워커에 복사본?"
  for w in worker1 worker3; do
    printf "  %s: %s\n" "$w" "$(kubectl --context "kind-$w" -n "$NS" get jobsets --no-headers 2>/dev/null | awk '{print $1}' | tr '\n' ' ')"
  done
}

run() {
  log "worker2 노드 ${CORDON_NODES[*]} cordon (5대 중 3대만 남김)"
  for n in "${CORDON_NODES[@]}"; do kubectl --context "kind-$TARGET" cordon "$n"; done

  log "JobSet 제출 (관리 클러스터, queue-heavy)"
  kubectl --context "$MGR" apply -f "$ROOT/examples/gang/gang-4gpu.yaml"

  local wl=""
  for _ in $(seq 1 30); do wl="$(mgr_wl || true)"; [ -n "$wl" ] && break; sleep 2; done
  [ -z "$wl" ] && { echo "관리 클러스터에 Workload가 안 생김"; exit 1; }
  echo "Workload: $wl"

  log "$TARGET 수동 지명 (디스패처 역할)"
  kubectl --context "$MGR" -n "$NS" patch workload "$wl" --subresource=status --type=merge \
    -p "{\"status\":{\"nominatedClusterNames\":[\"$TARGET\"]}}"

  log "${WATCH_SECONDS}s 동안 15초마다 관찰"
  local end=$(( $(date +%s) + WATCH_SECONDS ))
  while [ "$(date +%s)" -lt "$end" ]; do
    snapshot "$wl"
    sleep 15
  done

  log "worker2 이벤트 (Workload 관련)"
  kubectl --context "kind-$TARGET" -n "$NS" get events --sort-by=.lastTimestamp \
    | grep -Ei 'workload|evict|podsready|admitted' | tail -20 || true
  log "끝. 상태를 더 보고 싶으면 그대로 두고, 정리는 ./run-scenario.sh cleanup"
}

cleanup() {
  log "정리"
  kubectl --context "$MGR" -n "$NS" delete jobset gang-4gpu --ignore-not-found
  for n in "${CORDON_NODES[@]}"; do kubectl --context "kind-$TARGET" uncordon "$n"; done
}

case "${1:-}" in
  setup) setup ;;
  run) run ;;
  cleanup) cleanup ;;
  *) echo "사용법: $0 {setup|run|cleanup}"; exit 1 ;;
esac
