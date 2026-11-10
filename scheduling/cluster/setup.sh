#!/usr/bin/env bash
# 로컬 kind 환경을 처음부터 만든다 (우분투 VM). 이미 있는 클러스터는 지우지 않고 건너뛴다.
#   ./setup.sh            관리 1 + 워커 3 전부
#   RECREATE=1 ./setup.sh 같은 이름의 kind 클러스터를 지우고 다시 (네임스페이스를 바꾼 뒤 한 번 필요: 11/06 platform, 11/10 kueue-system·jobset-system)
#
# 만드는 것 (11/05 K8s 아키텍처와 같은 배치):
#   manager  컨트롤 플레인 1 + worker 3 (platform-worker=true)  Kueue(+MultiKueue, MAP) -> kueue-system · JobSet -> jobset-system 각 3중화, 컨트롤러 -> platform
#   workerN  컨트롤 플레인 1 + KWOK 가짜 GPU 노드 5 (학습 4 + 예비 1)  Kueue -> kueue-system · JobSet -> jobset-system
#   MultiKueue kubeconfig Secret workerN-secret (kueue-system, 키 kubeconfig, 주소는 kind 내부망)
# 그다음: scenarios/bootstrap.sh (큐·CRD·KWOK 노드), 컨트롤러는 controllers/run-local.sh 또는 README의 클러스터 안 배포
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"   # scheduling/
WORKERS=(worker1 worker2 worker3)
say() { echo; echo "[$(date +%H:%M:%S)] === $*"; }

create() {   # 이름 설정파일
  if kind get clusters 2>/dev/null | grep -qx "$1"; then
    if [ "${RECREATE:-0}" = 1 ]; then kind delete cluster --name "$1"; else echo "  $1 이미 있음 -> 건너뜀"; return; fi
  fi
  kind create cluster --name "$1" --config "$2"
}

# 웹훅이 뜨기 전 apply는 실패할 수 있어서 몇 번 다시 시도
apply_retry() {
  local i
  for i in $(seq 1 20); do "$@" && return 0; sleep 6; done
  echo "실패: $*" >&2; return 1
}

install_common() {   # context 오버레이(manager|worker, Kueue·JobSet 공통)
  local ctx=$1
  kubectl --context "$ctx" apply --server-side --force-conflicts -k "$ROOT/kueue/$2"
  kubectl --context "$ctx" -n kueue-system rollout status deploy/kueue-controller-manager --timeout=300s
  kubectl --context "$ctx" apply --server-side --force-conflicts -k "$ROOT/jobset/$3"
  kubectl --context "$ctx" -n jobset-system rollout status deploy/jobset-controller-manager --timeout=300s
}

say "관리 클러스터"
create manager "$ROOT/cluster/kind/manager.yaml"
install_common kind-manager manager manager
kubectl --context kind-manager create namespace platform --dry-run=client -o yaml | kubectl --context kind-manager apply -f -   # 컨트롤러·Lease

for w in "${WORKERS[@]}"; do
  say "$w"
  create "$w" "$ROOT/cluster/kind/worker.yaml"
  install_common "kind-$w" worker base
  kubectl --context "kind-$w" apply -f "$ROOT/kwok/base/kwok.yaml"
  apply_retry kubectl --context "kind-$w" apply -f "$ROOT/kwok/base/stage-fast.yaml"
done

say "MultiKueue kubeconfig Secret (관리 클러스터 kueue-system)"
for w in "${WORKERS[@]}"; do
  kind get kubeconfig --name "$w" --internal > "/tmp/$w.kubeconfig"
  kubectl --context kind-manager -n kueue-system create secret generic "$w-secret" \
    --from-file=kubeconfig="/tmp/$w.kubeconfig" --dry-run=client -o yaml | kubectl --context kind-manager apply -f -
  rm -f "/tmp/$w.kubeconfig"
done

say "다음: scenarios/bootstrap.sh"
