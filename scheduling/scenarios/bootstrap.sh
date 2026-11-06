#!/usr/bin/env bash
# 시나리오 돌리기 전 준비를 한 번에 (여러 번 돌려도 같은 결과). 클러스터 자체는 cluster/setup.sh로 먼저 만든다
#   관리: 큐(training 네임스페이스), 재배치 요청 CRD
#   워커: 큐, KWOK 노드(라벨·taint), pod-complete 지연, Kueue 설정(갱·재큐 끔), 클러스터 이름 ConfigMap
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MGR=kind-manager
WORKERS=(worker1 worker2 worker3)
GPU_SEL=node-role.devoops.site/gpu=true
say() { echo; echo "[$(date +%H:%M:%S)] === $*"; }

say "관리 클러스터"
kubectl --context $MGR apply -k "$ROOT/queues/manager" >/dev/null
kubectl --context $MGR apply -f "$ROOT/controllers/deploy/crd-relocationrequest.yaml" >/dev/null
# kind 전용: S3·DynamoDB 대신 복구 원본·펜싱 기록 ConfigMap을 두는 곳
kubectl --context $MGR create namespace scheduling-local-store --dry-run=client -o yaml | kubectl --context $MGR apply -f - >/dev/null
kubectl --context $MGR get clusterqueues
kubectl --context $MGR get multikueueclusters

for w in "${WORKERS[@]}"; do
  ctx=kind-$w
  say "$w"
  kubectl --context $ctx apply -k "$ROOT/queues/worker/overlays/kind" >/dev/null
  # ckpt-init·ckpt-sidecar가 owner(DynamoDB)에 쓰는 내 클러스터 이름 (JobSet 템플릿 4절 추가분, 워커마다 같은 이름 · 다른 값)
  kubectl --context $ctx -n training create configmap devoops-cluster-info --from-literal=CLUSTER_NAME="$w" \
    --dry-run=client -o yaml | kubectl --context $ctx apply -f - >/dev/null
  kubectl --context $ctx apply -f "$ROOT/kwok/nodes.yaml" >/dev/null
  kubectl --context $ctx apply -f "$ROOT/examples/kwok/pod-complete-delay.yaml" >/dev/null
  kubectl --context $ctx apply --server-side --force-conflicts -k "$ROOT/kueue/worker" >/dev/null
  kubectl --context $ctx -n platform rollout restart deploy/kueue-controller-manager >/dev/null
  kubectl --context $ctx -n platform rollout status deploy/kueue-controller-manager --timeout=180s
  kubectl --context $ctx -n training get localqueues -o custom-columns=LQ:.metadata.name,CQ:.spec.clusterQueue --no-headers | sed 's/^/  /'
  kubectl --context $ctx get clusterqueues --no-headers | sed 's/^/  /'
  kubectl --context $ctx get nodes -l "$GPU_SEL" -L devoops/spare --no-headers | sed 's/^/  /'
done

say "MultiKueue 연결 확인 (Active가 전부 True여야 함)"
sleep 10
kubectl --context $MGR get multikueueclusters -o custom-columns=NAME:.metadata.name,ACTIVE:.status.conditions[0].status
say "준비 끝"
