#!/usr/bin/env bash
# 시나리오 돌리기 전 준비를 한 번에 (여러 번 돌려도 같은 결과)
#   관리: 큐, 재배치 요청 CRD, job-placement 네임스페이스
#   워커: 큐(LocalQueue가 예전 CQ를 가리키면 지우고 다시), KWOK 노드 라벨, pod-complete 지연, Kueue 갱 설정
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MGR=kind-manager
WORKERS=(worker1 worker2 worker3)
say() { echo; echo "[$(date +%H:%M:%S)] === $*"; }

say "관리 클러스터"
kubectl --context $MGR apply -k "$ROOT/queues/manager" >/dev/null
kubectl --context $MGR apply -f "$ROOT/controllers/deploy/crd-relocationrequest.yaml" >/dev/null
kubectl --context $MGR get ns job-placement >/dev/null 2>&1 || kubectl --context $MGR create ns job-placement >/dev/null
kubectl --context $MGR get clusterqueues
kubectl --context $MGR get multikueueclusters

for w in "${WORKERS[@]}"; do
  ctx=kind-$w
  say "$w"
  # LocalQueue.spec.clusterQueue는 못 바꿈 -> 원하는 CQ와 다르면 지우고 다시 만듦
  #   lq-heavy, lq-light -> train-cq / lq-recovery -> recovery-cq
  while read -r lq cq; do
    [ -z "$lq" ] && continue
    want=train-cq; [ "$lq" = lq-recovery ] && want=recovery-cq
    if [ "$cq" != "$want" ]; then
      echo "  LocalQueue $lq: $cq -> $want 로 다시 만듦"
      kubectl --context $ctx -n ml delete localqueue "$lq"
    fi
  done < <(kubectl --context $ctx -n ml get localqueues -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.clusterQueue}{"\n"}{end}' 2>/dev/null)
  kubectl --context $ctx apply -k "$ROOT/queues/worker/overlays/kind" >/dev/null
  kubectl --context $ctx apply -f "$ROOT/kwok/nodes.yaml" >/dev/null
  kubectl --context $ctx label node gpu-node-5 scheduling.gpu-project.io/spare- >/dev/null 2>&1 || true
  kubectl --context $ctx apply -f "$ROOT/examples/kwok/pod-complete-delay.yaml" >/dev/null
  kubectl --context $ctx apply --server-side --force-conflicts -k "$ROOT/kueue/worker" >/dev/null
  kubectl --context $ctx -n kueue-system rollout restart deploy/kueue-controller-manager >/dev/null
  kubectl --context $ctx -n kueue-system rollout status deploy/kueue-controller-manager --timeout=180s
  kubectl --context $ctx -n ml get localqueues -o custom-columns=LQ:.metadata.name,CQ:.spec.clusterQueue --no-headers | sed 's/^/  /'
  kubectl --context $ctx get clusterqueues --no-headers | sed 's/^/  /'
  kubectl --context $ctx get nodes -l node-role=gpu -L devoops/spare --no-headers | sed 's/^/  /'
done

say "MultiKueue 연결 확인 (Active가 전부 True여야 함)"
sleep 10
kubectl --context $MGR get multikueueclusters -o custom-columns=NAME:.metadata.name,ACTIVE:.status.conditions[0].status
say "준비 끝"
