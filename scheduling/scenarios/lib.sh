# 시나리오 공통 함수. 각 시나리오 스크립트가 source 한다.
# 전제: 다른 터미널에서 controllers/run-local.sh 가 돌고 있음
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # scheduling/
MGR=kind-manager
WORKERS=(worker1 worker2 worker3)
NS=ml

ts() { date +%H:%M:%S; }
say() { echo; echo "[$(ts)] === $*"; }
expect() { echo; echo "  예상: $*"; }

# submit 이름 GPU수 큐 [유지시간] [추가 어노테이션 "k: v" ...]
# Pod GPU수개 x GPU 1장 (노드당 GPU 1개 모양에 맞춤)
submit() {
  local name=$1 gpus=$2 queue=$3 dur=${4:-20m}
  shift $(( $# < 4 ? $# : 4 ))
  local ann=""
  for a in "$@"; do ann+="    $a"$'\n'; done
  kubectl --context $MGR apply -f - >/dev/null <<EOF
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: $name
  namespace: $NS
  labels:
    kueue.x-k8s.io/queue-name: $queue
    kueue.x-k8s.io/priority-class: $([ "$queue" = lq-recovery ] && echo recovery || echo training)
  annotations:
    scenario/gpus: "$gpus"
$ann
spec:
  suspend: true
  replicatedJobs:
  - name: trainer
    replicas: 1
    template:
      spec:
        parallelism: $gpus
        completions: $gpus
        backoffLimit: 0
        template:
          metadata:
            annotations:
              pod-complete.stage.kwok.x-k8s.io/delay: "$dur"
          spec:
            restartPolicy: Never
            containers:
            - name: main
              image: busybox:1.36
              resources:
                requests: {nvidia.com/gpu: 1}
                limits: {nvidia.com/gpu: 1}
EOF
  echo "  제출: $name (GPU $gpus, $queue, $dur)"
}

# 관리 클러스터 Workload 한눈에
show() {
  echo "--- [$(ts)] 관리 클러스터 Workload"
  kubectl --context $MGR -n $NS get workloads -o custom-columns=\
'NAME:.metadata.ownerReferences[0].name,PRIO:.spec.priority,QUOTA:.status.conditions[?(@.type=="QuotaReserved")].status,NOMINATED:.status.nominatedClusterNames,CLUSTER:.status.clusterName,ADMITTED:.status.conditions[?(@.type=="Admitted")].status' \
    2>/dev/null | sort || true
}

# 워커별 GPU 노드와 그 위 Pod
show_nodes() {
  for w in "${WORKERS[@]}"; do
    echo "--- $w"
    kubectl --context kind-$w get pods -A -o wide --no-headers 2>/dev/null \
      | awk '$1=="ml"{printf "  %-45s %-10s %s\n",$2,$4,$8}' || echo "  (접속 안 됨)"
  done
}

cluster_of() {  # JobSet 이름 -> 들어간 워커
  kubectl --context $MGR -n $NS get workloads -o json 2>/dev/null | python3 -c "
import json,sys
for w in json.load(sys.stdin)['items']:
    if any(o['name']=='$1' for o in w['metadata'].get('ownerReferences',[])):
        print(w.get('status',{}).get('clusterName','')); break"
}

wait_admitted() {  # JobSet 이름, 최대 초
  local name=$1 max=${2:-120} t=0
  while [ $t -lt $max ]; do
    [ -n "$(cluster_of "$name")" ] && { echo "  $name -> $(cluster_of "$name") (${t}s)"; return 0; }
    sleep 3; t=$((t+3))
  done
  echo "  $name: ${max}s 안에 입장 못 함"; return 1
}

show_rr() {
  echo "--- [$(ts)] 재배치 요청"
  kubectl --context $MGR -n $NS get relocationrequests 2>/dev/null || true
}

rr_times() {  # 요청 이름 -> 단계별 시각
  kubectl --context $MGR -n $NS get relocationrequest "$1" -o jsonpath=\
'{"  phase="}{.status.phase}{"  "}{.status.message}{"\n  accepted   "}{.status.acceptedAt}{"\n  isolated   "}{.status.isolatedAt}{"\n  stopped    "}{.status.stoppedAt}{"\n  resubmitted "}{.status.resubmittedAt}{"\n  admitted   "}{.status.admittedAt}{"\n"}'
}

relocate() {  # 이름 requestId action jobset [excludeNodes csv]
  local nodes=""
  [ -n "${5:-}" ] && nodes="  excludeNodes: [${5}]"
  kubectl --context $MGR apply -f - <<EOF
apiVersion: scheduling.devoops.site/v1alpha1
kind: RelocationRequest
metadata:
  name: $1
  namespace: $NS
spec:
  requestId: $2
  action: $3
  jobRef: $NS/$4
  reason: hardware
$nodes
EOF
}

watch_for() {  # 초 간격 명령...
  local secs=$1 every=$2; shift 2
  local end=$(( $(date +%s) + secs ))
  while [ "$(date +%s)" -lt "$end" ]; do "$@"; sleep "$every"; done
}

reset() {
  say "초기화 (잡·요청 삭제, 노드·펜싱·네트워크 원상복구)"
  kubectl --context $MGR -n $NS delete relocationrequests --all --ignore-not-found >/dev/null 2>&1 || true
  kubectl --context $MGR -n $NS delete jobsets --all --ignore-not-found --wait=false >/dev/null
  for w in "${WORKERS[@]}"; do
    docker network connect kind "$w-control-plane" 2>/dev/null || true
    kubectl --context $MGR label multikueuecluster "$w" devoops/fenced- >/dev/null 2>&1 || true
    for n in $(kubectl --context kind-$w get nodes -l node-role=gpu -o name 2>/dev/null); do
      kubectl --context kind-$w uncordon "$n" >/dev/null
      kubectl --context kind-$w taint "$n" devoops/faulty- >/dev/null 2>&1 || true
    done
    kubectl --context kind-$w -n $NS delete deploy spare-placeholder --ignore-not-found >/dev/null 2>&1 || true
  done
  # 워커 쪽 복사본이 정리될 때까지
  for _ in $(seq 1 40); do
    left=0
    for w in "${WORKERS[@]}"; do
      left=$(( left + $(kubectl --context kind-$w -n $NS get pods --no-headers 2>/dev/null | wc -l) ))
    done
    [ $left -eq 0 ] && break
    sleep 3
  done
  echo "  완료"
}
