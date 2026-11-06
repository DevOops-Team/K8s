# 시나리오 공통 함수. 각 시나리오 스크립트가 source 한다.
# 전제: 다른 터미널에서 controllers/run-local.sh 가 돌고 있음
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # scheduling/
MGR=kind-manager
WORKERS=(worker1 worker2 worker3)
NS=training          # 학습 잡·재배치 요청 (JobSet 템플릿 합의안)
CTL_NS=platform      # 컨트롤러·Kueue. kind에서는 S3·DynamoDB 대신 여기 ConfigMap (recovery_store.py)
GPU_SEL=node-role.devoops.site/gpu=true

ts() { date +%H:%M:%S; }
say() { echo; echo "[$(ts)] === $*"; }
expect() { echo; echo "  예상: $*"; }

# 잡 이름: 백엔드처럼 job-{작업ID}-g{세대 4자리}. 시나리오에서는 작업ID만 쓴다
#   jsname train-a     -> job-train-a-g0001
#   jsname train-a 2   -> job-train-a-g0002
#   jsname job-...     -> 그대로
jsname() {
  case "$1" in job-*) echo "$1" ;; *) printf 'job-%s-g%04d\n' "$1" "${2:-1}" ;; esac
}

# submit 작업ID GPU수 큐 [유지시간] [추가 어노테이션 "k: v" ...]
# 백엔드 흉내: JobSet 템플릿 합의안 모양(이름·라벨·subdomain·GENERATION·--master-addr·toleration)으로 1세대를 제출하고
#   복구 원본(S3 code/{id}/jobset.json 대신 ConfigMap recovery-code-<id>)과
#   펜싱 기록(DynamoDB 대신 ConfigMap recovery-fencing-<id>, generation=1)을 남긴다.
# Pod GPU수개 x GPU 1장 (노드당 GPU 1개 모양에 맞춤). 학습 대신 KWOK이 유지시간 뒤 Pod를 끝냄
submit() {
  local id=$1 gpus=$2 queue=$3 dur=${4:-20m}
  shift $(( $# < 4 ? $# : 4 ))
  local name ann="" f
  name=$(jsname "$id")
  for a in "$@"; do ann+="    $a"$'\n'; done
  f=$(mktemp)
  cat > "$f" <<EOF
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: $name
  namespace: $NS
  labels:
    kueue.x-k8s.io/queue-name: $queue
    kueue.x-k8s.io/priority-class: $([ "$queue" = queue-recovery ] && echo recovery || echo training)
    devoops/job-id: "$id"
    devoops/generation: "1"
  annotations:
    scenario/gpus: "$gpus"
$ann
spec:
  failurePolicy: {maxRestarts: 0}
  network: {enableDNSHostnames: true, subdomain: $name}
  replicatedJobs:
  - name: workers
    replicas: 1
    template:
      spec:
        completionMode: Indexed
        parallelism: $gpus
        completions: $gpus
        backoffLimit: 0
        template:
          metadata:
            labels: {devoops/job-id: "$id", devoops/generation: "1"}
            annotations:
              pod-complete.stage.kwok.x-k8s.io/delay: "$dur"
          spec:
            restartPolicy: Never
            tolerations:
            - {key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}
            containers:
            - name: trainer
              image: busybox:1.36
              command: [sh, -c]
              args: ["exec sleep infinity --master-addr=$name-workers-0-0.$name"]
              env:
              - {name: JOB_ID, value: "$id"}
              - {name: GENERATION, value: "1"}
              resources:
                requests: {nvidia.com/gpu: 1}
                limits: {nvidia.com/gpu: 1}
EOF
  kubectl --context $MGR create configmap "recovery-code-$id" -n $CTL_NS \
    --from-file=jobset.json=<(kubectl --context $MGR create --dry-run=client -o json -f "$f") \
    --dry-run=client -o yaml | kubectl --context $MGR apply -f - >/dev/null
  kubectl --context $MGR create configmap "recovery-fencing-$id" -n $CTL_NS \
    --from-literal=job_id="$id" --from-literal=generation=1 --from-literal=retry_count=0 \
    --dry-run=client -o yaml | kubectl --context $MGR apply -f - >/dev/null
  kubectl --context $MGR apply -f "$f" >/dev/null
  rm -f "$f"
  echo "  제출: $name (GPU $gpus, $queue, $dur)"
}

# 잡 투입기 흉내: GPU 수로 큐를 정한다 (설계: 한 클러스터 학습 노드의 절반 초과 -> Heavy)
#   한 클러스터 학습 노드 수를 넘는 잡은 어디에도 못 들어가므로 거부 (종료 코드 2)
HEAVY_MIN_GPUS=${HEAVY_MIN_GPUS:-3}
MAX_JOB_GPUS=${MAX_JOB_GPUS:-4}
queue_for() {
  [ "$1" -gt "$MAX_JOB_GPUS" ] && return 2
  if [ "$1" -ge "$HEAVY_MIN_GPUS" ]; then echo queue-heavy; else echo queue-light; fi
}
# submit_auto 작업ID GPU수 [유지시간] [추가 어노테이션 ...]
submit_auto() {
  local q
  q=$(queue_for "$2") || { echo "  거부: $1 (GPU $2 > 한 클러스터 학습 노드 $MAX_JOB_GPUS)"; return 2; }
  submit "$1" "$2" "$q" "${3:-20m}" "${@:4}"
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
      | awk -v ns="$NS" '$1==ns{printf "  %-45s %-10s %s\n",$2,$4,$8}' || echo "  (접속 안 됨)"
  done
}

cluster_of() {  # 작업ID 또는 JobSet 이름 -> 들어간 워커
  local name; name=$(jsname "$1")
  kubectl --context $MGR -n $NS get workloads -o json 2>/dev/null | python3 -c "
import json,sys
for w in json.load(sys.stdin)['items']:
    if any(o['name']=='$name' for o in w['metadata'].get('ownerReferences',[])):
        print(w.get('status',{}).get('clusterName','')); break"
}

wait_admitted() {  # 작업ID 또는 JobSet 이름, 최대 초
  local name; name=$(jsname "$1")
  local max=${2:-120} t=0
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

# relocate 요청이름 recoveryId action 작업ID [excludeNodes csv]
# 복구 컨트롤러 흉내: 펜싱 기록에서 세대 +1 (owner 비움, GENERATION_ADVANCED, from/to gen, retry_count+1)을 한 뒤 요청을 만든다.
# 같은 recoveryId를 다시 부르면 세대를 또 올리지 않는다 (규약 5절)
relocate() {
  local id=$4 nodes="" cm="recovery-fencing-$4" gen rid
  [ -n "${5:-}" ] && nodes="  excludeNodes: [${5}]"
  gen=$(kubectl --context $MGR -n $CTL_NS get cm "$cm" -o jsonpath='{.data.generation}')
  rid=$(kubectl --context $MGR -n $CTL_NS get cm "$cm" -o jsonpath='{.data.recovery_id}')
  if [ "$rid" != "$2" ]; then
    kubectl --context $MGR -n $CTL_NS patch cm "$cm" --type merge -p "{\"data\": {
      \"generation\": \"$((gen + 1))\", \"owner\": \"\", \"recovery_id\": \"$2\",
      \"recovery_phase\": \"GENERATION_ADVANCED\", \"recovery_from_gen\": \"$gen\",
      \"recovery_to_gen\": \"$((gen + 1))\", \"retry_count\": \"$(( $(kubectl --context $MGR -n $CTL_NS get cm "$cm" -o jsonpath='{.data.retry_count}') + 1 ))\",
      \"new_jobset_name\": \"\", \"new_jobset_uid\": \"\"}}" >/dev/null
    echo "  (복구 컨트롤러 흉내) $id 세대 $gen -> $((gen + 1)), recovery_id=$2"
  else
    gen=$(kubectl --context $MGR -n $CTL_NS get cm "$cm" -o jsonpath='{.data.recovery_from_gen}')
  fi
  kubectl --context $MGR apply -f - <<EOF
apiVersion: scheduling.devoops.site/v1alpha1
kind: RelocationRequest
metadata:
  name: $1
  namespace: $NS
spec:
  requestId: $2
  action: $3
  jobRef: $NS/$(jsname "$id" "$gen")
  reason: hardware
$nodes
EOF
}

watch_for() {  # 초 간격 명령...
  local secs=$1 every=$2; shift 2
  local end=$(( $(date +%s) + secs ))
  while [ "$(date +%s)" -lt "$end" ]; do "$@"; sleep "$every"; done
}

# 워커 네트워크 끊기/붙이기. 다시 붙일 때 원래 IP로 (IP가 바뀌면 kind 클러스터 안 apiserver·etcd 주소가 어긋나 깨짐)
# 원래 IP = API 서버 advertise 주소. 끊을 때 저장한 IP는 그걸 못 읽을 때만 씀
# (저장 시점에 이미 IP가 뒤바뀌어 있었으면 저장값도 틀림)
IPDIR="${TMPDIR:-/tmp}/kind-scenario-ip"
orig_ip() {
  docker exec "$1-control-plane" grep -oP '(?<=--advertise-address=)\S+' /etc/kubernetes/manifests/kube-apiserver.yaml 2>/dev/null \
    || cat "$IPDIR/$1" 2>/dev/null || true
}
cut_off() {
  local c="$1-control-plane"; mkdir -p "$IPDIR"
  docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$c" > "$IPDIR/$1"
  docker network disconnect kind "$c"
  echo "  $1 끊음 (IP $(cat "$IPDIR/$1") 저장)"
}
reconnect() {
  local c="$1-control-plane" ip now
  ip=$(orig_ip "$1")
  if ! docker inspect -f '{{json .NetworkSettings.Networks}}' "$c" | grep -q '"kind"'; then
    if [ -n "$ip" ]; then docker network connect --ip "$ip" kind "$c"
    else docker network connect kind "$c"; fi
    echo "  $1 다시 연결 (IP $(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$c"))"
  fi
  now=$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "$c")
  [ -z "$ip" ] || [ "$now" = "$ip" ] || echo "  경고: $1 IP가 $now (원래 $ip). 워커를 전부 끊고 reconnect로 다시 붙일 것" >&2
}

reset() {
  local w n left   # 부르는 쪽 변수(예: 반복문의 n)를 덮어쓰지 않게
  say "초기화 (잡·요청 삭제, 노드·펜싱·네트워크 원상복구)"
  kubectl --context $MGR -n $NS delete relocationrequests --all --ignore-not-found >/dev/null 2>&1 || true
  kubectl --context $MGR -n $NS delete jobsets --all --ignore-not-found --wait=false >/dev/null
  kubectl --context $MGR -n $CTL_NS get cm -o name 2>/dev/null | grep -E '/recovery-(code|fencing)-' \
    | xargs -r kubectl --context $MGR -n $CTL_NS delete --ignore-not-found >/dev/null
  for w in "${WORKERS[@]}"; do
    reconnect "$w"
    kubectl --context $MGR label multikueuecluster "$w" devoops/fenced- >/dev/null 2>&1 || true
    for n in $(kubectl --context kind-$w get nodes -l "$GPU_SEL" -o name 2>/dev/null); do
      kubectl --context kind-$w uncordon "$n" >/dev/null
      kubectl --context kind-$w taint nodes "${n#node/}" devoops/faulty- >/dev/null 2>&1 || true   # taint는 node/이름 형식 못 받음
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
