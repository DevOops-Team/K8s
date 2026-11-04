#!/usr/bin/env bash
# 컨트롤러 3개를 노트북(우분투 VM)에서 바로 실행. Ctrl-C로 셋 다 종료.
#   ./run-local.sh                 셋 다
#   ./run-local.sh dispatcher      하나만
# 이미지 빌드 없이 코드를 고치고 바로 다시 돌리는 용도. 클러스터 안 배포는 README 참고.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
  .venv/bin/pip install -q -r requirements.txt
fi

export KUBE_CONTEXT=${KUBE_CONTEXT:-kind-manager}
export WORKER_CONTEXTS=${WORKER_CONTEXTS:-worker1=kind-worker1,worker2=kind-worker2,worker3=kind-worker3}
export PYTHONUNBUFFERED=1

# CRD와 재배치 실행기 잡 사본 보관 네임스페이스가 없으면 먼저 만듦
kubectl --context "$KUBE_CONTEXT" apply -f deploy/crd-relocationrequest.yaml >/dev/null
kubectl --context "$KUBE_CONTEXT" get ns job-placement >/dev/null 2>&1 \
  || kubectl --context "$KUBE_CONTEXT" create ns job-placement

names=("${@:-dispatcher relocator priority}")
read -r -a names <<< "${names[*]}"
pids=()
trap 'kill "${pids[@]}" 2>/dev/null; wait' INT TERM EXIT
for n in "${names[@]}"; do
  .venv/bin/python "$n.py" &
  pids+=($!)
done
wait
