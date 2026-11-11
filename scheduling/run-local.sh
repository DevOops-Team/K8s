#!/usr/bin/env bash
# 컨트롤러 2개(dispatcher, relocator)를 노트북(우분투 VM)에서 바로 실행. Ctrl-C로 둘 다 종료.
#   ./run-local.sh                 둘 다
#   ./run-local.sh dispatcher      하나만
# 이미지 빌드 없이 코드를 고치고 바로 다시 돌리는 용도. 클러스터 안 배포는 README 참고.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
  .venv/bin/pip install -q -r common/requirements.txt
fi

export KUBE_CONTEXT=${KUBE_CONTEXT:-kind-manager}
export WORKER_CONTEXTS=${WORKER_CONTEXTS:-worker1=kind-worker1,worker2=kind-worker2,worker3=kind-worker3}
export PYTHONUNBUFFERED=1


names=("${@:-dispatcher relocator}")
read -r -a names <<< "${names[*]}"
pids=()
trap 'kill "${pids[@]}" 2>/dev/null; wait' INT TERM EXIT
for n in "${names[@]}"; do
  .venv/bin/python "$n/$n.py" &
  pids+=($!)
done
wait
