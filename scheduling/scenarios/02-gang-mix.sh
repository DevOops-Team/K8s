#!/usr/bin/env bash
# 2. 갱 스케줄링: 1~4장 잡을 섞어 넣었을 때 반쪽만 배치된 잡 0건
#    1차 방어 = 디스패처가 노드 단위로 다 들어가는 곳만 지명
#    2차 방어 = 워커 Kueue waitForPodsReady (반쪽이면 2분 뒤 통째로 내림)
source "$(dirname "$0")/lib.sh"
reset

say "4,3,2,1,4,2,1장 잡 제출 (합 17장 > 일반 잡이 쓸 수 있는 12장)"
expect "들어간 잡은 Pod가 전부 Running, 못 들어간 잡은 Pod 0개 (관리 클러스터에서 대기)"
i=0
for g in 4 3 2 1 4 2 1; do i=$((i+1)); submit "mix-$i" "$g" lq-heavy 20m; done

check_partial() {
  echo "--- [$(ts)] 반쪽 배치 검사"
  local bad=0
  for w in "${WORKERS[@]}"; do
    while read -r js running total; do
      [ -z "$js" ] && continue
      if [ "$running" -ne "$total" ]; then echo "  반쪽: $w $js Running $running/$total"; bad=1; fi
    done < <(kubectl --context kind-$w -n $NS get pods -o json 2>/dev/null | python3 -c "
import json,sys,collections
r=collections.Counter(); t=collections.Counter()
for p in json.load(sys.stdin)['items']:
    js=p['metadata']['labels'].get('jobset.sigs.k8s.io/jobset-name')
    if not js: continue
    t[js]+=1; r[js]+= p['status'].get('phase')=='Running'
for js in t: print(js, r[js], t[js])")
  done
  [ $bad -eq 0 ] && echo "  반쪽 0건"
}

watch_for 120 20 check_partial
show
show_nodes
