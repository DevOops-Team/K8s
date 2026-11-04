#!/usr/bin/env bash
# 1. 빈 패킹: 1장 잡 여러 개 -> 한 워커를 채운 뒤 다음 워커로. 큰 잡이 들어갈 빈 워커가 남음
source "$(dirname "$0")/lib.sh"
reset

say "1장 잡 6개 제출 (3초 간격)"
expect "worker1에 4개, worker2에 2개, worker3는 비어 있음 (이름순 동률 -> worker1부터)"
for i in 1 2 3 4 5 6; do submit "bin-$i" 1 lq-light 20m; sleep 3; done

sleep 20
show
show_nodes
say "확인: CLUSTER 열이 worker1 x4, worker2 x2 인가?"
