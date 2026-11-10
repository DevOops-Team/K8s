#!/usr/bin/env bash
# 11. 학습 큐가 찬 상태에서 4장 잡 복구 (설계 문제 1번 = 정본 R10 확인용)
#
# 4번은 1장 복구 잡이라 복구 몫 안에 들어가서 이 경우를 못 본다.
# 복구 몫이 3이면 4장 복구 잡은 학습 몫 1을 빌려야 한다. Kueue는 한 주기 안에서 "안 빌리는 잡"을 먼저 보고
# (KEP-83 정렬: 안 빌림 -> 우선순위 -> 생성 시각), 그런 잡이 기다리면 cohort 빌려 쓰기를 막는다.
# 그래서 옛 잡이 내려놓은 Heavy 몫 4를 Heavy 대기 잡 wait-c가 먼저 가져가고 복구 잡은 기다릴 수 있다.
# 11/10에 관리 cq-recovery를 잡 최대 크기 4로 올렸다 (정본 R10). 지금 설정에서는 OK가 나와야 한다.
#
# 판정 (끝에 한 줄):
#   OK      복구 잡이 wait-c보다 먼저(또는 wait-c 없이) 관리 클러스터 쿼터를 받음
#   PROBLEM wait-c가 먼저 쿼터를 받고, 복구 잡은 WAIT초 안에 못 받음 -> 문제 1 재현
#   UNCLEAR 그 밖 (로그 확인)
#   WAIT=180 ./11-recovery-under-load.sh   지금 설정 그대로 (복구 몫 4, OK 기대)
#   BEFORE=1 ./11-recovery-under-load.sh   예전 설정: 관리 cq-recovery 몫 4 -> 3으로 잠깐 바꿔 돌리고 끝나면 4로 되돌림 (PROBLEM 기대)
source "$(dirname "$0")/lib.sh"
reset
WAIT=${WAIT:-180}

set_recovery_quota() {
  kubectl --context $MGR patch clusterqueue cq-recovery --type json     -p "[{\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$1\"}]" >/dev/null
  echo "  관리 cq-recovery nominalQuota = $1"
}
if [ -n "${BEFORE:-}" ]; then
  say "예전 설정으로 비교: 복구 몫 3 (4장 복구 잡이 학습 몫 1을 빌려야 함)"
  set_recovery_quota 3
  trap 'set_recovery_quota 4' EXIT
fi

# JobSet 이름 -> 관리 클러스터 Workload의 QuotaReserved=True 시각 (없으면 빈칸)
quota_at() {
  kubectl --context $MGR -n $NS get workloads -o json 2>/dev/null | python3 -c "
import json,sys
for w in json.load(sys.stdin)['items']:
    if any(o['name']=='$1' for o in w['metadata'].get('ownerReferences',[])):
        for c in w.get('status',{}).get('conditions',[]):
            if c['type']=='QuotaReserved' and c['status']=='True': print(c['lastTransitionTime'])
        break"
}

show_cq() {
  echo "--- [$(ts)] 관리 클러스터 CQ (예약 GPU / 대기 Workload)"
  kubectl --context $MGR get clusterqueues -o json | python3 -c "
import json,sys
for cq in json.load(sys.stdin)['items']:
    st=cq.get('status',{})
    used=sum(int(r['total']) for f in st.get('flavorsReservation',[]) for r in f['resources'] if r['name']=='nvidia.com/gpu')
    print(f\"  {cq['metadata']['name']:12} 예약 {used:>2}  대기 {st.get('pendingWorkloads',0)}\")"
}

say "학습 12장 채우기: Heavy 8 (train-a 4장 worker1, train-b 4장 worker2) + Light 4 (lite-1·2 각 2장 worker3)"
kubectl --context kind-worker1 apply -f "$ROOT/examples/spare/placeholder.yaml" >/dev/null
submit train-a 4 queue-heavy 30m '"devoops/target-cluster": "worker1"'
submit train-b 4 queue-heavy 30m '"devoops/target-cluster": "worker2"'
submit lite-1 2 queue-light 30m '"devoops/target-cluster": "worker3"'
submit lite-2 2 queue-light 30m '"devoops/target-cluster": "worker3"'
for j in train-a train-b lite-1 lite-2; do wait_admitted $j 120; done

say "Heavy 대기 잡 wait-c (4장, 자기 몫 8 안에 들어가는 크기) 제출"
expect "Heavy 예약 8이라 wait-c는 쿼터 없이 대기"
submit wait-c 4 queue-heavy 30m
sleep 20
show_cq
[ -n "$(quota_at "$(jsname wait-c)")" ] && echo "  주의: wait-c가 벌써 쿼터를 받음 -> 채우기가 안 됨, 결과 UNCLEAR일 수 있음"

bad=$(kubectl --context kind-worker1 -n $NS get pods -l jobset.sigs.k8s.io/jobset-name=$(jsname train-a) \
      -o jsonpath='{.items[0].spec.nodeName}')
say "train-a의 $bad 고장 가정 -> moveToSpare"
expect "문제가 없다면: job-train-a-g0002(복구)가 wait-c보다 먼저 쿼터를 받고 worker1(3대 + 예비)에 입장"
relocate rr-load evt-011 moveToSpare train-a "$bad"

new=$(jsname train-a 2)
end=$(( $(date +%s) + WAIT ))
while [ "$(date +%s)" -lt "$end" ]; do
  r=$(quota_at "$new"); c=$(quota_at "$(jsname wait-c)")
  [ -n "$r" ] && break
  sleep 5
done
show_cq
show_rr
rr_times rr-load
r=$(quota_at "$new"); c=$(quota_at "$(jsname wait-c)")
echo "  쿼터 받은 시각: 복구 $new=${r:-없음}  wait-c=${c:-없음}"
echo "  wait-c 워커: $(cluster_of wait-c)  (쿼터만 받고 워커가 비어 있으면 2번 문제도 같이 보이는 것)"

if [ -n "$r" ] && { [ -z "$c" ] || [[ "$r" < "$c" ]] || [ "$r" = "$c" ]; }; then
  echo "결과: OK - 복구 잡이 먼저 쿼터를 받음"
elif [ -n "$c" ] && [ -z "$r" ]; then
  echo "결과: PROBLEM - wait-c가 옛 잡 몫을 가져가고 복구 잡은 ${WAIT}s 안에 쿼터를 못 받음 (설계 문제 1 재현)"
elif [ -n "$c" ] && [ -n "$r" ]; then
  echo "결과: PROBLEM - wait-c가 먼저 쿼터를 받고 복구 잡은 늦게 받음 (설계 문제 1 재현)"
else
  echo "결과: UNCLEAR - 둘 다 쿼터를 못 받음 (실행기·펜싱 기록 단계 확인)"
fi
