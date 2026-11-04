# 시나리오 1~9 (실습 전용)

전제: 다른 터미널에서 `../controllers/run-local.sh` 실행 중. 각 스크립트는 시작할 때 `reset`으로 잡·요청·노드 상태를 원상복구한다.

| # | 스크립트 | 기대 결과 |
|---|---|---|
| 1 | `01-binpack.sh` | 1장 잡 6개 -> worker1 4, worker2 2, worker3 0 |
| 2 | `02-gang-mix.sh` | 1~4장 섞어서 -> 반쪽 배치 0건 |
| 3 | `03-starvation.sh` | 1장 잡이 계속 와도 4장 잡이 일정 시간 안에 입장 |
| 4 | `04-recovery-first.sh` | 대기열이 찬 상태에서 복구 잡이 먼저 입장 |
| 5 | `05-move-to-spare.sh` | 고장 노드 격리, 자리 표시 Pod 밀려나고 잡이 예비 노드에 |
| 6 | `06-evacuate.sh` | worker2 끊고 대피 -> 다른 워커로, worker2엔 새 잡 안 감 |
| 7 | `07-all-down.sh` | 워커 전부 끊김 -> 디스패처가 지명 멈춤 |
| 8 | `08-no-spare.sh` | 예비 없으면 자리 날 때까지 대기, 5번과 시각 비교 |
| 9 | `09-duplicate.sh` | 같은 requestId 두 번 -> 한 번만 실행 |

6·7번은 `docker network disconnect`를 쓴다 (sudo 없이 docker가 되는 사용자 기준).
