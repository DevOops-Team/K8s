# 장애복구 시나리오 4~11 (실습 전용)

평소 학습 스케줄링(큐 설정, 빈 패킹, 갱 배치, 큰 잡 입장 등)은 `../e2e/`로 옮겼다 (pytest, 자동 판정).
여기 남은 것은 장애복구 시나리오이고 아직 결과를 눈으로 비교한다. 번호는 예전 그대로 둔다.

전제: `../cluster/setup.sh`로 클러스터를 만들고 `./bootstrap.sh`로 큐를 깐 뒤, 다른 터미널에서 `../controllers/run-local.sh` 실행 중. 각 스크립트는 시작할 때 `reset`으로 잡·요청·노드 상태를 원상복구한다.

이름은 JobSet 템플릿 합의안을 따른다: 네임스페이스 `training`, 큐 `queue-heavy/light/recovery`, 잡 `job-<작업ID>-g<세대 4자리>`. 스크립트에서는 작업ID만 쓰고 `jsname`이 이름으로 바꾼다(`jsname train-a 2` = `job-train-a-g0002`).
`submit`은 백엔드처럼 복구 원본·펜싱 기록(kind에서는 `scheduling-local-store` 네임스페이스 ConfigMap)을 남기고, `relocate`는 복구 컨트롤러처럼 세대 +1(`GENERATION_ADVANCED`)을 한 뒤 재배치 요청을 만든다.

| # | 스크립트 | 기대 결과 |
|---|---|---|
| 4 | `04-recovery-first.sh` | 대기열이 찬 상태에서 복구 잡이 먼저 입장 |
| 5 | `05-move-to-spare.sh` | 고장 노드 격리, 자리 표시 Pod 밀려나고 잡이 예비 노드에 |
| 6 | `06-evacuate.sh` | worker2 끊고 대피 -> 다른 워커로, worker2엔 새 잡 안 감 |
| 7 | `07-all-down.sh` | 워커 전부 끊김 -> 디스패처가 지명 멈춤 |
| 8 | `08-no-spare.sh` | 예비 없으면 자리 날 때까지 대기, 5번과 시각 비교 |
| 9 | `09-duplicate.sh` | 같은 requestId 두 번 -> 한 번만 실행 |
| 10 | `10-move-to-other-cluster.sh` | 예비 소진·SDC: 의심 노드만 격리, 잡은 다른 워커로, worker2 펜싱 안 함 (6번과 비교) |
| 11 | `11-recovery-under-load.sh` | 학습 12장이 찬 상태 + Heavy 대기 잡에서 4장 잡 moveToSpare -> 복구 잡이 먼저 쿼터를 받는지 (끝 줄 OK/PROBLEM). `FIX=B`면 복구 몫 4로 비교 |

6·7번은 `docker network disconnect`를 쓴다 (sudo 없이 docker가 되는 사용자 기준).
