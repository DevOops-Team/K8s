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
| 10 | `10-move-to-other-cluster.sh` | 예비 소진·SDC: 의심 노드만 격리, 잡은 다른 워커로, worker2 펜싱 안 함 (6번과 비교) |

6·7번은 `docker network disconnect`를 쓴다 (sudo 없이 docker가 되는 사용자 기준).

## 평소 학습 자동 검증 (`normal-training.sh`)

위 1~9는 결과를 눈으로 비교하지만, 이 스크립트는 PASS/FAIL을 스스로 판정한다. 준비와 컨트롤러 실행까지 한 번에 한다.

```bash
./normal-training.sh            # 설계 기본값 (aging 720 GPU·s, 비워 두기 120s), 약 20분
FAST=1 ./normal-training.sh     # 시간 값 축소 (240 GPU·s / 60s), 약 10분
./normal-training.sh 5 7        # 골라서
```

7번은 컨트롤러 도움 없이 Kueue 설정만으로 통과해야 한다. Heavy 잡이 자기 몫 안에서 기다리는 동안 Kueue가 같은 cohort의 새 빌려 쓰기를 막기 때문이다(KEP-83). 11/05 대조군 실행(배분 조정 컨트롤러 없이)에서 big이 146s에 쿼터를 받는 걸 확인하고 Light 조이기 기능을 뺐다. 큐 설정(cohort, 선점, 큐 전략)을 바꾸면 7번을 다시 돌린다. 7번은 big 자리를 모으는 동안 논 GPU·초도 찍는다(판정 없음, 정보).

| # | 확인 | 문서 위치 |
|---|---|---|
| 1 | 큐 몫 8/4/3, Heavy StrictFIFO, Recovery lendingLimit 0 | 배분(1) |
| 2 | 3장 이상 Heavy, 5장 이상 거부 (`lib.sh`의 `queue_for`가 잡 투입기 흉내) | 입력 |
| 3 | 1장 6개 -> worker1 4, worker2 2 | 배치(1) |
| 4 | Heavy 대기 없으면 Light가 12장까지 빌려 씀 | 배분(1)·(2) |
| 5 | 앞의 4장 잡이 막히면 뒤 3장 잡도 대기 | 배분(1) |
| 6 | 4장·2장·1장 잡이 대기 시간 x GPU 수 순서로 aged | 배분(3) |
| 7 | 작은 잡이 계속 와도 4장 잡 입장 (big 대기 중 Light가 몫 4 넘게 새로 못 받음) | 배분(2)·배치(1) |
