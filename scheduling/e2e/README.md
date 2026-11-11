# 스케줄링 e2e

kind 로컬 클러스터에서 평소 학습·장애복구 스케줄링 규칙(정본 「학습 스케줄링 정본」 3·4절)을 자동으로 판정한다.
11/10에 bash 장애복구 시나리오(`scenarios/04~11`)를 정본 기준으로 다시 써서 `tests/recovery/`로 옮기고 bash는 지웠다.

## 준비와 실행

```bash
../cluster/setup.sh          # 클러스터 (처음 한 번)
./bootstrap.sh               # 큐·CRD·입장 정책·KWOK 노드
./run.sh normal              # 평소 학습 01~07, 약 20분
./run.sh recovery            # 장애복구 04~13, 약 30분
./run.sh compare             # 비교 실험 (큐 분리, 복구 몫 3 vs 4), 약 30분
./run.sh -k gang             # 골라서 (pytest 인자 그대로)
```

컨트롤러는 케이스가 알아서 띄운다. `controllers/run-local.sh`를 따로 켜 두면 Lease를 다투니 끄고 돌린다.
결과는 `logs/<시각>/`의 `results.json`(항목별 체크·측정값), `summary.md`, `pytest.log`, `controllers-*.log`에 남는다.

## 판정

| 판정 | 뜻 | 할 일 |
|---|---|---|
| PASS | 기대대로 됨 | |
| FAIL | 상황은 제대로 만들어졌는데 결과가 기대와 다름 | 스케줄링 동작이나 설정을 본다 |
| INCONCLUSIVE | 검사하려던 상황 자체가 안 만들어짐 (전제 조건 실패) | 케이스의 상황 만들기나 타이밍을 고친다 |
| INVALID | 인프라 문제로 결과를 믿을 수 없음 (웹훅 timeout, 컨트롤러 종료, API 서버 응답 없음) | 클러스터 상태를 보고 다시 돌린다 |

비교 실험(compare)은 가설을 판정한다. PASS는 가설이 맞음, FAIL은 가설이 틀림이다.
각 실험은 먼저 대조군(B)에서 문제가 되는 상황이 실제로 생겼는지 확인하고, 안 생겼으면 INCONCLUSIVE로 끝낸다.

## 구조

```
run.sh                실행기 (venv 준비, 로그 폴더, 종료 코드)
conftest.py           케이스 준비(컨트롤러 설정 -> 초기화 -> 감시 시작), 판정 변환, 결과 기록
harness/
  env.py              접속·이름·시간 값. 이름은 controllers/keys.py와 같은 것을 씀
  cluster.py          조회: Workload(작업ID로), ClusterQueue 사용량, 우선순위, 워커 Pod
  jobs.py             제출 (JobSet 템플릿 합의안 모양 + 복구 원본·펜싱 ConfigMap), 잡 투입기 큐 규칙
  reset.py            초기화 (끊어 둔 워커 다시 연결 포함)
  network.py          워커 네트워크 끊기·다시 붙이기 (docker network, 감시에서 뺌)
  recovery.py         복구 컨트롤러 흉내(세대 +1 -> RR), RR·펜싱 기록·워커 조회, 이중 실행 감시, 재제출 공통 판정
  controllers.py      컨트롤러를 설정대로 띄움/내림, 비워 두기 로그 찾기
  health.py           감시 (INVALID 근거)
  situations.py       상황: Light 채우기, 워커 조각내기, 잡 스트림
  measure.py          큰 잡 쿼터·입장 시각, 논 GPU·s
  experiment.py       비교 실험의 팔 하나 실행
  verdict.py          판정 예외, 전제 조건, 항목별 체크
tests/normal/         평소 학습 01~07, 케이스당 파일 하나
tests/recovery/       장애복구 04~13
tests/compare/        비교 실험 (큐 분리, 복구 몫)
```

## 케이스 쓰는 법

### 후속 비교 실험: 찜 vs 엄격한 FIFO (2026-11-11 기록)

상태: 테스트 계획 기록. 비교 모드 구현·실험 실행은 아직 하지 않았다. 현재 찜이 최선이라는 결론은 보류한다.

목적은 큰 잡의 굶주림을 줄이는 효과와, 이를 위해 발생하는 GPU 유휴 시간·작은 잡의 지연을 비교하는 것이다.
기존 queue_split 비교는 큐 구성이 달라지므로 이 비교의 결과로 대신하지 않는다.

- A: 현재 찜. 관리 쿼터를 확보했지만 배치할 워커가 없는 잡이 워커 하나를 예약하고 뒤 순서 학습 잡의 해당 워커 배치를 막는다.
- B: 디스패처의 엄격한 FIFO. 관리 쿼터를 확보한 일반 학습 잡을 생성 시각 순으로 처리하고, 앞 잡이 배치되지 못하면 뒤 학습 잡의 신규 지명을 모든 워커에서 중단한다. 기존 실행·지명은 유지한다.
- 이 실험의 FIFO는 관리 ClusterQueue의 StrictFIFO 설정과 구분한다. 관리 큐 구성·쿼터·우선순위는 A/B에서 동일하게 두고, 관리 쿼터 확보 이후 디스패처 정책만 비교한다. 쿼터를 못 받은 잡까지 포함하는 전역 FIFO는 별도 실험이다.
- 복구 우선 처리와 재배치 중 자리 보호는 두 모드에서 동일하게 유지한다. 일반 학습 잡의 찜과 복구 자리 보호를 함께 끄지 않는다.

조건: 워커 3개, 워커당 학습 GPU 4 + 예비 1, 잡 크기 1~4, 선점 없음. 동일한 잡 목록·제출 간격·실행 시간·초기 배치로 양쪽을 실행하고 초기화 후 여러 차례 반복한다. 실행 시간 정보는 시나리오 재현에만 사용하고 디스패처에는 제공하지 않는다.

| 상황 | 확인할 것 |
|---|---|
| 4-GPU 잡이 쿼터를 확보했으나 빈 GPU가 워커별 2/1/1로 흩어지고, 뒤에 1-GPU 잡이 계속 도착 | 큰 잡 입장과 작은 잡 지연, 다른 워커 활용 차이 |
| 찜 후보에는 오래 도는 잡이 남고 다른 워커는 먼저 비는 상황 | 예약 유지에 따른 비용, 다른 워커에 먼저 배치 가능한지 |
| 1~4-GPU 잡 혼합 및 큰 잡 여러 개 대기 | 반복적인 밀림과 작은 잡의 긴 대기가 생기는지 |
| 조각화가 없고 부하가 낮은 상황 | 불필요한 대기·유휴가 생기는지 |

각 잡에 제출, 관리 QuotaReserved, 워커 지명, 워커 입장, 전체 학습 Pod Running 시각을 따로 기록한다.
관리 쿼터 전 대기 / 쿼터 후 지명 대기 / 지명 후 워커 입장 대기 / 입장 후 Pod 실행 대기를 분리하고, 실제 학습 시작 신호가 있으면 별도로 기록한다.
크기별 대기 시간(중앙값·p95·최대), 제한 시간 내 미입장 수, 완료 잡 수, GPU 사용률, 정책 때문에 비워 둔 GPU·초를 비교한다. 정책 유휴량은 단순 빈 GPU 전체와 구분한다.

전제인 쿼터 확보·조각화·후속 잡 도착이 재현되지 않으면 INCONCLUSIVE, 인프라 장애는 INVALID로 기록한다.
관측 종료까지 미입장한 잡은 대기 시간 통계에서 조용히 제외하지 말고 별도 표시한다. 유한한 실험으로 굶주림이 영원히 없다는 보장은 하지 않는다.
결과에는 큰 잡 대기 감소와 작은 잡 지연·유휴 비용을 함께 남기고, 정책 선택 근거와 남은 한계를 기록한다.

```python
pytestmark = [pytest.mark.normal, pytest.mark.doc("배분(1)"),
              pytest.mark.controllers("dispatcher", RENOMINATE_SECONDS=30)]   # 없으면 dispatcher·relocator 기본값

def test_something(kube, check, info):
    """한 줄 설명 (결과 표에 그대로 나감)"""
    fill_light(kube, 9)                                   # 상황 (안 만들어지면 INCONCLUSIVE)
    precondition(조건, "상황이 안 만들어진 이유")             # 케이스 고유의 전제 조건
    submit_auto(kube, "h4", 4, "20m")                     # 제출 실패가 인프라 탓이면 INVALID
    check.eq("h4 대기", C.wl(kube, "h4").quota, False)    # 판정 항목. 하나라도 틀리면 FAIL
    info["측정"] = "..."                                  # 판정과 무관한 기록
```

## 케이스 목록

| 케이스 | 확인하는 것 | 컨트롤러 |
|---|---|---|
| normal/01 queue_config | 몫 8/4/4, StrictFIFO, borrowingLimit 없음, lendingLimit 0, cohort, 선점 안 함 | 상관없음 |
| normal/02 routing | 1·2장 Light, 3·4장 Heavy, 5장 거부, 3장 잡이 cq-heavy로 | 기본 |
| normal/03 binpack | 빈 워커에 1장 6개 -> worker1 4, worker2 2 | 기본 |
| normal/04 light_borrow | Heavy 대기 없으면 Light가 12장까지 | 기본 |
| normal/05 heavy_fifo | 4장 잡이 막히면 뒤의 3장 잡도 대기 | 기본 |
| normal/06 gang | 1~4장 잡을 섞어 넣어도 반쪽 배치 0건 | 기본 |
| normal/07 big_job_admission | 작은 잡이 계속 와도 큰 잡 입장 (KEP-83) | 기본 |
| compare/queue_split | 큐 분리 vs 큐 1개: 큰 잡 쿼터 시각, 분리했을 때 쿼터 뒤 워커 배치까지 (찜) | 기본 |
| compare/recovery_quota | 관리 cq-recovery 몫 3 vs 4: 학습 12장 + Heavy 대기에서 4장 복구 잡이 쿼터를 받는지 (R10) | 기본 |

### 장애복구 (`tests/recovery/`, 정본 4절 · I1·I2·I4·I6·I7)

복구 컨트롤러 몫(펜싱 기록 세대 +1 -> RR 생성)은 하네스가 흉내 내고, relocator·디스패처는 진짜를 띄운다.
재제출이 있는 케이스는 공통으로 재제출본(queue-recovery, recovery 우선순위, recovery-id, 예비 toleration, target/exclude),
다른 세대 JobSet 없음, 펜싱 기록 NEW_RUN_SUBMITTED·new_jobset_name·uid, OLD_RUN_STOPPED 뒤 재제출(I6)을 본다.

| 케이스 | 확인하는 것 | 규칙 |
|---|---|---|
| 04 recovery_first | 학습 12장 + 대기 2개에서 restartInPlace -> 복구 잡(1000, cq-recovery)이 원래 클러스터에 먼저, 대기 학습 잡은 그 클러스터를 못 가져감 | R1·R2·3.4 |
| 05 move_to_spare | 고장 노드 cordon+faulty, 새 세대가 같은 워커 학습 3 + 예비 1에, 자리 표시 Pod 밀려남, 펜싱 없음, 두 세대 동시 Running 없음 | R8·I1·I4 |
| 06 evacuate | 워커 끊김 + evacuateCluster -> 펜싱, 증거 없으면 WaitingForStop(재제출 없음), 사람 확인 뒤 다른 워커로, 새 잡도 끊긴 워커로 안 감 | R8·R9·I6·I7 |
| 07 all_down | 워커 전부 끊김 -> 지명 없음, 다시 붙으면 입장 | 3.4 |
| 08 no_spare | 예비 없이 moveToSpare -> Pod 0개로 대기(반쪽 배치 없음), 예비 확보 뒤 입장 | R5·R8·I2 |
| 09 duplicate | 같은 requestId 두 번 -> Done 하나 + Duplicate, 새 세대 하나 | R7·I1 |
| 10 move_to_other_cluster | 의심 노드만 격리, 펜싱 없음, 새 세대는 다른 워커, 원래 워커는 새 잡 받음 | R8·I7 |
| 11 recovery_under_load | 학습 12장 + Heavy 대기에서 4장 moveToSpare -> 복구 잡이 자기 몫으로 쿼터·원래 워커 입장 | R10 |
| 12 fencing_gate | 세대 +1 전 RR은 Pending, 아무것도 안 함 -> 세대 +1 뒤 진행 | R7·I6 |
| 13 admission_policy | 권한 없는 사용자는 복구 큐·우선순위·디스패처 어노테이션·RR 못 만듦, 학습 잡은 관리자도 예비 toleration 못 닮 (dry-run) | 3.1·I4 |

옛 bash 시나리오와 다른 점: 04는 queue-recovery 잡을 손으로 넣던 것을 restartInPlace 재제출로 바꿈(예비 taint 때문에 손으로 넣은 잡은 예비 노드에 못 감).
11은 "복구 잡이 wait-c보다 먼저 쿼터"를 기대하던 것을 "복구 몫으로 쿼터·원래 워커 입장"으로 바꿈(풀린 Heavy 몫은 옛 잡을 내리는 순간 wait-c가 먼저 받는 게 정상).
06에서 다시 연결한 뒤 끊긴 워커의 옛 복사본이 치워지는지는 정본에 기대가 없어 판정하지 않고 기록만 한다.

기본 = dispatcher·relocator. 배분 조정 컨트롤러(aging)와 시간 기준 비워 두기는 11/09에 뺐고, 11/10에 디스패처 찜을 넣었다 (`../controllers/README.md`의 "뺀 것").
