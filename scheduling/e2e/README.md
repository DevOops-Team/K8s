# 평소 학습 스케줄링 e2e

kind 로컬 클러스터에서 평소 학습 스케줄링 규칙을 자동으로 판정한다. 장애복구 시나리오는 아직 `../scenarios/`(bash)에 있다.

## 준비와 실행

```bash
../cluster/setup.sh          # 클러스터 (처음 한 번)
../scenarios/bootstrap.sh    # 큐·CRD·KWOK 노드
./run.sh normal              # 검사 1~7, 약 20분
./run.sh compare             # 비교 실험, 약 20분
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
  reset.py            초기화
  controllers.py      컨트롤러를 설정대로 띄움/내림, 비워 두기 로그 찾기
  health.py           감시 (INVALID 근거)
  situations.py       상황: Light 채우기, 워커 조각내기, 잡 스트림
  measure.py          큰 잡 쿼터·입장 시각, 논 GPU·s
  experiment.py       비교 실험의 팔 하나 실행
  verdict.py          판정 예외, 전제 조건, 항목별 체크
tests/normal/         검사 1~7, 케이스당 파일 하나
tests/compare/        비교 실험 (큐 분리)
```

## 케이스 쓰는 법

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

기본 = dispatcher·relocator. 배분 조정 컨트롤러(aging)와 시간 기준 비워 두기는 11/09에 뺐고, 11/10에 디스패처 찜을 넣었다 (`../controllers/README.md`의 "뺀 것").
