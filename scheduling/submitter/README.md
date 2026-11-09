# 잡 투입기 (실험 부하 만들기)

시나리오 파일대로 학습 작업을 정해진 시각에 제출한다. 데이터 파트(김현수)에서 11/06 이관받음.
나눈 선: 데이터 = trace 가공 → 시나리오 파일, 쿠버네티스 = 이 투입기 (제출 경로는 백엔드 API, 큐 배정·jobset.json 저장은 백엔드).

시나리오 파일 (`example.csv`): `제출 시각(초), GPU 수, step 수, 방식[, 실행 시간(초)]`

```bash
python submit.py example.csv                 # 백엔드 POST /api/jobs (BACKEND_URL)
python submit.py example.csv --kind          # kind: 백엔드 없이 같은 규칙으로 직접 제출 (KWOK, 실행 시간 칸 사용)
python submit.py example.csv --kind --speed 10
```

클러스터 안에서 돌릴 때는 `job.yaml` (platform 네임스페이스, 실험 때만 도는 Job + training JobSet 읽기 권한).

결과 `results-<시각>.csv`: 작업ID · JobSet 이름 · 큐 · 제출 시각 · 응답, 앞뒤 줄에 실험 시작·종료 사건.

## 실험 시작·종료 기록 (김현수 11/09 합의)

DB에 직접 쓰지 않고 Kafka `job.lifecycle`(기본 `172.30.0.40:9092`, `KAFKA_BOOTSTRAP`)에 사건만 올린다. DB의 `experiments.started_at·ended_at`은 데이터 수집기가 채운다.

- 첫 제출 전 `experiment.started`: payload = `exp_id` + `name`·`scenario`·`policy`·`fault_type`·`spare_enabled`·`checkpoint_every_steps` (안 준 칸은 뺌)
- 모든 잡이 끝난 뒤 `experiment.ended`: payload = `exp_id`만
- 봉투: `event_id`(uuid) · `schema_version` 1 · `type` · `occurred_at`(UTC 밀리초) · `source` `submitter@onprem`. 키 = `exp_id`, acks=all
- "다 끝남" = 관리 클러스터 `training`에서 같은 `devoops/job-id`의 JobSet이 하나 이상 있고 전부 Completed·Failed (재배치로 세대가 바뀌어도 됨)
- `--wait-timeout`(기본 6시간) 안에 안 끝나면 `experiment.ended`를 보내지 않고 종료 코드 1. `--no-wait`도 ended를 안 보냄
- Kafka 전달 확인을 못 받으면 바로 멈춤 (기록 없이 실험이 도는 걸 막으려고). 로컬은 `--no-kafka`

```bash
python submit.py scenario.csv --exp-id exp-0001 --policy ours --fault-type xid79 --spare --checkpoint-every-steps 100
```

필요 패키지: `confluent-kafka`(Kafka), `kubernetes`(JobSet 조회·`--kind`). `job.yaml`은 시작할 때 /tmp에 설치함.

## 아직 정할 것

- step 수·실행 시간을 백엔드에 넘길 칸: 서비스 API는 `max_steps`를 사용자에게 열지 않음 → 노찬경과 정하기 (지금은 결과 파일에만 남김)
- `exp_id`를 누가 매기는지 (지금은 `--exp-id`, 기본 `exp-<prefix>`) → 수집기가 started 사건으로 행을 만드는지 김현수와 확인
- 장애 주입 시점은 이 파일에 넣지 않고 따로 (복구·모니터링과 정함)
