# 잡 투입기 (실험 부하 만들기)

시나리오 파일대로 학습 작업을 정해진 시각에 제출한다. 데이터 파트(김현수)에서 11/06 이관받음.
나눈 선: 데이터 = trace 가공 → 시나리오 파일, 쿠버네티스 = 이 투입기 (제출 경로는 백엔드 API, 큐 배정·jobset.json 저장은 백엔드).

시나리오 파일 (`example.csv`): `제출 시각(초), GPU 수, step 수, 방식[, 실행 시간(초)]`

```bash
python submit.py example.csv                 # 백엔드 POST /api/jobs (BACKEND_URL)
python submit.py example.csv --kind          # kind: 백엔드 없이 같은 규칙으로 직접 제출 (KWOK, 실행 시간 칸 사용)
python submit.py example.csv --kind --speed 10
```

결과 `results-<시각>.csv`: 첫 줄에 이미지·exp_id·시나리오, 작업ID · JobSet 이름 · 큐 · 제출 시각 · 응답, 앞뒤 줄에 실험 시작·종료 사건.

## 실험마다 Job으로 띄우기 (`launch.py`)

투입기는 상시 Pod가 아니라 실험 하나당 관리 클러스터 `platform`의 Job 하나로 돈다. `launch.py`가 한 번에 띄운다.

```bash
python launch.py scenario.csv --exp-id exp-0001 --image <투입기 이미지> --context <관리 클러스터> \
  -- --policy ours --fault-type xid79 --spare --checkpoint-every-steps 100
kubectl -n platform logs -f job/submitter-exp-0001   # 진행 상황, 끝나면 결과 CSV 전체가 로그에 찍힘
```

- 시나리오 CSV → ConfigMap `submitter-<exp_id>`, Job `submitter-<exp_id>`. ConfigMap은 Job에 딸려 Job이 지워질 때(끝나고 24시간) 같이 지워진다.
- 같은 exp_id의 ConfigMap·Job이 이미 있으면 띄우지 않고 종료 코드 1 (같은 실험이 두 번 돌면 측정이 섞임). 다시 돌리려면 새 exp_id를 쓴다.
- Job은 한 번만 돈다(`backoffLimit: 0`). 실패하면 그 실험은 버리고 새 exp_id로 다시 띄운다.
- `--dry-run`: 만들 ConfigMap·Job만 출력.

**실행 인터페이스 (실험 자동화 스크립트가 넘길 값)**

| 인자 | 필수 | 뜻 |
| --- | --- | --- |
| 시나리오 CSV 경로 | O | 위 형식 |
| `--exp-id` | O | 실험 ID. `experiment.started`·`ended`와 모든 `POST /api/jobs`에 같은 값이 들어간다. 소문자·숫자·`-`, 53자 이하 |
| `--image` | O | 투입기 이미지(태그 또는 digest). 결과 첫 줄에 남는다. `SUBMITTER_IMAGE`로도 줌 |
| `--context` | | 관리 클러스터 kubeconfig context (기본 현재 context) |
| `--backend-url` `--kafka-bootstrap` `--kafka-topic` `--model` `--dataset-id` `--dataset-version` | | 투입기 환경 변수. 안 주면 같은 이름의 환경 변수, 그것도 없으면 `submit.py` 기본값 |
| `--` 뒤 | | `submit.py`에 그대로: `--policy` `--fault-type` `--spare`/`--no-spare` `--checkpoint-every-steps` `--wait-timeout` `--speed` `--name` `--prefix` |

종료 코드: 0 = 띄움, 1 = 같은 exp_id가 이미 있음. 실험이 끝났는지는 Job 상태(`Complete`/`Failed`)로 본다. 투입기는 모든 잡이 끝나 `experiment.ended`를 보낸 뒤에 성공 종료하고, 시간 초과 땐 ended 없이 실패 종료한다.

**클러스터에 있어야 하는 것 (Gitops)**: ServiceAccount `submitter`(platform), `training`의 JobSet `get`·`list` Role·RoleBinding. 실행하는 사람에게는 `platform`의 Job·ConfigMap 생성·삭제·patch 권한. 이미지 빌드는 `Dockerfile`(이 폴더 기준).

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

필요 패키지: `requirements.txt` (`confluent-kafka` Kafka, `kubernetes` JobSet 조회·`--kind`·`launch.py`). 이미지에는 미리 들어 있다.

## 실험과 작업 연결 (GPU-150)

실험 ID는 투입기 `--exp-id`로 지정하며, 생략하면 `exp-<prefix>`를 쓴다. 데이터 수집기가 `experiment.started`를 받아 `experiments` 행을 만든다 (GPU-77 확인).

- 백엔드 모드는 모든 `POST /api/jobs` 요청 본문에 같은 `exp_id`를 넣는다. `experiment.started/ended`에도 이 값을 쓴다.
- Backend는 `exp_id`를 받아 `job.submitted` payload와 JobSet 주석 `devoops/exp-id`에 전달해야 한다 (노찬경 연계). 수집기는 `job.submitted.payload.exp_id`로 `job_runs.exp_id`를 채운다. Backend가 필드를 받지 않거나 버리면 투입기 수정만으로 실험별 결과를 연결할 수 없다.
- `--kind` 직접 제출도 JobSet과 복구 원본의 `devoops/exp-id` 주석에 같은 값을 넣는다. 이 경로는 Backend를 거치지 않아 `job.submitted`를 발행하지 않으므로 DB 연결을 검증하는 용도로 쓰지 않는다.

로컬 검증: 저장소 루트에서 `python -B -m unittest discover -s scheduling/submitter -p 'test_*.py'` (`launch.py` 포함).
실제 연계 검증은 Backend 반영 뒤 작은 시나리오로 `job.submitted.payload.exp_id`, JobSet 주석, `job_runs.exp_id`가 같은지 확인한다.

## 아직 정할 것

- step 수·실행 시간을 백엔드에 넘길 칸: 서비스 API는 `max_steps`를 사용자에게 열지 않음 → 노찬경과 정하기 (지금은 결과 파일에만 남김)
- 장애 주입 시점은 이 파일에 넣지 않고 따로 (복구·모니터링과 정함)
