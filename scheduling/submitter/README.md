# 잡 투입기 (실험 부하 만들기)

시나리오 파일대로 학습 작업을 정해진 시각에 제출한다. 데이터 파트(김현수)에서 11/06 이관받음.
나눈 선: 데이터 = trace 가공 → 시나리오 파일, 쿠버네티스 = 이 투입기 (제출 경로는 백엔드 API, 큐 배정·jobset.json 저장은 백엔드).

시나리오 파일 (`example.csv`): `제출 시각(초), GPU 수, step 수, 방식[, 실행 시간(초)]`

```bash
python submit.py example.csv                 # 백엔드 POST /api/jobs (BACKEND_URL)
python submit.py example.csv --kind          # kind: 백엔드 없이 같은 규칙으로 직접 제출 (KWOK, 실행 시간 칸 사용)
python submit.py example.csv --kind --speed 10
```

클러스터 안에서 돌릴 때는 `job.yaml` (platform 네임스페이스, 실험 때만 도는 Job).

결과 `results-<시각>.csv`: 작업ID · JobSet 이름 · 큐 · 제출 시각 · 응답, 마지막 줄에 실험 시작·종료 시각.

## 아직 정할 것

- step 수·실행 시간을 백엔드에 넘길 칸: 서비스 API는 `max_steps`를 사용자에게 열지 않음 → 노찬경과 정하기 (지금은 결과 파일에만 남김)
- `experiments.started_at·ended_at` 기록 경로(DB 직접인지 API인지)와 스키마 → 김현수 답 오면 `record_experiment()`만 바꿈
- 장애 주입 시점은 이 파일에 넣지 않고 따로 (복구·모니터링과 정함)
