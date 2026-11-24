# 실제 배치 기록 `devoops/placement` (GPU-268)

장애가 나면 복구 컨트롤러가 Backend `GET /internal/v1/recovery/jobs/{id}`로 작업을 조회한다
(`recovery-controller/docs/backend-metadata-api.md`). 응답의 `cluster`·`active_nodes`·`placement_generation`·`observed_at`은
실제 EKS 워커의 배치여야 하는데, Backend는 관리 클러스터만 읽는다. 그래서 워커 Pod를 이미 읽고 있는
dispatcher가 관리 클러스터 Workload에 실제 배치를 남기고, Backend는 그 주석을 읽는다.

## 기록 위치·형식

관리 클러스터 `training` 네임스페이스의 Workload(소유자 = 관리 JobSet `job-{id}-g{NNNN}`), 주석 `devoops/placement`, 값은 JSON 한 줄.

```json
{"version":1,"jobset":"job-dd0d1281-g0001","generation":1,"cluster":"eks-a","nodes":["ip-10-0-1-5","ip-10-0-2-7"],"observed_at":1800000000}
```

| 필드 | 뜻 | 출처 |
|---|---|---|
| version | 형식 버전. 지금 1 | |
| jobset | 관리 JobSet 이름 | Workload ownerReference |
| generation | 실행 세대 | JobSet 이름의 `g{NNNN}` |
| cluster | 실제 실행 EKS | Workload `status.clusterName` (MultiKueue 배정). 이 워커의 Pod만 센다 |
| nodes | 참여 노드 전체, 정렬 | 그 워커에서 같은 네임스페이스·같은 JobSet 이름(`jobset.sigs.k8s.io/jobset-name`)의 Running GPU Pod `spec.nodeName` |
| observed_at | 위 Pod 목록을 실제로 읽은 시각 (Unix 초) | dispatcher 루프의 워커 Pod 조회 시각 |

Backend 매핑: `cluster` → `cluster`, `nodes` → `active_nodes`, `generation` → `placement_generation`, `observed_at` → `observed_at`.
`jobset_uid`는 이 주석에 없다. Backend가 관리 JobSet에서 직접 읽는다. Backend는 주석의 `jobset`·`generation`이
조회한 작업·세대와 같은지 확인하고, 다르거나 주석이 없거나 형식이 틀리면 200 대신 503을 준다.

## 언제 쓰고 언제 안 쓰나

쓴다 (모두 만족):
- Workload가 배정됨(`status.clusterName`), 끝나지 않음(Finished 아님), 지워지는 중 아님, 소유자가 `job-{id}-g{NNNN}` JobSet
- 그 워커 조회가 이번 루프에서 성공함
- Running GPU Pod 수 = 서로 다른 노드 수 = 요청 GPU 수 (노드당 GPU 1장·Pod당 GPU 1장 구성)

안 쓴다:
- 위 조건 중 하나라도 어긋남: Pod 일부 죽음·재시작 중, 스케줄 전, 워커 끊김. **이때 기존 주석은 그대로 둔다**
- 내용이 같고 마지막 기록 뒤 `PLACEMENT_REFRESH_SECONDS`(기본 60초)가 안 지남
- 기존 주석의 관측 시각이 같거나 더 새로움 (리더가 바뀐 직후 옛 관측으로 덮지 않음)

지우지 않는다. 장애가 나면 조건이 깨져 갱신이 멈추고, 장애 직전 마지막 배치가 남는다.
복구로 생긴 새 세대 JobSet은 새 Workload를 갖고 거기에 새로 기록된다.
기록 실패는 그 잡만 건너뛰며 지명 로직에는 영향이 없다. 추가 API 조회는 없다(용량 계산에 쓰는 Pod 목록을 같이 쓴다).
추가 권한도 없다(dispatcher는 이미 Workload patch, 워커 pods list 권한이 있음).

## 조회 전에 지우면 안 되는 것

주석은 Workload에 있고 Workload는 관리 JobSet을 따라 지워진다. 따라서 **장애 작업의 관리 JobSet·Workload는
복구 컨트롤러의 Backend 조회가 끝나기 전에 지우면 안 된다.** 현재 흐름에서 옛 JobSet을 지우는 것은 relocator이고,
이는 조회 → 등록 → 세대 증가 → RelocationRequest 뒤라서 순서가 맞다. Backend·사람·다른 정리 작업이
실패한 JobSet을 먼저 지우면 조회는 404/503이 되고 복구는 HOLD가 된다.

## 쓰기 권한 (신뢰 경계)

관리 클러스터에서 Workload를 patch할 수 있는 주체는 dispatcher·Kueue 컨트롤러·클러스터 관리자뿐이다
(Backend·relocator·job-watcher는 읽기만). 키 단위로는 막혀 있지 않으므로 Gitops에 다음 정책을 요청한다:
**`devoops/placement`를 붙이기·바꾸기·떼기는 `system:serviceaccount:platform:dispatcher`와 클러스터 관리자만.**
값이 그대로인 수정(Kueue의 Workload 갱신 등)은 통과시킨다. 기존 `scheduling-dispatch-annotations` 정책과 같은 모양으로,
대상 리소스만 `kueue.x-k8s.io` `workloads`다.

## 신선도 — 추후 반드시 검토

복구 컨트롤러는 `observed_at`이 `backend_metadata.max_age_seconds`(기본 300초, 상한 3600초)보다 오래되면 거절하고 HOLD한다.
장애가 나면 dispatcher 갱신이 멈추므로 조회 시점의 나이 ≈ 마지막 갱신 뒤 지난 시간(최대 60초) + 장애 → 조회까지 걸린 시간이다.

- 11/24 EKS 시험(rank kill -9)은 조회까지 약 50초 → 나이 최대 약 110초로 통과 범위다.
- **장애 감지가 약 4분보다 늦는 경우**(heartbeat 기반 감지, 노드 전체 사망, 네트워크 단절 등)는 기본값으로 거절된다.
- 검토할 것: 장애 유형별 감지 → 조회 시간 실측, `max_age_seconds` 상향(Gitops) 여부, 또는 "장애 직전 스냅샷"을
  관측 시각과 별도로 허용할지 계약 변경 여부. 결정 전까지 늦게 감지된 장애의 HOLD는 정상 동작으로 본다.
