# 실제 배치 기록 `devoops/placement` (GPU-268·287)

복구 컨트롤러는 장애를 어느 작업·세대·노드에 맞출지 알려면 작업이 지금 실제로 어느 EKS·노드에서 도는지 알아야 한다.
관리 클러스터만 보는 쪽은 이 정보를 모르므로, 워커 Pod를 이미 읽고 있는 dispatcher가 관리 클러스터 Workload에
실제 배치를 남긴다. Data 파트의 job-watcher가 이 주석을 읽어 `job.placement` 사건으로 발행하고,
복구 컨트롤러가 그 사건으로 `job#<id>`의 배치를 등록한다(`recovery-controller/integrations/job_registration.py`).

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
| observed_at | 이 배치를 처음 관측한 시각 (Unix 초) | 그 배치가 기록된 루프의 워커 Pod 조회 시각 |

`job.placement` 매핑: `cluster` → `placement_cluster`, `nodes` → `active_nodes`, `generation` → `placement_generation`,
`observed_at` → `observed_at`. `jobset_uid`는 이 주석에 없다. job-watcher가 관리 JobSet `metadata.uid`에서 직접 읽는다.

## 언제 쓰고 언제 안 쓰나

쓴다 (모두 만족):
- Workload가 배정됨(`status.clusterName`), 끝나지 않음(Finished 아님), 지워지는 중 아님, 소유자가 `job-{id}-g{NNNN}` JobSet
- 그 워커 조회가 이번 루프에서 성공함
- Running GPU Pod 수 = 서로 다른 노드 수 = 요청 GPU 수 (노드당 GPU 1장·Pod당 GPU 1장 구성)
- 기존 주석이 없거나 깨졌거나, `jobset`·`generation`·`cluster`·`nodes` 중 하나라도 다름

안 쓴다:
- 위 조건 중 하나라도 어긋남: Pod 일부 죽음·재시작 중, 스케줄 전, 워커 끊김. **이때 기존 주석은 그대로 둔다**
- 내용이 같음. 시간이 지나도 관측 시각만 바꾸는 갱신은 하지 않는다
- 내용은 다르지만 기존 주석의 관측 시각이 같거나 더 새로움 (리더가 바뀐 직후 옛 관측으로 덮지 않음)

같은 내용을 다시 쓰지 않는 이유: Workload를 patch할 때마다 Kueue MultiKueue가 admitted Event를 다시 남기고,
그 Event가 실행 기록에 중복 줄로 쌓인다. 따라서 patch는 배치가 바뀔 때 한 번만 한다.

지우지 않는다. 장애가 나면 조건이 깨져 기록이 멈추고, 장애 직전 마지막 배치가 남는다.
복구로 생긴 새 세대 JobSet은 새 Workload를 갖고 거기에 새로 기록된다.
기록 실패는 그 잡만 건너뛰며 다음 루프에서 다시 시도한다. 지명 로직에는 영향이 없다.
추가 API 조회는 없다(용량 계산에 쓰는 Pod 목록을 같이 쓴다).
추가 권한도 없다(dispatcher는 이미 Workload patch, 워커 pods list 권한이 있음).

## 관측 시각의 나이

복구 컨트롤러는 `job.placement`를 받을 때 한 번 `받은 시각 - observed_at`이 `max_age_seconds`를 넘는지 본다.
장애 판정 때마다 다시 비교하지 않는다. job-watcher는 클러스터·노드가 바뀔 때 사건을 보내고, dispatcher도
같은 때 주석을 새로 쓰므로 사건이 바로 발행되면 나이는 루프 간격(`INTERVAL_SECONDS`) 수준이다.

주석 값이 바뀌지 않으면 `observed_at`도 그대로라, 나중에 같은 주석을 다시 읽어 발행하면 나이가 크게 잡힌다.
job-watcher가 재시작 뒤 다시 발행하는 경우 등의 처리는 job-watcher 쪽 동작을 따른다.

## 지우기 전에 전달돼야 하는 것

주석은 Workload에 있고 Workload는 관리 JobSet을 따라 지워진다. 따라서 **장애 작업의 관리 JobSet·Workload는
배치가 `job.placement`로 전달되기 전에 지우면 안 된다.** 현재 흐름에서 옛 JobSet을 지우는 것은 relocator이고,
이는 복구 컨트롤러의 등록 → 세대 증가 → RelocationRequest 뒤라서 순서가 맞다.

## 쓰기 권한 (신뢰 경계)

관리 클러스터에서 Workload를 patch할 수 있는 주체는 dispatcher·Kueue 컨트롤러·클러스터 관리자뿐이다
(job-watcher·relocator는 읽기만). 키 단위로는 막혀 있지 않으므로 Gitops에 다음 정책을 요청한다:
**`devoops/placement`를 붙이기·바꾸기·떼기는 `system:serviceaccount:platform:dispatcher`와 클러스터 관리자만.**
값이 그대로인 수정(Kueue의 Workload 갱신 등)은 통과시킨다. 기존 `scheduling-dispatch-annotations` 정책과 같은 모양으로,
대상 리소스만 `kueue.x-k8s.io` `workloads`다.
