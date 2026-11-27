# 스케줄링 컨트롤러 2개 (최소 기능)

`restartInPlace`(Pod만 죽은 경우)는 제자리 재시작이다. 복구 컨트롤러가 RR `spec.sourceCluster`·`spec.nodes`로 옛 세대 노드를
주면, 재제출본은 `kubernetes.io/hostname` required nodeAffinity와 `target-cluster`로 그 노드에만 뜬다(1·2에서 돌던 잡은 1·2로만).
relocator는 노드·클러스터를 조회하거나 판단하지 않는다. 노드가 Pod 수보다 적은 등 실행할 수 없으면 접수 단계에서 `Failed`.
`spec.nodes`가 없으면(노드 소실·NotReady) 노드를 고정하지 않고 아래 `moveToSpare`처럼 원래 EKS 우선이다. 고정한 노드는 `status.pinnedNodes`.

두 action의 차이는 고장 노드 격리 여부 하나다. `restartInPlace` = 같은 클러스터에서 새 세대로 다시 시작(격리 없음),
`moveToSpare` = 고장 노드를 격리하고 다시 시작(이름과 달리 꼭 예비 노드로 가지는 않음). 어느 action이 오는지와
`spec.nodes`를 보낼지는 복구 컨트롤러가 정한다(Pod 장애는 nodes 있음, 노드 장애는 없음).

모든 복구 재제출본은 예비 노드 toleration을 가진다. 노드 고정이 없으면 비예비 노드 선호
(`preferred nodeAffinity devoops/spare NotIn [true]`, weight 100)도 넣어, 학습 노드에 자리가 있으면 예비를 쓰지 않는다.
예비는 다음 장애를 위해 남기는 자원이라 학습 노드가 모자랄 때만 쓴다. 노드 고정이 있으면 선호를 넣지 않는다(GPU-306).

ADR-38: `moveToSpare` 재제출본은 `devoops/recovery-source-cluster`로 원래 EKS를 우선한다.
원래 EKS가 부족하면 디스패처가 다른 EKS를 검토하고, 모두 부족하면 대기한다. 일반 `target-cluster`는 계속 강제 제한이다.
복구 완료 판정 연계와 GPU-164의 6/2 ↔ 8/4 테스트 프로필은 [변경·배포 인계](../docs/gpu164-165-handoff.md)를 참고한다.

| 파일 | 일감 | 보는 것 | 바꾸는 것 |
|---|---|---|---|
| `dispatcher.py` | GPU-67 | 쿼터는 잡았고 워커는 미정인 Workload, 진행 중인 `RelocationRequest`(읽기만) | `status.nominatedClusterNames`, 찜 어노테이션 `devoops/reserved-cluster` |
| `relocator.py` | GPU-68 | 복구 컨트롤러의 `RelocationRequest` 입력·제어 신호 | 노드 cordon·taint, MultiKueueCluster 펜싱 라벨, 잡 삭제·재제출, 요청 status 반환. EC2·S3·DynamoDB 직접 조회 없음 |

- `keys.py`: 다른 파트와 맞물리는 이름(라벨·어노테이션·CRD). 이름 바꿀 땐 여기만.
- `placement.py`·`recovery_render.py`: 디스패처(빈 패킹)·재배치 실행기 판단만 떼어 낸 순수 함수. `python -m unittest test_*.py`
- 재배치 실행기는 S3·DynamoDB에 가지 않는다(ADR-50). 입력은 RR spec(`toGeneration`·`stopDeadline`·`jobSet` 원본 등), 결과는 RR `status.recoveryProgress`(옛 실행 종료 증거, 관리 클러스터 새 JobSet 이름·UID)뿐이다. 원본 JobSet에서 JobSet 템플릿 6절 값만 바꿔 `job-{id}-g{4자리}`로 제출한다. 펜싱 표 기록(`OLD_RUN_STOPPED`·`NEW_RUN_SUBMITTED`·`new_jobset_name/uid`)은 복구 컨트롤러가 status를 보고 한다. 컨트롤러 신호는 RR 주석 두 개: `devoops/recovery-stop-recorded=true`(OLD_RUN_STOPPED 기록됨 → 재제출), `devoops/recovery-cancel`(만든 JobSet 철회 → `Cancelled`). kind에서는 e2e 하네스가 복구 컨트롤러를 흉내 내며 `scheduling-local-store`의 `recovery-code-<id>`·`recovery-fencing-<id>`를 쓴다.
- 워커 접속: 재배치 실행기는 `kueue-system/<워커>-relocator-kubeconfig`(EKS별 relocator SA, `deploy/worker/relocator-rbac.yaml`)를 먼저 쓰고, 없으면 MultiKueue Secret.
- 둘 다 "N초마다 전체를 다시 보고 하나를 바꾼다"(폴링 루프). 상태는 쿠버네티스 객체에만 있어서 재시작해도 이어서 동작.
- HA: 컨트롤러마다 레플리카 3개를 `platform-worker=true` 노드(worker1~3)에 하나씩 둠. `leader.py`가 Lease `platform/scheduling-<이름>`으로 리더 하나만 일하게 함(`python -m unittest -v test_leader.py`). 리더 노드가 죽으면 15s(`LEASE_SECONDS`) 안에 넘어가고, 정상 종료면 Lease를 내려놓아 바로 넘어감. `run-local.sh`도 같은 Lease를 다투므로 클러스터 안 Deployment와 동시에 돌려도 하나만 일함.

## 실행

노트북에서 바로 (코드 고치고 바로 재실행):

```bash
./run-local.sh            # 둘 다, Ctrl-C로 종료
./run-local.sh dispatcher # 하나만
```

클러스터 안 (우리 망과 같은 방식, 워커 kubeconfig는 MultiKueue Secret을 읽음):

```bash
docker build -f scheduling/dispatcher/Dockerfile -t scheduling-dispatcher:dev scheduling
docker build -f scheduling/relocator/Dockerfile -t scheduling-relocator:dev scheduling
```

Build from the repository root with `scheduling/` as the context.
Default commands: `python dispatcher/dispatcher.py` and `python relocator/relocator.py`.
Common modules are copied into `/app` (`PYTHONPATH=/app`).
For local execution, run `python scheduling/dispatcher/dispatcher.py` or
`python scheduling/relocator/relocator.py` from the repository root.

### Layout and tests

- `dispatcher/`: dispatcher, placement, tests and Dockerfile.
- `relocator/`: relocator, controller contract, tests and Dockerfile.
- `common/`: keys, kube, leader, recovery_render, tests and requirements.txt.

```bash
python -m pip install -r scheduling/common/requirements.txt
export PYTHONPATH="$PWD/scheduling/common:$PWD/scheduling/dispatcher:$PWD/scheduling/relocator"
python -m unittest discover -s scheduling/common -p 'test_*.py'
python -m unittest discover -s scheduling/dispatcher -p 'test_*.py'
python -m unittest discover -s scheduling/relocator -p 'test_*.py'
```

### CI / deployment handoff

CI는 저장소 루트 `.github/workflows/ci.yml` 하나다 (GPU-127, 담당 노찬경).

- 이미지: `ghcr.io/devoops-team/dispatcher`, `ghcr.io/devoops-team/relocator` (Dockerfile은 각 폴더, 빌드 컨텍스트는 `scheduling/`)
- PR · develop push: `common` + 자기 폴더 테스트 → 이미지 빌드 (push 안 함)
- main push: 자기 폴더나 `scheduling/common/`이 바뀐 이미지만 `main`, `sha-<7자리>`로 push
  → K8s Variable `DEPLOY_IMAGES`에 든 이미지만 봇이 Gitops `onprem/controllers/<이름>/overlays/onprem` digest 갱신
- 운영 매니페스트(CRD · RBAC · Deployment)는 Gitops에서 관리한다. Deployment는 Dockerfile 기본 CMD를 쓴다.
  CRD는 `run-local.sh` 실행 전에 설치돼 있어야 한다.

## 뺀 것 (11/09)

워커 하나가 GPU 5장(학습 4 + 예비 1)이고 잡이 최대 4장인 규모에서 효과가 없어서 뺐다. 근거는 `e2e/` 실행 결과.

| 뺀 것 | 이유 |
|---|---|
| 배분 조정 컨트롤러 `allocator.py` (GPU-69, aging: 대기 시간 x GPU 수로 `training-aged` 올리기) | 선점을 안 쓰므로 우선순위는 줄 순서에만 쓰이는데, 먼저 온 잡은 이미 앞에 있음. Light(BestEffortFIFO)는 1장만 비면 순서와 상관없이 1장 잡이 들어가서 올려도 소용없고, Heavy(StrictFIFO)에선 4장 잡이 먼저 aged 되어 먼저 온 3장 잡을 새치기함 |
| 디스패처 비워 두기 (`STARVE_SECONDS`) | 기다린 시간을 기준으로 비우는 방식은 뺐다. 켬/끔 비교에서 입장 시각이 같았음 (185s vs 192s, 192s vs 189s). 다만 그 비교는 4장 빈 워커가 안 생기는 상황을 못 만들었고, 11/10 compare에서 쿼터 뒤 142s 지연이 나와 아래 "찜"으로 다시 넣음 |
| 노드 단위 배치 계산 | 노드당 GPU 1장, Pod당 GPU 1장이라 "들어가는 노드가 있나"가 "남은 GPU 수"와 같음. cordon·NotReady 노드를 빼는 것과 예비 노드를 복구 잡에만 주는 것은 그대로. 입장했지만 노드를 아직 못 잡은 GPU Pod도 사용 중으로 센다(워커 blockAdmission을 끈 뒤 같은 자리를 두 번 내주지 않게, 복구 잡 Pod는 예비 자리부터) |

## 찜 (11/10)

관리 Kueue는 워커 3개 합계로 쿼터를 준다. 4장 잡이 쿼터를 받아도 4장이 한 워커에 비어 있다는 보장이 없고,
쿼터를 받은 잡은 더는 "대기 중"이 아니라서 Light가 빈 칸을 계속 메운다 (11/10 compare: 쿼터 뒤 142s, 상한 없음).

- 쿼터는 받았는데 들어갈 워커가 없는 잡은 워커 하나를 찜한다 (`devoops/reserved-cluster`, Workload 어노테이션).
- 찜한 워커에는 **뒤 순서 학습 잡**을 안 보낸다. 다른 워커에는 그대로 보낸다. 먼저 온 잡은 영향 없음.
- 복구 잡은 학습 잡의 찜을 무시한다. 복구 잡의 찜은 학습 잡·뒤 순서 복구 잡이 지킨다.
- 한 번 찜하면 고정. 그 워커가 펜싱·끊김·exclude되거나 cordon으로 영영 못 들어가게 되면 다시 고른다.
- 고르는 기준은 지금 빈 GPU가 가장 많은 곳. 찜한 잡이 다른 워커에 먼저 들어갈 수 있게 되면 거기로 가고 찜은 지운다.
- 워커 하나에 찜 하나 (먼저 온 순).

남은 한계: 잡이 언제 끝날지 모르므로 "가장 빨리 비는 워커"를 고르지는 못한다. 찜한 워커에 오래 도는 잡이 있으면
그 잡이 끝날 때까지 기다린다 (그동안 그 워커의 빈 칸은 놂). 잡에 최대 실행 시간을 받으면 풀 수 있다.

## 재배치 자리 지키기 · 종료 확인 범위 (11/10)

- **자리 지키기 (디스패처)**: 같은 클러스터로 다시 띄우는 재배치(`restartInPlace`·`moveToSpare`)가 격리 단계를 지나면,
  디스패처는 그 클러스터를 재제출 잡 몫으로 찜한 것처럼 다룬다. 재배치 실행기는 옛 잡을 먼저 내리고 새 잡을 넣는데,
  선점이 없어서 그 사이 풀린 자리를 쿼터를 쥐고 기다리던 학습 잡이 가져갈 수 있었다. 새 잡이 지명되거나
  `RELOCATION_HOLD_SECONDS`가 지나면 푼다. 디스패처는 `RelocationRequest`를 읽기만 한다.
  - 관리 클러스터 쿼터는 따로 지킨다: `cq-recovery`를 잡 최대 크기 4로 둬서 4장 복구 잡도 학습 몫을 안 빌리고
    바로 쿼터를 받는다 (11/10, 정본 R10. 3일 땐 Heavy 대기 잡이 옛 잡 몫을 먼저 가져갔음 = 시나리오 11).
- **격리 대상 (재배치 실행기)**: cordon 클러스터는 RR `spec.sourceCluster`, 펜싱 클러스터는 `spec.excludeClusters`다.
  Workload·워커 Pod를 보고 정하지 않는다(GPU-295). 없으면 `Failed`.
- **종료 확인 범위 (재배치 실행기)**: 옛 실행이 멈췄는지는 원래 워커(`spec.sourceCluster`)뿐 아니라 디스패처가 지명·입장시켰던
  워커(`status.seenClusters`, 관리 클러스터 Workload에서 모음)에서도 확인한다. 지명만 된 상태에서도 워커에 복사본·Pod가 있을 수 있다.
- **디스패처가 읽는 어노테이션**(`devoops/target-cluster`·`exclude-clusters`·`manual-dispatch`)은 재배치 실행기와
  클러스터 관리자만 붙이거나 바꿀 수 있다 (`deploy/admission-policy.yaml`의 `scheduling-dispatch-annotations`).

## EKS OFF 때 fenced 확인 (11/11, GPU-125)

디스패처는 루프를 시작할 때 `devoops/fenced` 라벨을 한 번 읽는다. 라벨을 붙이기 직전에 시작된 루프 하나는
옛 정보로 그 클러스터에 지명할 수 있다. 그래서 루프를 끝까지 돌면 그 루프에서 fenced로 본 MultiKueueCluster에
`devoops/fenced-ack=<그 루프 시작 시각>`(RFC3339 UTC, 초 버림)을 남긴다. 루프가 중간에 실패하면 안 남긴다.

```
T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
kubectl label multikueuecluster eks-a devoops/fenced=true
# fenced-ack가 T보다 뒤가 될 때까지 (보통 5~10초)
kubectl get multikueuecluster eks-a -o jsonpath='{.metadata.annotations.devoops/fenced-ack}'
# 그다음 OFF 전 확인(clusterName·nominatedClusterNames·reserved-cluster·RelocationRequest)을 다시 돌린다
```

- 비교는 `ack > T`(같으면 안 됨). 둘 다 초를 버리므로 같은 초 안에서 라벨보다 먼저 시작한 루프는 통과하지 못한다.
- 라벨을 떼도 ack는 지우지 않는다. 다음 OFF에서는 새 T와 비교하니 옛 ack는 통과하지 못한다.
- 리더가 바뀌는 순간 옛 리더가 돌던 루프는 이 ack로 덮이지 않는다. 그래서 ack 뒤에도 OFF 전 확인을 한 번 더 돌린다.
- 위처럼 손으로 붙인 라벨은 표시가 없어서 ON 때 자동으로 풀리지 않는다. 실제 OFF는 `ops/eks_gate.py fence`를 쓴다.

## EKS OFF 펜싱 자동 해제 (GPU-277)

`eks_gate fence`는 라벨과 함께 `devoops/fenced-by=eks-off`를 붙이고, relocator는 격리할 때 `devoops/fenced-by=relocator`를 쓴다.
디스패처는 `eks-off` 펜싱만 푼다. 펜싱 직후 워커 ClusterQueue UID를 `devoops/fenced-worker-id`에 남기고,
EKS를 다시 만들어 UID가 바뀌고 예비 GPU가 보이면 라벨과 표시를 지운다. 같은 워커가 잠깐 끊겼다 돌아온 경우는 풀지 않는다.
relocator 펜싱, 표시 없는 라벨, 둘이 겹친 경우(relocator가 표시를 덮어씀)는 그대로 둔다. 자세한 표는 `ops/README.md`.

## 쿼터 단계 예약 (GPU-251)

`dispatcher/quota_hold.py`는 워커 지명·찜보다 먼저 관리 Workload를 확인한다.
Light 2장 잡 또는 복구 몫이 부족한 큰 복구 잡이 기다리는 동안, 늦게 온 학습 잡이 서로 다른 Workload로
3번 먼저 쿼터를 받으면 경쟁 대기 잡의 `spec.active=false`와 `devoops/held-for=<namespace/name>`·`held-at`을 기록한다.
Light는 같은 ClusterQueue의 뒤 Light 잡만, 복구는 같은 cohort의 Heavy·Light 대기 잡을 보류한다.
한 번에 하나를 보호하며 복구가 우선이다. 실행 중인 잡은 계속 실행하므로, 필요한 쿼터가 모이면 Kueue가 보호한 잡을 입장시킨다.

- 이미 쿼터를 받았거나 admission·지명이 있는 잡, 복구 잡, 수동 디스패치 잡, 원래 비활성이던 잡은 보류하지 않는다.
- 모든 변경은 읽은 `resourceVersion`으로 patch한다. Kueue가 먼저 쿼터를 줬으면 409로 건너뛰고 다음 루프에서 재평가한다.
- 쿼터 확보·보호 대상 삭제/완료/비활성·조건 해소·기능 꺼짐 때 우리 보류를 해제한다. `held-for-uid`로 이름 재사용을 구별한다.
- `quota-bypasses`에 대기 시작 시각과 관측한 UID를 N개까지 남겨 재시작·리더 교체 후 이어간다. 같은 잡을 여러 번 세지 않는다.
- 단일 GPU flavor, 같은 평면 cohort의 현행 큐 구성을 대상으로 한다. CQ/LQ 설정은 수정하지 않는다.
- 폴링 사이에 제출·완료되어 사라진 잡은 집계하지 못할 수 있다. 새 잡이 다음 루프 전에 쿼터를 받는 틈도 있어 N은 실시간 하드 제한이 아니다.

ADR-41 구현으로 **기본 켜짐**이다. kind/KWOK에서 Light 2장·복구 4장 꺼짐/켜짐 비교,
리더 재시작·보류 해제·워커 입장을 확인한 뒤 기본값을 켰다. 끄려면 `QUOTA_HOLD_ENABLED=false`를 준다.
Gitops 담당자 인계: 디스패처 환경변수 3개와 관리 Workload `patch`, ClusterQueue·LocalQueue `get/list` 권한 확인.
추가 파드·Secret·CRD·이미지 실행 명령 변경은 없다. 운영 매니페스트·CI workflow는 이 작업에서 변경하지 않는다.
잡 투입기는 보류된 JobSet을 계속 대기 중으로 취급하므로 실험 종료를 앞당기지 않는다.

## 조정 값 (환경 변수)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `QUOTA_HOLD_ENABLED` | true | GPU-251 / ADR-41 쿼터 예약. 켜면 뒤 잡이 3번 먼저 쿼터를 받은 Light 2장 잡·학습 몫을 빌려야 하는 복구 잡을 보호한다. 끄면 디스패처가 남긴 보류부터 해제 |
| `QUOTA_HOLD_BYPASS_LIGHT` / `QUOTA_HOLD_BYPASS_RECOVERY` | 3 / 3 | 새치기한 서로 다른 Workload 수. 폴링 횟수가 아니며, 복구 기준도 3회로 통일(2026-11-22 사용자 지시) |
| `RENOMINATE_SECONDS` | 60 | 지명했는데 이 시간 안에 워커 입장 못 하면 다른 곳으로 |
| `RELOCATION_HOLD_SECONDS` | 300 | 진행 중인 재배치의 원래 클러스터를 재제출 잡 몫으로 지키는 최대 시간 (격리 시각, 옛 실행 종료 뒤엔 그 시각부터) |
| `LEASE_SECONDS` / `RENEW_SECONDS` | 15 / 5 | 리더가 이만큼 갱신 안 하면 남이 가져감 / 갱신 주기 |
| `LEADER_ELECTION` | 1 | 0이면 리더 선출 끔 |
| `STOP_WAIT_SECONDS` | 120 | 옛 잡이 이 시간 안에 안 내려가면 `WaitingForStop` (계속 기다림) |
| `STOP_HOLD_SECONDS` | 900 | RR `spec.stopDeadline` 우선. 없으면 RR 생성부터 이 시간을 넘기면 `Held`. 복구 컨트롤러와 재개 절차가 필요하며 사람 확인 주석만으로 양쪽 HOLD가 풀리지는 않음 |
| `SELF_FENCE_EVIDENCE` | off | `on`이면 워커 EKS API에 닿지 않을 때 복구 컨트롤러가 RR에 붙인 `devoops/self-fence-verified`(펜싱 표 `self_fence_*` 확인 결과)를 그 워커의 옛 실행 종료로 인정 (ADR-49). 요청·옛 세대·워커가 맞고, 복구 원본 Pod 수 N과 `worldSize`가 같고, 번호 0~N-1 전부와 번호마다 Pod UID 하나가 있어야 함. 복구 원본이 Pod를 다시 만들지 않는 모양(`maxRestarts` 0, replicas 1, Indexed, `backoffLimit` 0, `podReplacementPolicy: Failed`, `restartPolicy: Never`)일 때만. 증거 kind `self_fence`. ADR-49 남은 구멍 반영·시험 통과 전에는 켜지 않음 |
| `ADMIT_TIMEOUT_SECONDS` | 900 | 재제출 잡이 이 시간 안에 입장 못 하면 `AdmitDelayed`. JobSet은 그대로 두고 계속 기다림 (실패 판단은 복구 컨트롤러) |

## 아직 없는 것

노드당 GPU 여러 개일 때의 배치(지금은 노드당 GPU 1장이라 남은 GPU 수만 셈), 재배치 실패 시 되돌리기, 장애복구 파트 HTTP API(`contracts.md`의 fence/restart/status) 어댑터.

### 옛 실행 종료 기록

펜싱 시각은 epoch seconds(숫자/문자열) 또는 시간대가 있는 RFC3339를 읽는다. 잘못된 시각은 오류로 보류하며 RR 시각으로 대체하지 않는다.
`OLD_RUN_STOPPED` 조건부 갱신에 `old_run_stop_evidence` JSON 문자열을 함께 저장한다(AWS/ConfigMap 동일 형식).
필드는 `generation`, `namespace`, `jobset_name`, `clusters`, `kind`(`automatic`/`self_fence`/`manual`), `checks`(워커별 `cluster`·`kind`·`detail`), `confirmed_at`, `confirmed_by`, `recorded_by`, `detail`이다. relocator가 status로 보고하면 복구 컨트롤러가 펜싱 표에 기록한다.
API 소멸 확인과 컨트롤러의 자기 펜싱 확인이 섞이면 전체 `kind`는 `self_fence`, 세부 경로는 `checks`로 구분한다. 과거 `ec2` 증거가 이미 기록된 RR은 그 증거를 보존하지만 새 EC2 증거를 직접 생산하지 않는다. 워커에 보내기 전이면 `clusters`·`checks`는 빈 배열이다.
수동 확인은 기존 `devoops/old-execution-stopped=true`를 따른다. 어노테이션만으로 작성자를 알 수 없어 `confirmed_by`는 `unknown (RR annotation)`으로 기록한다. 자동 확인은 `relocator`, `recorded_by`는 `POD_NAME` 또는 호스트명·PID이며 `confirmed_at`은 relocator가 확인한 시각이다.

GPU-297: `EC2_EVIDENCE`·`EC2_CLUSTERS`·`EC2_NODEGROUPS`는 제거됐다. 옛 설정이 남아 있어도 AWS에 접근하지 않는다. 여러 격리 대상 중 Node 객체가 이미 없으면 그 대상의 cordon만 건너뛰고 나머지를 격리한다. Node 없음은 옛 실행 종료 증거가 아니므로, 옛 Pod·JobSet·Workload가 남거나 워커 API에 닿지 못하면 기존 종료 대기를 유지한다. 다중 노드 장애·노드그룹 0대의 일반 종료 증거 전달과 HOLD 재개에 필요한 복구 컨트롤러 요청은 [연계 계약 7절](relocator/RECOVERY_CONTRACT.md#7-gpu-297-복구-컨트롤러에-요청할-연계)에 정리했다.
