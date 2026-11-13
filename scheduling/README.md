# 스케줄링 컨트롤러 2개 (최소 기능)

| 파일 | 일감 | 보는 것 | 바꾸는 것 |
|---|---|---|---|
| `dispatcher.py` | GPU-67 | 쿼터는 잡았고 워커는 미정인 Workload, 진행 중인 `RelocationRequest`(읽기만) | `status.nominatedClusterNames`, 찜 어노테이션 `devoops/reserved-cluster` |
| `relocator.py` | GPU-68 | `RelocationRequest` + 펜싱 기록(DynamoDB) | 노드 cordon·taint, MultiKueueCluster 펜싱 라벨, 잡 삭제·재제출, 펜싱 기록 `recovery_phase`, 요청 status |

- `keys.py`: 다른 파트와 맞물리는 이름(라벨·어노테이션·CRD). 이름 바꿀 땐 여기만.
- `placement.py`·`recovery_render.py`: 디스패처(빈 패킹)·재배치 실행기 판단만 떼어 낸 순수 함수. `python -m unittest test_*.py`
- 재배치 실행기는 복구 컨트롤러가 DynamoDB 세대 +1(`GENERATION_ADVANCED`)을 한 요청만 처리한다. 복구 원본(S3 `code/{job_id}/jobset.json`)에서 JobSet 템플릿 6절 값만 바꿔 `job-{id}-g{4자리}`로 제출하고, `OLD_RUN_STOPPED`·`NEW_RUN_SUBMITTED`·`new_jobset_name/uid`를 조건부로 기록한다(체크포인트 규약 5·8절). 저장소는 `recovery_store.py`: 우리 망 `RECOVERY_STORE=aws`, kind `configmap`(전용 네임스페이스 `scheduling-local-store`의 `recovery-code-<id>`·`recovery-fencing-<id>`를 시나리오가 백엔드·복구 컨트롤러 대신 씀).
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
- `relocator/`: relocator, recovery_store, ec2_evidence, tests and Dockerfile.
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
| 노드 단위 배치 계산 | 노드당 GPU 1장, Pod당 GPU 1장이라 "들어가는 노드가 있나"가 "남은 GPU 수"와 같음. cordon·NotReady 노드를 빼는 것과 예비 노드를 복구 잡에만 주는 것은 그대로 |

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
- **종료 확인 범위 (재배치 실행기)**: 옛 실행이 멈췄는지는 입장했던 워커(`sourceCluster`)뿐 아니라 디스패처가 지명만
  했던 워커(`status.seenClusters`)에서도 확인한다. 지명만 된 상태에서도 워커에 복사본·Pod가 있을 수 있다.
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

## 조정 값 (환경 변수)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `RENOMINATE_SECONDS` | 60 | 지명했는데 이 시간 안에 워커 입장 못 하면 다른 곳으로 |
| `RELOCATION_HOLD_SECONDS` | 300 | 진행 중인 재배치의 원래 클러스터를 재제출 잡 몫으로 지키는 최대 시간 (격리 시각, 옛 실행 종료 뒤엔 그 시각부터) |
| `LEASE_SECONDS` / `RENEW_SECONDS` | 15 / 5 | 리더가 이만큼 갱신 안 하면 남이 가져감 / 갱신 주기 |
| `LEADER_ELECTION` | 1 | 0이면 리더 선출 끔 |
| `RECOVERY_STORE` | configmap | 복구 원본·펜싱 기록 저장소 (`aws`면 `CKPT_BUCKET`·`FENCING_TABLE`·`AWS_REGION`) |
| `FENCING_WAIT_SECONDS` | 300 | 요청을 받고 세대 +1 기록이 이 시간 안에 안 보이면 Failed |
| `STOP_WAIT_SECONDS` | 120 | 옛 잡이 이 시간 안에 안 내려가면 `WaitingForStop` (계속 기다림) |
| `STOP_HOLD_SECONDS` | 900 | 펜싱 `stop_wait_deadline` 우선. 없으면 `stop_wait_started_at`부터 이 시간, 둘 다 없으면 RR 생성부터 이 시간을 넘기면 `Held`. 이후엔 `devoops/old-execution-stopped=true` 어노테이션으로만 재개 |
| `EC2_EVIDENCE` | off | 워커 EKS API에 닿지 않을 때 그 클러스터 학습·예비 인스턴스가 전부 stopped/terminated면 옛 실행 종료로 인정 (`ec2_evidence.py`, 기록한 옛 노드만 보지 않음 — 망 분리 중 다른 노드로 다시 뜬 Pod). `describe` = 조회만, `terminate` = 워커가 `STOP_WAIT_SECONDS` 넘게 안 닿으면 격리 때 기록한 옛 노드 인스턴스만 한 번 강제 종료(ID로 조회 후 태그 확인, 증거는 여전히 클러스터 전체 기준). 필요 IAM: `ec2:DescribeInstances`(+`ec2:TerminateInstances`, `eks:nodegroup-name` 태그 조건). 고정 노드그룹은 ASG가 대체 인스턴스를 띄우므로 강제 종료 뒤에도 보통 사람 확인으로 넘어감 |
| `EC2_CLUSTERS` / `EC2_NODEGROUPS` | `{}` / training,spare | MultiKueueCluster 이름 → EKS 클러스터 이름(JSON, 없으면 같은 이름, `eks:cluster-name` 태그) / 찾을 학습·예비 관리형 노드그룹 이름(`eks:nodegroup-name` 태그, 실제 이름으로 넣을 것) |
| `ADMIT_TIMEOUT_SECONDS` | 900 | 재제출 잡이 이 시간 안에 입장 못 하면 `AdmitDelayed`. JobSet은 그대로 두고 계속 기다림 (실패 판단은 복구 컨트롤러) |

## 아직 없는 것

노드당 GPU 여러 개일 때의 배치(지금은 노드당 GPU 1장이라 남은 GPU 수만 셈), 재배치 실패 시 되돌리기, 장애복구 파트 HTTP API(`contracts.md`의 fence/restart/status) 어댑터.

### 옛 실행 종료 기록

펜싱 시각은 epoch seconds(숫자/문자열) 또는 시간대가 있는 RFC3339를 읽는다. 잘못된 시각은 오류로 보류하며 RR 시각으로 대체하지 않는다.
`OLD_RUN_STOPPED` 조건부 갱신에 `old_run_stop_evidence` JSON 문자열을 함께 저장한다(AWS/ConfigMap 동일 형식).
필드는 `generation`, `namespace`, `jobset_name`, `clusters`, `kind`(`automatic`/`ec2`/`manual`), `checks`(워커별 `cluster`·`kind`·`detail`), `confirmed_at`, `confirmed_by`, `recorded_by`, `detail`이다.
EC2와 API 확인이 섞이면 전체 `kind`는 `ec2`, 세부 경로는 `checks`로 구분한다. 워커에 보내기 전이면 `clusters`·`checks`는 빈 배열이다.
수동 확인은 기존 `devoops/old-execution-stopped=true`를 따른다. 어노테이션만으로 작성자를 알 수 없어 `confirmed_by`는 `unknown (RR annotation)`으로 기록한다. 자동 확인은 `relocator`, `recorded_by`는 `POD_NAME` 또는 호스트명·PID이며 `confirmed_at`은 relocator가 확인한 시각이다.
