# 스케줄링 컨트롤러 2개 (최소 기능)

| 파일 | 일감 | 보는 것 | 바꾸는 것 |
|---|---|---|---|
| `dispatcher.py` | GPU-67 | 쿼터는 잡았고 워커는 미정인 Workload, 진행 중인 `RelocationRequest`(읽기만) | `status.nominatedClusterNames`, 찜 어노테이션 `devoops/reserved-cluster` |
| `relocator.py` | GPU-68 | `RelocationRequest` + 펜싱 기록(DynamoDB) | 노드 cordon·taint, MultiKueueCluster 펜싱 라벨, 잡 삭제·재제출, 펜싱 기록 `recovery_phase`, 요청 status |

- `keys.py`: 다른 파트와 맞물리는 이름(라벨·어노테이션·CRD). 이름 바꿀 땐 여기만.
- `placement.py`·`recovery_render.py`: 디스패처(빈 패킹)·재배치 실행기 판단만 떼어 낸 순수 함수. `python -m unittest test_*.py`
- 재배치 실행기는 복구 컨트롤러가 DynamoDB 세대 +1(`GENERATION_ADVANCED`)을 한 요청만 처리한다. 복구 원본(S3 `code/{job_id}/jobset.json`)에서 JobSet 템플릿 6절 값만 바꿔 `job-{id}-g{4자리}`로 제출하고, `OLD_RUN_STOPPED`·`NEW_RUN_SUBMITTED`·`new_jobset_name/uid`를 조건부로 기록한다(체크포인트 규약 5·8절). 저장소는 `recovery_store.py`: 우리 망 `RECOVERY_STORE=aws`, kind `configmap`(전용 네임스페이스 `scheduling-local-store`의 `recovery-code-<id>`·`recovery-fencing-<id>`를 시나리오가 백엔드·복구 컨트롤러 대신 씀).
- 워커 접속: 재배치 실행기는 `platform/<워커>-relocator-kubeconfig`(EKS별 relocator SA, `deploy/worker/relocator-rbac.yaml`)를 먼저 쓰고, 없으면 MultiKueue Secret.
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
docker build -t scheduling-controllers:dev .
kind load docker-image scheduling-controllers:dev --name manager
kubectl --context kind-manager apply -k deploy/
kubectl --context kind-manager -n platform get pods -l 'app in (dispatcher,relocator)' -o wide   # 레플리카 3개씩 manager-worker/2/3에 하나씩
kubectl --context kind-manager -n platform get lease | grep scheduling-   # holder = 지금 리더 Pod
```

로컬 kind manager는 `cluster/kind/manager.yaml`에 `platform-worker=true` 워커 노드 3개가 있어야 함(없으면 Pod 3개 중 일부 Pending) → 예전 manager 클러스터는 다시 만들어야 함.

## 뺀 것 (11/09)

워커 하나가 GPU 5장(학습 4 + 예비 1)이고 잡이 최대 4장인 규모에서 효과가 없어서 뺐다. 근거는 `../e2e/` 실행 결과.

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
  - 관리 클러스터 쿼터는 따로다. 4장 복구 잡은 `cq-recovery` 3 + 학습 몫 1을 빌려야 해서 쿼터를 늦게 받을 수 있다
    (시나리오 11, 대책 B = `cq-recovery` 4). 그동안 지켜 둔 클러스터는 비어 있고, 시간이 지나면 풀린다.
- **종료 확인 범위 (재배치 실행기)**: 옛 실행이 멈췄는지는 입장했던 워커(`sourceCluster`)뿐 아니라 디스패처가 지명만
  했던 워커(`status.seenClusters`)에서도 확인한다. 지명만 된 상태에서도 워커에 복사본·Pod가 있을 수 있다.
- **디스패처가 읽는 어노테이션**(`devoops/target-cluster`·`exclude-clusters`·`manual-dispatch`)은 재배치 실행기와
  클러스터 관리자만 붙이거나 바꿀 수 있다 (`deploy/admission-policy.yaml`의 `scheduling-dispatch-annotations`).

## 조정 값 (환경 변수)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `RENOMINATE_SECONDS` | 60 | 지명했는데 이 시간 안에 워커 입장 못 하면 다른 곳으로 |
| `RELOCATION_HOLD_SECONDS` | 300 | 진행 중인 재배치의 원래 클러스터를 재제출 잡 몫으로 지키는 최대 시간 (격리 시각, 옛 실행 종료 뒤엔 그 시각부터) |
| `LEASE_SECONDS` / `RENEW_SECONDS` | 15 / 5 | 리더가 이만큼 갱신 안 하면 남이 가져감 / 갱신 주기 |
| `LEADER_ELECTION` | 1 | 0이면 리더 선출 끔 |
| `RECOVERY_STORE` | configmap | 복구 원본·펜싱 기록 저장소 (`aws`면 `CKPT_BUCKET`·`FENCING_TABLE`·`AWS_REGION`) |
| `FENCING_WAIT_SECONDS` | 300 | 요청을 받고 세대 +1 기록이 이 시간 안에 안 보이면 Failed |
| `STOP_WAIT_SECONDS` | 60 | 옛 잡이 이 시간 안에 안 내려가면 `WaitingForStop` (계속 기다림) |
| `STOP_HOLD_SECONDS` | 900 | 요청 생성부터 이 시간 안에 옛 실행 종료를 확인 못 하면 `Held`. 이후엔 `devoops/old-execution-stopped=true` 어노테이션으로만 재개 (복구 컨트롤러 HOLD 15분과 맞춤) |
| `EC2_EVIDENCE` | off | 워커 EKS API에 닿지 않을 때 그 클러스터 학습·예비 인스턴스가 전부 stopped/terminated면 옛 실행 종료로 인정 (`ec2_evidence.py`, 기록한 옛 노드만 보지 않음 — 망 분리 중 다른 노드로 다시 뜬 Pod). `describe` = 조회만, `terminate` = 워커가 `STOP_WAIT_SECONDS` 넘게 안 닿으면 격리 때 기록한 옛 노드 인스턴스만 한 번 강제 종료(ID로 조회 후 태그 확인, 증거는 여전히 클러스터 전체 기준). 필요 IAM: `ec2:DescribeInstances`(+`ec2:TerminateInstances`, Karpenter NodePool 태그 조건) |
| `EC2_CLUSTERS` / `EC2_NODEPOOLS` | `{}` / training,spare | MultiKueueCluster 이름 → EKS 클러스터 이름(JSON, 없으면 같은 이름) / 찾을 Karpenter NodePool (`karpenter.sh/nodepool` 태그) |
| `ADMIT_TIMEOUT_SECONDS` | 900 | 재제출 잡이 이 시간 안에 입장 못 하면 `AdmitDelayed`. JobSet은 그대로 두고 계속 기다림 (실패 판단은 복구 컨트롤러) |

## 아직 없는 것

노드당 GPU 여러 개일 때의 배치(지금은 노드당 GPU 1장이라 남은 GPU 수만 셈), 재배치 실패 시 되돌리기, 장애복구 파트 HTTP API(`contracts.md`의 fence/restart/status) 어댑터.
