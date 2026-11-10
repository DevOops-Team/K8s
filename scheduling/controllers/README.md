# 스케줄링 컨트롤러 2개 (최소 기능)

| 파일 | 일감 | 보는 것 | 바꾸는 것 |
|---|---|---|---|
| `dispatcher.py` | GPU-67 | 쿼터는 잡았고 워커는 미정인 Workload | `status.nominatedClusterNames` |
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
| 디스패처 비워 두기 (`STARVE_SECONDS`) | 큰 잡이 쿼터를 받으면 그 몫은 다른 잡이 못 쓰고, 디스패처는 먼저 온 순으로 보므로 4장 빈 워커가 생기면 큰 잡이 먼저 가져감. 켬/끔 비교에서 입장 시각이 같았음 (185s vs 192s, 192s vs 189s) |
| 노드 단위 배치 계산 | 노드당 GPU 1장, Pod당 GPU 1장이라 "들어가는 노드가 있나"가 "남은 GPU 수"와 같음. cordon·NotReady 노드를 빼는 것과 예비 노드를 복구 잡에만 주는 것은 그대로 |

## 조정 값 (환경 변수)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `RENOMINATE_SECONDS` | 60 | 지명했는데 이 시간 안에 워커 입장 못 하면 다른 곳으로 |
| `LEASE_SECONDS` / `RENEW_SECONDS` | 15 / 5 | 리더가 이만큼 갱신 안 하면 남이 가져감 / 갱신 주기 |
| `LEADER_ELECTION` | 1 | 0이면 리더 선출 끔 |
| `RECOVERY_STORE` | configmap | 복구 원본·펜싱 기록 저장소 (`aws`면 `CKPT_BUCKET`·`FENCING_TABLE`·`AWS_REGION`) |
| `FENCING_WAIT_SECONDS` | 300 | 요청을 받고 세대 +1 기록이 이 시간 안에 안 보이면 Failed |
| `STOP_WAIT_SECONDS` | 60 | 옛 잡이 이 시간 안에 안 내려가면 `WaitingForStop` (계속 기다림) |
| `STOP_HOLD_SECONDS` | 900 | 요청 생성부터 이 시간 안에 옛 실행 종료를 확인 못 하면 `Held`. 이후엔 `devoops/old-execution-stopped=true` 어노테이션으로만 재개 (복구 컨트롤러 HOLD 15분과 맞춤) |
| `EC2_EVIDENCE` | off | 워커 EKS API에 닿지 않을 때 EC2 인스턴스 상태를 옛 실행 종료 증거로 씀 (`ec2_evidence.py`). `describe` = 조회만, `terminate` = 워커가 `STOP_WAIT_SECONDS` 넘게 안 닿으면 격리 때 기록한 옛 노드 인스턴스만 한 번 강제 종료. 필요 IAM: `ec2:DescribeInstances`(+`ec2:TerminateInstances`, Karpenter NodePool 태그 조건) |
| `EC2_CLUSTERS` / `EC2_NODEPOOLS` | `{}` / training,spare | MultiKueueCluster 이름 → EKS 클러스터 이름(JSON, 없으면 같은 이름) / 찾을 Karpenter NodePool (`karpenter.sh/nodepool` 태그) |
| `ADMIT_TIMEOUT_SECONDS` | 900 | 재제출 잡이 이 시간 안에 입장 못 하면 `AdmitDelayed`. JobSet은 그대로 두고 계속 기다림 (실패 판단은 복구 컨트롤러) |

## 아직 없는 것

노드당 GPU 여러 개일 때의 배치(지금은 노드당 GPU 1장이라 남은 GPU 수만 셈), 재배치 실패 시 되돌리기, 장애복구 파트 HTTP API(`contracts.md`의 fence/restart/status) 어댑터.
