# 스케줄링 컨트롤러 3개 (최소 기능)

| 파일 | 일감 | 보는 것 | 바꾸는 것 |
|---|---|---|---|
| `dispatcher.py` | GPU-67 | 쿼터는 잡았고 워커는 미정인 Workload | `status.nominatedClusterNames` |
| `relocator.py` | GPU-68 | `RelocationRequest` | 노드 cordon·taint, MultiKueueCluster 펜싱 라벨, 잡 삭제·재제출, 요청 status |
| `allocator.py` | GPU-69 | JobSet + Workload 대기 시간 x GPU 수 | JobSet `kueue.x-k8s.io/priority-class` 라벨 |

- `keys.py`: 다른 파트와 맞물리는 이름(라벨·어노테이션·CRD). 이름 바꿀 땐 여기만.
- `placement.py`·`allocation_rules.py`: 디스패처(빈 패킹)·배분 조정 컨트롤러 판단만 떼어 낸 순수 함수. `python -m unittest -v test_placement.py test_allocator.py`
- 셋 다 "N초마다 전체를 다시 보고 하나를 바꾼다"(폴링 루프). 상태는 쿠버네티스 객체에만 있어서 재시작해도 이어서 동작.
- HA: 컨트롤러마다 레플리카 3개를 `platform-worker=true` 노드(worker1~3)에 하나씩 둠. `leader.py`가 Lease `job-placement/job-placement-<이름>`으로 리더 하나만 일하게 함(`python -m unittest -v test_leader.py`). 리더 노드가 죽으면 15s(`LEASE_SECONDS`) 안에 넘어가고, 정상 종료면 Lease를 내려놓아 바로 넘어감. `run-local.sh`도 같은 Lease를 다투므로 클러스터 안 Deployment와 동시에 돌려도 하나만 일함.

## 실행

노트북에서 바로 (코드 고치고 바로 재실행):

```bash
./run-local.sh            # 셋 다, Ctrl-C로 종료
./run-local.sh dispatcher # 하나만
```

클러스터 안 (우리 망과 같은 방식, 워커 kubeconfig는 MultiKueue Secret을 읽음):

```bash
docker build -t job-placement:dev .
kind load docker-image job-placement:dev --name manager
kubectl --context kind-manager apply -k deploy/
kubectl --context kind-manager -n job-placement get pods -o wide   # 3개씩 manager-worker/2/3에 하나씩
kubectl --context kind-manager -n job-placement get lease          # holder = 지금 리더 Pod
```

로컬 kind manager는 `cluster/kind/manager.yaml`에 `platform-worker=true` 워커 노드 3개가 있어야 함(없으면 Pod 3개 중 일부 Pending) → 예전 manager 클러스터는 다시 만들어야 함.

## 조정 값 (환경 변수)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `RENOMINATE_SECONDS` | 60 | 지명했는데 이 시간 안에 워커 입장 못 하면 다른 곳으로 |
| `STARVE_SECONDS` | 120 | 이보다 오래 못 들어간 잡이 있으면 가장 빈 워커를 비워 둠 |
| `AGING_GPU_SECONDS` | 720 | 대기 시간 x GPU 수가 이보다 크면 `training-aged`(500)로 (4장 3분, 1장 12분) |
| `LEASE_SECONDS` / `RENEW_SECONDS` | 15 / 5 | 리더가 이만큼 갱신 안 하면 남이 가져감 / 갱신 주기 |
| `LEADER_ELECTION` | 1 | 0이면 리더 선출 끔 |
| `STOP_WAIT_SECONDS` | 60 | 재배치 때 옛 잡이 내려가길 기다리는 최대 시간 |

## 아직 없는 것

노드당 GPU 여러 개일 때의 정교한 배치(지금은 first-fit), 재배치 실패 시 되돌리기, 장애복구 파트 HTTP API(`contracts.md`의 fence/restart/status) 어댑터.
