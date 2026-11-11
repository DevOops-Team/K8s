# EKS OFF/ON 확인

Python 3.12와 `kubernetes==31.0.0`을 쓴다. 저장소 루트에서 실행한다.
접속할 kubeconfig context와 조회·patch 권한을 먼저 준비한다.

## OFF

1. Gitops 담당자가 예비 Pod Deployment replicas를 0으로 변경하고 Argo sync한다.
2. `eks_spare.py`로 rollout 반영과 실제 Pod 0개를 확인한다. 종료 중 Pod도 없어야 한다.
3. `eks_gate.py fence`로 펜싱하고 ACK와 남은 잡을 확인한다.
4. 두 명령이 모두 0이면 EKS를 내린다. 1이면 출력된 잡·RR을 정리하고 다시 확인한다.

```sh
python scheduling/ops/eks_spare.py eks-a off --worker-context <워커-context>
python scheduling/ops/eks_gate.py fence eks-a --manager-context <관리-context>
python scheduling/ops/eks_gate.py off-check eks-a --manager-context <관리-context>
```

`fence`는 UTC 기준 시각을 기록하고 라벨을 붙인 뒤, 그 시각보다 큰 ACK를 기다린다.
기본 대기는 60초이며 `--ack-timeout 120`으로 바꾼다. 실패해도 라벨은 유지한다.
OFF를 취소할 때는 `kubectl --context <관리-context> label multikueuecluster eks-a devoops/fenced-`로 지운다.

## ON

IaC 담당자가 EKS를 다시 만든다. Gitops·Argo 담당자가 아래 인터페이스에 맞는 구성요소를 복원한다.
새 API 주소·CA·토큰에 맞게 Argo 클러스터 등록, 관리 클러스터의 MultiKueue 및 relocator 접속 Secret도 갱신한다.
Git에서 예비 Pod replicas를 1로 복원하고 Argo sync한 뒤 실행한다.

```sh
python scheduling/ops/eks_spare.py eks-a on --worker-context <워커-context>
python scheduling/ops/eks_gate.py on-check eks-a --manager-context <관리-context> --worker-context <워커-context> --unfence
```

컨트롤러·큐·클러스터 이름·예비 Pod·relocator ServiceAccount·Active 연결을 모두 확인한 뒤 라벨을 지운다.
ACK 어노테이션은 남긴다. 예비 Pod 확인을 생략하려면 `--no-spare`를 쓴다.
context 기본값은 관리 `kind-manager`, 워커 `kind-<cluster>`다.
큐와 구성요소 이름이 EKS에서 달라지면 `eks_gate.py` 맨 위 상수를 바꾼다.

`eks_spare.py`는 조회만 한다. 기본 300초 동안 2초 간격으로 기다린다 (`--timeout`으로 변경).
ON은 replicas 1, 관측된 최신 generation, availableReplicas 1 이상, 종료 중이 아닌 Running·Ready Pod를 확인한다.
OFF는 replicas 0, 관측된 최신 generation, 실제 예비 Pod 0개를 확인한다.
Deployment를 삭제하는 방식은 통과시키지 않는다. Git에서 replicas를 바꾸는 방식을 쓴다.
두 상태 모두 클러스터 이름 ConfigMap으로 잘못된 context를 걸러낸다.

| 종료 코드 | 뜻 |
|---|---|
| 0 | 확인 통과 |
| 1 | 잡·RR이 남았거나 ON 준비 항목이 부족함 |
| 2 | 사용법 오류·접속/API 실패·ACK 또는 예비 Pod 대기 타임아웃 |

## Gitops·Argo 담당자에게 전달할 인터페이스

운영 매니페스트·Argo Application·CI workflow는 해당 담당자가 작성한다. 이 디렉터리의 도구는 설치나 sync를 하지 않는다.
Git revision 반영 여부와 Argo Synced·Healthy는 Argo 담당자가 확인한다. 도구는 Kubernetes의 실제 결과를 확인한다.

| 워커 리소스 | 확인 기준 |
|---|---|
| Kueue v0.20.4 | `kueue-system/kueue-controller-manager` Deployment availableReplicas ≥ 1 |
| JobSet v0.13.0 | `jobset-system/jobset-controller-manager` Deployment availableReplicas ≥ 1 |
| ClusterQueue | `train-cq`, `recovery-cq` |
| LocalQueue | `training/queue-heavy`, `queue-light`, `queue-recovery` |
| 클러스터 이름 | `training/devoops-cluster-info` ConfigMap의 `CLUSTER_NAME=eks-a/b/c` |
| 예비 Pod | `training/spare-placeholder` Deployment, priorityClassName `spare-placeholder`, 라벨 `app=spare-placeholder` |
| 예비 용량 | 학습 노드와 분리한 예비 GPU 노드, `devoops/spare=true:NoSchedule` taint와 해당 toleration |
| relocator | `platform/relocator` ServiceAccount, nodes get/list/patch, training의 jobsets get·workloads list·pods list |

MultiKueue가 JobSet을 복사할 수 있는 워커 접속 권한과 학습/복구 GPU 용량에 맞는 큐 quota도 구성한다.
예비 Pod는 GPU를 요청하고 낮은 우선순위로 복구 잡에 선점돼야 한다. 실제 EKS에는 KWOK 설정을 넣지 않는다.
ServiceAccount 존재 확인만으로 RBAC 전체와 토큰 유효성을 검증하지는 않는다. 권한과 접속 Secret은 배포 담당자가 검증한다.

관리 디스패처는 MultiKueueCluster get/list/patch 권한이 필요하다.
`devoops/fenced=true`를 본 루프가 끝나면 `devoops/fenced-ack`에 그 루프 시작 UTC 시각을 쓴다.
`fence`는 `ack > T`일 때만 통과시키고, ACK 이후 남은 Workload·RR을 다시 검사한다.
기존 ACK 구현을 사용하며, 루프 실패·patch 실패·펜싱된 클러스터 선택은 단위 테스트로 검증한다.

리더가 바뀌는 순간 옛 리더의 루프는 새 리더의 ACK로 덮이지 않는다.
그래서 `fence`는 ACK 뒤에도 `off-check`를 한 번 더 실행한다.
이 검사는 조회 시점의 상태다. OFF 절차 중에는 새 RR 생성 등 별도 운영 작업을 조율한다.

단위 테스트는 `scheduling/ops`에서 `../../.venv/Scripts/python -m unittest -v test_eks_gate.py test_eks_spare.py`로 실행한다.
실제 클러스터 ACK·배정 확인은 `scheduling/e2e/tests/recovery/test_14_fenced_ack.py`를 실행한다.
