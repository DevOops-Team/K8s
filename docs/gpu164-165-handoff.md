# GPU-164·165 설정과 검증 인계

## GPU-164: 테스트 규모와 원복

운영 큐 매니페스트는 Gitops 담당자가 적용한다. 이 변경은 컨트롤러와 kind/KWOK 테스트만 포함한다.
테스트 프로필은 `scheduling/e2e/harness/topology.py` 한곳에서 관리한다.

| E2E_PROFILE | 워커 | 학습/예비 합계 | 관리 Heavy/Light/Recovery |
|---|---|---|---|
| gpu10 (기본) | worker1, worker2 | 8/2 | 6/2/4 |
| gpu10-5-3 | worker1, worker2 | 8/2 | 5/3/4 |
| full | worker1, worker2, worker3 | 12/3 | 8/4/4 |

6/2는 Heavy 최소 요청 3장과 Light 최대 요청 2장을 각각 정수 배수로 둔 기본안이다.
5/3보다 처리량이 우수하다는 실측 결론은 아니다. 두 안 모두 차용을 유지하며 비교할 수 있다.
나중에 기존 8/4로 돌아갈 때는 `E2E_PROFILE=full`을 선택하고 담당자가 관리 큐·워커 목록을 같은 프로필에 맞춘다.
코드 백업 사본이나 임시 숫자 덮어쓰기는 필요 없다.

워커별 학습 quota 4, 복구 전용 quota 1(학습에 대여하지 않음), 학습 노드 4와 `devoops/spare=true:NoSchedule` 예비 노드 1을 기준으로 한다.
관리 Recovery quota 4는 최대 잡 크기의 쿼터이며 물리 예비 GPU 2장을 의미하지 않는다.
관리 Heavy/Light/Recovery는 같은 training cohort, 기존 FIFO·무선점·차용 정책을 유지한다.
한 클러스터가 포화되면 다른 클러스터 전체 장애를 즉시 수용할 수 없으며, 관리 quota 획득과 실제 입장을 따로 확인해야 한다.
실제 EKS GPU 수·Ready 상태·배포 설정 반영은 이 로컬 코드 검사로 확인하지 않았다.

## GPU-165: 배치 계약

- `restartInPlace`와 `moveToSpare`는 새 JobSet에 `devoops/recovery-source-cluster=<원래 EKS>`를 기록한다.
- `devoops/target-cluster`는 일반 학습·명시적 고정 제출에서 계속 강제 제한이다. 복구 원래 클러스터 선호와 구분한다.
- 디스패처는 격리 후 일반 GPU+예비 GPU 및 쿼터가 충분하면 원래 EKS를 우선한다. 부족하면 다른 정상 EKS 중 학습 찜 없는 곳을 먼저 고른다.
- 찜 있는 곳만 수용 가능하면 복구 우선으로 배치하고 잡·클러스터를 로그에 남긴다. 다른 복구의 보호 자리는 침범하지 않는다.
- 어디에도 전체 잡 자리가 없으면 원래 EKS에서 대기한다. 이미 지명했으나 입장하지 못한 잡은 기존 RENOMINATE_SECONDS 경과 후 후보를 다시 본다.
- 원래 클러스터가 부족한 재제출본에는 이전 자리 보호를 유지하지 않고, 다른 곳을 지명한 루프에서도 자기 자리 보호를 해제한다.
- `moveToOtherCluster`·`evacuateCluster`는 기존처럼 source를 exclude하고 원래 우선 힌트를 기록하지 않는다.
- relocator는 기존 `RelocationRequest.status.sourceCluster/targetCluster`로 원래/실제 입장 클러스터를 기록한다. 새 CRD 필드는 없다.
- 복구 컨트롤러는 두 노드 복구 액션이 다른 EKS에서 재개해도 완료 판정을 진행한다. 기존 세대·복구 ID·JobSet UID·현재 owner와 일치하는 배치·복원·학습 step 증거가 모두 필요하다.
- 고장 노드 이름 제외는 원래 클러스터에 재개할 때 적용한다. 서로 다른 클러스터의 동명 노드는 별개다. 클러스터 대피가 실패한 source로 돌아오는 경우는 계속 HOLD한다.
- ADR-37의 이전 허용 여부·가용성 사전 확인/HOLD 조건은 바꾸지 않았다.

추가 IAM·RBAC 동사나 환경 변수는 없다. 새 JobSet annotation을 읽는 dispatcher와 이를 만드는 relocator를 함께 배포하고, 다른 EKS 완료를 허용하는 복구 컨트롤러도 함께 배포한다.
기존에 이미 만들어져 target-cluster로 고정된 재제출 JobSet은 이 코드가 소급 변경하지 않는다. 새로운 RR로 세대를 임의 증가시켜 우회하지 않는다.

## VM 검증

설치된 kind-manager/worker1/worker2, Kueue·JobSet·RR CRD/RBAC·KWOK GPU 노드가 있는 VM에서 저장소 루트 기준:

```bash
git pull --ff-only origin develop
cd scheduling/e2e
E2E_PROFILE=gpu10 bash run.sh tests/recovery/test_15_cross_cluster.py
```

이 테스트는 `restartInPlace`와 `moveToSpare`를 각각 실행한다. A에서 4장 잡을 실행한 뒤 예비와 학습 노드 하나를 사용 불가로 만들어 A에 3장만 남긴다.
새 세대는 B에 Pod 4개가 Running이어야 한다. 원래 실행 종료, action 유지, source/target status, 원래 클러스터 우선 힌트, 격리 여부, 두 세대 동시 실행 여부도 확인한다.
KWOK 테스트는 복구 컨트롤러의 세대 증가·RR 생성을 하네스가 모사한다. 실제 S3 체크포인트 복원과 trainer step 증가·Kafka/DynamoDB 완료까지 증명하지는 않는다.
클러스터 없이 검증하는 복구 컨트롤러 단위 테스트는 다른 EKS의 유효한 증거로 완료하고 잘못된 증거는 거부하는 경로를 검사한다.

결과는 `scheduling/e2e/logs/<시각>/summary.md`, `results.json`, `controllers-*.log`에 남는다.
같은 VM에서 별도 실행한 run-local 컨트롤러는 먼저 중지한다. 운영 클러스터에서는 이 초기화 하네스를 실행하지 않는다.
`cluster/setup.sh`·`bootstrap.sh`는 Gitops로 이동한 매니페스트 경로를 참조하는 기존 스크립트이므로 새 환경 설치 도구로 사용하지 않는다. 이미 설치된 테스트 클러스터가 없으면 담당자의 현재 kind 설치 절차가 먼저 필요하다.
