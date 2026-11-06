# 학습 복구 컨트롤러

Kafka 수신 → DynamoDB 영속 inbox → 중복 확인 → 복구 판단 → 실행 권한 전환 → 이전 실행 종료 확인 → 재배치 요청 → 체크포인트 로드·첫 step 완료 확인 → 결과 Kafka 발행을 구현했습니다. 실제 인프라에 연결하거나 적용하지 않았습니다. HTTP 재배치 실행기는 다른 담당이 구현해야 하며 contracts.md는 이번 코드의 제안 규약입니다. 펜싱 테이블은 현수의 체크포인트 규약 §5·9에 맞췄습니다. 실제 gpu.fault v1·재배치 API 규약은 담당자와 최종 연결해야 합니다.

## 실행 모드

- config.example.json: 외부 연결 없는 관찰용 HTTP 프로세스.
- config.kafka.example.json: Kafka/DynamoDB에 연결하되 기본 mode=observe, execution_enabled=false. 이벤트를 OBSERVED로 기록하고 recovery.action에 결과 발행. 실행 번호 변경·재시작 없음.
- 실제 실행은 mode=execute, execution_enabled=true를 함께 지정. A 정책을 실제 스케줄링 설정에 반영·검토한 후 redispatch_policy=controller-only, scheduler_policy_confirmed=true도 지정해야 합니다. executor_url·자격증명·규약 구현을 먼저 준비. observe로 이미 처리한 사건을 같은 그룹/테이블에서 execute로 바꿔 자동 재실행하지 않습니다. 새 실험·사건 ID를 사용하세요.

배포 시 Pod 3개를 서로 다른 워커에 배치합니다. Kafka 모드에서는 Kubernetes Lease를 얻은 리더 한 개만 Kafka를 소비하고 복구를 실행합니다. 대기 Pod는 Lease를 확인하며, 인계 시 DynamoDB inbox와 진행 상태를 읽어 작업을 이어갑니다. 리더는 12초 안에 Lease를 갱신하지 못하면 종료하고, 대기 Pod는 마지막으로 관찰한 갱신 이후 30초가 지나면 인계를 시도합니다. 전환 중 처리 중이던 외부 요청은 같은 Idempotency-Key로 재시도될 수 있으므로, HTTP 실행기는 해당 키를 영속적으로 중복 제거해야 합니다. 운영 중 Lease를 삭제하거나 초기화하지 않습니다. 이미지 교체 시 Recreate 전략으로 기존 Pod를 먼저 종료합니다.

## 필요한 설정

1. Kafka 주소·입력 gpu.fault·출력 recovery.action·전용 consumer group.
2. fencing_table: 현수의 펜싱 테이블. 문자열 PK=job_id, generation/owner/approved_gen/approved_step/heartbeat_at/ttl 사용. 예시 이름 devoops-job-fencing. 코드가 테이블을 생성하거나 기존 승인 필드를 초기화하지 않습니다.
3. recovery_table: 기존 우리 복구 진행/inbox 테이블. 문자열 PK=pk. 사건·단계·원본 JobSet 참조·배치 메타데이터용이며 승인본/실행 권한의 원본이 아닙니다. 두 테이블의 키 구조가 달라 분리합니다. 실제 이름은 인프라 담당과 연결합니다.
4. 백엔드/작업 제어 서비스가 recovery_table에 job-record.example.json 형식의 원본 jobset_uri·jobset_sha256·GPU 수·실제 cluster/active_nodes를 먼저 등록해야 합니다. fencing-record.example.json은 현수 펜싱 테이블용 별도 예시입니다. 임의 사건의 경로·GPU 수를 신뢰하지 않습니다.
5. HTTPS 재배치 실행기 API. fence/restart/status 계약은 contracts.md 참고.
6. Kafka/AWS/실행기 자격증명. Pod ServiceAccount token은 전용 Kubernetes Lease의 읽기와 갱신에 사용합니다. 아래 Secret을 별도로 준비하고 Git에 실제 값을 저장하지 마세요.

Secret 이름 platform/recovery-controller-credentials, 키:
- KAFKA_CLIENT_CONFIG_JSON: confluent-kafka 공통 인증/TLS JSON. 예: security.protocol, sasl.mechanism, sasl.username, sasl.password. 사설 CA는 이미지 또는 별도 읽기 전용 볼륨으로 준비.
- EXECUTOR_TOKEN: execute 모드의 Bearer token.
- AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY 및 필요한 경우 AWS_SESSION_TOKEN: 온프레미스 Pod의 AWS 인증 예시. 운영에서는 단기 자격증명 경로를 준비. EKS Pod용 IAM만으로 온프레미스 Pod가 인증되는 것은 아닙니다.

DynamoDB 권한: 우리 recovery_table의 GetItem/PutItem/UpdateItem/Scan, 현수 fencing_table의 GetItem/UpdateItem. init은 owner 선점을 위한 GetItem/UpdateItem 권한이 별도로 필요합니다. Kafka 권한: 입력 읽기·그룹 사용·결과 쓰기. S3 다운로드·노드 변경·JobSet 생성은 HTTP 실행기와 학습 코드의 역할이며 이 Pod에는 그 권한을 주지 않습니다.

## 판단 정책

software_transient → 같은 EKS 재시작, software_permanent → REJECTED,
node_hardware/spot_interruption → 같은 EKS 정상 노드로 이동,
straggler/unknown → REVIEW,
az_failure → 이전 실행을 확실히 멈춘 뒤에만 다른 EKS 대피 요청.

실행 번호 변경은 이 컨트롤러의 DynamoDB CAS 한 곳이 담당합니다. 실행기는 번호를 다시 올리지 않습니다. 권한 전환은 이전 프로세스를 종료하지 않으므로 fence가 실제 종료를 확인하기 전에는 restart를 호출하지 않습니다. 새 세대로 전환할 때 generation+1·recovery_id 기록·REMOVE owner를 하나의 조건부 갱신으로 처리합니다. 승인본 approved_gen/approved_step은 보존합니다. 같은 recovery_id 재처리에서는 새 init이 획득한 owner를 다시 지우지 않습니다. init이 새 세대 owner를 선점하고 사이드카가 generation+owner 조건으로 승인·하트비트를 갱신합니다. 컨트롤러는 완료 전에 실제 target owner도 강한 일관성으로 확인합니다.

체크포인트 URI를 요청/env로 넘기지 않습니다. 실행기에는 원본 jobset.json의 S3 URI/sha256과 새 generation을 전달하고, init이 현수 테이블의 승인본을 직접 읽습니다. 승인본이 없는 복구는 기본 REVIEW입니다. 작업별 allow_restart_from_scratch=true를 명시한 실험만 처음부터 재시작할 수 있습니다. GPU 수는 원본과 동일하게 유지해야 합니다.

## 로컬 테스트·빌드

```bash
cd controllers/recovery-controller
python3 -m unittest discover -v
python3 controller.py --event-file example-event.json
python3 controller.py --config config.example.json
# 빌드 환경에서 실행. Kubernetes 노드에 Docker Engine 설치는 불필요
docker build -t ghcr.io/YOUR_ORG/recovery-controller:VERSION .
```

예제 파일과 단위 테스트는 외부 SDK 설치 없이 실행됩니다. Kafka 실행에는 requirements.txt의 패키지가 필요합니다. 이미지 빌드·패키지 다운로드·실제 Kafka/AWS 통합 검증은 별도 수행해야 합니다.

## Argo CD 없이 수동 배포

namespace platform·platform-worker=true 노드와 private 이미지용 ghcr-pull Secret이 필요합니다. 기본 Git manifest는 replicas=0입니다. 실제 이미지 빌드·게시 후 digest로 고정하고 렌더링하세요. 렌더러는 클러스터를 변경하지 않습니다.

```bash
# gpu-k8s 루트
python3 -m pip install -r requirements-dev.txt
mkdir -p .build
python3 controllers/recovery-controller/render.py \
  --image ghcr.io/YOUR_ORG/recovery-controller@sha256:ACTUAL_DIGEST \
  --config controllers/recovery-controller/config.example.json \
  > .build/recovery-controller.yaml
# 렌더가 성공한 경우에만 적용
kubectl --context YOUR_CONTEXT apply -f .build/recovery-controller.yaml
kubectl --context YOUR_CONTEXT -n platform rollout status deployment/recovery-controller
kubectl --context YOUR_CONTEXT -n platform logs deployment/recovery-controller
```

Kafka 모드는 실제 값으로 채운 로컬 JSON을 --config로 지정하세요. .local.json은 Git 제외입니다. /healthz는 프로세스, /readyz는 Kafka 초기 접속·worker 최근 scan 상태를 나타내며 학습 정상 여부를 보장하지 않습니다. Service/Ingress는 필요 없고 probe는 Pod IP를 사용합니다.

## 재시도·운영 한계

- 입력 이벤트를 DynamoDB에 저장한 뒤 Kafka offset을 동기 커밋합니다. 저장 실패는 커밋하지 않으며 종료합니다. 재전달은 event_id+본문 hash로 중복 제거합니다. 잘못된 JSON·규약 위반도 커밋하지 않아 수정/격리 운영이 필요합니다. DLQ는 미구현입니다.
- worker는 단계를 DynamoDB에 저장하고 다시 시작해 이어갑니다. 응답 유실은 같은 recovery_id로 재요청합니다. 펜싱 미확인·일시 장애는 현재 단계에 남아 재조회합니다. 운영용 전체 타임아웃·재시도 예산·수동 해제 API는 아직 없습니다.
- 격리·중단·재제출·실제 학습 step 판단의 진실성은 실행기 구현에 의존합니다. 단순 accepted/Pod Running만으로 성공 처리하지 않습니다.
- 결과 이벤트는 최소 한 번 전달입니다. 발행 후 기록 전 종료되면 중복 가능하므로 event_id로 수집기가 중복 제거해야 합니다. 중간 상태/감지 시간/ETTR 통합은 추가 구현 대상입니다.
- 소규모 실험용 table Scan을 사용합니다. 규모가 커지면 pending/outbox GSI·작업 보존/삭제 정책을 추가해야 합니다.
- 펜싱 테이블의 세대/owner/승인 정보와 우리 복구 기록의 역할을 구분합니다. 실행기는 init owner 선점·사이드카 generation+owner 조건을 실제로 지켜야 전체 펜싱이 성립합니다. 클러스터 owner는 같은 EKS 내부의 중복 실행을 막는 별도 실행 ID가 아닙니다.
- 현재 Argo CD 적용·이미지 게시·실제 장애 주입은 수행하지 않았습니다. 향후 GitOps 전환 시 실제 이미지·설정·replicas를 Git에 먼저 반영하세요.

참고: https://docs.confluent.io/kafka-clients/python/current/index.html
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html


## A안 + owner 2차 방어

onprem/scheduling/controller-only.config.example.yaml은 스케줄링 담당의 기존 Kueue Configuration에 병합할 예시입니다. v1beta2 기준 timeout/requeuingStrategy를 사용하고 backoffLimitCount=0으로 두었습니다. workerLostTimeout=8760h는 예시의 긴 대기 시간이며 영구 비활성화 스위치가 아닙니다. 실제 학습·장애 처리 시간에 맞게 정하고, 원본 작업 취소/정리 전에 같은 세대가 재배치되지 않는지 시험해야 합니다. 이 파일은 기본 Kustomization에 넣지 않았으며 기존 external dispatcher/MAP 설정을 덮어쓰지 않습니다. scheduler_policy_confirmed는 운영자 확인 값이지 런타임 설정을 직접 읽어 증명하는 기능이 아닙니다.

각 eks/eks-{a,b,c}/resources.yaml에 namespace training의 devoops-cluster-info ConfigMap을 추가했습니다. CLUSTER_NAME은 각각 eks-a/eks-b/eks-c입니다. 사건 cluster·owner·스케줄러 alias도 이 문자열로 통일하세요. 현수 ckpt-agent를 여기서 새로 구현하거나 배포하지는 않습니다.

jobsets.py의 render_recovery는 실행기가 재사용할 수 있는 원본 checksum 검증·합의 필드 변경 함수입니다. 이미지·GPU 수·학습 설정은 보존하고 이름/subdomain·복구 큐/우선순위·generation·recovery-id·torchrun master address만 변경합니다. CLUSTER_NAME ConfigMap 참조와 자동 재시도 비활성화를 검사합니다. 이 함수는 외부 실행기 서비스 자체를 구현한 것은 아니며, 실행기에서 호출해 매니저 API에 제출해야 합니다. 라우팅 어노테이션은 스케줄링 담당과 합의한 값 형식으로 실행기에서 적용하세요.

기존 config의 dynamodb_table은 recovery_table/fencing_table로 바뀌었습니다. 기존 pk=job# 형식 레코드를 현수 펜싱 테이블에 넣지 마세요. 테이블 생성·마이그레이션·현재 학습 owner 강제 제거는 수행하지 않았습니다. 동작 중 작업에 적용하기 전에 두 테이블 초기 등록·권한·자격증명·이벤트 규약을 시험하세요.
