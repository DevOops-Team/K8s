# 학습 복구 컨트롤러

Kafka 수신 → DynamoDB 영속 inbox → 중복 확인 → 복구 판단 → 실행 권한 전환 → 이전 실행 종료 확인 → 재배치 요청 → 체크포인트 로드·첫 step 완료 확인 → 결과 Kafka 발행을 구현했습니다. 실제 인프라에 연결하거나 적용하지 않았습니다. HTTP 재배치 실행기는 다른 담당이 구현해야 하며 contracts.md는 이번 코드의 제안 규약입니다. 팀의 gpu.fault v1/ADR-10과 맞춰야 합니다.

## 실행 모드

- config.example.json: 외부 연결 없는 관찰용 HTTP 프로세스.
- config.kafka.example.json: Kafka/DynamoDB에 연결하되 기본 mode=observe, execution_enabled=false. 이벤트를 OBSERVED로 기록하고 recovery.action에 결과 발행. 실행 번호 변경·재시작 없음.
- 실제 실행은 mode=execute, execution_enabled=true를 함께 지정. executor_url·자격증명·규약 구현을 먼저 준비. observe로 이미 처리한 사건을 같은 그룹/테이블에서 execute로 바꿔 자동 재실행하지 않습니다. 새 실험·사건 ID를 사용하세요.

현재 단일 Pod/단일 workflow worker, Recreate 배포 전략을 사용합니다. 리더 선출을 구현하지 않았으므로 복제 수는 1개만 지원합니다. DynamoDB는 재시작 시 상태를 보존하고 작업별 CAS 잠금을 사용합니다. HTTP 실행기는 반드시 Idempotency-Key를 영속 중복 제거해야 합니다.

## 필요한 설정

1. Kafka 주소·입력 gpu.fault·출력 recovery.action·전용 consumer group.
2. DynamoDB 테이블(문자열 partition key pk, sort key 없음). 기존 데이터 테이블과 규약을 맞추기 전에는 별도 실험 테이블 사용. 이 코드는 테이블을 생성하지 않습니다.
3. 작업 제어 서비스가 job-record.example.json 형식의 현재 작업 레코드를 먼저 등록하고, 승인 checkpoint_uri·active_nodes를 갱신해야 합니다. 임의 이벤트의 경로·GPU 수를 신뢰하지 않습니다.
4. HTTPS 재배치 실행기 API. fence/restart/status 계약은 contracts.md 참고.
5. Kafka/AWS/실행기 자격증명. Pod ServiceAccount token은 사용하지 않습니다. 아래 Secret을 별도로 준비하고 Git에 실제 값을 저장하지 마세요.

Secret 이름 platform/recovery-controller-credentials, 키:
- KAFKA_CLIENT_CONFIG_JSON: confluent-kafka 공통 인증/TLS JSON. 예: security.protocol, sasl.mechanism, sasl.username, sasl.password. 사설 CA는 이미지 또는 별도 읽기 전용 볼륨으로 준비.
- EXECUTOR_TOKEN: execute 모드의 Bearer token.
- AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY 및 필요한 경우 AWS_SESSION_TOKEN: 온프레미스 Pod의 AWS 인증 예시. 운영에서는 단기 자격증명 경로를 준비. EKS Pod용 IAM만으로 온프레미스 Pod가 인증되는 것은 아닙니다.

DynamoDB 권한: 해당 테이블의 GetItem/PutItem/UpdateItem/Scan. Kafka 권한: 입력 읽기·그룹 사용·결과 쓰기. S3 다운로드·노드 변경·JobSet 생성은 HTTP 실행기와 학습 코드의 역할이며 이 Pod에는 그 권한을 주지 않습니다.

## 판단 정책

software_transient → 같은 EKS 재시작, software_permanent → REJECTED,
node_hardware/spot_interruption → 같은 EKS 정상 노드로 이동,
straggler/unknown → REVIEW,
az_failure → 이전 실행을 확실히 멈춘 뒤에만 다른 EKS 대피 요청.

실행 번호 변경은 이 컨트롤러의 DynamoDB CAS 한 곳이 담당합니다. 실행기는 번호를 다시 올리지 않습니다. 권한 전환은 이전 프로세스를 종료하지 않으므로 fence가 실제 종료를 확인하기 전에는 restart를 호출하지 않습니다. GPU 수와 승인 S3 경로는 유지합니다. 최초 체크포인트가 없으면 자동 새 실행을 만들지 않고 검토 대상으로 남깁니다.

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
- 최신 authoritative job record를 갱신하는 작업 제어 서비스와 체크포인트 승인자는 generation/recovery_id 조건을 지켜야 합니다. 이전 학습의 결과 승인도 현재 generation 조건부 검사로 막아야 전체 펜싱이 성립합니다.
- 현재 Argo CD 적용·이미지 게시·실제 장애 주입은 수행하지 않았습니다. 향후 GitOps 전환 시 실제 이미지·설정·replicas를 Git에 먼저 반영하세요.

참고: https://docs.confluent.io/kafka-clients/python/current/index.html
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
