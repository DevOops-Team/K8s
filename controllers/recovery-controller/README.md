# 학습 복구 컨트롤러

Kafka gpu.fault → DynamoDB 영속 inbox → 판단·세대 증가 → 관리 클러스터
RelocationRequest 생성 → DynamoDB 단계 확인 → 학습 진행 확인 → Kafka 결과 발행 구조입니다.
최신 역할·필드·액션 대응과 미연결 범위는 [contracts.md](contracts.md)를 기준으로 합니다.

## 배포와 인증

Deployment·ServiceAccount는 platform/recovery-controller입니다. 최신 스케줄링 문서와 일치하는 현재 배포 기준이며, 과거 IAM 요청에 적힌 recovery namespace를 추가하지 않습니다. 이름을 바꾸려면 Argo destination·Lease·RBAC subject·Secret·온프레 AWS 인증 연결을 함께 검토해야 합니다. [팀 정합성 기준](../../docs/team-contract.md)을 참고합니다.
기본 매니페스트는 replicas=0이고 렌더러는 실제 digest로 고정된 이미지와 3개 복제본을 생성합니다.
Kafka 모드에서는 Lease 리더만 소비·복구 작업을 실행합니다.
관찰 모드의 사건은 OBSERVED로 끝나며 실행 모드 전환 뒤 자동 재실행하지 않습니다.

실행은 mode=execute, execution_enabled=true, redispatch_policy=controller-only,
scheduler_policy_confirmed=true를 요구합니다. job_namespace는 기본 training입니다.
해당 namespace에 scheduling RelocationRequest CRD와 relocator를 먼저 설치해야 합니다.
요청 생성은 create/get 권한만 사용합니다. Node·NodeClaim 변경 권한은 relocator 담당입니다.

AWS 인증은 boto3 기본 인증 체인을 사용합니다. Roles Anywhere helper, 인증서/키,
AWS config 마운트와 AWS_PROFILE/AWS_CONFIG_FILE은 별도 배포 연결이 필요합니다.
리전은 예시 us-east-2, fencing_table은 devoops-prod-fencing입니다.
recovery_table의 실제 이름과 초기 job 배치 메타데이터는 별도 준비합니다.
DynamoDB 권한은 fencing GetItem/UpdateItem, recovery GetItem/PutItem/UpdateItem/Scan입니다.
Kafka 인증은 platform/recovery-controller-credentials의 KAFKA_CLIENT_CONFIG_JSON으로 공급합니다.
config.kafka.example.json의 브로커 3개 주소는 11/09 데이터 파트의 전환 목표입니다. 실제 KRaft 설치·advertised.listeners·인증/ACL 확인 후 사용합니다.
모니터링 서비스 VIP 172.30.0.35는 예약 상태이므로 watchdog은 기본 비활성화입니다. Alertmanager의 실제 URL·포트와 접근 경로를 확인한 뒤 URL을 입력하고 활성화합니다.
이 컨트롤러는 EC2/S3/SQS API나 EKS API를 직접 호출하지 않습니다.

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


## 연결 상태와 검증 경계

단위 테스트는 요청 재전송, 세대 증가 중복 방지, hold/refuse/none 비실행,
옛 승인본·새 배치 증거 부재 시 완료 금지를 검증합니다.
실제 Kafka/AWS/Kubernetes 통합·이미지 게시·클러스터 배포는 별도 작업입니다.
새 참여 노드 메타데이터 생산자가 연결되기 전에는 RESUMING에서 완료를 보류합니다.
회수 예고 evacuate_preemptive(legacy spot_interruption)는 QUARANTINE_AND_RETRY → moveToSpare로 처리합니다.
세대를 먼저 증가시키고 옛 실행 종료 확인 후 같은 클러스터의 예비 노드로 재배치해 마지막 승인본에서 재개합니다.
마지막 저장 후 세대 증가(PLANNED_MIGRATE)는 향후 확장으로 보류합니다. 이미 HOLD된 사건은 자동 재개하지 않습니다.
moveToOtherCluster 자동 승격은 미구현입니다. Watchdog HOLD는 아래와 같이 구현했습니다.

전체 제한과 기존 HTTP 복구에서의 전환 절차는 contracts.md를 참고하세요.

## Watchdog 단절 시 HOLD — ADR-18 후속 보완

config.watchdog.enabled=true로 활성화합니다. 예시 Alertmanager 주소는
http://172.30.0.35:9093/api/v2/alerts입니다. 호스트는 새 모니터링 클러스터의 서비스 VIP로 변경했습니다.
`/api/v2/alerts`는 기존 조회 경로를 유지한 값이며, 모니터링 스택 기동 후 태림 담당자의 정확한 경로 확인이 필요합니다.
이 변경은 로컬 설정 반영이며 실제 배포·접속 검증 완료를 뜻하지 않습니다.
관리 클러스터 리더가 30초마다 active Watchdog의 updatedAt을 확인합니다.
오래 캐시된 firing 또는 단순 HTTP 200을 새 생존 신호로 인정하지 않습니다.
Watchdog 갱신이 300초 동안 없고 같은 기간 유효한 gpu.fault도 수신하지 못하면
monitoring_hold=true로 신규 RECEIVED 사건의 세대 증가·재배치 요청을 보류합니다.
진행 중 복구의 단계 확인은 계속하며 학습 자동 재시작은 수행하지 않습니다.
첫 관찰 후에도 300초 유예하고, 시각은 recovery_table의 watchdog#monitoring에
저장하여 리더 교체 후 이어갑니다. Alertmanager와 관리 노드는 시계를 동기화해야 합니다.
유효한 Watchdog 복귀 또는 fault 수신 시 보류 조건을 재평가합니다.

단절 회차별 HOLD 알림은 영속 inbox/outbox를 통해 recovery.action에 발행합니다.
action=HOLD, state=HOLD, job_id/generation=null,
reason=watchdog_missing_and_no_gpu_fault입니다.
event_id=watchdog-<마지막 생존/최초 관찰 epoch>:result로 최소 한 번 전달하므로
수집기는 event_id로 중복 제거해야 합니다. 실제 Slack 전달은 결과 토픽 소비 측 연결이 필요합니다.
기존 Monitoring의 Watchdog → healthchecks.io(1분 주기·5분 미수신 알림)는 유지합니다.
모니터링 주소·네트워크·실제 단절 통합 검증과 배포는 별도 확인 대상입니다.

## 표 기준 상태 전환과 학습 진행 신호

CLAIMED → GENERATION_ADVANCED → OLD_RUN_STOPPED → NEW_RUN_SUBMITTED →
RESUMING → COMPLETED 순서입니다. CLAIMED 기록은 기존 승인본을 조건부로 확인한 후
복원 기준 recovery_resume_gen/recovery_resume_step을 고정합니다.
프로세스가 CLAIMED 직후 중단되어도 같은 recovery_id로 이어가며,
세대 증가와 retry_count 증가는 한 번만 수행합니다.

COMPLETED는 checkpoint 승인과 독립입니다. 예를 들어 generation=2 / owner=eks-b에서
1번 실행 1200스텝을 복원하고 1201스텝 계산이 완료됐음을 확인하면,
approved_gen=1 / approved_step=1200 상태 그대로 복구 완료가 가능합니다.
이후 사이드카가 2번 실행의 1500스텝을 승인하면 approved 필드만 바뀝니다.
컨트롤러는 승인본을 새 세대로 임의 변경하지 않습니다.

진행 증거는 recovery_table의 기존 job#ID 레코드에 넣는 progress 객체를 읽습니다.
progress-record.example.json을 참고하세요. 신뢰된 학습/모니터링 수집 측이 다음을 기록해야 합니다.
- recovery_id, generation, cluster, jobset_uid(펜싱 기록 new_jobset_uid와 일치)
- checkpoint_loaded=true, restored_gen, restored_step(고정된 복원 기준과 일치)
- completed_step(정수, restored_step보다 큰 실제 완료 스텝)
- 같은 레코드의 placement_generation, cluster, active_nodes(요청 GPU 수만큼 실제 새 참여 노드)
승인본이 없으면 확정 규약 10절에 따라 처음부터 재시작하고 경고를 남깁니다. 해당 작업의 allow_restart_from_scratch=false는 더 엄격한 보류 정책이며, 재개 완료에는 started_from_scratch=true와 실제 step 증가가 필요합니다.
Pod Running, owner 선점, RR Done, heartbeat만으로 COMPLETED로 처리하지 않습니다.
이 진행 정보의 생산자와 AWS 실제 연결은 아직 별도 통합 대상입니다.
기존 progress가 없는 레코드는 RESUMING에 머물며 자동 성공으로 간주하지 않습니다.

## 제한 정책 (2026-11-09 로컬 구현)

- 일시적 DynamoDB/Kubernetes API 오류는 같은 단계에서 총 3회(최초 포함). 시도 전 기록하며, 리더 교체·응답 유실도 보수적으로 시도 1회로 계산한다. 정상 조회 반복은 재시도 횟수에 포함하지 않으며 단계 시각을 연장하지 않는다. 영구 오류는 즉시 HOLD. SDK 내부 재시도는 꺼 이중 재시도를 방지한다.
- 단계별 대기 기본값: 소유권/세대 전환·완료 기록 300초, 옛 실행 종료 900초, 재제출 900초, 복원·실제 학습 진행 확인 1800초. 정책 예시값으로 retry_policy에서 변경 가능하며 체크포인트 문서의 확정 시간으로 간주하지 않는다. 초과 시 HOLD. HOLD는 요청 취소·이미 시작한 relocator 작업의 중단을 뜻하지 않으며 자동 새 세대 제출을 멈춘다.
- 작업 전체 자동 복구 최대 3회. fencing retry_count를 조건부로 검사하고 세대 전환과 한 번만 증가시킨다. 4번째는 action=NEEDS_CONFIG_FIX, state=REJECTED. 3번째 복구 진행 중 재조회·재처리는 허용한다.
- Kafka 결과 outbox는 삭제하지 않는다. 전송 실패 시 5→10→20초…최대 300초 간격으로 재전송한다. 전송 시도·다음 시각은 별도 저장하며, 이미 완료된 학습 복구를 다시 수행하지 않는다. 결과 event_id는 동일하므로 수집기는 중복 제거해야 한다.
- HOLD 알림은 구조화 로그와 recovery.action 사건으로 내보낸다. Slack 직접 전송은 없고 모니터링/수집기 알림 연결이 필요하다. Kafka 장애 중에는 사건이 쌓이므로 로그 기반 알림 경로를 따로 확보해야 한다.
- DB 자체 장애로 시도 기록을 저장할 수 없으면 외부 복구 동작을 실행하지 않는다. DB 연결 상태 확인과 Lease/Watchdog 폴링은 상시 감시이며 3회 중단 대상이 아니다.
- 이 정책은 복구 컨트롤러 범위다. EKS ckpt-init의 S3 다운로드·체크섬 검증 재시도, relocator 내부 API 호출 및 단계별 취소 정책은 각 담당 구현과 별도로 맞춰야 한다.

## 2026-11-09 체크포인트·장애 복구 규약 대조 보완

- 승인본이 전혀 없는 복구는 규약 10절대로 처음부터 재시작합니다. 불완전한 승인 필드는 계속 HOLD하고, 일시 다운로드 실패를 승인본 없음으로 취급하지 않습니다. 확인된 코드 7은 아래 fallback 정책으로 구분합니다. 더 엄격한 작업별 `allow_restart_from_scratch=false`도 지원합니다.
- 복원 기준은 CLAIMED에서 고정합니다. 승인본 없음의 기준은 `resume_gen=null`, `resume_step=0`이며, 진행 신호 `started_from_scratch=true`와 실제 첫 step 증가가 있어야 완료합니다.
- 경고 `checkpoint_warning=NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH`는 영속 inbox와 최종 recovery.action에 보존합니다. 이것은 새 승인 체크포인트를 임의로 만드는 동작이 아닙니다.
- 작업당 자동 복구 한도는 `retry_policy.max_recovery_attempts`로 설정하며 기본 3회입니다. 세대 전환의 조건부 갱신에도 같은 상한을 사용하고, 진행 중인 마지막 허용 시도의 재처리는 계속 허용합니다.
- relocator는 Pod 조회 권한 403을 이전 실행 종료로 인정하지 않습니다. 실행기 자격증명에 해당 namespace의 Pod 조회가 필요하며, 실제 Pod 0개 확인 또는 별도 검증한 운영자 종료 증거가 있어야 다음 단계로 갑니다.
- 종료를 대신하는 클러스터 펜싱 증거는 문서상 합의 대기이므로 자동 승인하지 않습니다. SDC 이전 정상본 판단·예비 소진 승격 신호·사전 체크포인트 handshake는 추가 합의/연동 대상으로 유지합니다.

## ckpt-init 실패 연동

`init_failure.py`에 종료 코드 2·3·4·5·6·7 판단과 Pod 종료 상태 → 사건 변환 어댑터가 있습니다. 코드 7의 확인된 손상 승인본은 recovery_table에 영속 격리하며, 같은 승인본으로 새 claim/재배치 요청을 만들지 않습니다. `prev_approved`가 있으면 이전 승인본으로 전환하고 없거나 손상이 확인되면 처음부터 학습합니다. 이전 승인본도 코드 7이면 다음 세대는 처음부터 학습합니다. 실제 파일 검증은 ckpt-init이 수행합니다. 선택·포인터 전환은 조건부 갱신하고 retry_count는 초기화하지 않습니다. recovery.action에는 stage·retry_count·exit_code·failure_class를 추가했습니다.

실제 EKS 탐지기에서 어댑터를 호출해 Kafka로 보내는 연결, 신뢰된 현재 JobSet 메타데이터 기록, 소비자의 사건 스키마 적용은 담당 파트와 통합해야 합니다. 상세 입력·판단·제한은 [contracts.md의 ckpt-init 절](contracts.md#ckpt-init-종료-사건과-판단-2026-11-09-로컬-구현)을 따릅니다.


## 복구 증거 v1

배치·복원·첫 step의 기록/검증 계약과 남은 생산자 인증 연동은 [evidence-contract.md](evidence-contract.md)를 따릅니다. EvidenceWriter의 조건부 기록 함수와 Workflow의 버전·주체·신선도·현재 실행 검사를 구현했습니다. 실제 생산자/인증 어댑터는 별도 연결이 필요하며 producer 문자열만으로 인증했다고 보지 않습니다. 증거 누락은 제한 시간까지 대기하고, 신뢰된 실패 또는 시간 초과는 HOLD합니다.
