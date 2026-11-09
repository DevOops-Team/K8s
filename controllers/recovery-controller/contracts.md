# 복구 연동 규약 — ADR-19 CRD 연결

## 역할과 요청

복구 컨트롤러는 Kafka 판단·세대 증가·RelocationRequest 생성·실제 학습 재개 확인을 담당합니다.
relocator는 노드 격리·옛 실행 종료 확인·JobSet 작성과 재제출을 담당합니다.
이 컨트롤러는 관리 Kubernetes API만 호출하며 EKS Node/NodeClaim/JobSet을 변경하지 않습니다.

관리 클러스터의 scheduling.devoops.site/v1alpha1, RelocationRequest를
job_namespace(기본 training)에 생성합니다. 이 namespace는 실제 JobSet과 같아야 합니다.
배포 렌더러가 해당 namespace의 create/get RoleBinding을 platform/recovery-controller에 연결합니다.
CRD와 namespace는 scheduling 구성을 먼저 설치해야 합니다.

- requestId = fencing recovery_id = 입력 event_id
- jobRef = training/job-{job_id}-g{원래 세대 4자리}
- action = restartInPlace / moveToSpare / evacuateCluster
- moveToSpare에는 excludeNodes, evacuateCluster에는 excludeClusters 설정
- reason은 현재 CRD enum에 맞춰 변환
- 리소스 이름은 event_id의 SHA256에서 생성. POST 응답 유실은 같은 이름 재요청,
  409이면 GET하여 spec 전체 일치를 확인합니다.
- RR status는 읽거나 판단에 사용하지 않습니다. old-execution-stopped 어노테이션을 자동 생성하지 않습니다.

## 액션 변환

| suggested_action | 컨트롤러 액션 | 요청 |
|---|---|---|
| restart_in_place | RETRY_GROUP | restartInPlace |
| isolate_migrate | QUARANTINE_AND_RETRY | moveToSpare |
| reset_then_migrate | QUARANTINE_AND_RETRY | moveToSpare. GPU reset은 후속 운영 |
| evacuate_preemptive | PLANNED_MIGRATE | 현재 HOLD. 사전 체크포인트 요청/응답과 제한 시간 계약 필요 |
| fence_evacuate | FENCE_AND_EVACUATE | evacuateCluster |
| hold | HOLD | 없음 |
| refuse | NEEDS_CONFIG_FIX | 없음. 결과 state=REJECTED |
| none | NONE | 없음. 결과 state=IGNORED |

기존 fault_type 입력도 새 내부 액션으로 변환합니다.
collector_healthy=false인 비-site 이벤트는 HOLD로 낮춥니다.
job_id/generation을 특정하지 못한 신호는 기록하고 HOLD합니다.
실제 이벤트 cluster는 eks-a/b/c 계약을 유지합니다. worker-a 같은 별칭은 생산자와 맞춰야 합니다.
원본 v1과 payload 봉투 입력 모두 수용하며 Kafka rule_version 헤더를 inbox에 보관합니다.

예비 소진·SDC의 moveToOtherCluster 자동 승격은 아직 구현하지 않았습니다.
클러스터 전체 펜싱인 evacuateCluster로 대신 승격하지 않습니다.
Watchdog 단절은 아래 HOLD 규칙으로 처리합니다.

## DynamoDB 진행

fencing_table(PK job_id)과 recovery_table(PK pk)은 분리합니다.
첫 세대 증가 조건은 기존 세대·source owner·다른 recovery_id 없음입니다.
첫 조건부 UpdateItem은 CLAIMED와 recovery_id/from/to, 복원 기준을 기록합니다.
이 단계에서 generation·owner·retry_count는 유지합니다. 다음 조건부 UpdateItem에서
generation+1, owner 제거, GENERATION_ADVANCED, retry_count+1을 기록합니다.
승인본은 유지합니다. 재처리는 세대를 다시 올리거나 새 owner를 지우지 않습니다.

relocator가 OLD_RUN_STOPPED와 NEW_RUN_SUBMITTED(new_jobset_name/uid 포함)를 기록합니다.
컨트롤러는 NEW_RUN_SUBMITTED 이후 새 owner가 확인되면 RESUMING을 기록합니다.
COMPLETED는 새 세대의 progress 증거, 복원 지점보다 높은 completed_step,
실제 새 배치 정보와 owner를 확인한 뒤에만 기록합니다.

새 배치 정보는 recovery_table의 기존 job#ID 항목에
placement_generation=새 세대, cluster=새 owner, active_nodes=실제 참여 노드 목록으로 반영되어야 합니다.
참여 노드 수는 gpu_count와 일치하고 중복·고장 노드가 없어야 합니다.
이 정보의 생산자 연결은 별도 통합 작업입니다. 현재 relocator는 이 테이블을 갱신하지 않으므로,
해당 연결 전에는 안전하게 RESUMING에 머뭅니다. Pod Running이나 RR Done만으로 완료하지 않습니다.

완료 기록 후 복구 잠금을 해제하고 COMPLETED 단계는 남깁니다.
별도 inbox 결과는 recovery.action으로 최소 한 번 발행합니다.
HOLD/IGNORED는 별도 dedup_key 소비 없이 저장하며 같은 사건의 새 event_id 재판정을 받을 수 있습니다.
event_id와 원본 내용의 충돌은 거부합니다. 사건 간 dedup_key 영속 소비는 추가 통합 대상입니다.

## 적용·전환 경계

executor_url와 EXECUTOR_TOKEN은 더 이상 사용하지 않습니다.
이전 HTTP 실행기와 진행 중인 복구를 동시에 운영하지 않습니다.
이미 진행 중인 HTTP 복구는 먼저 종료 또는 수동 점검한 뒤 CRD 방식으로 전환합니다.
기존 render_recovery 함수는 이전 import 호환을 위해 남기지만 이미지/런타임은 사용하지 않습니다.
운영 재시도 상한·단계 대기는 구현되어 있습니다(아래 제한 정책). 사전 저장 handshake·SDC 승격·독립 펜싱 증거 자동 검증은 미구현입니다.


## 11/05 용어 통일

외부 액션은 RETRY_GROUP / QUARANTINE_AND_RETRY / PLANNED_MIGRATE /
FENCE_AND_EVACUATE / NEEDS_CONFIG_FIX / HOLD 로 통일합니다.
새 내부 검토 결과도 HOLD로 기록하고 상세 원인은 reason에 남깁니다.
기존 inbox REVIEW 레코드는 읽을 때 HOLD로 변환합니다.
moveToOtherCluster는 같은 클러스터 재시도의 승격 경로이며 evacuateCluster와 구분합니다.
Watchdog 단절은 ADR-18 후속 보완으로 HOLD·알림을 구현했습니다.

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

- 승인본이 전혀 없는 복구는 규약 10절대로 처음부터 재시작합니다. 불완전한 승인 필드는 계속 HOLD하고, 기존 승인본 다운로드/검증 실패를 승인본 없음으로 취급하지 않습니다. 더 엄격한 작업별 `allow_restart_from_scratch=false`도 지원합니다.
- 복원 기준은 CLAIMED에서 고정합니다. 승인본 없음의 기준은 `resume_gen=null`, `resume_step=0`이며, 진행 신호 `started_from_scratch=true`와 실제 첫 step 증가가 있어야 완료합니다.
- 경고 `checkpoint_warning=NO_APPROVED_CHECKPOINT_RESTART_FROM_SCRATCH`는 영속 inbox와 최종 recovery.action에 보존합니다. 이것은 새 승인 체크포인트를 임의로 만드는 동작이 아닙니다.
- 작업당 자동 복구 한도는 `retry_policy.max_recovery_attempts`로 설정하며 기본 3회입니다. 세대 전환의 조건부 갱신에도 같은 상한을 사용하고, 진행 중인 마지막 허용 시도의 재처리는 계속 허용합니다.
- relocator는 Pod 조회 권한 403을 이전 실행 종료로 인정하지 않습니다. 실행기 자격증명에 해당 namespace의 Pod 조회가 필요하며, 실제 Pod 0개 확인 또는 별도 검증한 운영자 종료 증거가 있어야 다음 단계로 갑니다.
- 종료를 대신하는 클러스터 펜싱 증거는 문서상 합의 대기이므로 자동 승인하지 않습니다. SDC 이전 정상본 판단·예비 소진 승격 신호·사전 체크포인트 handshake는 추가 합의/연동 대상으로 유지합니다.
