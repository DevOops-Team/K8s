# 복구 증거 기록 계약 v1

상태: 한송이 컨트롤러 측 로컬 구현. 아래 생산자 이름·인증 매핑은 공동 연동을 위한 제안이며, 담당자 합의나 실제 인증 연결 완료를 뜻하지 않는다. 실제 배치·파일 복원·학습은 해당 생산자가 수행한다.

## 누가 무엇을 기록하는가

| 정보 | 사실 생성·협의 담당 | 신뢰된 기록 경로에 전달할 내용 |
|---|---|---|
| placement | 김성모 스케줄링 | 새 관리 클러스터 JobSet UID, 실제 EKS, 참여 노드 전체, 실행 세대·복구 ID |
| progress | 김현수·노찬경 데이터/학습 | trainer의 실제 상태 복원 결과와 첫 업데이트 완료. ckpt-init 다운로드 성공만으로 checkpoint_loaded를 선언하지 않음 |
| 완료 판단 | 한송이 복구 컨트롤러 | 증거를 fencing 원본과 대조하고 COMPLETED 또는 대기/HOLD 처리 |

기록 대상은 recovery_table의 `job#{job_id}`다. generation·owner·recovery_id·new_jobset_uid·recovery_resume_*의 원본은 fencing 테이블이다. 처음 작업을 등록하는 경로는 별도이며 EvidenceWriter는 없는 job 항목을 생성하지 않는다.

## GPU-254: 관리 JobSet UID와 생산자 연결

`fencing.new_jobset_uid`는 **relocator가 관리 클러스터 API로 생성한 JobSet의
metadata.uid**다. EKS의 MultiKueue 복사본 UID나 Pod UID를 넣지 않는다.
컨트롤러는 이 값을 기록하지 않고 배치·학습 증거와 대조한다.

rank 0 사이드카는 실제 실행 설정의 JOB_ID·GENERATION·CLUSTER를 기준으로
현재 fencing generation·owner·recovery_id·phase를 확인한다. `new_jobset_name`이
`job-{JOB_ID}-g{GENERATION:04d}`와 같고 `new_jobset_uid`가 비어 있지 않을 때,
그 관리 UID를 progress.jobset_uid에 사용한다. 이 경로에는 JOBSET_UID 환경변수나
EKS API 조회를 추가할 필요가 없다. 불일치·누락이면 기록하지 않고 다음 관측에서 재확인한다.

쓰기 순간 DynamoDB 조건은 다음을 모두 검사한다. 사전 읽기 후 Python 비교만으로
조건부 쓰기를 대신할 수 없다.

```text
generation = :my_generation AND #owner = :my_cluster
AND recovery_id = :recovery_id AND new_jobset_uid = :manager_jobset_uid
AND recovery_phase IN (:submitted, :resuming)
# :submitted = NEW_RUN_SUBMITTED, :resuming = RESUMING
```

펜싱에서 읽은 UID를 그대로 쓰는 것은 복구 식별자 대조이며 **보고 Pod의 실제 JobSet
소속을 독립 검증하는 인증 수단은 아니다.** 배포된 신뢰된 사이드카·IAM 쓰기 경계와
실제 trainer의 복원/완료 신호를 전제로 한다. 위 조건만으로 같은 세대·클러스터의
임의 쓰기 주체까지 구분한다고 가정하지 않는다. 배치 생산자는 별도로 실제 EKS의
Pod/Job/JobSet 소속을 확인해 참여 노드 정보를 수집하고 관리 UID와 연결해야 한다.

컨트롤러의 fencing 입력은 위 이름·관리 UID 및 기존 세대·owner·복구 ID를 검사한 뒤
EvidenceWriter에 전달한다. Writer가 현재 fencing·복원점·시각을 재검사하므로,
첫 읽기 이후 권한이 바뀌어도 과거 증거로 복구를 완료하지 않는다.

### 60초 관측 갱신과 event_id

새 관측에서 observed_at·completed_step·outcome이 바뀌면 **새 event_id**를 발급한다.
예: `j1-g0002-r1-progress-2000000000-1`, 다음 관측은
`j1-g0002-r1-progress-2000000060-2`. timestamp만으로 충돌하지 않도록 관측 번호나
UUID를 함께 사용한다. 동일 관측의 재전송은 ID와 본문 전체를 그대로 유지한다.

같은 event_id에 시각/step만 바꿔 60초마다 덮어쓰면 컨트롤러는 내용 충돌로 거부한다.
이는 실제 중복 전송이 수신 시각을 계속 새로 만들어 만료를 회피하지 못하게 하는 기존 규칙이다.
새 관측 ID로 갱신하면 낮은 step·오래된 관측·현재 실행의 실패 latch 검사를 그대로 적용한다.
새 ID여도 failure를 후속 success로 덮지 않는다. 관측 시각은 실제 trainer 상태를 확인한
시각이며 재전송 시각으로 바꾸지 않는다.

## 입력 형식

```json
{
  "schema_version": 1,
  "kind": "placement",
  "event_id": "placement-j1-g2-1",
  "job_id": "j1",
  "recovery_id": "fault-1",
  "generation": 2,
  "cluster": "eks-a",
  "jobset_uid": "manager-jobset-uid",
  "observed_at": 2000000000,
  "active_nodes": ["gpu-node-1", "gpu-node-2"]
}
```

```json
{
  "schema_version": 1,
  "kind": "progress",
  "event_id": "progress-j1-g2-1",
  "job_id": "j1",
  "recovery_id": "fault-1",
  "generation": 2,
  "cluster": "eks-a",
  "jobset_uid": "manager-jobset-uid",
  "observed_at": 2000000000,
  "outcome": "succeeded",
  "checkpoint_loaded": true,
  "restored_gen": 1,
  "restored_step": 1000,
  "completed_step": 1001
}
```

시각은 Unix 초, 숫자는 정수다. 예시 시각은 실행 시 실제 값으로 바꾼다. 처음부터 학습이면 `started_from_scratch: true`, `completed_step > 0`을 보낸다. 복원/학습 실패는 동일 식별 필드와 `outcome: failed`, `failure_reason`을 보낸다. 단순 Pod Running·컨테이너 시작·다운로드 성공은 첫 step 증거가 아니다. 분산 학습 생산자는 참여 그룹의 실제 업데이트 완료를 확인해 보고해야 한다.

## 컨트롤러 측 기록 API

`EvidenceWriter(store, policy=..., clock=...).write(message, authenticated_producer=...)`를 신뢰된 입력 어댑터에서 호출한다. producer 기본 역할 이름은 placement=`scheduling`, progress=`training`이며 설정 evidence_policy에서 명시한다.

**authenticated_producer를 JSON/body 또는 Kafka 헤더의 자기 선언 값으로 채우면 안 된다.** 실제 서비스 IAM·인증된 채널을 확인한 어댑터가 구성한 매핑을 전달해야 한다. 이 함수 자체는 네트워크 인증·서명 검증 서버가 아니다. 인증 어댑터/실제 생산자 호출/최소 IAM 연결은 아직 별도 작업이다. 임의 직접 DynamoDB 쓰기 권한을 주면 이 검증 경계를 우회하므로, 계약에 참여하는 기록 서비스만 job 증거 필드를 쓰게 해야 한다. 기존 컨트롤러 권한만으로 외부 생산자 인증이 생기지 않는다.

입력에는 producer·received_at을 넣지 않는다. 기록 경로가 이 두 값을 부여하고 digest를 계산한다. placement_evidence Map에 버전·event_id·producer·observed_at·received_at·digest를, progress Map에는 사실 필드와 이 메타데이터를 함께 저장한다. job 최상위에는 placement_generation·jobset_uid·recovery_id·cluster·active_nodes를 기록한다.

조건부 갱신으로 다음을 보장한다.

- fencing의 현재 복구 ID·세대·JobSet UID와 실행 클러스터를 확인한다. 두 테이블을 하나의 트랜잭션으로 묶은 것은 아니므로 독자는 완료 직전에 다시 fencing을 대조한다.
- placement가 기록된 뒤 같은 복구·세대·UID의 progress만 기록한다. 이전 세대는 현재 배치를 덮지 못한다.
- 현재 저장된 event_id와 같은 재전송은 내용 hash를 대조하고 receipt 시간을 갱신하지 않는다. 더 오래된 관측과 낮은 step은 덮어쓰지 않는다.
- 현재 snapshot event_id를 CAS 조건에 포함하여 동시 기록이 있으면 재조회한다. 반환 false는 중복/늦은 입력/조건 충돌일 수 있으며 성공했다고 간주하지 않는다.
- 실패 증거는 같은 복구의 성공 증거로 자동 덮지 않는다. 새 배치 세대로 넘어갈 때 이전 progress를 제거한다. 전역 사건 이력·DLQ는 이 현재 상태 projection과 별도 계약이다.

## 검증·완료·HOLD

Workflow.tick은 두 증거의 v1 메타데이터, 허용 생산자 역할, 기본 최근 300초 이내 관측·수신 시각(미래 오차 30초)을 검사한다. 예시 기본값이며 환경 측정/담당 합의 후 조정한다. producer 문자열 검사는 IAM 인증을 대체하지 않는다.

모든 증거의 recovery_id·generation·cluster·jobset_uid가 현재 fencing 원본과 같아야 한다. 이전 승인본 복원은 고정된 recovery_resume_gen/step과 일치하고 completed_step이 기준보다 커야 한다. 처음부터 학습이면 실제 scratch 시작과 step>0을 요구한다. 참여 노드 수·중복·배치 세대와 격리 대상 재사용 여부도 확인한다.

조건을 만족하면 RECOVERED와 증거 ID·JobSet UID·완료 step을 사건에 고정한다. finalize에서 현재 세대·owner·UID를 다시 검사하고 fencing을 조건부 COMPLETED로 바꾼 뒤에만 성공 결과를 발행한다.

증거가 없거나 오래됐거나 식별이 다르면 RESUMING으로 대기하며 이유를 저장한다. 신뢰된 현재 실행의 명시적 복원/학습 실패는 즉시 HOLD한다. 재개 전체 대기 기본 30분을 넘으면 HOLD하며 NEW_RUN_SUBMITTED→RESUMING 전환이나 정상 폴링으로 시간이 다시 시작되지 않는다. HOLD를 자동 해제하거나 학습 재시도 횟수를 초기화하지 않는다.

## 실제 연동 전에 남은 일

- 김성모·김현수·노찬경과 역할 이름·사건 형식·기록 어댑터·시계 동기화를 합의한다.
- 초기 job 등록, 실제 생산자 호출, 인증 경로·IAM과 테이블 이름을 연결한다.
- 관찰 모드 이후 실제 새 배치→복원→첫 step→COMPLETED, 실패·누락·지연·중복·재기동을 통합 시험한다.

## DynamoDB 학습 진행 수신부 (11/10 로컬 구현)

`integrations/fencing_input.py`가 fencing의 전용 Map을 읽어 progress를 EvidenceWriter에 연결한다.
제안 속성은 recovery_training_evidence이며 실제 생산자 필드·조건부 쓰기·IAM 합의 전까지
기본 비활성이다. ../examples/config.kafka.example.json의 fencing_progress_input 설정을 참조한다.
기존 placement 입력·작업 등록을 대체하지 않고, 복원·학습 사실을 생성하지 않는다.
어댑터와 중복·옛 세대·owner·UID·시각·복원 기준 거부 테스트만 로컬 구현했다.

활성화 전에는 Data 수정 이미지 배포, 조건부 쓰기와 전용 속성의 IAM 범위,
첫 증거 및 새 ID의 60초 갱신·같은 관측 재전송·옛 실행 거부를 실제 환경에서 확인한다.
그 뒤 `enabled=true`, `producer_contract_confirmed=true`를 배포 담당과 반영한다.
이 변경은 Gitops의 기본 비활성/observe 설정을 변경하지 않는다. 초기 job 등록과
새 placement 증거가 없으면 progress를 수신해도 COMPLETED로 처리하지 않는다.
