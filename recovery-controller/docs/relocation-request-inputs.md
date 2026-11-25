# 복구 컨트롤러 → 재배치기 입력 (GPU-268)

별도 HTTP 서버 없이 관리 클러스터 Kubernetes API의 기존 RelocationRequest로 전달한다.
`spec.requestId/jobRef/action/reason/excludeNodes/excludeClusters`는 기존 계약과 불변성을 유지한다.
데이터는 `metadata.annotations["devoops/recovery-inputs"]`에 JSON 문자열로 넣는다.
CRD의 알 수 없는 spec 필드 pruning을 피하므로 CRD 스키마 변경은 필요 없다.

## 입력 및 함수별 소비

| 필드 | 복구 컨트롤러 출처 | 재배치기 소비 함수 |
|---|---|---|
| schema_version=1, job_id, request_id | 장애 사건과 요청 ID | 모든 입력의 작업·요청 일치 검사 |
| observed_at | 컨트롤러 현재 epoch seconds | 60초 초과/5초 이상 미래 입력 보류 |
| fencing.generation/recovery_id/from_gen/to_gen/phase | fencing 테이블 일관 읽기 | accept, to_gen, advance |
| fencing.stop_wait_started_at/stop_wait_deadline | 같은 fencing 항목 | stop_held |
| fencing.new_jobset_name/new_jobset_uid | 같은 fencing 항목 | 진행 단계 재확인 |
| original_jobset | 등록된 jobset_uri의 S3 원본, jobset_sha256 검증 | build_new → resubmit |
| original_sha256 | 등록된 원본 SHA-256 | 전달 원본의 검증 이력 |

`from_gen/to_gen/phase`의 실제 JSON 키는 `recovery_from_gen/recovery_to_gen/recovery_phase`다.
전체 fencing 항목, progress 증거, 자격증명은 보내지 않는다. JSON UTF-8 180,000바이트를 넘으면 HOLD한다.
원본이 너무 크면 이 경로로 실행하지 않는다. 포인터만 보내 재배치기에서 S3를 다시 읽는 우회는 없다.

최초 요청 생성과 409 재시도에도 입력이 들어간다. 이후 workflow polling마다 같은 요청의 주석만
resourceVersion 조건으로 갱신한다. 새 요청을 만들거나 spec을 바꾸지 않는다. 최종 상태 요청은 갱신하지 않는다.
입력 갱신 오류는 기존 Runner의 3회 API 재시도·단계별 deadline/HOLD 정책을 적용한다.

## 실행 권한과 쓰기

재배치기 운영 main은 RequestStore를 필수 사용한다. AWS 어댑터에서 S3 GetObject·DynamoDB GetItem 코드를 제거했다.
get_fencing/get_jobset은 전달 JSON을 읽으며, 실행 직전에는 DB **조건부 UpdateItem**으로 실제 복구 ID·세대와
phase가 바뀌지 않았는지 검사한다. 입력 스냅샷만 믿고 노드 격리나 재제출을 승인하지 않는다.
DB 상태 쓰기는 기존 relocator 책임을 유지하며, OLD_RUN_STOPPED와 NEW_RUN_SUBMITTED 및 증거·UID를 조건부 갱신한다.
동일 복구의 phase만 먼저 진행됐다면 새 입력을 기다린다. 복구 권한을 잃었다면 기존 UID 기반 철회 경로를 유지한다.

ConfigMapStore의 읽기 메서드는 로컬 테스트 어댑터에만 남는다. 운영 AWS 입력을 읽는 용도가 아니다.
K8s 현장 조작과 **실제 종료/입장 확인**, 끊긴 워커의 EC2 종료 증거는 실행기 책임으로 남는다.
이것들은 DB/S3 작업 메타데이터 조회와 구분되며, 오래된 전달값으로 대체하지 않는다.
RESUMING/COMPLETED와 recovery_training_evidence 판정 규칙은 바꾸지 않는다.

## 컨트롤러에 아직 없거나 확인이 필요한 정보

1. 초기 `job#ID`의 jobset_uri·jobset_sha256·gpu_count·cluster·active_nodes 등 등록 메타데이터.
   Backend 조회는 1차 변경으로 제거했다. 실제 항목이 없으면 HOLD하며 자동으로 만들어내지 않는다.
2. 원본 S3 JobSet 내용: 기존 컨트롤러는 URI/해시만 보유했다. 이제 S3에서 직접 읽되 **GetObject IAM**이 필요하다.
3. 워커 kubeconfig와 현장 Node/Pod 목록, 옛 Pod의 EC2 instanceId, 워커 단절 시 EC2 상태/종료 증거:
   컨트롤러에는 현재 수집 경로·권한이 없다. 이 변경에서는 재배치기의 검증 경로를 유지한다.
   이 정보까지 컨트롤러 발신으로 옮기려면 worker/EC2 권한·실시간 관측 계약을 별도로 공급해야 한다.
4. dispatcher의 새 세대 `devoops/placement` → recovery_table 완료 증거 연결은 별도 계약이며 이번 전달로 해결되지 않는다.

## 배포 담당자에게 필요한 연결

- 복구 컨트롤러: 원본 버킷 `code/*/jobset.json` GetObject, 관리 RR get/create/**patch** 권한.
- 재배치기: S3 GetObject/fencing GetItem은 운영 코드에서 사용하지 않는다. fencing UpdateItem은 유지.
- 주석을 **recovery-controller SA만 생성·갱신**하도록 RBAC와 VAP에서 제한해야 한다.
  API의 JSON 문자열 source/name은 인증이 아니다. relocator는 status만 기록하도록 역할을 분리한다.
  기존의 RR 생성 권한을 다른 주체에 남기면서 입력 주석까지 자유롭게 쓰게 하면 안 된다.
- 양쪽 시계 동기화가 필요하다. 활성화 전 서명 없는 주석에 대한 위 작성자 제한을 검증한다.
- 이 저장소 변경으로 Gitops 운영 매니페스트·RBAC·VAP·CI를 수정하거나 배포하지 않았다.
  코드 머지와 실제 운영 전환을 구분한다. 기존 RR에 입력이 없으면 컨트롤러 갱신 전까지 실행기는 보류한다.

## Controller-owned storage handoff (GPU-268, receiver work pending)

This change edits **recovery-controller only**. Existing relocator code, CRD,
RBAC, IAM and deployment manifests are unchanged. The existing relocator still
performs DynamoDB UpdateItem; this commit does not remove those calls.

The recovery controller now has an optional RR-status → fencing projection.
S3 original reads already run in `RelocationInputs`. Storage operations required
by recovery belong to this controller; no new S3 write/delete operation is needed
for relocation and none is invented here. Pod/JobSet deletion remains relocator's
Kubernetes execution responsibility. Training/sidecar ownership of checkpoint,
owner and heartbeat fields is unchanged.

Enable only after Seongmo's receiver implementation and deployment are ready:

```json
"relocation_status_input": {
  "enabled": true,
  "producer_contract_confirmed": true
}
```

Both values default to false. Confirmation means: RR status can only be written
by the trusted relocator ServiceAccount, RR requests can only be created by the
controller, CRD preserves the fields below, and relocator's direct fencing writes
have been removed. These are deployment prerequisites, not facts established by
this patch. Do not enable two simultaneous phase writers. Keep existing
annotation input delivery until the spec/CRD migration is coordinated; this patch
does not silently add spec fields that the current CRD could prune.

### Receiver result contract to implement

The relocator must persist `status.recoveryProgress` before acknowledging the
execution stage. The field is a proposed integration contract, **not a claim that
the current relocator produces it**:

```json
{
  "schemaVersion": 1,
  "requestId": "same-as-spec.requestId",
  "jobId": "abc",
  "fromGeneration": 1,
  "toGeneration": 2,
  "phase": "NEW_RUN_SUBMITTED",
  "oldRunStopEvidence": {
    "generation": 1,
    "namespace": "training",
    "jobset_name": "job-abc-g0001",
    "clusters": ["eks-a"],
    "kind": "automatic",
    "checks": [
      {"cluster": "eks-a", "kind": "automatic", "detail": "JobSet/Workload/Pods absent"}
    ],
    "confirmed_at": "2026-11-25T01:00:00Z"
  },
  "newJobSet": {"name": "job-abc-g0002", "uid": "manager-jobset-uid"}
}
```

- For OLD_RUN_STOPPED, omit `newJobSet`; for NEW_RUN_SUBMITTED, include it and
  retain **the same immutable oldRunStopEvidence**. Report the manager JobSet UID,
  not a MultiKueue worker-copy UID.
- Stop evidence must cover every possibly running worker cluster. `kind` and
  per-cluster checks use `automatic` or `ec2`. Stop timestamps must be timezone-aware,
  within this RR's lifetime (30-second skew) and not in the future. Historical
  evidence stays valid across retries; it is not a 60-second lease.
- Manual stop confirmation is not accepted by this new input. It needs a separate
  authenticated operator contract; an arbitrary annotation or phase label is
  insufficient. Never infer proof from `Done`, `stoppedAt` or `message` alone.
- Controller writes OLD_RUN_STOPPED before NEW_RUN_SUBMITTED, conditioned on
  recovery ID, from/to generations and previous phase. The second CAS also binds
  the exact stop evidence. Replays never regress RESUMING, replace a UID or modify
  generation/owner. Lost authority or conflicting proof fails closed.
- Controller consumes this result even when the outer RR phase is Done; it does
  not recreate the RR or refresh terminal inputs. Receiver must preserve result
  fields until controller has recorded them.
- The receiver must wait for controller's forwarded OLD_RUN_STOPPED fencing
  acknowledgement before submitting the new JobSet. New init must not acquire
  the owner until NEW_RUN_SUBMITTED is recorded; retain/retry the result on
  controller/API outages. Cancellation/authority withdrawal needs a coordinated
  receiver protocol before adopting the full ADR-50 spec-only design.

Still required from scheduling: receiver/status implementation, CRD preservation,
status-writer isolation, removing relocator's DB access, and integration E2E. These
files are deliberately owned by Seongmo and are not edited here. New-placement
and training-resume evidence remain separate from these stop/submission results.
