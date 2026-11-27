# 복구 컨트롤러 ↔ relocator 입력·결과 (ADR-50, GPU-276)

relocator 2.0.0(GPU-272)부터 relocator는 S3·DynamoDB에 가지 않는다. 펜싱 표 기록, S3 원본 읽기·해시 대조는
모두 복구 컨트롤러가 하고, relocator와는 관리 클러스터 RelocationRequest(RR) 하나로만 주고받는다.
relocator 쪽 계약 원문은 `scheduling/relocator/RECOVERY_CONTRACT.md`다.

## 1. RR spec (생성 때 고정, 이후 변경 없음)

| spec 필드 | 컨트롤러 출처 | 코드 |
|---|---|---|
| `requestId` · `jobRef` · `action` · `reason` · `excludeNodes` · `excludeClusters` | 장애 사건·정책 (기존) | `RelocationClient.submit` |
| `toGeneration` | 옛 세대 + 1 (= 펜싱 `recovery_to_gen`) | `RelocationInputs` |
| `stopDeadline` | 펜싱 `stop_wait_deadline` (epoch 초 정수) | `RelocationInputs` |
| `jobSet` | recovery_table `job#ID`의 `jobset_uri` S3 원본, `jobset_sha256` 대조 | `RelocationInputs` |

- 펜싱 `recovery_id` · `generation` · `recovery_from_gen` · `recovery_to_gen`이 이 복구와 다르면 RR을 만들지 않는다(HOLD).
- 원본은 180,000바이트까지. 넘거나 해시가 다르면 HOLD.
- 409(응답 유실·리더 교체) 재시도는 기존 RR의 spec이 새로 계산한 spec과 **전부 같을 때만** 받아들인다.
- 이전의 `devoops/recovery-inputs` 주석과 refresh의 주석 갱신 PATCH는 없앴다(60초 스냅샷·시계 맞춤 불필요).

## 2. 진행 중 신호 (컨트롤러 → relocator, RR 주석)

| 주석 | 붙이는 때 | 코드 |
|---|---|---|
| `devoops/recovery-stop-recorded: "true"` | `status.recoveryProgress`의 OLD_RUN_STOPPED를 펜싱 표에 조건부 기록한 **뒤** | `RelocationClient.refresh` |
| `devoops/recovery-cancel: "<이유>"` | 펜싱 `recovery_id`·`generation`이 이 복구가 아님(권한 잃음). Done·Failed RR에도 붙인다 | `Workflow.cancel_relocation` → `RelocationClient.cancel` |

- 두 주석 모두 `resourceVersion` 조건 merge-patch다. 이미 같은 값이면 PATCH하지 않는다.
- Cancelled·Duplicate RR에는 확인·취소를 다시 붙이지 않는다.
- 권한을 잃으면(Runner의 종료 대기 검사·workflow 검사·상태 투영 어느 쪽이든) 사건은 `HOLD`(`recovery authority changed`)로 끝나고,
  `relocation_cancelled=false`를 먼저 저장한 뒤 취소 주석을 붙인다. 실패하면 HOLD 폴링이 5초부터 최대 300초 간격으로 다시 붙이고, 성공하면 `true`로 바꾼다. 상태 투영 중 다른 이유의
  RecoveryBlocked(증거 형식 등)는 취소가 아니며 기존 Runner 재시도·HOLD 정책을 따른다.

## 3. 결과 (relocator → 컨트롤러, RR status)

`status.recoveryProgress` v1 → `integrations/relocation_status.py`가 펜싱 표에
OLD_RUN_STOPPED → NEW_RUN_SUBMITTED 순서로 조건부 기록한다. 형식은 RECOVERY_CONTRACT.md 4절.
relocator가 펜싱을 쓰지 않으므로 **execute 배포는 반드시** 아래 설정을 켠다. 꺼 두면 확인 주석이
붙지 않아 relocator가 재제출하지 않는다.

```json
"relocation_status_input": {"enabled": true, "producer_contract_confirmed": true}
```

## 4. 아직 받을 수 없는 것 (relocator 담당과 확인 필요)

- **사람 확인 증거**: `devoops/old-execution-stopped`로 relocator가 쓰는 `kind: manual`, `checks: []` 증거는
  계속 거부한다(HOLD). 인증된 운영자 확인 경로가 따로 정해져야 한다.
- **워커에 보내지기 전 잡**: 증거 `clusters: []`는 거부한다. 장애 사건의 클러스터가 증거에 있어야 한다.

## 5. 배포에 필요한 것

- 복구 컨트롤러 Role `recovery-controller-relocation`: `create, get, list, watch, patch` (Gitops에 이미 있음).
- 복구 컨트롤러 IAM: 원본 버킷 `code/*/jobset.json` GetObject.
- CRD `toGeneration` · `stopDeadline` · `jobSet` 필드(relocator 2.0.0과 함께 이미 배포).

## GPU-304 제안: 서명한 일반 종료 증거 (기본 off)

`verified_stop_input`이 전체 범위·재생성 차단에 대한 KMS 서명을 확인하고 선택적으로 EC2 상태를 조회해
`devoops/old-run-stop-verified` 주석을 작성한다. raw manual 주석을 신뢰하도록 완화하지 않는다.
relocator가 동일 증거를 반환해야 기존 HOLD→펜싱→ACK가 이어진다. 수신부는 별도 담당 구현이 필요하다.
[계약과 IAM/운영 도구](verified-stop-evidence.md).
