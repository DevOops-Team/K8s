# relocator ↔ 복구 컨트롤러 계약 (ADR-50, GPU-272)

relocator는 S3·DynamoDB에 가지 않는다. 입력은 RelocationRequest(RR) spec, 결과는 RR status뿐이다.
펜싱 표 기록과 원본 읽기·해시 대조는 복구 컨트롤러가 한다.

## 1. 흐름

| 순서 | 주체 | 하는 일 |
| --- | --- | --- |
| 1 | 복구 컨트롤러 | 펜싱 표 세대 +1 → S3 원본 읽기·해시 대조 → RR 생성 (spec에 원본·새 세대·종료 마감) |
| 2 | relocator | 접수 → 격리(cordon·taint 또는 클러스터 펜싱) → 옛 잡 삭제 · 종료 확인 |
| 3 | relocator | `status.recoveryProgress` = `OLD_RUN_STOPPED` + 종료 증거 |
| 4 | 복구 컨트롤러 | 펜싱 표에 `OLD_RUN_STOPPED` 조건부 기록 → RR에 `devoops/recovery-stop-recorded: "true"` |
| 5 | relocator | 원본에서 6절 값만 바꿔 `job-{id}-g{새 세대 4자리}` 제출 → `status.recoveryProgress` = `NEW_RUN_SUBMITTED` + 새 JobSet 이름·UID |
| 6 | 복구 컨트롤러 | 펜싱 표에 `NEW_RUN_SUBMITTED`·`new_jobset_name`·`new_jobset_uid` 조건부 기록 |
| 7 | relocator | 새 잡 입장 확인 → `Done` |
| 언제든 | 복구 컨트롤러 | 권한 잃음·이미 끝난 복구 → RR에 `devoops/recovery-cancel: "<이유>"` → relocator가 만든 JobSet 철회 → `Cancelled` |

relocator는 4번의 확인 주석이 붙기 전에는 새 세대를 내지 않는다(종료 기록 → 재제출 순서).

## 2. RR spec (복구 컨트롤러가 채움, 만든 뒤 변경 불가)

| 필드 | 형식 | 설명 |
| --- | --- | --- |
| `requestId` | string | 펜싱 표 `recovery_id` (기존) |
| `jobRef` | string | `training/job-{id}-g{옛 세대 4자리}` (기존) |
| `action` | enum | `restartInPlace` · `moveToSpare` · `evacuateCluster` (기존) |
| `excludeNodes` | array string | moveToSpare: `sourceCluster`에서 cordon·taint할 노드 (기존) |
| `excludeClusters` | array string | evacuateCluster: 펜싱할 클러스터, 재제출본도 피한다. evacuateCluster인데 비었으면 접수 단계 `Failed`. 없는 MultiKueueCluster면 격리 단계 `Failed` |
| `reason` | string | 기존 그대로 |
| **`toGeneration`** | integer | 새 세대. 반드시 `jobRef` 옛 세대 + 1. 아니면 relocator가 `Failed` |
| **`stopDeadline`** | RFC3339(시간대 필수) 또는 epoch 초 | 옛 실행 종료 마감 = 펜싱 표 `stop_wait_deadline`. 지나면 `Held`. 없으면 RR 생성 + 15분 |
| **`jobSet`** | object | 해시를 대조한 원본 JobSet 전체 (`kind: JobSet`). 없으면 `Failed` |
| **`sourceCluster`** | string | 옛 세대가 돌던 클러스터 (모든 action). moveToSpare cordon 클러스터, restartInPlace·moveToSpare 원래 클러스터 우선 힌트, EC2 강제 종료 클러스터. moveToSpare인데 `excludeNodes`가 있고 이 값이 없으면 접수 단계 `Failed`. status `sourceCluster`에 그대로 기록한다(디스패처 자리 지키기·EKS 끄기 검사가 읽음) |
| **`nodes`** | array `[{name, instanceId?}]` | 옛 세대가 쓰던 노드 (모든 action). restartInPlace면 재제출본을 이 노드·`sourceCluster`에 고정. 없으면 고정 안 함(원래 클러스터 우선). 있는데 `sourceCluster` 없음·name 없음/중복·Pod 수보다 적음이면 접수 단계 `Failed`. `instanceId`는 워커가 끊겼을 때 EC2 강제 종료 대상(terminate 모드) |

relocator는 대상 클러스터·노드를 스스로 조회해서 정하지 않는다 (GPU-295). 관리 클러스터 Workload `status.clusterName`·
`nominatedClusterNames`는 옛 실행 종료 확인을 할 워커(`status.seenClusters`)를 모으는 데만 쓰고, 워커 Pod로 옛 노드를 찾아
`status.oldNodes`에 적던 일은 하지 않는다.

relocator는 원본의 `metadata.namespace`가 요청 네임스페이스와 다르거나 `devoops/job-id` 라벨이 요청 잡과 다르면 거부한다.

## 3. 복구 컨트롤러 → relocator 주석 (RR metadata)

| 주석 | 값 | 의미 |
| --- | --- | --- |
| `devoops/recovery-stop-recorded` | `"true"` | 펜싱 표에 `OLD_RUN_STOPPED`를 기록함. 재제출해도 됨 |
| `devoops/recovery-cancel` | 이유 문자열 | 취소. relocator는 새 세대 이름 JobSet 중 `devoops/recovery-id`가 이 요청인 것만 uid 조건으로 지우고 `Cancelled`. 철회 실패 시 다음 루프에서 다시 |

- 취소는 `Done`·`Failed` 요청에도 받는다(재제출·입장 뒤 권한을 잃은 경우).
- RR 수정은 기존 입장 정책 `scheduling-relocation-request-writers`로 복구 컨트롤러 SA(와 관리자)만 할 수 있다.
- 사람 확인 주석 `devoops/old-execution-stopped`는 그대로 받는다(아래 5. 주의).

### 자기 펜싱 확인 `devoops/self-fence-verified` (ADR-49, GPU-255, relocator `SELF_FENCE_EVIDENCE=on`일 때만)

사이드카 기록은 RR을 만든 뒤에 생기고 spec은 만든 뒤 바꿀 수 없어서, 다른 제어 신호처럼 주석으로 받는다.
복구 컨트롤러가 펜싱 표 `self_fence_*`를 읽어 확인한 뒤 값(JSON)을 붙인다.

```json
{"requestId": "evt-1", "generation": 1, "cluster": "eks-a", "worldSize": 2,
 "ranks": [0, 1], "pods": ["0:<pod uid>", "1:<pod uid>"], "recordedAt": "2026-11-25T01:00:07Z"}
```

| 칸 | 펜싱 표 | relocator 확인 |
| --- | --- | --- |
| `requestId` | `recovery_id` | = spec.requestId |
| `generation` | `self_fence_gen` | = jobRef 옛 세대 (정수) |
| `cluster` | `self_fence_cluster` | = 닿지 않는 그 워커 |
| `worldSize` | `self_fence_world_size` | = 복구 원본 Pod 수 N (spec.jobSet parallelism) |
| `ranks` | `self_fence_ranks` | 정확히 0~N-1 (정수, 중복 없음) |
| `pods` | `self_fence_pods` (ADR-49 남은 구멍 4) | 번호 0~N-1마다 Pod UID 정확히 하나 |
| `recordedAt` | `self_fence_at` | 비어 있지 않음 |

- 워커 API에 닿을 때는 쓰지 않는다. 닿는데 JobSet·Workload·Pod가 남아 있으면 이 주석이 있어도 멈춤이 아니다.
- 복구 원본(spec.jobSet)이 Pod를 다시 만들지 않는 모양일 때만 쓴다: `maxRestarts` 0, replicatedJob 하나(replicas 1), `completionMode: Indexed`, `backoffLimit` 0, `podReplacementPolicy: Failed`, `restartPolicy: Never`.
- 하나라도 어긋나면 이 증거를 쓰지 않고 EC2 증거 → `WaitingForStop`/`Held`로 넘어간다.
- 쓰면 `oldRunStopEvidence.kind`와 그 워커의 `checks[].kind`가 `self_fence`다 (섞이면 ec2 > self_fence > automatic).
- 컨트롤러가 붙이기 전 확인할 것(ADR-49 쓰기 조건): `generation > self_fence_gen`, 그리고 `recovery_from_gen`·`recovery_to_gen`·`recovery_id`·`GENERATION_ADVANCED`가 이 복구와 맞을 것.

## 4. RR status.recoveryProgress (relocator가 씀)

복구 컨트롤러 `integrations/relocation_status.py` 계약 v1과 같다. 단위 테스트 `test_progress_contract.py`가 그 검사 코드에 실제로 넣어 확인한다.

```json
{
  "schemaVersion": 1,
  "requestId": "evt-1",
  "jobId": "abc",
  "fromGeneration": 1,
  "toGeneration": 2,
  "phase": "NEW_RUN_SUBMITTED",
  "oldRunStopEvidence": {
    "generation": 1, "namespace": "training", "jobset_name": "job-abc-g0001",
    "clusters": ["eks-a"], "kind": "automatic",
    "checks": [{"cluster": "eks-a", "kind": "automatic", "detail": "eks-a에서도 사라짐"}],
    "confirmed_at": "2026-11-25T01:00:00Z",
    "confirmed_by": "relocator", "recorded_by": "relocator-pod", "detail": "옛 Workload 삭제 확인 (...)"
  },
  "newJobSet": {"name": "job-abc-g0002", "uid": "<관리 클러스터 JobSet UID>"}
}
```

- `OLD_RUN_STOPPED`일 때는 `newJobSet`이 없다. `NEW_RUN_SUBMITTED`에서도 `oldRunStopEvidence`는 처음 값 그대로다.
- `newJobSet.uid`는 관리 클러스터 JobSet UID다(MultiKueue 워커 사본 UID 아님).
- 기록용 status 필드(`newJobUid`, `cancelledAt` 등)는 판단에 쓰지 않는다.
- status `phase`에 `Cancelled`가 추가됐다.

## 5. 알려진 차이 (복구 컨트롤러 담당과 확인할 것)

- **사람 확인 경로**: relocator는 `devoops/old-execution-stopped=true`를 받으면 `kind: manual`, `checks: []` 증거를 쓴다. 지금 컨트롤러 계약은 manual을 받지 않아 펜싱 표에 기록되지 않고 HOLD로 남는다. 워커가 끊겨 EC2 증거도 없을 때 사람이 풀 경로가 따로 필요하다.
- **워커에 보내지기 전 잡**: 옛 잡이 어느 워커에도 안 보내졌으면 `clusters`가 빈 목록이다. 컨트롤러 계약은 빈 목록과 사건 클러스터가 빠진 목록을 거부한다.
- **확인 주석 대기 시간**: relocator는 확인 주석을 기한 없이 기다린다. 기한 판단과 HOLD는 컨트롤러 몫이다.
- **자기 펜싱 연결**: 컨트롤러 `self_fence_input`이 펜싱 기록을 검증해 `devoops/self-fence-verified`를 붙이고, `relocation_status.py`가 해당 주석과 일치하는 `self_fence` 결과만 받는다. 기본 off이며 `relocation_status_input`·execute와 생산자 계약 확인이 필요하다. relocator `SELF_FENCE_EVIDENCE`도 별도 off다. jobspec·사이드카 보강 및 EKS 시험 통과 전에는 켜지 않는다. 상세: `recovery-controller/docs/self-fence-input.md`.

## 6. 배포 담당(Gitops·인프라)에 전달할 변경

운영 매니페스트는 이 저장소에서 고치지 않는다. 두 컨트롤러가 같은 계약으로 함께 배포되어야 한다. 한쪽만 바뀌면 relocator가 `Failed`(spec 필드 없음)하거나 컨트롤러가 진행하지 못한다.

GPU-295: 복구 컨트롤러는 옛 세대 등록 배치(`cluster`·`active_nodes`)로 모든 action에 `sourceCluster`·`nodes`를 채운다(`relocation_inputs.py`). 배치 계약에 인스턴스 ID가 없어 `nodes[].instanceId`는 비어 있고, 그동안 EC2 terminate 모드의 강제 종료 대상은 없다(종료 증거 판단은 그대로). 두 필드가 없는 옛 요청(GPU-295 전에 만든 진행 중 RR)은 moveToSpare(`excludeNodes` 있음)면 접수 단계 `Failed`, 나머지는 원래 클러스터 우선 힌트 없이 재제출된다. CRD에는 두 필드가 이미 있다(Gitops #115).

**CRD `crd-relocationrequest.yaml` spec.properties에 추가** (spec은 unknown 필드를 버리므로 필수):

```yaml
toGeneration:
  type: integer
  minimum: 2
  description: 새 세대 = jobRef 옛 세대 + 1
stopDeadline:
  x-kubernetes-int-or-string: true
  description: 옛 실행 종료 마감 (RFC3339 시간대 포함 또는 epoch 초) = 펜싱 stop_wait_deadline
jobSet:
  type: object
  x-kubernetes-preserve-unknown-fields: true
  description: 복구 컨트롤러가 S3에서 읽고 해시를 대조한 원본 JobSet
```

- `required`에 `toGeneration`·`jobSet` 추가를 권장한다.
- `status`는 이미 `x-kubernetes-preserve-unknown-fields: true`라 `recoveryProgress`는 그대로 남는다. phase 설명에 `Cancelled`를 추가한다.

**relocator Deployment env 제거**: `RECOVERY_STORE`, `FENCING_TABLE`, `CKPT_BUCKET`, `FENCING_WAIT_SECONDS`(있다면). `AWS_REGION`과 자격 증명은 EC2 종료 증거에 계속 필요하다.

**IAM**
- relocator: S3 `GetObject`, DynamoDB `GetItem`·`UpdateItem` 제거. EC2 `DescribeInstances`(종료 모드면 `TerminateInstances`)는 유지.
- 복구 컨트롤러: S3 `code/*/jobset.json` `GetObject` (컨트롤러 담당 문서와 같음).

**RBAC**
- 복구 컨트롤러 Role `recovery-controller-relocation`: 지금 `create, get` → 주석을 붙이려면 `patch` 추가, status를 지켜보려면 `list`·`watch` 추가.
- relocator: 변경 없음(RR `status` patch, JobSet get·create·delete 기존 그대로).

**입장 정책**: 변경 없음. RR 생성·수정은 복구 컨트롤러만, status는 relocator만이라는 기존 두 정책이 제어 주석과 status를 그대로 지킨다.
