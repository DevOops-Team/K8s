# 복구 연동 규약 제안 v2 — A안 + 현수 펜싱 테이블

복구 컨트롤러가 판단·세대 전환·진행을 관리하고, 재배치 실행기가 격리·실제 종료 확인·JobSet 제출을 수행합니다. 실제 실행기 API의 최종 합의가 필요합니다.

## 1. 두 저장 역할

- fencing_table: 현수 규약 그대로 PK=job_id. generation, owner, approved_gen, approved_step, heartbeat_at, ttl. 승인본·실행 권한의 유일한 원본.
- recovery_table: 기존 우리 진행 기록 PK=pk. event#ID는 사건/중복 hash/state/outbox, job#ID는 cluster, active_nodes, gpu_count, 원본 jobset_uri/jobset_sha256, allow_restart_from_scratch(선택). placement_generation은 배치 기록 버전일 뿐 승인·권한 기준이 아님.

우리 복구 컨트롤러가 fencing_table에 추가하는 recovery_id/recovery_from_gen/recovery_to_gen은 재처리용 잠금·세대 전환 증거입니다. 승인 필드와 별개이며 데이터 담당과 필드 소유권을 공유해야 합니다.

## 2. 실행 권한 전환

현 세대 N과 원본 cluster의 owner(또는 owner 없음)를 조건으로 단일 UpdateItem 수행:

```text
SET generation=N+1, recovery_id=요청ID,
    recovery_from_gen=N, recovery_to_gen=N+1
REMOVE owner
조건: generation=N AND (owner 없음 OR owner=source_cluster)
      AND recovery_id 없음
```

approved_gen/approved_step/heartbeat_at/ttl은 수정하지 않습니다. 같은 recovery_id가 이미 기록돼 있으면 다시 세대를 올리거나 owner를 지우지 않습니다. 다른 복구 잠금은 덮어쓰지 않습니다. 현재 placement에 등록된 노드가 아닌 사건은 거부합니다.

init은 generation 일치와 (owner 없음 또는 내 cluster) 조건으로 owner를 선점해야 합니다. 다른 owner면 시작하지 않습니다. 각 rank가 동일 클러스터에서 같은 선점을 반복하는 것은 허용됩니다. 사이드카는 generation+owner 조건으로 승인·하트비트 갱신, 권한 상실 시 업로드 중단·STOP 요청을 해야 합니다. S3 업로드 보호와 실제 trainer 종료도 데이터/학습 구현에서 필요합니다. owner만으로 같은 클러스터 내부 중복 실행을 막을 수는 없습니다.

## 3. 입력 사건

example-event.json 참고: event_id, job_id, cluster(eks-a/b/c), node, generation(사건 당시 세대), fault_type. 같은 ID의 다른 본문은 오류로 거부합니다. 승인 체크포인트 주소나 실행 권한을 사건 본문에서 신뢰하지 않습니다. 실제 팀 gpu.fault v1과 필드를 맞춰야 합니다.

## 4. 실행기 HTTPS API

모두 POST, Bearer 인증, Idempotency-Key=RECOVERY_ID:OPERATION. 실행기는 키를 영속 중복 제거하고 응답 유실 후 같은 요청에도 같은 JobSet을 반환해야 합니다. 모든 응답은 recovery_id와 generation을 요청과 일치시킵니다.

공통 요청: recovery_id, job_id, source_cluster, failed_node, old_generation, generation, gpu_count, jobset_uri, jobset_sha256, checkpoint_source=dynamodb-approved, action. status는 target_cluster 추가.
**checkpoint_uri·승인본 env는 전달하지 않습니다.** init이 FENCING_TABLE에서 approved_gen/approved_step을 읽고 gen-{4자리}/step-{8자리}/manifest.json을 복원합니다. 모든 rank의 복원 기준과 무결성 검사도 데이터 담당이 보장해야 합니다.

- /v1/recoveries/fence: 노드 신규 배치 차단·기존 그룹 종료. 실제 중단 또는 검증된 펜싱 확인 후에만 old_execution_stopped=true. 단순 Pod API 삭제·VPN 단절은 충분하지 않음.
- /v1/recoveries/restart: 종료 증거와 현재 generation을 확인. S3 원본 jobset_uri를 읽고 sha256 검증 후 합의한 필드만 변경·매니저 원본 경로에 제출. jobsets.render_recovery를 재사용할 수 있음. gpu_count/이미지/데이터/config는 보존. FENCING_TABLE은 실제 현수 테이블과 일치해야 함. ckpt-init/sidecar의 CLUSTER_NAME은 대상 워커의 devoops-cluster-info ConfigMap에서 읽음. accepted=true, target_cluster 반환. AZ 대피는 다른 EKS, 나머지는 원래 EKS. target/exclude 어노테이션은 담당자의 합의 형식 사용.
- /v1/recoveries/status: checkpoint_loaded=true, step_completed=true를 실제 학습 신호로 입증하고 target_cluster·generation·active_nodes(중복 없는 실제 참여 노드 N개)를 반환. 노드 이전이면 failed_node는 제외. Running 상태만으로 성공하지 않음.

컨트롤러도 완료 직전 현수 테이블에서 generation·owner를 강한 일관성으로 확인합니다. 완료 시 우리 placement를 갱신한 후 복구 잠금만 지웁니다. init이 선점한 owner와 승인본은 보존합니다. 실행기는 세대 변경·owner 제거를 다시 수행하지 않습니다.

## 5. A안과 설정

Kueue requeuingStrategy.backoffLimitCount=0, MultiKueue workerLostTimeout을 충분히 크게 설정해 컨트롤러가 재시작을 관리합니다. timeout을 늘리는 것은 영구 재디스패치 비활성화가 아닙니다. 예시는 onprem/scheduling/controller-only.config.example.yaml이며 스케줄링 담당의 전체 config에 병합·검증해야 합니다. 실행 모드는 scheduler_policy_confirmed=true를 요구하지만 이 값만으로 실제 설정을 증명하지는 않습니다.

## 6. 결과·한계

결과 recovery.action은 event_id=원본ID:result, job_id, state, generation. 최소 한 번 전달이므로 수집기가 중복 제거해야 합니다. DLQ·전체 timeout·시도 예산·리더 선출은 추가 구현 대상입니다. 현수의 init/sidecar 구현·재배치 실행기 서비스·실제 S3/AWS/Kafka 연동은 이 저장소 변경만으로 설치되지 않습니다.
