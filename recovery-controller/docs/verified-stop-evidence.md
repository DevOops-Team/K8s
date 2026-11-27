# 일반 종료 증거·수동 승인 — GPU-304 제안 계약

이 PR은 **복구 컨트롤러와 운영자 도구만** 구현한다. 성모 담당 relocator 수신부와 운영 매니페스트/IAM은 수정하지 않는다.
기본 off이며, 이 코드만 배포해도 현재 Held가 풀리는 것은 아니다. 기존 `old-execution-stopped=true`의
`kind=manual, checks=[]`는 여전히 거부한다.

## 신뢰 경계

S3의 전용 승인 문서를 허용된 AWS KMS 비대칭 RSA SIGN_VERIFY 키로 서명하고 컨트롤러가 Verify한다.
고정 key ARN만 허용하고 mutable alias는 금지한다. `actorArn`은 도구가 STS에서 가져오는 감사용 서명 주장이다.
실제 인증 경계는 pinned key와 Sign IAM/키 정책이며, 사람/세션은 도구가 출력하는 KMS Sign RequestId와 CloudTrail로 감사한다.
공용 서명 주체를 개인 승인으로 해석하지 않는다. 토큰·개인키·비밀값을 RR/DynamoDB에 저장하지 않는다.

- `manual`: 권한 있는 승인자가 전체 옛 실행의 실제 종료와 재생성 차단을 확인했다는 서명된 attestation.
- `ec2`: 같은 scope/block attestation에 더해 컨트롤러가 모든 EC2 ID의 stopped/terminated 상태를 조회한다.
  누락·다른 계정/리전·페이지 잔여·running·stopping·shutting-down은 승인하지 않는다.
- **완전 자동 종료 판정은 아니다.** 전체 과거 실행 범위와 ASG 대체 노드·남은 Workload 복사본·중지 인스턴스 재시작을
  포함한 재생성 차단은 아직 기계적으로 자동 증명하지 않는다. 승인자가 근거를 확보할 수 없으면 서명하지 않고 HOLD한다.
- StopInstances/TerminateInstances, 강제 Pod 삭제, generation 변경 명령은 이 도구에 없다.

## 정상 실행 중 참여 정보 수집

`tools/stop_evidence.py collect`는 워커 Pod UID·Indexed Job rank·nodeName, Node providerID의 EC2 ID와
관리 JobSet UID를 읽어 0600 파일에 저장한다. Pod 개수·rank·작업/세대 라벨·providerID가 불완전하면 실패한다.

```bash
# recovery-controller 디렉토리에서. 기존 kubeconfig·AWS 인증 사용
python tools/stop_evidence.py collect \
  --context <worker-context> --manager-context <manager-context> \
  --job-id <id> --generation 1 --gpus 2 --cluster eks-a \
  --account <12-digit-account> --region us-east-2 --output placement.json
```

출력은 한 시점의 관측이며 전체 과거 실행의 증거가 아니다. 장애 전에 확보하고 Pod/노드 교체 시 갱신한다.
이 PR은 job-watcher/Data를 바꾸거나 이 파일을 Kafka에 자동 발행하지 않는다. 생산자 연동은 별도 인계다.
모든 RR seenClusters의 복사본도 검토한다. 현재는 클러스터별 GPU 수만큼 정확한 rank 0..N-1과 유일한 Pod/EC2 ID를 요구한다.
교체/중복 실행 이력을 이 범위로 표현할 수 없으면 자동 추정하지 않고 HOLD한다.

## 승인 문서와 제출

아래는 스키마 예시이며 그대로 승인하지 않는다. 실제 RR UID·옛 관리 JobSet UID·전체 참여 목록으로 채워야 한다.
세 boolean을 확인 없이 true로 바꾸지 않는다. `approvedAt`, `actorArn`, `domain`, `schemaVersion`은 approve 도구가 넣는다.

```json
{
  "requestId": "<recovery-id>", "requestUid": "<RR-uid>", "jobId": "<job-id>",
  "namespace": "training", "fromGeneration": 1, "toGeneration": 2,
  "sourceCluster": "eks-a", "oldJobsetUid": "<old-manager-JobSet-uid>",
  "mode": "ec2", "scopeComplete": true, "recreationBlocked": true, "executionStopped": true,
  "reason": "<approval ticket>", "blockEvidence": "<verified recreation/restart block across all workers>",
  "clusters": ["eks-a"],
  "participants": [
    {"rank": 0, "podUid": "<uid>", "node": "<node>", "instanceId": "<i-id>",
     "cluster": "eks-a", "accountId": "<account>", "region": "us-east-2"}
  ]
}
```

```bash
python tools/stop_evidence.py approve \
  --document reviewed-approval.json --key-id <immutable-kms-key-arn> \
  --bucket <approval-bucket> --region us-east-2 --confirm-scope-and-block
```

S3: `recovery-stop-approvals/{job_id}/{sha256(requestId)}.json`.기본 IfNoneMatch=*로 덮어쓰지 않는다.
잘못된 **미수락** 문서는 ETag 확인 후 `--expected-etag`로 CAS 교체한다. 수락된 증거는
`event#ID.verified_stop_evidence` JSON 문자열에 조건부 고정하며 변경하지 않는다.
최초 승인은 현재 300초 이내(+30초 오차), RR 생성 이후(-30초 오차)여야 한다.
수락 후에는 같은 RR UID·전체 cluster scope·현재 펜싱 권한에서만 재사용한다. 단순 재시작/시간 경과로 재승인하지 않는다.
승인자에게 recovery_table/펜싱 쓰기나 RR patch 권한을 주지 않는다. 승인 S3 prefix와 controller audit writer를 분리한다.

## 기본 설정 및 배포 인계

```json
"verified_stop_input": {
  "enabled": false, "producer_contract_confirmed": false,
  "signing_keys": [], "approval_bucket": "", "cluster_accounts": {},
  "ec2_enabled": false, "max_age_seconds": 300
}
```

활성화 선행조건:
1. execute + relocation_status_input 및 생산자/권한 계약 승인.
2. key ARN과 bucket, cluster_accounts의 `eks-a: {"account_id":"...","region":"us-east-2"}` 등 실제 값 고정.
3. 컨트롤러: 전용 prefix s3:GetObject, pinned key kms:Verify, EC2 모드일 때 ec2:DescribeInstances.
   현재 AWS 자격 증명으로 조회 가능한 계정만 지원하며 자동 AssumeRole은 없다. 종료/ASG 변경 권한은 불필요하다.
4. 승인자: 전용 key kms:Sign·STS caller 조회·전용 prefix PutObject. 키 정책·IAM으로 승인 주체 제한, CloudTrail/S3 버전 보존 검토.
5. 기존 RR annotation controller/admin 쓰기 제한 유지. GitOps config/checksum·IAM·KMS 자원 준비는 별도 담당 인계.

## 제안 RR annotation — 성모 담당 수신 구현 필요

`devoops/old-run-stop-verified` = 검증된 oldRunStopEvidence 객체 전체의 canonical JSON.
requestId/requestUid/jobId/generation(옛 세대)/toGeneration, namespace/jobset_name, clusters/checks,
oldJobsetUid, participants, recreationBlocked/blockEvidence, confirmed_at/confirmed_by, verificationId,
approval(keyId·actorArn·approvedAt·reason), ec2Observations를 포함한다.

relocator는 현재 RR UID·requestId·세대·source/seenClusters를 대조한 뒤, 기한 전후 모두 이 증거를
**변경 없이** status.recoveryProgress.oldRunStopEvidence로 돌려줘야 한다. `manual, checks=[]`로 재구성하거나
confirmed_at을 새로 만들면 거부한다. 객체 잔존을 이 증거로 우회하는 실행 정책은 상대 수신부에서 검토·구현해야 한다.

순서:
1. 컨트롤러 서명·범위·권한·선택적 EC2 검증 → DDB audit 고정 → annotation PATCH.
2. relocator가 같은 증거로 OLD_RUN_STOPPED status 반환.
3. 컨트롤러가 저장 audit/annotation과 정확히 대조 → 펜싱 조건부 OLD_RUN_STOPPED 기록.
4. 기존 recovery-stop-recorded=true ACK → relocator 재제출 → 기존 NEW_RUN_SUBMITTED/RESUMING/COMPLETED.

기존 **종료 대기 마감 HOLD만** 같은 권한에서 재개한다. API/설정/복원 실패·취소 HOLD는 풀지 않는다.
새 RR·세대 증가·retry_count 초기화·stopDeadline 연장은 없다. 기존 hold_resume_history에 감사 이력을 남긴다.
잘못된 승인 문서는 stop_evidence_error에 남기고 일반 종료 관측을 방해하지 않는다. AWS API 오류는 기존 제한 정책을 따른다.
승인 회수는 allowlist/기능 비활성화와 진행 중 RR 취소를 별도로 판단한다. 이미 전달된 ACK를 지우는 것을 롤백으로 보지 않는다.

## 검증 및 후속

로컬 테스트: 서명 변조, 다른 복구/세대/UID, 부분·중복 rank, 계정/리전 오류, EC2 전이/누락/페이지/오류,
CAS·재시작·status 위조, HOLD→펜싱→ACK 순서. 실제 KMS/EC2·IAM·relocator 수신·EKS 시험은 아직 하지 않았다.
완전 자동 범위 생산/재생성 차단 증명, EC2 배치 이력 자동 갱신, RR 뒤 추가 장애 정책은 후속이다.
이 KMS/S3 승인 경로는 기존 확정 ADR이 아니라 리뷰 가능한 **신규 제안 계약**이다.
