# 장애 발생 시 Backend 조회 → 복구 작업 등록 (GPU-119)

정상 작업은 이 경로에서 `recovery_table`에 미리 넣지 않는다. Kafka inbox에 저장한
복구 가능한 장애 사건을 execute 처리할 때 `job#<job_id>`가 없으면 Backend를 조회한다.
검증·조건부 최초 등록이 성공한 뒤 기존 claim/세대 증가/relocator 요청을 이어 간다.
사건 `event#`는 조회 이전에 영속화되므로 조회 실패도 HOLD/outbox로 추적한다.

## Backend 구현 요청 계약 v1 — 아직 서버 미구현

`GET /internal/v1/recovery/jobs/{job_id}?generation={source_generation}`

- 서비스 전용 읽기 API. 사용자 화면의 GET /api/jobs/{id} 응답과 별개다.
- HTTPS(인증서 검증 유지), `Authorization: Bearer <서비스 전용 자격 증명>`.
  Backend는 실제 인증·이 엔드포인트 조회 권한을 검사한다. JSON의 source 문자열을 인증으로 쓰지 않는다.
- 관리망에서만 노출하고 Secret으로 인증 정보를 공급한다. Gitops/Backend 인증 구현은 별도 연결이다.
- 200 응답은 아래 JSON 객체, 최대 64 KiB. 모든 숫자는 정수, 시각은 Unix 초.

```json
{
  "schema_version": 1,
  "job_id": "abc123",
  "generation": 1,
  "recovery_eligible": true,
  "jobset_uri": "s3://training-bucket/code/abc123/jobset.json",
  "jobset_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "gpu_count": 2,
  "cluster": "eks-a",
  "active_nodes": ["gpu-node-1", "gpu-node-2"],
  "placement_generation": 1,
  "jobset_name": "job-abc123-g0001",
  "jobset_uid": "MANAGEMENT-JOBSET-UID",
  "observed_at": 1800000000,
  "allow_restart_from_scratch": true
}
```

| 필드 | 사실의 원본·조건 |
|---|---|
| job_id, generation | 요청한 작업·장애 원본 세대와 정확히 일치 |
| recovery_eligible | 사용자 취소·중단·정상 완료·삭제 작업은 false. 실패 작업만으로는 제외하지 않음 |
| jobset_uri, jobset_sha256 | Backend가 제출 때 저장한 원본 `code/<job_id>/jobset.json`과 그 원본 바이트 해시. config.json 해시 아님. 최초 제출 시 보존한 digest/불변 원본을 기준으로 하며, 임의 변경 객체를 다시 해시해 신뢰하지 않음 |
| gpu_count | 전체 요청 GPU 수(1~4) |
| cluster, active_nodes | 스케줄링/신뢰된 배치 기록의 실제 워커·전체 참여 노드. 장애 노드 하나를 전체로 대체하거나 S3 원본의 미지정 배치를 추정하지 않음 |
| placement_generation | 이 배치가 속한 원본 실행 세대 |
| jobset_name, jobset_uid | 관리 클러스터 JobSet 이름·UID. 워커 MultiKueue 복사본 UID가 아님. 이름은 job-{id}-g{generation:04d} |
| observed_at | 실제 배치/작업 상태를 확인한 시각. API 응답 시각으로 오래된 캐시를 새 관측처럼 바꾸지 않음. 장애 직전의 신뢰된 배치 스냅샷 가능 |
| allow_restart_from_scratch | 선택 bool, 생략 시 기존 컨트롤러 정책과 같이 true. 승인본 선택·손상 fallback 자체는 기존 fencing 로직 사용 |

Backend가 실제 배치 정보를 아직 모르면 스케줄링과 연결해야 한다. 빈 노드·가짜 UID로
200을 만들지 않는다. 배치 준비 미완료/일시 upstream 오류는 503으로 응답한다.
401/403 인증·권한 오류, 404 실제 작업 없음, 422 잘못된 요청/영구 메타데이터 오류를 사용한다.
응답에는 비밀번호·kubeconfig·AWS 자격 증명·승인 포인터를 넣지 않는다.

## 컨트롤러 구현

- 신뢰하는 고정 HTTPS origin에만 요청하며 redirect를 따르지 않는다.
- SDK 추가 재시도 없이 timeout 5초(설정 상한 10초), 기존 Runner의 단계당 총 3회·5분 RECEIVED 제한 적용.
  연결/timeout·408·429·5xx 등은 기존 재시도 규칙, 401/403/404/422 및 잘못된 본문은 HOLD.
- 작업·세대·클러스터·장애 노드 포함·노드 수/중복·원본 URI/해시·관리 이름/UID·관측 신선도를 검증한다.
- 조회 전후 fencing을 강한 일관성으로 읽어 세대·owner·복구 소유권을 확인한다.
  오래된 세대는 STALE, 다른 복구 소유권은 HOLD. 등록 시 fencing의 generation/owner/승인본은 변경하지 않는다.
- `PutItem ConditionExpression=attribute_not_exists(pk)`로 생성한다. 이미 있는 항목은
  API로 갱신/덮어쓰지 않는다. 동시 생성 충돌은 강한 일관성 재조회로 원본 주소·해시·GPU 수를 대조한다.
  응답 유실 뒤 재처리도 기존 항목을 재사용한다.
- 허용 필드만 저장한다. `registration_source`, `registration_event_id`, `registered_at`을 추가한다.
  API 응답은 복구 완료용 placement/progress 증거를 대신하지 않는다.
- 두 테이블은 단일 트랜잭션이 아니다. 조회 후 권한 변경은 기존 claim의 세대 CAS가 최종 차단한다.
- observe·분류기 hold/refuse/none·job.failed-only 안전망·이미 terminal인 사건은 등록하지 않는다.
  기존 HOLD 사건도 자동 해제/재실행하지 않는다.

## 배포 연결 (이번 변경에서 운영 설정 변경 안 함)

Backend API·서비스 인증·배치 데이터 제공이 구현된 뒤 Gitops 설정으로 활성화한다.

```json
"backend_metadata": {
  "enabled": true,
  "base_url": "https://BACKEND-INTERNAL-HOST",
  "token_env": "BACKEND_RECOVERY_API_TOKEN",
  "timeout_seconds": 5,
  "max_age_seconds": 300,
  "future_skew_seconds": 30
}
```

컨트롤러 Pod에 토큰을 Secret 환경변수로 전달한다. 내부 CA이면 검증 가능한 신뢰 번들을
배포한다(검증 비활성화 금지). 기본 disabled이며 기존 운영 동작을 유지한다.
추가 AWS API 권한은 없다(기존 recovery PutItem/GetItem, fencing GetItem 사용).

## 검증

시험 작업은 job# 미등록 상태에서 장애 사건을 보내고 API 조회 → job# 생성 → 세대 전환 순서를 확인한다.
조회 실패·오래된/다른 세대·취소 작업에서는 세대 증가와 RR 생성이 없어야 한다.
동일 사건 재처리·동시 사건·DB 응답 유실에서 항목 덮어쓰기와 중복 복구가 없어야 한다.
이후 새 배치 증거·GPU-254 복원/첫 step 입력까지 연결해 COMPLETED를 따로 확인한다.
