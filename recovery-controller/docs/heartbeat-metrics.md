# Fencing heartbeat metrics (GPU-224 / ADR-44)

Kafka source 모드(observe/execute 모두)에서 기존 HTTP :8080/metrics에 노출한다.
disabled 모드는 AWS를 조회하지 않는다. 리더·대기 replica 모두 독립적인 읽기 전용
수집기를 실행한다. 복구 판단·claim·세대·승인 상태를 변경하지 않는다.

| 지표 | 의미 |
| --- | --- |
| recovery_fencing_heartbeat_age_seconds{job_id,generation} | 현재 UTC 시각에서 heartbeat_at을 뺀 초. 미래 시각은 0으로 제한 |
| recovery_fencing_heartbeat_scrape_success | 마지막 전체 DB 조회 성공 및 60초 이내 snapshot이면 1, 아니면 0 |
| recovery_fencing_heartbeat_read_errors_total | DB 조회/수집 초기화 실패 횟수. 작업 라벨 없음 |
| recovery_fencing_heartbeat_invalid_items | 최근 성공 조회에서 신원·UTC heartbeat 누락/오류 항목 수 |
| recovery_fencing_heartbeat_last_success_timestamp_seconds | 마지막 전체 조회 성공 Unix 시각, 최초 성공 전 0 |

30초 주기로 config.fencing_table(운영 devoops-prod-fencing)을 페이지 끝까지
consistent Scan하고 job_id·generation·heartbeat_at만 가져온다. /metrics 요청은
캐시만 읽으며, 경과 시간은 scrape 시점에 계산한다. 한 페이지라도 실패하면
부분 결과와 이전 age 시계열을 내보내지 않고 success=0·오류 카운터로 표시한다.
누락·잘못된 값도 age=0으로 만들지 않고 invalid_items에 집계한다.
정상 갱신에서 사라진 항목·옛 generation 라벨은 제거한다. 정상 종료된 작업도
DB에 남으면 노출되므로 모니터링 담당자가 trainer Pod와 job_id/generation을
연결해 실행 중인 작업만 알람 판단한다. 미래 heartbeat는 시계 동기화 점검 대상이다.

## 배포 담당 확인

- 기존 AWS 인증을 재사용한다. 단, **fencing 테이블 dynamodb:Scan 권한 필요**.
  기존 GetItem 권한만으로 모든 작업 열거는 불가능하다. 권한이 없다면 배포 전에
  담당자와 조회 방식/권한을 합의한다. 이 변경은 IAM·Gitops를 수정하지 않는다.
- 기존 Prometheus target이 이 이미지의 모든 replica /metrics를 수집하는지 확인한다.
  중복 replica의 age를 합산하지 않는다. 조회 실패·success=0·데이터 누락 알림도
  함께 구성하며, heartbeat age만으로 노드 장애 또는 학습 종료를 확정하지 않는다.
- 30초마다 replica별 전체 테이블 Scan이므로 항목 수·RCU·라벨 수를 확인한다.
  ProjectionExpression은 네트워크 전송량을 줄이지만 Scan 읽기 비용을 줄이지 않는다.
- 테스트는 모의 DB로 수행하며 실제 IAM·DynamoDB·Prometheus 연결 검증은 별도다.
