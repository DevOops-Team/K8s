# 복구 규약 대조 — 2026-11-09

기준: [체크포인트 규약 v13](https://devoops-team.atlassian.net/wiki/spaces/GPUF/pages/9699330), [장애 복구 규약 v1](https://devoops-team.atlassian.net/wiki/spaces/GPUF/pages/17268776).
기존 로컬 미커밋 변경 위에 필요한 보완만 반영했습니다. 서버 배포·원격 push는 수행하지 않습니다.

| 규약 | 확인 결과 |
|---|---|
| CLAIMED → 세대+1·owner 제거 → 단계별 재개 | 기존 구현 확인. 같은 복구의 중복 증가 방지·retry_count 원자 갱신 유지 |
| 승인본·이전 승인본 유지 | 컨트롤러는 approved/prev_approved를 변경하지 않음. CLAIMED의 복원 기준 고정 유지 |
| 승인본 없는 복구 | 기본 처음부터 재시작으로 정합화. 영속 경고·결과 사건 경고 추가. 불완전 승인·손상된 승인본은 별개 |
| COMPLETED | 기존 progress의 세대·복구 ID·새 JobSet UID·복원 기준·실제 step 증가 검사 유지. 처음부터 시작도 실제 진행 증거 필요 |
| API 재시도·단계 제한 | 기존 API 총3회·300/900/900/1800초 기본값 확인. 정상 조회는 오류 재시도와 구분 |
| 작업 전체 재시도 | 기존 하드코딩 3회를 설정 가능한 max_recovery_attempts로 변경. 기본 3회 유지 |
| Kafka 결과 재전송 | 기존 영속 outbox·최대300초 backoff 확인. 경고 필드 추가. 직접 Slack 전송은 없음 |
| 이전 실행 종료 | Pod 조회 403을 종료로 처리하던 relocator 경로 수정. 조회 불가 시 대기 |

## 별도 담당·미합의 경계

- ckpt-init·사이드카의 실제 DCP shard 다운로드/업로드·manifest/checksum·STOP·승인 조건은 해당 구현 저장소와 통합해야 합니다. 이 복구 컨트롤러에 S3 전송 코드를 중복 구현하지 않습니다.
- 승인본 손상 시 prev_approved 후보의 정상 여부와 SDC 복구점, 예비 소진 신호, 사전 체크포인트 handshake는 확정된 입력/계약이 없어 자동 진행하지 않습니다.
- evacuateCluster에서 실제 종료 대신 인정할 독립 펜싱 증거는 합의 전입니다. 접속 불가나 generation 증가만으로 종료를 확인하지 않습니다.
- progress/placement 생산자와 실제 DynamoDB·Kafka·Kubernetes 연결, 시간 제한의 상호 정합성은 통합 시험이 남습니다.
- 기존 운영자가 붙이는 종료 확인 annotation은 사람의 실제 확인을 전제로 합니다. 컨트롤러가 자동으로 생성하지 않습니다.
