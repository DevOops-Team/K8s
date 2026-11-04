# 연동 규약 제안 v1

팀의 확정 API가 아니라 현재 어댑터가 사용하는 제안입니다. 실제 배포 전에 담당자 간 일치 여부를 확인해야 합니다.

## 입력 이벤트

example-event.json 참고. event_id는 전역 고유 사건 ID, job_id는 논리 학습 ID, cluster는 eks-a/b/c, node는 사건 당시 참여 노드, generation은 사건 당시 실행 번호(양의 정수), fault_type은 README의 분류입니다. 같은 ID의 다른 본문은 오류로 거부합니다. 사건 ID를 바꾸어 같은 사건을 반복 발행하지 않도록 생산자에서도 중복 관리를 해야 합니다.

## DynamoDB

pk=event#EVENT_ID: event, digest, state, published 및 단계 결과.
pk=job#JOB_ID: generation, cluster, gpu_count(1..4), active_nodes, checkpoint_uri(승인된 s3:// 경로).
컨트롤러는 기존 generation·cluster·active_nodes 조건으로 generation+1 및 recovery_id 잠금을 한 번에 기록합니다. 재시작 시 같은 recovery_id면 다시 증가시키지 않습니다. 다른 사건의 잠금은 덮어쓰지 않습니다. RECOVERED 확인 후 cluster 갱신·잠금 제거. checkpoint_uri는 보존합니다.
재배치 실행기는 새 노드 명단을 해당 generation/recovery_id 조건으로 active_nodes에 갱신해야 다음 사건을 정확히 검증할 수 있습니다. 이 컨트롤러는 S3 객체를 직접 검증하거나 다운로드하지 않습니다.

## 실행기 HTTPS API

모두 POST, Authorization: Bearer TOKEN, Idempotency-Key: RECOVERY_ID:OPERATION.
요청 필드: recovery_id, job_id, source_cluster, failed_node, old_generation, generation, checkpoint_uri, gpu_count, action. status에는 target_cluster 추가.
응답에는 recovery_id와 generation을 반드시 그대로 돌려줍니다. 같은 멱등 키는 부수효과를 한 번만 수행하고 기존 결과를 재조회해야 합니다. generation이 이미 바뀐 요청은 거부해야 합니다.

- /v1/recoveries/fence: 노드 고장이면 신규 배치를 차단하고 이전 학습 그룹을 정리. 원격 프로세스 종료 또는 검증된 펜싱을 확인한 경우에만 old_execution_stopped=true. 단순 API 객체 삭제/VPN 단절은 충분한 근거가 아님.
- /v1/recoveries/restart: 종료 확인을 실행기에서도 검증하고 매니저 원본 경로로 복구 작업 제출. 같은 멱등 키의 동일 JobSet은 중복 생성하지 않음. accepted=true, target_cluster 반환. AZ 대피는 다른 EKS, 나머지는 source_cluster 유지. 승인된 GPU 수·체크포인트 유지. 노드 격리는 실행기 담당, 노드 확보는 Karpenter 담당.
- /v1/recoveries/status: 같은 generation·target_cluster를 확인하고 checkpoint_loaded=true 및 step_completed=true를 실제 학습 신호로 입증. Running 상태만으로 반환하지 않음.

작업 제출 성공 후 실행기/학습 코드가 실제 checkpoint 로드와 step 진행 정보를 제공해야 합니다. 이 API 없이 컨트롤러만으로 자동 복구가 완료되지는 않습니다.

## 출력 이벤트

recovery.action: event_id=원본ID:result, job_id, state, generation. 전송은 최소 한 번이며 수집기는 event_id로 중복 제거. 현재 terminal 결과만 발행합니다. 관찰 모드의 OBSERVED는 실제 조치가 아닙니다.
