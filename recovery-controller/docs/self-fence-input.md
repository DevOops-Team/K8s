# VPN/API 단절 시 자기 펜싱 입력 (GPU-255, ADR-49/50)

이 코드는 종료 기록을 검증·전달하며 직접 학습 종료나 워커 API 호출을 하지 않는다.
VPN 단절만으로 종료를 인정하지 않는다. 워커 API 접근 가능 시 실제 소멸 확인이 우선이다.

## 기본 비활성화

```json
"self_fence_input": {"enabled": false, "producer_contract_confirmed": false}
```

활성화하려면 execute와 `relocation_status_input.enabled=true`가 먼저 필요하다.
컨트롤러의 두 self_fence 플래그와 relocator `SELF_FENCE_EVIDENCE=on`은 jobspec/사이드카
보강 및 EKS 시험 통과 후 함께 승인한다. 이번 변경은 운영 설정·IAM·Gitops를 바꾸지 않는다.

## 검증과 전달

- 매 reconcile에서 강한 일관성 fencing 조회. generation=옛 세대+1,
  recovery_from_gen/to_gen, recovery_id 일치 및 GENERATION_ADVANCED에서만 주석 생산.
- self_fence_gen=옛 세대, cluster=원래 워커, world_size=원본 Pod 수(1..4),
  번호 정확히 0..N-1, 번호마다 서로 다른 Pod UID 하나, 유효한 시간대 포함 기록 시각 검사.
- 원본은 단일 replicatedJob, maxRestarts=0(명시), Indexed, backoffLimit=0,
  podReplacementPolicy=Failed, restartPolicy=Never여야 한다. 이는 계약 검사이지
  모든 상황에서 Pod 재생성이 불가능하다는 증명이 아니다. 실제 동작 시험이 필요하다.
- self_fence_at은 펜싱 표의 세대 전환 시각(stop_wait_started_at)-30초 이후, 현재+30초 이하여야 한다.
  RR 생성이 지연돼도 기준은 같으며, status 재검사 때도 현재 펜싱 표의 같은 값을 사용한다.
  복구 중에는 오래된 조회라는 이유로 전체 종료 보고를 만료시키지 않는다.
- 일부/잘못된 보고는 주석을 생산하지 않고 기존 종료 대기를 유지한다. DB 오류는 기존
  API 오류 정책을 따른다. 취소 요청에는 생산하지 않는다.
- RR resourceVersion을 조건으로 주석 PATCH, 재조회 후 status를 처리한다.
  검증 완료 주석은 지연·중복 보고가 와도 바꾸지 않는다. 상충한 주석은 HOLD 대상이다.
- DB와 Kubernetes는 단일 트랜잭션이 아니다. 주석은 해당 RR/세대에만 유효하며
  status 반영 시 기존 Store 조건부 쓰기로 현재 실행 권한을 다시 검사한다.

## 종료 결과 수용

`self_fence` kind는 단순 allowlist 확장으로 인정하지 않는다. 활성화 상태, 같은 RR의
검증 주석·원본·인원수·클러스터를 재검사한다. 혼합 증거의 전체 kind는
`ec2 > self_fence > automatic`. 이후 기존 OLD_RUN_STOPPED 조건부 기록과
recovery-stop-recorded 주석 장벽을 통과해야 재제출한다. manual 증거는 여전히 별도다.

## 생산자/배포 선행 조건

Data는 self_fence_pods(index:PodUID), 실제 trainer·자식 종료 확인, 현재 복구에 묶인
조건부 쓰기를 구현해야 한다. 현재 Data main의 기존 record_self_fence는 rank 집합만
기록하는 버전이어서 이 입력을 충족하지 못하며, 이를 우회해 인정하지 않는다.
RR 주석은 기존 컨트롤러/관리자 쓰기 입장 정책과 patch RBAC로 보호한다.
컨트롤러는 기존 fencing GetItem 권한을 사용하며 relocator에 DB 권한을 추가하지 않는다.

시험: 전원/일부 종료, 사이드카 단독 재시작, NCCL/trainer 정체, 동일 index 대체 Pod,
옛 세대·복구 보고 혼입, 취소·권한 전환·응답 유실, 워커 API 복구, 프로세스 재시작 시도.
실제 EKS 종료 보장과 운영 활성화는 이번 로컬 테스트로 검증한 것이 아니다.
