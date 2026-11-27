# Pod/Node 재시작과 GPU 이전 구분 (GPU-304)

컨트롤러만 변경한다. 감지 가능 여부를 EKS API에 실시간 질문하는 정책이 아니라, 수신한 source·분류·Xid·추천을 이용하는 규칙이다.
이 정책이 종료 증거 부족이나 Held를 해결하는 것은 아니다. 기존 펜싱·종료 확인·재시도 한도는 그대로 적용한다.

## 선택 기준

- 일반 Pod 종료/삭제/실패: RETRY_GROUP → restartInPlace. 옛 노드가 정상이라는 기존 계약대로 nodes를 전달해 노드 고정 유지.
- source=node의 일반 Node 소실/NotReady: RETRY_GROUP → restartInPlace. node 신원과 옛 배치 소속을 검증하고 **spec.nodes를 생략**한다.
  현재 relocator pin_spec은 nodes가 없으면 고정하지 않는다. 원래 클러스터 선호 힌트는 유지하고 디스패처가 정상 용량을 선택한다.
- GPU/드라이버 격리: dcgm·dmesg(호환 xid/gpu/driver)의 hw/driver 신호나 이전 추천, 명확한 하드웨어/드라이버 Xid
  48/63/64/79/119/120은 QUARANTINE_AND_RETRY → moveToSpare. 제외 노드 스냅샷·cordon/taint 경로 유지.
- Pod/Node 봉투에 같은 GPU 원인 근거가 있으면 GPU 이전 우선. hw/driver 판정+GPU correlated source도 인정한다.
  gpu_uuid가 붙었다는 이유만으로 이전하지 않는다. Xid 13 등 앱 오류의 restart_in_place 추천은 재시작 유지.
- Spot은 GPU 고장이 아니어도 제거 예정 노드 재사용을 피하기 위해 moveToSpare. 마지막 승인본 복원 정책 유지.
- 클러스터 장애는 evacuateCluster. OOM/설정의 refuse, 불확실한 hold, none은 그대로 거절/보류/무시.
- straggler는 기존 전용 승인 조건·정책 스위치·최대 1회·가용 용량 검사를 유지한다.
- source 없는 legacy node_hardware는 기존 moveToSpare 호환 경로. 새 정상 노드 분류에는 source=node를 반드시 제공한다.

## 일반 노드의 restartInPlace가 의미하는 것

Kubernetes에서 옛 노드를 살리거나 자동 복구됐다고 가정하는 것이 아니다. 그룹 전체를 새 세대로 재제출한다.
죽은 노드에 재고정하지 않으려고 기존 relocator의 **nodes 부재 = pin 없음** 계약을 사용한다.
노드의 실제 배치 가능 여부는 Kubernetes/Kueue가 결정한다. 새 GPU 수를 임의로 줄이지 않는다.
이 경로는 별도 faulty cordon/taint를 요청하지 않고 Kubernetes의 Node 상태·taint·스케줄링 제약을 따른다.
노드가 영구 고장인데 Ready로 잘못 보고되는 등 Kubernetes가 제외하지 못하는 경우는 일반 Node 재시작 정책의 전제가 아니며,
명시적 격리 신호/운영 조치가 필요하다. 자동으로 가용한 노드가 있다고 가정하지 않는다.

옛 노드 전체를 잃지 않도록 RR `devoops/recovery-old-placement`에 다음 감사 스냅샷을 남긴다:

```json
{"cluster":"eks-a","generation":1,"nodes":[{"name":"old-node-1"},{"name":"old-node-2"}]}
```

이 주석은 종료 승인이나 새 목적지 지시가 아니다. stopDeadline·sourceCluster·jobSet·toGeneration은 기존 spec을 유지한다.
409 재시도는 spec과 이 스냅샷을 함께 비교하고, 다른 요청/배치로 덮어쓰지 않는다. 기존 spec 불변 계약은 유지한다.
향후 GPU-304 독립 종료 증거 수신부는 일반 노드 restart의 옛 실행 범위를 spec.nodes가 아니라 이 감사 스냅샷/등록 배치에서
확인해야 한다. 이전 PR #79의 제안 검증부와 함께 적용할 때는 이 입력 차이를 먼저 맞춰야 한다.

## 진행 중 요청·HOLD

새 정책을 배포했다고 이미 만든 moveToSpare RR을 restartInPlace로 수정하지 않는다.
claim 결과 저장 시 recovery_action과 routing_reason을 기록하고 다음 사이클/리더 교체에서 유지한다.
기존 진행 기록의 action을 사용하고, HOLD로 action이 덮였고 별도 snapshot이 없는 구버전 기록은
기존 RR action을 읽어 판단을 복원한다. 기존 RR을 찾지 못하거나 유효 action이 없으면 진행하지 않는다.
legacy spec 호환은 기존 계약을 유지하며, 생성돼 있는 불변 RR의 action·배치를 재작성하지 않는다.

## 인계·검증

성모 리뷰 포인트: source=node의 nodes 생략 fallback, 원래 클러스터 선호, faulty 격리 미요청, 옛 실행 감사 범위.
relocator/CRD/운영 매니페스트는 이 PR에서 수정하지 않는다. 문서상 spec.nodes가 모든 요청에 항상 있다고 가정한 부분은
이 일반 노드 재시작 예외로 조정해야 한다. 종료 확인을 생략하지 않는다.

로컬 검증: 실패 영역별 매핑, GPU 상관 우선, raw node/gpu_uuid만으로 오판하지 않음, Spot/refuse/HOLD 유지,
실제 RelocationInputs와 RR 생성·409 재시도, native 노드 배치 검증이 claim 전에 실패하는지,
진행 중/구버전 HOLD의 action 보존. EKS 실제 재배치·GPU 장애·NotReady 종단 간 시험은 배포 후 필요하다.
