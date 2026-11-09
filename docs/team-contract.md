# namespace·계정·접속 주소 정합성

대조일 2026-11-09. 실제 배포 완료를 뜻하지 않는다. 이름을 변경할 때 GitOps·RBAC·Secret·AWS 인증 주체를 함께 갱신하며, 과거 문서의 다른 이름을 병행 배포하지 않는다.

| 항목 | 코드 기준 | 근거·확인 상태 |
|---|---|---|
| 복구 컨트롤러 | namespace `platform`, Deployment·ServiceAccount·Lease `recovery-controller` | 최신 스케줄링 문서의 `platform/recovery-controller`와 일치. 과거 IAM 요청의 `recovery` namespace는 이전 표기로 구분. 외부 IAM 연결은 담당 확인 필요 |
| 컨트롤러 비밀 입력 | `platform/recovery-controller-credentials`, `platform/ghcr-pull` | Pod와 같은 namespace. AWS 인증 경로는 별도 연결 |
| 복구 요청 | 관리 클러스터 `training`의 RelocationRequest | Role/RoleBinding은 training에 있고 subject는 platform/recovery-controller. job_namespace 변경 시 요청과 RBAC도 함께 변경 |
| 학습 작업 계정 | EKS `training/trainer` | 체크포인트 규약 §13. 온프레 컨트롤러의 AWS 인증과 별개 |
| Frontend·Backend | namespace `platform`, SA `frontend`·`backend` | 기존 서비스 배포 유지 |
| Argo CD·Kueue | `argocd`·`kueue-system` | 전용 namespace 유지, MultiKueue kubeconfig는 별도 자격증명 |
| 관리 API | `https://172.30.0.120:6443` | 인프라 설정값 v4에서 사용 중. 실제 kubeconfig context와 인증 파일은 개인 입력 |
| Kafka 목표 목록 | `.40:9092,.41:9092,.42:9092` (모두 172.30.0.x) | 데이터 파트 3대 전환 계획. VM 주소와 서비스 준비 완료는 구분. 인증·advertised.listeners 확인 후 사용 |
| 모니터링 | API `.30:6443`, 서비스 `.35` | 예약 상태. Alertmanager 실제 URL/포트 미확인, watchdog 기본 비활성화 |
| AWS | us-east-2, `devoops-prod-fencing` | 복구 전용 inbox 테이블 실제 이름·온프레 인증은 별도 합의/준비. 임의 이름 생성 안 함 |

과거 네트워크 문서의 단일 Kafka 주소보다 최신 데이터 파트의 3대 전환 목표를 예시 설정에 반영했다. 이 설정은 observe·execution_enabled=false이며 주소 변경만으로 Kafka/AWS 연결이나 복구가 활성화되지 않는다. 배포 전 실제 접근과 readiness를 확인한다.

Ansible은 ADR-11에 따라 개인 PC에서 수동 실행한다. Kubernetes 안의 학습 복구 컨트롤러 운영과 별개이며, 전용 VM/timer 설치를 전제로 하지 않는다. Cloudflare DNS·pfSense 진입점 변경은 [dns-entrypoint.md](dns-entrypoint.md)를 따른다.

## 근거

- [스케줄링 방식 — 권한 표 platform/recovery-controller](https://devoops-team.atlassian.net/wiki/spaces/GPUF/pages/8617985)
- [체크포인트 규약 §13](https://devoops-team.atlassian.net/wiki/spaces/GPUF/pages/9699330)
- [온프레 인프라 설정값 v4](https://devoops-team.atlassian.net/wiki/spaces/GPUF/pages/9732117)
- [데이터 파트 설계 — Kafka 전환](https://devoops-team.atlassian.net/wiki/spaces/GPUF/pages/2916376)
- [ADR-11 — 채택된 개인 PC 수동 실행](https://devoops-team.atlassian.net/wiki/spaces/GPUF/pages/11730951)
