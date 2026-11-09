# Pod 내부 프로세스 자동 재시작

별도 Calico 재생성 코드는 제거했습니다. kubelet이 restartPolicy와 livenessProbe에 따라 컨테이너를 다시 시작합니다. 외부 Ansible 복구기는 kubelet/containerd 서비스 중단만 담당합니다.

| 우리 선언으로 관리하는 서비스 | 생존 검사 |
|---|---|
| frontend / backend | 현재 열린 HTTP 포트 TCP 검사. startupProbe 성공 후 적용 |
| recovery-controller | /healthz HTTP. Kafka/worker 연결 상태는 /readyz로 분리 |
| Traefik | 공식 차트의 /ping 기반 프로브 |
| cert-manager controller / webhook | 공식 차트의 /livez 기반 프로브 |
| cert-manager cainjector | metrics 포트 TCP. 별도 매니페스트로 관리 |

기본 연속 실패 3회는 재시작 조건이며 총 재시작 횟수 상한이 아닙니다. cert-manager controller는 공식 기본값의 느린 허용치 8회를 유지합니다. TCP 검사는 포트 응답만 확인하므로 앱 전체 교착을 검출하지 못할 수 있습니다. frontend/backend에 외부 의존성을 검사하지 않는 실제 /livez가 구현되면 HTTP 프로브로 전환하세요. 존재하지 않는 HTTP 경로를 임의로 지정하지 않았습니다.

공식 cert-manager 차트 v1.21.3은 cainjector livenessProbe를 렌더링하지 않습니다. 차트 cainjector.enabled=false로 중복 배포를 막고, 동일 버전 차트에서 추출한 Deployment/ServiceAccount/Service/RBAC를 onprem/platform/cert-manager/cainjector에서 관리합니다. 기존 2개 replica·워커 배치·권한을 유지합니다. cert-manager 버전 변경 시 cainjector 이미지·매니페스트도 함께 갱신해야 합니다. CA 주입 기능은 제거하지 않았습니다.

Kubespray 설치 Calico·CoreDNS와 Kubernetes 시스템 컴포넌트는 upstream 프로브를 사용합니다. EKS Kueue/JobSet/Karpenter는 해당 설치 담당의 공식 설정 영역이며 이 저장소에 임의 엔드포인트를 생성하지 않습니다. Argo CD bootstrap은 아직 적용 전이며 공식 HA 매니페스트의 기본 프로브를 유지합니다. 위 표는 이 작업에서 직접 관리·추가한 서비스 범위입니다.

전체 manifest 검증은 직접 관리하는 모든 Deployment/StatefulSet/DaemonSet의 각 컨테이너에 livenessProbe가 있는지 확인합니다. Helm 차트는 별도로 렌더링해 확인합니다. Job/init 컨테이너는 완료돼야 하는 실행이므로 장기 서비스와 같은 생존 프로브를 추가하지 않습니다.

실제 서버 적용·이미지 실행·Argo 동기화·장애 주입은 하지 않았습니다.
