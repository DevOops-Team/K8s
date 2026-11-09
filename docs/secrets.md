# Git에 넣지 않는 입력
- platform/ghcr-pull, 각 EKS training/ghcr-pull: gpu-iac의 secrets bootstrap.
- cert-manager/cloudflare-dns-api-token(api-token): gpu-iac의 secrets bootstrap.
- kueue-system/eks-a-kubeconfig 등(kubeconfig): gpu-iac의 remote-secret bootstrap.
- frontend-tls와 ACME 계정 Secret: cert-manager가 생성/갱신.
- Argo CD repository/cluster credential: argocd CLI 또는 해당 Secret 관리 담당이 등록.
비밀값·.env·개인키·실제 kubeconfig를 이 저장소에 커밋하지 않습니다.
앱별 외부 서비스 Secret은 담당 파트에서 준비하고 Deployment envFrom/volume으로 이름만 참조합니다.

- platform/recovery-controller-credentials: Kafka KAFKA_CLIENT_CONFIG_JSON 등 컨트롤러 전용 비밀 입력. 다른 namespace의 Secret을 참조하지 않습니다.
- 온프레 AWS Roles Anywhere 인증서/키·프로파일 연결은 별도 준비합니다. EKS training/trainer Pod Identity와 구분합니다.
