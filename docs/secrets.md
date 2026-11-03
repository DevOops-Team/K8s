# Git에 넣지 않는 입력
- platform/ghcr-pull, 각 EKS training/ghcr-pull: gpu-iac의 secrets bootstrap.
- cert-manager/cloudflare-dns-api-token(api-token): gpu-iac의 secrets bootstrap.
- cloudflare-system/cloudflared-token(token): gpu-iac의 Cloudflare API 구성 후 등록.
- kueue-system/eks-a-kubeconfig 등(kubeconfig): gpu-iac의 remote-secret bootstrap.
- frontend-tls와 ACME 계정 Secret: cert-manager가 생성/갱신.
- Argo CD repository/cluster credential: argocd CLI 또는 해당 Secret 관리 담당이 등록.
비밀값·.env·개인키·실제 kubeconfig를 이 저장소에 커밋하지 않습니다.
앱별 외부 서비스 Secret은 담당 파트에서 준비하고 Deployment envFrom/volume으로 이름만 참조합니다.
