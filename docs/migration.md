# 운영 매니페스트 Gitops 인계

1. Gitops의 매니페스트 이전 PR을 먼저 병합하고 운영 브랜치 `main`에 반영합니다.
2. IaC의 기존 로컬 설정에서 `gitops_repo_url`을 Gitops로, `gitops_root_dir`을 [대상별 경로](bootstrap.md)로 바꿉니다. 각 root와 AppProject의 Argo diff를 확인합니다.
3. 기존 Application 이름을 유지한 채 source를 전환합니다. Gitops에 이미 이전한 프런트엔드·백엔드·TLS는 최신 Gitops 설정을 사용합니다. 리소스 소유권, 새 리소스 추가 여부, 배포 전제 조건을 확인하고 sync합니다.
4. 기존 K8s source를 참조하는 운영 Application이 모두 전환된 뒤 K8s의 제거 PR을 병합합니다.

매니페스트 경로는 복구 컨트롤러 `onprem/controllers/recovery-controller`, MultiKueue `onprem/multikueue/queues`, EKS 큐 `eks/scheduling/overlays/eks-{a,b,c}`입니다. 코드와 실습용 매니페스트는 K8s에서 관리합니다.
