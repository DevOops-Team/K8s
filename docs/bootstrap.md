# Gitops 등록 경로

Argo CD root와 AppProject는 [Gitops 저장소](https://github.com/DevOops-Team/Gitops)의 아래 경로에서 관리합니다. IaC의 `gitops_repo_url`과 `gitops_root_dir`에 해당 저장소 URL과 로컬 절대 경로를 지정합니다.

| 대상 | Gitops 경로 | Application |
| --- | --- | --- |
| 온프레미스 | `bootstrap/onprem` | `gpu-root` |
| EKS A | `bootstrap/eks-a` | `gpu-root-eks-a` |
| EKS B | `bootstrap/eks-b` | `gpu-root-eks-b` |
| EKS C | `bootstrap/eks-c` | `gpu-root-eks-c` |

EKS 자식 Application을 sync하기 전에 관리 Argo CD에 해당 EKS 이름을 등록하고 Kueue·JobSet 설치, 자원·권한·MultiKueue 연결을 확인합니다. 기존 Application 인계 순서는 [마이그레이션](migration.md)을 따릅니다.
