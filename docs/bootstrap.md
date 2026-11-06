# 대상별 bootstrap

두 저장소의 실제 경로는 달라도 됩니다. gpu-iac의 gitops_root_dir에 해당 대상의 bootstrap **절대 경로**를 지정합니다.

| 대상 | root 경로 | Application |
|---|---|---|
| onprem | argocd/bootstrap/onprem | gpu-root |
| EKS A | argocd/bootstrap/eks/eks-a | gpu-root-eks-a |
| EKS B | argocd/bootstrap/eks/eks-b | gpu-root-eks-b |
| EKS C | argocd/bootstrap/eks/eks-c | gpu-root-eks-c |

root/Application 리소스는 중앙 Argo가 있는 온프레미스 argocd namespace에 등록됩니다. EKS root의 자식 Application만 해당 EKS에 적용됩니다.

1. gpu-iac에서 온프레미스 Kubernetes/MAP/Argo CD를 준비합니다. UI는 우선 port-forward로 접근하고 GitHub 읽기 자격증명을 등록합니다.
2. GitHub는 https://github.com/DevOops-Team/K8s.git / develop을 사용합니다. 실제 앱 이미지·ACME 이메일은 configure.py로 설정합니다.
3. bootstrap.local.yml의 root 경로를 onprem으로 설정하고 온프레미스 Secret/root만 등록합니다.
4. EKS의 생성·네트워크·GPU·AWS 노드 자동 복구는 AWS 담당이 준비합니다. Kueue/JobSet은 스케줄링 담당이 각 EKS에 설치합니다.
5. gpu-iac/ansible/environments/eks/eks-a.example.yml을 eks-a.local.yml로 복사해 실제 AWS region/name/nodegroup, kubeconfig/context, node selector를 입력합니다. eks/preflight.yml로 읽기 전용 점검을 수행합니다.
6. Argo CLI 등으로 EKS를 eks-a라는 이름으로 등록합니다. cluster registration credential과 MultiKueue 전용 kubeconfig는 다릅니다.
7. eks/seed-registry.yml, eks/register-root.yml을 EKS A 입력으로 실행하고 gpu-root-eks-a → gpu-queues-eks-a를 sync합니다. B/C는 해당 파일로 각각 실행합니다.
8. onprem/register-worker-credential.yml은 선택한 EKS의 MultiKueue kubeconfig를 매니저에만 등록합니다. 이후 매니저의 gpu-multikueue를 sync합니다.
9. Cloudflare의 외부 API 구성과 cloudflared-token 공급 후 gpu-cloudflared를 sync합니다.

EKS가 없거나 접근·인증이 준비되지 않았으면 관련 명령은 성공하지 않습니다. 예시만으로 AWS 자원을 생성하지 않으며 실제 값·권한을 요구합니다.
AppProject/root 변경은 해당 bootstrap 경로로 register-root를 다시 실행합니다. 자식 Application 정의 변경은 해당 root를 sync하고, 이미지 digest 같은 소스 변경은 자동 sync를 켠 해당 자식 앱이 읽습니다.
