# 최초 부트스트랩

## 실행 환경

실행 PC/VM에 두 저장소를 내려받아 두되 경로가 같을 필요는 없습니다. gpu-iac/ansible/bootstrap.local.yml의 gitops_root_dir에 이 저장소 argocd/bootstrap의 절대 경로를 지정합니다.
Argo CD 서버·GitHub·EKS API에 연결 가능한 관리 환경을 사용합니다. cluster API나 Argo UI를 인터넷에 공개할 필요는 없습니다.

## 실행 순서

1. gpu-iac에서 VM 6대의 공통 준비와 Kubespray 설치, MAP 확인을 수행합니다.
2. bootstrap.local.yml의 manager_context와 확인 플래그를 설정한 뒤 bootstrap/install-argocd.yml을 실행합니다. 버전과 SHA256은 gpu-iac에 고정되어 있고 설치는 HA 공식 매니페스트에 worker nodeSelector를 적용합니다.
3. Argo UI/API는 우선 `kubectl --context CONTEXT -n argocd port-forward svc/argocd-server 8080:443`로 접속합니다. 초기 로그인 정보는 Kubernetes Secret을 로컬에서 확인하고 변경합니다. 비밀값을 문서·Git·채팅에 복사하지 않습니다.
4. GitHub 저장소는 https://github.com/DevOops-Team/K8s.git 이며 develop을 사용합니다. 이미지·이메일을 configure.py로 설정·검증한 뒤 develop에 push합니다. 두 폴더는 원래 다른 부모 Git 저장소 아래에 있었으므로 로컬 독립 Git 저장소를 사용하며 원격 주소는 사용자가 나중에 지정합니다. 자동 commit/push는 하지 않습니다.
5. 비공개 repo는 인증정보를 별도로 등록합니다. 예: `argocd repo add 실제주소 --ssh-private-key-path /절대경로/읽기전용배포키` (SSH repo 주소 사용 시). HTTPS 인증도 가능하며 토큰은 안전한 실행 환경에서 공급합니다.
6. EKS 3개를 `argocd cluster add 실제context --name eks-a` 형태로 등록합니다. 이 명령은 대상 클러스터 권한을 생성하므로 운영 접근 정책에 맞는 계정·범위를 검토하고 수행합니다. Argo CD 자체 cluster credential과 MultiKueue kubeconfig는 별개입니다.
7. GHCR_USERNAME/GHCR_TOKEN/CLOUDFLARE_DNS_API_TOKEN을 실행 환경에 공급하고 gpu-iac의 bootstrap/seed-secrets.yml을 실행합니다. worker registry 준비가 되었으면 seed_eks_credentials=true로 별도 실행합니다.
8. gpu-iac의 bootstrap/register-root.yml을 실행해 AppProject와 수동 sync root를 등록합니다. root는 로컬 자식 Deployment를 직접 적용하지 않고 Git을 읽는 Application을 등록합니다.
9. README의 sync 순서대로 진행합니다. 스케줄링 파트가 Kueue/JobSet·MAP 정책을 설치한 뒤 worker 큐를 sync하고, MultiKueue 전용 인증 파일을 제공받아 seed_multikueue_credentials=true로 Secret을 등록합니다. 인증정보는 토큰 유효기간/갱신도 관리해야 합니다.
10. frontend 인증서와 endpoint가 준비되면 gpu-iac의 network/configure-cloudflare.yml을 실행합니다. 외부 Tunnel·DNS·token Secret만 준비하고, cloudflared Deployment는 Argo CD로 sync합니다.

Cluster registration과 Secret 등록은 실행 환경 자격증명이 필요하므로 저장소만 push하는 것으로 완료되지 않습니다. GitHub URL과 develop 참조는 설정했습니다. 실제 이미지·ACME 이메일은 아직 없으므로 현재는 템플릿 검증 상태입니다.

AppProject 또는 root Application 자체를 변경하면 gpu-iac의 register-root.yml을 다시 실행해 반영합니다. root는 기본 수동 sync이므로 자식 Application 정의 변경 후에는 gpu-root도 sync합니다. 이미지 digest 변경은 해당 자식 앱이 직접 읽으므로 root 정의 변경이 아닙니다.
