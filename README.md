# K8s — gpu-k8s Kubernetes GitOps

이 저장소는 Argo CD가 관리하는 Kubernetes 매니페스트입니다. 실행 중인 클러스터에는 아직 적용하지 않았습니다.
GitHub 저장소는 https://github.com/DevOops-Team/K8s.git 이며 Argo CD는 develop 브랜치를 읽습니다. 앱 이미지·ACME 이메일은 아직 미정이고 최초 구성에는 자동 동기화를 넣지 않았습니다.

## 두 저장소의 경계

| gpu-iac | gpu-k8s |
|---|---|
| Terraform 자원 생성 영역, Ansible VM 준비, Kubespray·HA·API VIP·CoreDNS·MAP | 앱 Deployment·Service·RBAC, Traefik·cert-manager 설정, frontend Ingress·Certificate |
| Argo CD 최초 설치·업그레이드와 root 등록 | Argo Application/AppProject 및 일반 리소스의 지속 동기화 |
| 외부 Cloudflare Tunnel·DNS API 설정 | cloudflared Deployment·PDB |
| 비밀값 최초 등록·갱신, 별도 credential 관리 | 비밀값 없이 Secret 이름 참조 |
| Kubernetes 실행 기반 준비 | EKS 큐·ResourceFlavor·MultiKueue 연결 객체 |

Kueue v0.20.0·JobSet v0.13.0 컨트롤러와 Kueue MAP 정책·디스패처는 스케줄링 파트가 설치합니다. 여기서는 컨트롤러를 설치하지 않고 큐·연결만 관리합니다. 이전 Ansible의 리소스 적용 경로는 삭제했으므로 양쪽이 같은 Deployment 등을 수정하지 않습니다.

## 폴더

```text
argocd/
├─ bootstrap/           # AppProject + root Application; IaC가 최초 등록
└─ applications/        # root가 읽는 11개 구성 요소 Application
apps/
├─ frontend/base/       # Deployment·Service·ServiceAccount·PDB
├─ frontend/overlays/onprem/  # 실제 이미지 digest·replicas
├─ backend/base/        # 내부 ClusterIP Service·Deployment·RBAC
└─ backend/overlays/onprem/
platform/
├─ namespaces/          # 공유 namespace의 단일 소유자
├─ traefik/             # 버전 고정 Helm values
├─ cert-manager/        # 버전 고정 Helm values
├─ web/                 # devoops.site Certificate·Ingress·ClusterIssuer
└─ cloudflared/         # 2개 커넥터와 PDB
scheduling/
├─ manager/             # GPU 12장 매니저 큐·MultiKueue
└─ workers/eks-{a,b,c}/  # EKS별 GPU 4장 큐·연결용 RBAC
scripts/                # 로컬 값 설정·검증; 서버 적용 안 함
examples/               # 아직 활성화하지 않은 선택 워크로드 설명
```

frontend만 devoops.site로 공개합니다. backend는 ClusterIP:8000 예시이고 Ingress가 없습니다. 브라우저의 JavaScript는 ClusterIP에 직접 접속할 수 없으므로, frontend 서버의 내부 프록시/BFF 등 실제 앱의 통신 방식을 맞춰야 합니다. 브라우저에 backend.platform.svc 같은 주소를 넣지 않습니다.

## 1. 로컬 설정

Python 3.12+, kubectl을 준비합니다. 터미널의 현재 위치는 gpu-k8s입니다.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python scripts/validate.py
```

GitHub 저장소 주소·브랜치는 설정되어 있습니다. 앱 이미지와 이메일의 실제 값을 입력합니다. 아래 digest는 실제 GHCR 이미지 값으로 대체합니다.

```bash
.venv/bin/python scripts/configure.py \
  --repo-url https://github.com/DevOops-Team/K8s.git \
  --revision develop \
  --frontend-image ghcr.io/실제팀/frontend@sha256:실제64자리digest \
  --backend-image ghcr.io/실제팀/backend@sha256:실제64자리digest \
  --backend-port 8000 \
  --acme-email 실제이메일
.venv/bin/python scripts/validate.py --ready
```

configure.py는 로컬 파일만 변경합니다. 실제 이미지를 제공한 앱만 replicas=2로 바뀝니다. 초기 0개 복제본과 0으로 채운 digest는 미설정 표시이며 운영용 이미지가 아닙니다.
스크립트는 기존 repo URL도 새 URL로 갱신하므로 재실행할 수 있습니다. 변경 내용을 검토하고 Git에 커밋·push합니다. 루트와 Helm values source를 포함한 모든 Application URL을 같은 gpu-k8s 주소로 설정합니다.
각 EKS의 노드 라벨·CPU/메모리 할당량·training ServiceAccount IAM annotation은 scheduling/workers/eks-*/resources.yaml에서 실제 환경에 맞춥니다. IAM 자원 자체는 여기서 생성하지 않습니다.

## 2. Argo CD 최초 연결

자세한 순서는 [docs/bootstrap.md](docs/bootstrap.md)를 따릅니다.

1. gpu-iac에서 VM/Kubernetes/MAP를 준비하고 Argo CD를 최초 설치합니다.
2. DevOops-Team/K8s 저장소의 develop에 구성을 push합니다. 비공개 저장소 인증은 argocd repo add 등으로 등록합니다.
3. EKS를 Argo CD에 eks-a/eks-b/eks-c 이름으로 등록합니다. kubeconfig context와 Argo cluster name은 별개이며 Application 목적지 name은 후자입니다.
4. Secret을 gpu-iac에서 등록하고 root Application을 최초 등록합니다.
5. root를 sync한 뒤 아래 초기 순서대로 구성 요소를 sync합니다.

자동 동기화는 기본 꺼져 있고 prune도 꺼져 있습니다. sync-wave 숫자는 정렬 정보이며, **Application 리소스 생성 순서만으로 자식 앱 준비 완료를 보장하지 않습니다.** 처음에는 아래 단계별 Health/Sync 상태와 CRD 준비를 확인합니다. 준비 후 configure.py --enable-auto-sync로 자식 앱의 자동 배포를 켤 수 있습니다. 이때도 삭제는 자동 prune하지 않습니다.

## 3. 최초 sync 순서

```bash
argocd app sync gpu-root
argocd app sync gpu-foundation
argocd app sync gpu-traefik gpu-cert-manager
argocd app wait gpu-traefik gpu-cert-manager --health --timeout 600
argocd app sync gpu-frontend gpu-backend
argocd app wait gpu-frontend gpu-backend --health --timeout 600
argocd app sync gpu-web
```

cert-manager CRD와 controller, cloudflare-dns-api-token을 먼저 준비한 뒤 gpu-web을 sync합니다. Certificate/frontend-tls가 Ready인지 별도로 확인합니다.
스케줄링 파트의 Kueue/JobSet과 각 EKS 인증정보가 준비된 뒤:

```bash
argocd app sync gpu-queues-eks-a gpu-queues-eks-b gpu-queues-eks-c
# MultiKueue 전용 kubeconfig를 준비하고 gpu-iac의 remote Secret 등록 실행
argocd app sync gpu-multikueue
```

gpu-iac의 Cloudflare provider 구성으로 Tunnel·DNS·cloudflared-token을 준비한 뒤:

```bash
argocd app sync gpu-cloudflared
```

필요한 Secret 없이 먼저 sync하면 해당 앱이 Pending/Degraded일 수 있습니다. Namespace 이름, release 이름, 기존 Service selector는 이전 구성과 동일하게 유지했습니다. 이미 배포된 리소스가 있다면 [docs/migration.md](docs/migration.md) 순서로 인계합니다.

## 4. 이미지 자동 배포

CI가 앱 이미지를 빌드해 GHCR에 저장한 뒤 해당 앱 overlays/onprem/kustomization.yaml의 digest를 변경해 gpu-k8s에 커밋합니다. Argo CD 자동 sync를 활성화한 앱은 Git의 새 digest로 업데이트됩니다. Argo CD가 애플리케이션 코드를 빌드하거나 GHCR의 latest를 임의로 선택하지는 않습니다.

설정 변경으로 인한 Pod 재시작이 필요하면 Deployment의 Pod template도 함께 변경해야 합니다. 일반 ConfigMap/Secret 변경만으로 모든 앱이 자동 재시작된다고 가정하지 않습니다.

## 검증

validate.py는 모든 Kustomize 소스 빌드, 중복 리소스 소유권, 내부 전용 backend, frontend 라우팅, EKS GPU 할당량을 로컬에서 검사합니다. --ready는 Git URL·이미지·이메일의 미설정 값도 거부합니다. 실제 클러스터·DNS·권한·학습 재개를 검증하는 도구는 아닙니다.
웹 컨트롤러는 Argo CD가 고정 버전 공식 Helm chart와 Git의 values를 조합합니다. 버전은 versions.yaml과 각 Application에 기록되어 있고 변경 시 함께 갱신합니다. 공식 차트는 변경 불가능한 digest가 아니라 버전으로 고정되어 있으며, 로컬 초기 검증에는 기존 공식 아티팩트 checksum과 렌더링 결과도 확인했습니다.
비밀값 관리 위치는 [docs/secrets.md](docs/secrets.md), 소유권 인계는 [docs/migration.md](docs/migration.md)를 확인합니다.

공식 Argo CD CRD 스키마도 로컬에서 확인하려면, IaC bootstrap이 checksum 검증해 내려받은 파일을 지정합니다.

```bash
.venv/bin/python scripts/validate.py \
  --argocd-schema ../gpu-iac/ansible/.cache/argocd/upstream.yaml
```

이 옵션은 제공한 스키마에 대해 검사하며 네트워크 접근·배포를 수행하지 않습니다.
