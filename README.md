# K8s — 온프레미스와 EKS별 GitOps

GitHub: https://github.com/DevOops-Team/K8s.git / develop.
온프레미스·EKS A/B/C의 설정과 Argo CD root를 분리했습니다. 이 변경은 로컬 코드 업데이트이며 클러스터 적용은 하지 않았습니다.

```text
onprem/
├─ apps/frontend, backend/   # 플랫폼 앱; backend는 내부 ClusterIP만 사용
├─ platform/                # namespace·Traefik·cert-manager·웹·cloudflared
└─ scheduling/              # 매니저 큐·MultiKueue

eks/
├─ eks-a/                   # A 전용 큐·ResourceFlavor·SA·연결 권한
├─ eks-b/
└─ eks-c/
   ├─ resources.yaml        # Argo CD 적용 파일
   ├─ kustomization.yaml
   └─ settings.example.yaml # 로컬 설정 도구용 예시; 적용 리소스 아님

argocd/
├─ bootstrap/onprem/        # gpu-root + gpu-platform project
├─ bootstrap/eks/eks-a/     # gpu-root-eks-a + gpu-eks-a project
├─ bootstrap/eks/eks-b/
├─ bootstrap/eks/eks-c/
└─ applications/
   ├─ onprem/               # 온프레미스 구성 요소 Application 8개
   └─ eks/eks-{a,b,c}/       # EKS별 Application 1개
```

기본 argocd/bootstrap 및 argocd/applications 진입점은 **onprem만 포함**합니다. EKS는 해당 root를 따로 등록·sync해야 합니다. 각 AppProject의 목적지도 해당 환경으로 제한했습니다.
Argo CD는 여전히 온프레미스 관리 클러스터에서 실행하며 등록된 EKS API로 배포합니다. 폴더와 root를 분리한 것이 VPN 우회나 AWS 내부 Argo CD 설치를 의미하지 않습니다.

## 담당 범위

- gpu-iac: VM/Kubespray/HA/MAP, Argo bootstrap, Secret, 외부 Cloudflare API. EKS별 읽기 전용 준비 점검과 선택적 registry Secret 등록.
- gpu-k8s: 각 클러스터 안의 원하는 리소스 상태.
- AWS 담당: EKS·Managed Node Group·GPU AMI/드라이버·device plugin·IAM·접근 경로·Node Auto Repair·Node Monitoring Agent.
- 스케줄링 담당: 각 클러스터의 Kueue v0.20.0·JobSet v0.13.0 컨트롤러와 매니저 디스패처·MAP 정책. 이 저장소는 설치하지 않습니다.

## 로컬 준비·검증

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python scripts/validate.py --target onprem
.venv/bin/python scripts/validate.py --target eks-a
.venv/bin/python scripts/validate.py --target all
```

검증은 로컬 Kustomize 빌드·소유권·적용 대상·할당량 일관성을 확인합니다. AWS/EKS 실환경에는 접속하지 않습니다. --ready는 선택한 환경의 미설정 Git URL/앱 이미지/이메일을 추가 확인하지만 실환경 준비 완료를 보장하지 않습니다. --argocd-schema에 checksum 검증한 공식 Argo 설치 YAML을 주면 Application/AppProject 스키마도 확인합니다.

## 온프레미스 설정

```bash
.venv/bin/python scripts/configure.py \
  --repo-url https://github.com/DevOops-Team/K8s.git --revision develop \
  --frontend-image ghcr.io/실제팀/frontend@sha256:실제64자리digest \
  --backend-image ghcr.io/실제팀/backend@sha256:실제64자리digest \
  --backend-port 8000 --acme-email 실제이메일
.venv/bin/python scripts/validate.py --target onprem --ready
```

이미지가 없는 앱은 replicas=0을 유지합니다. 실제 이미지를 넣은 앱만 2개로 바뀝니다. 자동 sync는 기본 꺼져 있고 --enable-auto-sync를 쓰면 --scope에 지정한 환경의 자식 Application만 켭니다(기본 scope=onprem). root는 수동 sync, prune은 꺼진 상태입니다.
브라우저는 backend ClusterIP를 직접 호출할 수 없으므로 frontend 서버/BFF 등의 내부 연결은 앱 담당과 맞춥니다. backend Ingress는 없습니다.

## EKS 하나씩 설정

```bash
cp eks/eks-a/settings.example.yaml eks/eks-a/settings.local.yaml
# GPU 노드 라벨·taint·CPU/메모리 할당량·IAM annotation을 실제 값으로 수정
.venv/bin/python scripts/configure-eks.py --cluster eks-a --settings eks/eks-a/settings.local.yaml
.venv/bin/python scripts/validate.py --target all
```

B/C도 해당 파일로 별도 실행합니다. settings.local.yaml은 Git 제외이며, **생성된 resources.yaml은 커밋**합니다. 이 도구는 선택한 EKS만 수정하고 다른 EKS 리소스는 바꾸지 않습니다. 다만 매니저의 합계 할당량은 세 EKS를 합산해 onprem/scheduling/resources.yaml에 함께 갱신하므로 두 diff를 검토하고 각각 sync합니다.

설정 항목:

| 항목 | 의미 |
|---|---|
| cluster_alias | eks-a/b/c 중 선택 대상과 일치해야 함 |
| gpu_node_labels | 실제 GPU 노드 라벨과 일치해야 함 |
| gpu_node_taints | ResourceFlavor의 taint 정보. 학습 Pod의 toleration도 제출 코드에서 맞춰야 함 |
| gpu_quota | 기본 4장, 도구 허용 범위 1~4. 물리 GPU 5장은 AWS에서 준비 |
| cpu_quota | 해당 큐의 양의 정수 CPU 할당량 |
| memory_quota | 설정 도구는 정수 Gi 단위(예: 128Gi)를 사용 |
| training_service_account_annotations | IRSA 등 필요한 annotation. Pod Identity association은 AWS 담당이 구성 |

CPU 32 / 메모리 128Gi는 예시입니다. 실제 선택한 GPU 노드에서 **예약분을 제외하고 학습에 사용할 수 있는 자원**에 맞추며, 매니저 quota를 실물 예약으로 보지 않습니다. settings.example.yaml은 자동 적용되지 않습니다.

## 환경별 적용

1. gpu-iac에서 온프레미스 Kubernetes/MAP/Argo를 준비하고 Secret을 등록합니다.
2. argocd/bootstrap/onprem을 등록하고 gpu-root를 sync합니다.
3. foundation → Traefik/cert-manager → frontend/backend → 웹 인증서 순서로 health를 확인하며 sync합니다.
4. AWS·스케줄링 담당의 EKS 준비 후 gpu-iac의 EKS별 preflight를 실행합니다.
5. EKS를 Argo CD에 eks-a/b/c 이름으로 등록하고 각 registry Secret·전용 root를 따로 등록합니다.
6. gpu-root-eks-a → gpu-queues-eks-a를 sync합니다. B/C도 각각 실행합니다.
7. 각 EKS의 MultiKueue 전용 kubeconfig를 매니저 Secret으로 등록한 뒤 gpu-multikueue를 sync합니다.
8. production frontend 인증서·endpoint 준비 후 gpu-iac에서 Cloudflare 외부 리소스를 구성하고 gpu-cloudflared를 sync합니다.

```bash
# 온프레미스만
argocd app sync gpu-root
# 최초에는 구성 요소별로 sync/health 확인
argocd app sync gpu-foundation
argocd app sync gpu-traefik gpu-cert-manager
argocd app wait gpu-traefik gpu-cert-manager --health --timeout 600
argocd app sync gpu-frontend gpu-backend
argocd app wait gpu-frontend gpu-backend --health --timeout 600
argocd app sync gpu-web

# EKS A만: root를 먼저 등록한 뒤
argocd app sync gpu-root-eks-a
argocd app sync gpu-queues-eks-a
```

Application의 sync-wave는 자식 앱 건강 상태까지 자동 보장하지 않습니다. 초기에 단계별 검증을 수행하고, 준비 후 환경별 자동 sync를 선택합니다.

## VPN 단절과 장애 복구

온프레미스의 Argo/MultiKueue는 VPN을 유일한 EKS API 경로로 쓴다면 단절 중 EKS 변경·조회가 제한됩니다. EKS 내부 Kubernetes/JobSet과 AWS Node Auto Repair는 필요한 로컬 조건이 정상이라면 계속 동작합니다. 이 저장소 분리가 독립적인 AWS 복구 프로그램을 만드는 것은 아닙니다. kubelet/containerd SSH 복구기는 온프레미스 전용이며 EKS에 사용하지 않습니다.
노드가 교체되는 것과 DDP/FSDP 학습이 체크포인트에서 재개되는 것은 별개입니다. 학습 재개·실행 권한·VPN 단절 시 중복 실행 방지는 스케줄링/복구 코드에서 처리해야 합니다.

CI는 변경 대상 환경의 경로만 수정합니다. 예: frontend 이미지는 onprem/apps/frontend/overlays/onprem/kustomization.yaml. 자동 sync가 켜진 해당 자식 앱이 반영합니다.

[부트스트랩](docs/bootstrap.md) · [비밀값](docs/secrets.md) · [기존 구성 인계](docs/migration.md)
