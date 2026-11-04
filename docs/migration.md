# Ansible에서 Argo CD로 인계

이 작업에서는 실제 클러스터를 변경하지 않았습니다. 이미 설치된 경우 아래 절차로 인계합니다.

1. 기존 Ansible/Helm 배포 실행을 멈추고 현재 Deployment·Service·RBAC·Ingress·Certificate·큐·Helm values와 비밀이 아닌 설정을 백업합니다. Secret 백업은 별도 안전한 저장소에서 관리합니다.
2. 이 저장소의 namespace·이름·Service selector·replicas·이미지 digest·도메인·자원 할당량을 실제 값과 맞춥니다. 미설정 replicas=0을 운영 클러스터에 그대로 sync하지 않습니다.
3. Argo root/child의 diff를 검토합니다. Helm 이름은 기존 traefik/cert-manager release와 같지만, Argo CD는 Helm template으로 렌더링하며 기존 Helm release 이력을 이어받는 것은 아닙니다. 기존 release를 helm uninstall하지 않습니다.
4. 처음에는 자동 sync와 prune을 켜지 않은 상태로 component별 sync·health를 확인합니다. FailOnSharedResource가 이미 다른 Argo Application의 소유권을 감지하면 먼저 이전 소유권을 정상 인계합니다.
5. 기존 Ansible은 클러스터 설치·부트스트랩 Secret·Cloudflare 외부 API만 실행합니다. gpu-k8s가 소유한 Deployment·Service·Ingress·큐를 수동 apply/helm upgrade로 반복 수정하지 않습니다.
6. 이후 자동 sync를 켜면 수동 변경이 selfHeal로 되돌아갈 수 있습니다. 삭제는 Git diff를 검토하고 별도로 prune합니다.

Argo CD bootstrap 매니페스트와 외부 Secret은 gpu-iac의 명시적 부트스트랩 대상입니다. Argo CD 자체가 같은 설치 리소스를 self-manage하도록 구성하지 않았습니다.

## 온프레미스/EKS root 분리 인계

리소스와 자식 Application의 이름은 유지했지만 source path와 EKS project/root는 변경했습니다. 기존 gpu-root가 EKS 자식 Application까지 이미 관리하고 있었다면, 자동 prune을 켜지 말고 **EKS 자식 Application을 orphan 방식으로 분리한 뒤** 새 EKS root로 소유권을 인계합니다. 기존 Namespace/큐/Pod를 삭제하는 절차가 아닙니다. 현재 Argo의 tracking-id/instance label과 finalizer를 확인하고, FailOnSharedResource 충돌을 해결한 후 sync합니다. 실제 클러스터를 조회하거나 자동으로 소유권을 강제 변경하는 스크립트는 실행하지 않았습니다.

기존 source `apps/`, `platform/`, `scheduling/manager`, `scheduling/workers/eks-*`는 각각 `onprem/apps/`, `onprem/platform/`, `onprem/scheduling`, `eks/eks-*`로 이동했습니다. 기존 CI의 이미지 수정 경로도 갱신합니다.
