# 웹 진입점

운영 Traefik·MetalLB·Ingress·Certificate 설정은 [Gitops](https://github.com/DevOops-Team/Gitops/tree/main/onprem/platform)에 있습니다. 현재 Gitops의 HTTPS 진입점은 MetalLB VIP `172.30.0.125:443`입니다. pfSense의 전달 대상은 해당 VIP를 기준으로 운영 설정과 대조합니다.

## ADR-22 전환 순서

1. Gitops에서 MetalLB와 주소 풀·L2 광고 설정을 동기화한다. Traefik `LoadBalancer` Service에 `172.30.0.125`가 할당되고 HTTPS 포트 `443`이 노출되는지 확인한다.
2. 네트워크 담당자가 pfSense GUI에서 HTTPS 포트 전달 대상을 `172.30.0.125:443`으로 변경한다. 기존 `gpu-iac-traefik-https` 규칙이 있으면 같은 WAN 포트의 규칙이 중복되지 않도록 확인하고, 적용한 값을 IaC의 `ansible/EDGE.md`에 기록한다.
3. pfSense에서 새 전달 대상과 규칙의 트래픽 카운터를 확인하고, 외부 네트워크에서 `https://devoops.site/`에 접속해 인증서와 프런트엔드 응답을 확인한다.
4. 위 검증이 끝나면 IaC의 기존 NodePort용 `configure-pfsense-edge.yml`, `roles/pfsense-edge`, `pfsense-edge.example.yml`을 정리한다.
