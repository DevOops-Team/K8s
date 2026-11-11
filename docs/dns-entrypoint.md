# 웹 진입점

운영 Traefik·MetalLB·Ingress·Certificate 설정은 [Gitops](https://github.com/DevOops-Team/Gitops/tree/main/onprem/platform)에 있습니다. 현재 Gitops의 HTTPS 진입점은 MetalLB VIP `172.30.0.125:443`입니다. pfSense의 전달 대상은 해당 VIP를 기준으로 운영 설정과 대조합니다.
