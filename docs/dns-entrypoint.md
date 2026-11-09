# Cloudflare DNS-only 사용자 진입점

선택한 구성은 DNS-only(회색 구름)이며 HTTPS는 기존 cert-manager 인증서로 Traefik에서 종료한다.

```text
Cloudflare DNS: devoops.site → 공인 IP (A/AAAA, proxied=false)
사용자 HTTPS: 공인 IP:443 → 라우터 포트 전달 → pfSense WAN/VIP:443
             → TCP 포트 전달 → 지정 워커 IP:30443 → Traefik → frontend:3000
```

## 유지·제거 범위

- 유지: Traefik 3복제본·frontend Ingress/TLS Secret·cert-manager·Cloudflare DNS01 발급용 토큰.
- 변경: Traefik Service를 NodePort로 변경하고 websecure만 30443으로 공개. HTTP 80·관리 API는 공개하지 않는다.
- 제거: gpu-cloudflared Application·Deployment/PDB·cloudflare-system 선언·Tunnel 이미지 버전. DNS 플레이북은 Tunnel·token Secret을 생성하지 않는다.
- pfSense는 TCP443을 지정 워커:30443으로 DNAT한다. 인증서와 개인키는 Traefik에 유지한다. gpu-iac의 configure-pfsense-edge.yml로 설정한다. 진입 워커 자동 전환은 이번 구현에 포함하지 않으며 HAProxy는 후속 구성이다.

## 전환 순서 (운영 환경에서 별도 실행)

1. 현재 DNS 레코드 ID·내용, Tunnel ID·설정과 기존 Argo 리비전을 기록한다. 기존 Tunnel은 새 경로 검증 전까지 유지한다.
2. Traefik Application만 먼저 sync한다. 플랫폼 워커들의 실제 IP와 30443 접근성, frontend-tls Ready를 확인한다.
3. 네트워크 담당이 공인 IP·라우터 TCP443 전달·pfSense WAN/VIP·워커 반환 라우팅을 확인하고 gpu-iac의 pfSense 플레이북을 적용한다. NAT는 원본 클라이언트 IP를 유지하므로 노드 방화벽은 실제 소스와 라우팅에 맞춰 구성한다. 노드:30443으로 인터넷에서 직접 접근하는 경로는 차단한다. 기존 HAProxy와 같은 WAN/VIP:443에 중복 적용하지 않는다. CARP 및 설정 동기화는 별도다.
4. 외부 네트워크에서 `curl --resolve devoops.site:443:PUBLIC_IP https://devoops.site/`로 인증서·서비스를 검증한다. TLS를 끄는 `-k` 옵션은 사용하지 않는다. 이번 pfSense 포트 전달은 IPv4 전용이므로 DNS는 A 레코드를 사용하고 AAAA를 만들지 않는다.
5. gpu-iac `ansible/EDGE.md`에 따라 DNS-only 레코드를 적용한다. 기존 Tunnel CNAME 전환에는 기존 레코드 ID와 내용을 명시한다. DNS TTL과 이전 캐시 만료 뒤 일반 DNS 접속을 확인한다.
6. 기존 커넥터 트래픽이 사라진 뒤 root Application을 sync하고 잔존 gpu-cloudflared Application·Deployment/PDB·token Secret을 명시적으로 정리한다. 자동 prune이 꺼져 있으면 소스 삭제만으로 실제 리소스는 사라지지 않는다. Application finalizer/cascade 여부를 먼저 확인한다. cloudflare-system에 다른 리소스가 없는지 확인한 후 네임스페이스를 정리한다.
7. Cloudflare의 기존 Tunnel은 다른 hostname/커넥터가 사용하지 않는지 확인 후 삭제하고 Tunnel 전용 토큰을 폐기한다. DNS01 토큰은 유지한다.

DNS-only는 Cloudflare 프록시/WAF/터널을 경유하지 않는다. 공인 IP·443 인바운드 경로가 없으면 이 구성으로 접속할 수 없다. 백엔드의 기존 ClusterIP 구성은 유지한다.

## 롤백

기존 Tunnel을 정리하기 전에는 기록한 기존 DNS 레코드와 프록시 설정으로 되돌린다. 정리 후 롤백에는 이전 Git 리비전의 Application·Deployment와 Tunnel 자격증명 복원이 필요하다. DNS 캐시 때문에 경로가 즉시 바뀌지는 않는다.
