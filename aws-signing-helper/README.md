# aws-signing-helper (sidecar)

온프레 Pod가 IAM Roles Anywhere로 AWS에 붙기 위한 **인증 sidecar 이미지** (GPU-173).
`ghcr.io/devoops-team/aws-signing-helper` · upstream [rolesanywhere-credential-helper](https://github.com/aws/rolesanywhere-credential-helper) v1.8.6 (태그 커밋 고정 소스 빌드).

## 동작

```text
Pod
├─ 앱 컨테이너 (relocator · recovery-controller · Backend · Mimir …)   ← 수정 없음, 인증서 없음
│    env AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911/   ← 끝의 / 필수 (없으면 SDK가 …9911latest/… 로 붙임)
│    AWS SDK가 "EC2 메타데이터 서버"에서 임시 자격 증명을 받는다고 생각하고 sidecar에 묻는다
└─ sidecar: aws_signing_helper serve   ← 인증서(client.pem · client.key)는 여기에만 마운트
     client.key로 서명해 Roles Anywhere CreateSession → 1시간 임시 키를 IMDSv2 형식으로 내준다
```

- 개인 키는 sidecar에만 있다. 앱 컨테이너에서는 1시간짜리 임시 키만 얻을 수 있다.
- `serve`는 시작할 때 첫 자격 증명을 받지 못하면 종료한다 (설정 오류가 재시작으로 드러남). 받은 뒤에야 9911을 연다.
- 인증 방식 · helper 버전은 배포 쪽(Gitops)에서 한 번에 관리한다. 앱 레포는 손대지 않는다.

## Pod에 붙이는 법 (네이티브 sidecar, k8s 1.29+)

```yaml
spec:
  initContainers:
  - name: aws-auth
    image: ghcr.io/devoops-team/aws-signing-helper:1.0.0@sha256:...
    restartPolicy: Always            # 네이티브 sidecar: 앱보다 먼저 뜨고 끝까지 같이 산다
    args:
    - serve
    - --certificate=/var/run/devoops-aws/client.pem
    - --private-key=/var/run/devoops-aws/client.key
    - --trust-anchor-arn=$(TRUST_ANCHOR_ARN)
    - --profile-arn=$(PROFILE_ARN)
    - --role-arn=$(ROLE_ARN)
    - --region=us-east-2
    env:                             # ARN: IaC `terraform/run.sh output base -- workloads`
    - {name: TRUST_ANCHOR_ARN, valueFrom: {configMapKeyRef: {name: <app>-aws, key: trust_anchor_arn}}}
    - {name: PROFILE_ARN, valueFrom: {configMapKeyRef: {name: <app>-aws, key: profile_arn}}}
    - {name: ROLE_ARN, valueFrom: {configMapKeyRef: {name: <app>-aws, key: role_arn}}}
    startupProbe:                    # 첫 자격 증명을 받아 9911을 연 뒤에 앱이 시작한다
      tcpSocket: {port: 9911}
      periodSeconds: 2
      failureThreshold: 30
    volumeMounts:
    - {name: aws-cert, mountPath: /var/run/devoops-aws, readOnly: true}
  containers:
  - name: main
    env:
    - {name: AWS_EC2_METADATA_SERVICE_ENDPOINT, value: "http://127.0.0.1:9911/"}   # 끝의 / 필수
    - {name: AWS_REGION, value: us-east-2}
  volumes:
  - name: aws-cert
    secret: {secretName: <app>-aws-cert}   # client.pem · client.key (IaC bootstrap/certs.sh <app>)
```

## 버전 올리기

`Dockerfile`의 `HELPER_VERSION` · `HELPER_COMMIT`(릴리스 태그가 가리키는 커밋)과, upstream `go.mod`가 요구하는 Go 이미지를 함께 바꾼다.
빌드 단계가 `aws_signing_helper version` 출력이 `HELPER_VERSION`과 같은지 확인한다.

## 로컬 확인

```shell
docker buildx build --platform linux/amd64 --load -t aws-signing-helper:local aws-signing-helper
docker run --rm aws-signing-helper:local            # 1.8.6
```

## 실제 확인 (11/16)

relocator 인증서(새 CA v2) + 이 이미지 `serve` + 같은 네트워크의 AWS CLI(`AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911/`):
`sts get-caller-identity` = `assumed-role/devoops-prod-onprem-relocator/…`, 펜싱 테이블 GetItem 허용, 모니터링 버킷 AccessDenied (권한 밖).
