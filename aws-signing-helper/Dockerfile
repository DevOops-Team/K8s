# IAM Roles Anywhere 인증 sidecar 이미지 (GPU-173).
# 온프레 Pod에서 `aws_signing_helper serve`로 EC2 메타데이터 서버처럼 동작해, 같은 Pod의 앱(AWS SDK)에
# 1시간짜리 임시 자격 증명을 내준다. 앱 이미지는 helper도 인증서도 갖지 않는다 (인증서는 이 컨테이너에만 마운트).
#
# 공식 바이너리는 체크섬이 공개되지 않아, 릴리스 태그 커밋을 고정해 upstream Makefile과 같은 방법으로 빌드한다.
# 버전을 올릴 때: HELPER_VERSION · HELPER_COMMIT(태그가 가리키는 커밋)과 Go 이미지를 함께 바꾼다.

# upstream go.mod가 Go 1.27.3을 요구한다. cgo(PKCS#11 지원, -linkmode=external)라 glibc 기반 이미지에서 빌드한다
FROM golang:1.27.3-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0 AS build
ARG HELPER_VERSION=1.8.6
ARG HELPER_COMMIT=f17999e83885205544bda00553ffb907a16bf862
WORKDIR /src
RUN git init -q . \
 && git remote add origin https://github.com/aws/rolesanywhere-credential-helper.git \
 && git fetch -q --depth 1 origin "${HELPER_COMMIT}" \
 && git checkout -q FETCH_HEAD \
 && test "$(git rev-parse HEAD)" = "${HELPER_COMMIT}"
RUN go build -buildmode=pie \
      -ldflags "-X 'github.com/aws/rolesanywhere-credential-helper/cmd.Version=${HELPER_VERSION}' -linkmode=external -w -s" \
      -trimpath -o /out/aws_signing_helper main.go \
 && /out/aws_signing_helper version | grep -qx "${HELPER_VERSION}"

# glibc가 있는 distroless (cgo 바이너리). 셸 없음, uid 65532
FROM gcr.io/distroless/base-debian12:nonroot@sha256:7f0c72cd138b442ae0deeb69c08b1acf5525439ba251a49ad93c320a061567e5
COPY --from=build /out/aws_signing_helper /usr/local/bin/aws_signing_helper
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/aws_signing_helper"]
CMD ["version"]
