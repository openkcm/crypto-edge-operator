###############################################
# Builder stage: compile operator binary
###############################################
FROM golang:1.25-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /workspace

RUN apk add --no-cache git
ENV GOTOOLCHAIN=auto \
	GOPROXY=https://proxy.golang.org,direct \
	GOSUMDB=sum.golang.org \
	GOMODCACHE=/go/pkg/mod \
	GOCACHE=/root/.cache/go-build

# Use BuildKit cache mounts to speed up and stabilize dependency downloads and builds
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
	go mod download

COPY . .

# Build static binary (CGO disabled) for target OS/Arch
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
	go build -v -trimpath -ldflags "-s -w" -o crypto-edge-operator ./cmd/crypto-edge-operator

###############################################
# Final stage: minimal runtime image
###############################################
FROM alpine:3.23 AS runtime
ARG VERSION=dev
ARG COMMIT=unknown
RUN apk add --no-cache ca-certificates bash busybox coreutils curl
WORKDIR /
COPY --from=builder /workspace/crypto-edge-operator /crypto-edge-operator
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
	TZ=UTC \
	HELM_CACHE_HOME=/.cache/helm \
	HELM_CONFIG_HOME=/.config/helm \
	HELM_DATA_HOME=/.local/share/helm
# Pre-create writable helm cache/config/data directories for non-root UID
# Run as root for setup, then drop privileges
USER 0
RUN mkdir -p /.cache/helm/repository /.config/helm /.local/share/helm && \
	touch /.config/helm/repositories.yaml && \
	chown -R 65532:65532 /.cache /.config /.local
USER 65532:65532
ENTRYPOINT ["/crypto-edge-operator"]
CMD ["-help"]

LABEL org.opencontainers.image.title="crypto-edge-operator-debug" \
	org.opencontainers.image.source="https://github.com/openkcm/crypto-edge-operator" \
	org.opencontainers.image.revision="${COMMIT}" \
	org.opencontainers.image.version="${VERSION}" \
	org.opencontainers.image.licenses="Apache-2.0" \
	org.opencontainers.image.description="Debug build with shell and core utilities"
