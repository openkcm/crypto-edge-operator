###############################################
# Builder stage: compile operator binary
###############################################
FROM golang:1.26-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /workspace

RUN apk add --no-cache git
ENV GOTOOLCHAIN=auto

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build static binary (CGO disabled) for target OS/Arch
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
	go build -trimpath -ldflags "-s -w" -o crypto-edge-operator ./cmd/crypto-edge-operator

###############################################
# Final stage: minimal runtime image
###############################################
FROM alpine:3.20 AS runtime
ARG VERSION=dev
ARG COMMIT=unknown
RUN apk add --no-cache ca-certificates bash busybox coreutils curl
WORKDIR /
COPY --from=builder /workspace/crypto-edge-operator /crypto-edge-operator
USER 65532:65532
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
	TZ=UTC
ENTRYPOINT ["/crypto-edge-operator"]
CMD ["-help"]

LABEL org.opencontainers.image.title="crypto-edge-operator-debug" \
	org.opencontainers.image.source="https://github.com/openkcm/crypto-edge-operator" \
	org.opencontainers.image.revision="${COMMIT}" \
	org.opencontainers.image.version="${VERSION}" \
	org.opencontainers.image.licenses="Apache-2.0" \
	org.opencontainers.image.description="Debug build with shell and core utilities"
