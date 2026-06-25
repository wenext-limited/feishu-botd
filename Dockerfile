# syntax=docker/dockerfile:1.7

FROM golang:1.22-bookworm AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/feishu-botd \
    ./cmd/feishu-botd

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 feishu-botd \
    && useradd --system --uid 10001 --gid 10001 --home-dir /var/lib/feishu-botd --create-home feishu-botd \
    && mkdir -p /run/feishu-botd \
    && chown -R 10001:10001 /run/feishu-botd /var/lib/feishu-botd

COPY --from=builder /out/feishu-botd /usr/local/bin/feishu-botd

USER feishu-botd
WORKDIR /var/lib/feishu-botd

ENV FEISHU_BOTD_SOCKET=/run/feishu-botd/feishu-botd.sock

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS --unix-socket "$FEISHU_BOTD_SOCKET" http://localhost/healthz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/feishu-botd"]
