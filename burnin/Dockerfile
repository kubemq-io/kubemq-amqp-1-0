# Multi-stage, non-root burn-in image for the KubeMQ AMQP 1.0 burn-in harness.
# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /burnin-amqp10 .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
RUN adduser -D -u 1000 burnin
COPY --from=builder /burnin-amqp10 /usr/local/bin/burnin-amqp10
COPY burnin-config.yaml /etc/burnin/config.yaml

USER burnin
EXPOSE 8896

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8896/health || exit 1

ENTRYPOINT ["burnin-amqp10"]
CMD ["--config", "/etc/burnin/config.yaml"]
