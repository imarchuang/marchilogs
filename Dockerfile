# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY storage ./storage
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/marchilogs ./cmd/marchilogs

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl \
	&& adduser -D -u 10001 marchi \
	&& mkdir -p /data \
	&& chown marchi:marchi /data
COPY --from=build /out/marchilogs /usr/local/bin/marchilogs
USER marchi
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080
ENV MARCHILOGS_ADDR=:8080 \
	MARCHILOGS_DATA=/data \
	MARCHILOGS_STREAM_FIELDS=service,host
HEALTHCHECK --interval=10s --timeout=3s --start-period=3s --retries=3 \
	CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["marchilogs"]
