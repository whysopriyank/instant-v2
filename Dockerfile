# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS build
ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/instantd ./cmd/instantd \
    && mkdir -p /data /tmp && chown 65532:65532 /data /tmp

FROM scratch
ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=unknown
LABEL org.opencontainers.image.source="https://github.com/whysopriyank/instant-v2" \
      org.opencontainers.image.revision=$REVISION \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.created=$CREATED \
      org.opencontainers.image.licenses="NOASSERTION"
COPY --from=build /out/instantd /instantd
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /data /data
COPY --from=build --chown=65532:65532 /tmp /tmp
ENV INSTANT_V2_STORAGE_ROOT=/data
EXPOSE 8080
USER 65532:65532
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 CMD ["/instantd", "healthcheck"]
ENTRYPOINT ["/instantd"]
