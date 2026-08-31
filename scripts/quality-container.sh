#!/usr/bin/env bash
# Disposable startup smoke: no host database or operator credentials are used.
set -euo pipefail
command -v docker >/dev/null || { echo 'container-verify requires Docker' >&2; exit 2; }
command -v curl >/dev/null || { echo 'container-verify requires curl' >&2; exit 2; }
docker info >/dev/null 2>&1 || { echo 'container-verify requires a running Docker daemon' >&2; exit 2; }

scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/instant-quality-container.XXXXXX")
image_tag="instant-v2-quality:$(basename "$scratch_dir" | tr '[:upper:]' '[:lower:]')"
container_id=''
cleanup() {
  if test -n "$container_id"; then docker rm -f "$container_id" >/dev/null || true; fi
  docker image rm "$image_tag" >/dev/null 2>&1 || true
  rm -f "$scratch_dir/ca-certificates.crt"
  rmdir "$scratch_dir"
}
trap cleanup EXIT

docker build -t "$image_tag" .
test "$(docker image inspect --format '{{.Config.User}}' "$image_tag")" = '65532:65532'
container_id=$(docker create -p 127.0.0.1::8080 \
  -e INSTANT_V2_STORAGE_SECRET=container-verification-only \
  -e INSTANT_V2_HTTP_ADDR=:8080 "$image_tag")
docker cp "$container_id:/etc/ssl/certs/ca-certificates.crt" "$scratch_dir/ca-certificates.crt"
test -s "$scratch_dir/ca-certificates.crt"
docker start "$container_id" >/dev/null
address=$(docker port "$container_id" 8080/tcp)
for ((attempt=0; attempt<60; attempt++)); do
  if curl --fail --max-time 2 --silent "http://$address/health" >/dev/null; then
    echo 'container startup, health, non-root identity, and TLS trust store checks passed'
    exit 0
  fi
  if test "$(docker inspect --format '{{.State.Running}}' "$container_id")" != true; then break; fi
  sleep 1
done
docker logs "$container_id" >&2
echo 'container did not become healthy' >&2
exit 1
