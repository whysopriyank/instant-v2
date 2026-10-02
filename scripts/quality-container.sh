#!/usr/bin/env bash
# OP-004 smoke against a local build or an exact, already pulled release digest.
# Creates only prefixed disposable containers/network/volumes; no operator DB.
set -euo pipefail
for tool in docker curl jq python3; do
  command -v "$tool" >/dev/null || { echo "container-verify requires $tool" >&2; exit 2; }
done
docker info >/dev/null 2>&1 || { echo 'container-verify requires a running Docker daemon' >&2; exit 2; }
scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/instant-quality-container.XXXXXX")
prefix="iv2q-container-$(basename "$scratch_dir" | tr '[:upper:]' '[:lower:]')"
image="${CONTAINER_IMAGE:-$prefix:candidate}"
tools_image="$prefix:tools"
pg="$prefix-pg"
server="$prefix-server"
network="$prefix-net"
volume="$prefix-files"
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
evidence_dir="$scratch_dir/evidence"
if test -n "${CONTAINER_EVIDENCE_DIR:-}"; then
  test -n "${CONTAINER_IMAGE:-}" && test -n "${CONTAINER_IDENTITY_FILE:-}"
  evidence_dir="$CONTAINER_EVIDENCE_DIR"
  test ! -e "$evidence_dir" && test ! -L "$evidence_dir"
fi
mkdir -m 700 "$evidence_dir"
evidence_dir=$(cd "$evidence_dir" && pwd -P)
if test -n "${CONTAINER_EVIDENCE_DIR:-}"; then
  cp "$CONTAINER_IDENTITY_FILE" "$evidence_dir/identity.json"
  jq -e 'keys == ["binary_sha256","candidate_sha","configuration_sha256","endpoint_sha256","fixture_id","fixture_sha256","image_digest"] and
    (.candidate_sha | test("^[a-f0-9]{40}$")) and
    ([.binary_sha256,.configuration_sha256,.endpoint_sha256,.fixture_sha256] | all(test("^[a-f0-9]{64}$"))) and
    (.image_digest | test("^sha256:[a-f0-9]{64}$")) and (.fixture_id | length > 0)' "$evidence_dir/identity.json" >/dev/null
fi
hash_file() {
  if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | awk '{print $1}'
}
cleanup() {
  result=$?
  docker logs "$server" > "$evidence_dir/server-final.log" 2>&1 || true
  if test "$result" -ne 0; then cat "$evidence_dir/server-final.log" >&2; fi
  docker rm -f "$server" "$pg" "$prefix-setup" "$prefix-soak" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
  docker image rm "$tools_image" >/dev/null 2>&1 || true
  if test -z "${CONTAINER_IMAGE:-}"; then docker image rm "$image" >/dev/null 2>&1 || true; fi
  for container in "$server" "$pg" "$prefix-setup" "$prefix-soak"; do
    if docker container inspect "$container" >/dev/null 2>&1; then result=1; fi
  done
  if docker network inspect "$network" >/dev/null 2>&1; then result=1; fi
  if docker volume inspect "$volume" >/dev/null 2>&1; then result=1; fi
  printf '%s\n' "$result" > "$evidence_dir/cleanup-exit-status.txt"
  if test "$result" = 0; then touch "$evidence_dir/cleanup.complete"; fi
  rm -rf "$scratch_dir"
  exit "$result"
}
trap cleanup EXIT
if test -z "${CONTAINER_IMAGE:-}"; then
  docker build --build-arg REVISION="$(git rev-parse HEAD)" \
    --build-arg CREATED="$(date -u +%Y-%m-%dT%H:%M:%SZ)" -t "$image" .
else
  docker image inspect "$image" >/dev/null
fi
docker image inspect "$image" > "$evidence_dir/image-inspect.json"
test "$(docker image inspect --format '{{.Config.User}}' "$image")" = '65532:65532'
if test -n "${CONTAINER_EVIDENCE_DIR:-}"; then
  candidate_sha=$(jq -er .candidate_sha "$evidence_dir/identity.json")
  image_digest=$(jq -er .image_digest "$evidence_dir/identity.json")
  jq -e --arg candidate "$candidate_sha" --arg digest "$image_digest" '
    .[0].Config.Labels["org.opencontainers.image.revision"] == $candidate and
    (.[0].RepoDigests | any(endswith("@" + $digest)))' "$evidence_dir/image-inspect.json" >/dev/null
fi
docker image inspect --format '{{json .Config.Healthcheck.Test}}' "$image" | \
  jq -e '. == ["CMD", "/instantd", "healthcheck"]' >/dev/null
# Check values on the real image, not copied constants in a test model.
for key in source revision version created licenses; do
  docker image inspect --format '{{json .Config.Labels}}' "$image" | \
    jq -e --arg key "org.opencontainers.image.$key" '.[$key] | type == "string" and length > 0' >/dev/null
done
docker build -f deploy/Dockerfile.smoke -t "$tools_image" .
docker network create "$network" >/dev/null
docker volume create "$volume" >/dev/null
docker run -d --name "$pg" --network "$network" --network-alias postgres \
  -e POSTGRES_USER=instant -e POSTGRES_PASSWORD=smoke -e POSTGRES_DB=instant_bench_container \
  postgres:17@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232 \
  -c wal_level=logical >/dev/null
ready=0
for ((attempt=0; attempt<60; attempt++)); do
  if docker exec "$pg" pg_isready -U instant -d instant_bench_container >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
test "$ready" = 1
start_server() {
  docker run -d --name "$server" --network "$network" --network-alias instantd \
    -p 127.0.0.1::8080 -v "$volume:/data" \
    -e DATABASE_URL='postgres://instant:smoke@postgres:5432/instant_bench_container?sslmode=disable' \
    -e INSTANT_V2_STORAGE_SECRET=container-smoke-only \
    -e INSTANT_V2_STORAGE_ROOT=/data -e INSTANT_V2_HTTP_ADDR=:8080 \
    -e INSTANT_OAUTH_GOOGLE_CLIENT_ID=smoke-google \
    -e INSTANT_OAUTH_GOOGLE_CLIENT_SECRET=smoke-google-secret \
    -e INSTANT_OAUTH_GITHUB_CLIENT_ID=smoke-github \
    -e INSTANT_OAUTH_GITHUB_CLIENT_SECRET=smoke-github-secret "$image" >/dev/null
  ready=0
  for ((attempt=0; attempt<60; attempt++)); do
    if test "$(docker inspect --format '{{.State.Health.Status}}' "$server")" = healthy; then ready=1; break; fi
    if test "$(docker inspect --format '{{.State.Running}}' "$server")" != true; then break; fi
    sleep 1
  done
  test "$ready" = 1
  address=$(docker port "$server" 8080/tcp)
  docker exec "$server" /instantd healthcheck
  probe_status=$(curl --max-time 5 --silent -o "$evidence_dir/health-$probe_phase.json" \
    -w '%{http_code}' "http://$address/health")
  printf '%s\n' "$probe_status" > "$evidence_dir/health-$probe_phase.status"
  test "$probe_status" = 200
  jq -e '.ok == true and .db == true' "$evidence_dir/health-$probe_phase.json" >/dev/null
}
probe_phase=before

start_server
docker top "$server" -eo uid > "$evidence_dir/process-uid.txt"
actual_uid=$(awk 'NR == 2 {print $1}' "$evidence_dir/process-uid.txt")
test "$actual_uid" = 65532
docker cp "$server:/instantd" "$evidence_dir/instantd"
actual_binary_sha=$(hash_file "$evidence_dir/instantd")
if test -n "${CONTAINER_EVIDENCE_DIR:-}"; then
  test "$actual_binary_sha" = "$(jq -er .binary_sha256 "$evidence_dir/identity.json")"
fi
docker exec "$server" /instantd healthcheck --tls-url="${CONTAINER_TLS_URL:-https://www.google.com}" \
  > "$evidence_dir/tls-probe.log" 2>&1
tls_verified=true
docker run --rm --name "$prefix-setup" --network "$network" "$tools_image" /soaksetup \
  -database-url 'postgres://instant:smoke@postgres:5432/instant_bench_container?sslmode=disable' \
  -marker "$prefix" > "$evidence_dir/setup.txt"
app=$(awk -F= '$1 == "APP" {print $2}' "$evidence_dir/setup.txt")
attr=$(awk -F= '$1 == "ATTR" {print $2}' "$evidence_dir/setup.txt")
test -n "$app" && test -n "$attr"
# Existing harness proves actual init/query/transact/refresh on the candidate.
docker run --rm --name "$prefix-soak" --network "$network" \
  --user "$(id -u):$(id -g)" -v "$evidence_dir:/evidence" "$tools_image" /soak \
  -url ws://instantd:8080/runtime/session -app "$app" -attr "$attr" \
  -sessions 2 -duration 8s -ramp 1s -settle 1s -global-tx-rate 1 \
  -quiescence 10s -max-p99-lag 5s -sdk-version 1.0.65 -out /evidence/soak \
  > "$evidence_dir/soak.log" 2>&1
jq -e '.status == "complete" and .completed and .summary.success and
  .summary.transacts > 0 and .summary.dropped == 0 and .summary.unresolved == 0 and
  .summary.lag_samples == .summary.transacts' "$evidence_dir/soak/manifest.json" >/dev/null
transact_acked=$(jq -r '.summary.success and .summary.transacts > 0 and .summary.unresolved == 0' "$evidence_dir/soak/manifest.json")
subscription_converged=$(jq -r '.summary.success and .summary.lag_samples == .summary.transacts and .summary.dropped == 0' "$evidence_dir/soak/manifest.json")
admin=$(docker exec "$pg" psql -U instant -d instant_bench_container -Atc \
  "INSERT INTO app_admin_tokens (token,app_id) VALUES (gen_random_uuid(),'$app') RETURNING token" | head -n 1)
test -n "$admin"
curl --fail --max-time 5 --silent -H "X-admin-token: $admin" -H 'Content-Type: application/json' \
  -d "{\"app-id\":\"$app\",\"filename\":\"container-smoke.bin\"}" \
  "http://$address/storage/signed-upload-url" > "$scratch_dir/upload.json"
file_id=$(jq -er .data.id "$scratch_dir/upload.json")
upload_url=$(jq -er .data.url "$scratch_dir/upload.json")
printf 'container persistent blob %s\n' "$prefix" > "$scratch_dir/source"
curl --fail --max-time 5 --silent -X PUT --data-binary @"$scratch_dir/source" "$upload_url" >/dev/null
download_blob() {
  download_url=$(curl --fail --max-time 5 --silent -H "X-admin-token: $admin" \
    "http://$address/storage/signed-download-url?app-id=$app&id=$file_id" | jq -er .data.url)
  curl --fail --max-time 5 --silent "$download_url" > "$scratch_dir/download"
  cmp "$scratch_dir/source" "$scratch_dir/download"
}
download_blob
cp "$scratch_dir/download" "$evidence_dir/object-before.bin"
object_before=$(hash_file "$evidence_dir/object-before.bin")
docker logs "$server" > "$evidence_dir/server-before-restart.log" 2>&1
old_address=$address
TIMEFORMAT='%R'
{ time docker stop --time 30 "$server" >/dev/null 2> "$evidence_dir/stop.log"; } 2> "$evidence_dir/drain-seconds.txt"
drain_seconds=$(cat "$evidence_dir/drain-seconds.txt")
jq -en --argjson seconds "$drain_seconds" '$seconds >= 0 and $seconds <= 30' >/dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$server")" = 0
port_exit=0
curl --max-time 2 --silent "http://$old_address/health" > "$evidence_dir/port-release-probe.log" 2>&1 || port_exit=$?
printf '%s\n' "$port_exit" > "$evidence_dir/port-release-probe.status"
# Curl 7 is a failed connection; timeout/HTTP errors do not prove port release.
test "$port_exit" = 7
port_released=true
docker rm "$server" >/dev/null
probe_phase=after
start_server
download_blob
cp "$scratch_dir/download" "$evidence_dir/object-after.bin"
object_after=$(hash_file "$evidence_dir/object-after.bin")
# Exercise ZIP staging and the assembled file backend inside the scratch image.
# This fails if its nonroot process cannot create its temporary upload file.
restore_app=$(docker exec "$pg" psql -U instant -d instant_bench_container -Atc \
  "INSERT INTO apps (id,creator_id,title) SELECT gen_random_uuid(),creator_id,'container ZIP restore' FROM apps WHERE id='$app' RETURNING id" | head -n 1)
restore_admin=$(docker exec "$pg" psql -U instant -d instant_bench_container -Atc \
  "INSERT INTO app_admin_tokens (token,app_id) VALUES (gen_random_uuid(),'$restore_app') RETURNING token" | head -n 1)
restore_file=$(docker exec "$pg" psql -U instant -d instant_bench_container -Atc "SELECT gen_random_uuid()")
python3 - "$evidence_dir/restore.zip" "$restore_file" <<'PY'
import json, sys, uuid, zipfile
blob = b'container ZIP restored object\n'
location = str(uuid.uuid4())
with zipfile.ZipFile(sys.argv[1], 'w', compression=zipfile.ZIP_DEFLATED) as archive:
    archive.writestr('config.json', json.dumps({'title': 'container ZIP', 'schema': {'entities': {'$files': {'attrs': {'path': {'valueType': 'string', 'config': {'unique': True, 'indexed': True}}}}}}}))
    archive.writestr('entities/$files.jsonl', json.dumps({'entity': {'id': sys.argv[2], 'path': 'restored.txt', 'location-id': location, 'size': len(blob), 'content-type': 'text/plain', 'content-disposition': 'inline', 'key-version': 1}, 'createdAt': 1700000000000})+'\n')
    archive.writestr('files/'+location, blob)
PY
curl --fail --max-time 20 --silent -H "X-admin-token: $restore_admin" \
  --data-binary @"$evidence_dir/restore.zip" "http://$address/backup/$restore_app/restore-v1zip" > "$evidence_dir/zip-restore-response.json"
restore_url=$(curl --fail --max-time 5 --silent -H "X-admin-token: $restore_admin" \
  "http://$address/storage/signed-download-url?app-id=$restore_app&id=$restore_file" | jq -er .data.url)
curl --fail --max-time 5 --silent "$restore_url" > "$evidence_dir/zip-restored-object.bin"
printf 'container ZIP restored object\n' > "$scratch_dir/zip-expected"
cmp "$scratch_dir/zip-expected" "$evidence_dir/zip-restored-object.bin"
# Readiness must fail when the database is unavailable.
docker stop --time 10 "$pg" >/dev/null
if docker exec "$server" /instantd healthcheck > "$evidence_dir/db-unavailable-probe.log" 2>&1; then
  echo 'healthcheck accepted unavailable database' >&2; exit 1
fi
if test -n "${CONTAINER_EVIDENCE_DIR:-}"; then
  jq -n --slurpfile identity "$evidence_dir/identity.json" \
    --arg started "$started_at" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson uid "$actual_uid" --argjson health "$(cat "$evidence_dir/health-before.status")" \
    --argjson ready "$(cat "$evidence_dir/health-after.status")" --argjson tls "$tls_verified" \
    --argjson acked "$transact_acked" --argjson converged "$subscription_converged" \
    --arg before "$object_before" --arg after "$object_after" \
    --argjson drain "$drain_seconds" --argjson released "$port_released" \
    '{schema_version:1, measurement_class:"live", identity:$identity[0], started_at:$started, finished_at:$finished,
      observations:{uid:$uid,health_status:$health,ready_status:$ready,tls_verified:$tls,
        transact_acked:$acked,subscription_converged:$converged,object_sha256_before:$before,
        object_sha256_after:$after,drain_seconds:$drain,port_released:$released}}' > "$evidence_dir/facts.json"
fi
echo 'container DB/readiness, TLS, non-root, transact/subscription, blob persistence, drain and port-release smoke passed'
