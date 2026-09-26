#!/usr/bin/env bash
# QH-001 release-gate runner. Runs `make test-release` (i.e.
# scripts/quality-release-gate.sh) inside the qualification image with a
# fresh owned DB, RELEASE_GATE_MANIFEST pointing at the assembled evidence
# dir (mounted read-only, outside /src), and captures the full log.
#
# Evidence is assembled by campaign.sh lanes + `qualify record/manifest`;
# this script never provisions evidence, it only validates it.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd -P)

CAMPAIGN=""
CANDIDATE=""
WORKDIR=""

usage() {
  cat >&2 <<'EOF'
usage: gate.sh --campaign ID --candidate SHA --workdir DIR
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --campaign) CAMPAIGN=${2:-}; shift 2;;
    --candidate) CANDIDATE=${2:-}; shift 2;;
    --workdir) WORKDIR=${2:-}; shift 2;;
    -h|--help) usage;;
    *) echo "gate: unknown flag $1" >&2; usage;;
  esac
done

[[ $CAMPAIGN =~ ^[a-z0-9][a-z0-9-]{0,40}$ ]] || { echo "gate: --campaign must be non-empty [a-z0-9-]" >&2; exit 2; }
[[ $CANDIDATE =~ ^[0-9a-f]{40}$ ]] || { echo "gate: --candidate must be 40 lowercase hex characters" >&2; exit 2; }
[[ -n $WORKDIR && -d $WORKDIR/src ]] || { echo "gate: --workdir must contain the cloned candidate at src/" >&2; exit 2; }
[[ -f $WORKDIR/evidence/manifest.json ]] || { echo "gate: evidence manifest $WORKDIR/evidence/manifest.json is missing (assemble it first)" >&2; exit 2; }

command -v docker >/dev/null || { echo "gate: docker is required on the host" >&2; exit 2; }

PREFIX="iv2q-${CAMPAIGN}-"
PG="${PREFIX}gate-pg"
NET="${PREFIX}net"
QUAL_IMG_TAG="${PREFIX}qualify"

cleanup() {
  docker rm -f "$PG" >/dev/null 2>&1 || true
  [[ $NET == "$PREFIX"* ]] && docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

DB_NAME="instant_bench_qh001_gate_$(printf '%s' "$CAMPAIGN" | tr -c 'a-z0-9' '_')"
GATE_DB_URL="postgres://instant:instant@${PG}:5432/${DB_NAME}?sslmode=disable"

docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
docker run -d --name "$PG" --network "$NET" \
  -e POSTGRES_USER=instant -e POSTGRES_PASSWORD=instant -e POSTGRES_DB=postgres \
  "postgres:17@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232" \
  -c wal_level=logical -c max_connections=200 >/dev/null
ready=0
for _ in $(seq 1 30); do
  if docker exec "$PG" pg_isready -U instant -d postgres >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
[[ $ready == 1 ]] || { echo "gate: owned postgres never became ready" >&2; exit 1; }
docker exec "$PG" createdb -U instant "$DB_NAME" >/dev/null

# The evidence dir is mounted read-only at /evidence (outside /src, exactly
# as the gate expects a private snapshot source); the manifest path inside
# the container therefore differs from the host path by prefix only.
# /src is mounted read-write (not :ro): the gate runs `make build` (target
# `build` writes bin/instantd, then compares its sha256 to the qualified
# binary). bin/ is gitignored, so the gate's clean-tree assertion is
# unaffected.
docker run --rm --user "$(id -u):$(id -g)" -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro --network "$NET" \
  -v "$WORKDIR/src:/src" -v "$WORKDIR/evidence:/evidence:ro" \
  -e DATABASE_URL="$GATE_DB_URL" \
  -e RELEASE_GATE_MANIFEST=/evidence/manifest.json \
  -e RELEASE_CANDIDATE_SHA="$CANDIDATE" \
  -e RELEASE_CAMPAIGN_ID="$CAMPAIGN" \
  "$QUAL_IMG_TAG" bash -c 'cd /src && make test-release' 2>&1 | tee "$WORKDIR/evidence/gate.log"
status=${PIPESTATUS[0]}
echo "gate: make test-release exit=$status (full log: $WORKDIR/evidence/gate.log)"
exit "$status"
