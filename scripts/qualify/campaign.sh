#!/usr/bin/env bash
# QH-001 campaign orchestration. Runs one lane (build|native|recovery|soak)
# of a qualification campaign on an Ubuntu Docker host with no Go toolchain.
#
# Every resource created here is named iv2q-<campaign>-*, uses a private
# Docker network, publishes no ports beyond 127.0.0.1, and is removed on exit
# (trap), success or failure. Anything not carrying the prefix is never
# touched: cleanup helpers refuse unprefixed names.
#
# Lane placement:
#   build/native run INSIDE the qualification image (they need the Go
#     toolchain and must match the later `make test-release` environment).
#   recovery/soak execute the image-built qualify binary ON THE HOST so that
#     --pg-restart-cmd runs in host context, as the contract requires
#     ("--pg-restart-cmd, run by the host orchestrator"). Helpers are built
#     once in the image; the candidate comes from the build lane's
#     `make build`; later lanes refuse a binary sha mismatch.
#
# Layout under --workdir DIR:
#   src/                 cloned candidate (asserted HEAD == CANDIDATE_SHA, clean)
#   tools/               image-built host-runnable helpers (qualify, soak, soaksetup)
#   evidence/candidate/  bin/instantd + binary.sha256 + image.digest
#   evidence/<lane>/     lane result + raw artifacts; cleanup.complete on verified teardown
#   qualify/instantd.env per-campaign non-secret env file (DSN never in it)
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd -P)

PG_IMAGE="postgres:17@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232"
PG_HOST_PORT=55432
RECOVERY_ADDR="127.0.0.1:18080"
SOAK_ADDR="127.0.0.1:18081"

CAMPAIGN=""
CANDIDATE=""
BUNDLE=""
WORKDIR=""
LANE=""
HOST_ID=""
DRY_RUN=0

usage() {
  cat >&2 <<'EOF'
usage: campaign.sh --campaign ID --candidate SHA --bundle FILE --workdir DIR --lane {build|native|recovery|soak} --host-id NAME [--dry-run]
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --campaign) CAMPAIGN=${2:-}; shift 2;;
    --candidate) CANDIDATE=${2:-}; shift 2;;
    --bundle) BUNDLE=${2:-}; shift 2;;
    --workdir) WORKDIR=${2:-}; shift 2;;
    --lane) LANE=${2:-}; shift 2;;
    --host-id) HOST_ID=${2:-}; shift 2;;
    --dry-run) DRY_RUN=1; shift;;
    -h|--help) usage;;
    *) echo "campaign: unknown flag $1" >&2; usage;;
  esac
done

# ---- argument validation (also exercised hermetically by test-campaign.sh) ----
[[ $CAMPAIGN =~ ^[a-z0-9][a-z0-9-]{0,40}$ ]] || { echo "campaign: --campaign must be non-empty [a-z0-9-] (max 41 chars)" >&2; exit 2; }
[[ $CANDIDATE =~ ^[0-9a-f]{40}$ ]] || { echo "campaign: --candidate must be 40 lowercase hex characters" >&2; exit 2; }
[[ -n $BUNDLE && -f $BUNDLE && ! -L $BUNDLE ]] || { echo "campaign: --bundle must be a regular non-symlink file" >&2; exit 2; }
[[ -n $WORKDIR ]] || { echo "campaign: --workdir is required" >&2; exit 2; }
case "$LANE" in build|native|recovery|soak) ;; *) echo "campaign: --lane must be one of build|native|recovery|soak" >&2; exit 2;; esac
[[ -n $HOST_ID ]] || { echo "campaign: --host-id is required" >&2; exit 2; }

PREFIX="iv2q-${CAMPAIGN}-"
NET="${PREFIX}net"
PG="${PREFIX}pg"
QUAL_IMG_TAG="${PREFIX}qualify"

# require_prefix: prefix-safety gate. Any Docker resource this harness stops,
# removes, or inspects must carry the campaign prefix; anything else is a
# hard refusal (never stop/modify unowned containers).
require_prefix() {
  local name=$1
  [[ -n $name && $name == "$PREFIX"* ]] || { echo "campaign: refusing unprefixed resource '$name' (want prefix '$PREFIX')" >&2; return 1; }
}

# prefixed_only: filter helper — prints only lines starting with the prefix.
prefixed_only() {
  local prefix=$1
  grep -E "^${prefix//-/\\-}" || true
}

if [[ $DRY_RUN == 1 ]]; then
  echo "campaign dry-run ok: prefix=$PREFIX lane=$LANE candidate=$CANDIDATE host=$HOST_ID"
  echo "planned: network=$NET postgres=$PG image=$QUAL_IMG_TAG"
  exit 0
fi

command -v docker >/dev/null || { echo "campaign: docker is required on the host" >&2; exit 2; }
command -v git >/dev/null || { echo "campaign: git is required on the host" >&2; exit 2; }

mkdir -p "$WORKDIR/evidence" "$WORKDIR/qualify" "$WORKDIR/tools"
SRC="$WORKDIR/src"
EVIDENCE="$WORKDIR/evidence"
TOOLS="$WORKDIR/tools"

# reap_workdir_processes kills processes whose executable lives under this
# campaign's workdir (host-run qualify/instantd/soak). An interrupted lane
# would otherwise orphan them, holding ports and the owned database.
reap_workdir_processes() {
  local root d exe
  root=$(cd "$WORKDIR" && pwd -P) || return 0
  for d in /proc/[0-9]*; do
    exe=$(readlink "$d/exe" 2>/dev/null) || continue
    case "$exe" in "$root"/*) kill -9 "${d#/proc/}" 2>/dev/null || true;; esac
  done
}

cleanup() {
  reap_workdir_processes
  # Remove only prefixed resources; every name here derives from $PREFIX.
  require_prefix "$PG" || return 0
  docker rm -f "$PG" >/dev/null 2>&1 || true
  require_prefix "$NET" || return 0
  docker network rm "$NET" >/dev/null 2>&1 || true
  local vol
  while IFS= read -r vol; do
    [[ -z $vol ]] && continue
    require_prefix "$vol"
    docker volume rm "$vol" >/dev/null 2>&1 || true
  done < <(docker volume ls -q 2>/dev/null | prefixed_only "$PREFIX" || true)
}
trap cleanup EXIT

port_free() {
  ! (echo >/dev/tcp/127.0.0.1/$1) >/dev/null 2>&1
}

# ---- per-campaign non-secret configuration (DSN injected separately) ----
# F5 (QH-001 R1): this file is the non-secret configuration artifact only.
# Secrets are NEVER written here (or to evidence): they are generated below
# per campaign.sh invocation and passed to lane processes via the
# environment at start time.
if [[ ! -f $WORKDIR/qualify/instantd.env ]]; then
  cat >"$WORKDIR/qualify/instantd.env" <<EOF
INSTANT_V2_HTTP_ADDR=:8080
INSTANT_V2_STORAGE_ROOT=/var/lib/instantd/storage
INSTANT_V2_INVALIDATION_BUS=none
EOF
  chmod 600 "$WORKDIR/qualify/instantd.env"
fi

# ---- per-campaign generated secrets (F5, env-only, never persisted) ----
# internal/config refuses non-dev startup without INSTANT_V2_STORAGE_SECRET
# and INSTANT_V2_STORAGE_ROOT, and requires the four INSTANT_OAUTH_* values
# whenever DATABASE_URL is set (which the recovery/soak lanes always set).
# Generate fresh random values for this invocation and export them ONLY to
# lane child processes. Nothing here touches disk, evidence, or the env file.
rand_hex() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
unset INSTANT_V2_INSECURE_DEV_SECRETS || true
export INSTANT_V2_STORAGE_SECRET="qh001-$(rand_hex)"
export INSTANT_V2_STORAGE_ROOT="$WORKDIR/qualify/storage"
export INSTANT_OAUTH_GOOGLE_CLIENT_ID="qh001-google-id-$(rand_hex)"
export INSTANT_OAUTH_GOOGLE_CLIENT_SECRET="qh001-google-secret-$(rand_hex)"
export INSTANT_OAUTH_GITHUB_CLIENT_ID="qh001-github-id-$(rand_hex)"
export INSTANT_OAUTH_GITHUB_CLIENT_SECRET="qh001-github-secret-$(rand_hex)"
mkdir -p "$INSTANT_V2_STORAGE_ROOT"
CANDIDATE_BIN="$EVIDENCE/candidate/instantd"

# ---- candidate transport: clone the bundle, assert identity ----
if [[ ! -d $SRC/.git ]]; then
  rm -rf "$SRC"
  git clone -q "$BUNDLE" "$SRC"
fi
HEAD_SHA=$(git -C "$SRC" rev-parse HEAD)
[[ $HEAD_SHA == "$CANDIDATE" ]] || { echo "campaign: bundle HEAD $HEAD_SHA != candidate $CANDIDATE" >&2; exit 1; }
[[ -z $(git -C "$SRC" status --porcelain --untracked-files=all) ]] || { echo "campaign: candidate tree is dirty" >&2; exit 1; }

# ---- private network + owned postgres (loopback-published only) ----
if ! docker network inspect "$NET" >/dev/null 2>&1; then
  docker network create "$NET" >/dev/null
fi
DB_SANITIZED=$(printf '%s' "$CAMPAIGN" | tr -c 'a-z0-9' '_')
DB_NAME="instant_bench_qh001_${DB_SANITIZED}"
# Host-context DSN (127.0.0.1) and in-image DSN (container name) for one DB.
HOST_DB_URL="postgres://instant:instant@127.0.0.1:${PG_HOST_PORT}/${DB_NAME}?sslmode=disable"
IMAGE_DB_URL="postgres://instant:instant@${PG}:5432/${DB_NAME}?sslmode=disable"
if ! docker inspect "$PG" >/dev/null 2>&1; then
  port_free "$PG_HOST_PORT" || { echo "campaign: host port $PG_HOST_PORT is occupied" >&2; exit 1; }
  docker run -d --name "$PG" --network "$NET" \
    -p "127.0.0.1:${PG_HOST_PORT}:5432" \
    -e POSTGRES_USER=instant -e POSTGRES_PASSWORD=instant -e POSTGRES_DB=postgres \
    "$PG_IMAGE" -c wal_level=logical -c max_connections=200 >/dev/null
  ready=0
  for _ in $(seq 1 30); do
    if docker exec "$PG" pg_isready -U instant -d postgres >/dev/null 2>&1; then ready=1; break; fi
    sleep 1
  done
  [[ $ready == 1 ]] || { echo "campaign: owned postgres never became ready" >&2; exit 1; }
  docker exec "$PG" createdb -U instant "$DB_NAME" >/dev/null
fi

# ---- qualification image + image-built host tools ----
# Only helper binaries are built here (qualify, soak, soaksetup). THE
# candidate binary is `make build` output (src/bin/instantd) recorded by the
# build lane; recovery/soak execute exactly evidence/candidate/instantd.
docker build -q -f "$SCRIPT_DIR/Dockerfile" -t "$QUAL_IMG_TAG" "$REPO_ROOT" >/dev/null
IMAGE_DIGEST=$(docker inspect --format='{{.Id}}' "$QUAL_IMG_TAG")
docker run --rm --user "$(id -u):$(id -g)" -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro --network "$NET" -v "$SRC:/src:ro" -v "$TOOLS:/out" "$QUAL_IMG_TAG" \
  bash -c 'cd /src && go build -o /out/qualify ./cmd/qualify && go build -o /out/soak ./cmd/soak && go build -o /out/soaksetup ./cmd/soaksetup && chmod +x /out/*'

case "$LANE" in
  build)
    # Build the candidate twice in the same image+path; the gate re-runs
    # `make build` in this image and compares sha256, so the bytes must be
    # identical. Determinism assumptions: same image digest, same /src path,
    # same clean VCS state, no VCS/timestamp stamping in ldflags. (Go build
    # IDs hash content, not mtimes, so checkout time does not matter.)
    #
    # F1 (QH-001 R1): `make build` writes bin/instantd, so /src is mounted
    # read-write for the build lane ONLY. All other lanes keep :ro.
    # THE candidate binary is this make output (src/bin/instantd), copied to
    # evidence/candidate/instantd; recovery/soak execute exactly that file.
    # (bin/ is gitignored, so the candidate tree stays clean.)
    docker run --rm --user "$(id -u):$(id -g)" -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro --network "$NET" -v "$SRC:/src" "$QUAL_IMG_TAG" \
      bash -c 'cd /src && make build && sha256sum bin/instantd' | tee "$EVIDENCE/candidate-build1.txt" >/dev/null
    docker run --rm --user "$(id -u):$(id -g)" -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro --network "$NET" -v "$SRC:/src" "$QUAL_IMG_TAG" \
      bash -c 'cd /src && make build && sha256sum bin/instantd' | tee "$EVIDENCE/candidate-build2.txt" >/dev/null
    sha1=$(awk '$2 == "bin/instantd" {print $1}' "$EVIDENCE/candidate-build1.txt")
    sha2=$(awk '$2 == "bin/instantd" {print $1}' "$EVIDENCE/candidate-build2.txt")
    [[ $sha1 =~ ^[0-9a-f]{64}$ ]] || { echo "campaign: could not read candidate sha from build output" >&2; exit 1; }
    [[ $sha1 == "$sha2" ]] || { echo "campaign: non-deterministic build ($sha1 vs $sha2)" >&2; exit 1; }
    mkdir -p "$EVIDENCE/candidate"
    install -m 0755 "$SRC/bin/instantd" "$EVIDENCE/candidate/instantd"
    echo "$IMAGE_DIGEST" > "$EVIDENCE/candidate/image.digest"
    echo "$sha1" > "$EVIDENCE/candidate/binary.sha256"
    echo "campaign: build lane ok sha=$sha1"
    ;;
  native|recovery|soak)
    # Later lanes refuse to run when the binary differs from the build lane.
    [[ -f $EVIDENCE/candidate/binary.sha256 ]] || { echo "campaign: build lane must run first (no recorded binary sha)" >&2; exit 1; }
    [[ -x $CANDIDATE_BIN ]] || { echo "campaign: build lane must run first (no $CANDIDATE_BIN)" >&2; exit 1; }
    recorded=$(cat "$EVIDENCE/candidate/binary.sha256")
    current=$(sha256sum "$CANDIDATE_BIN" | awk '{print $1}')
    [[ $current == "$recorded" ]] || { echo "campaign: binary sha $current != build lane $recorded; refusing" >&2; exit 1; }
    mkdir -p "$EVIDENCE/$LANE"
    case "$LANE" in
      native)
        docker run --rm --user "$(id -u):$(id -g)" -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro --network "$NET" \
          -v "$SRC:/src:ro" -v "$EVIDENCE:/evidence" \
          -e DATABASE_URL="$IMAGE_DB_URL" -e INSTANT_TEST_INTEGRATION=1 \
          "$QUAL_IMG_TAG" bash -c 'go build -o /tmp/qualify ./cmd/qualify && INSTANT_TEST_INTEGRATION=1 /tmp/qualify native --run --out "/evidence/native/lane.json" --raw-log "/evidence/native/gotest.json"'
        ;;
      recovery)
        port_free 18080 || { echo "campaign: $RECOVERY_ADDR is occupied" >&2; exit 1; }
        DATABASE_URL="$HOST_DB_URL" "$TOOLS/qualify" recovery \
          --instantd "$CANDIDATE_BIN" --soaksetup "$TOOLS/soaksetup" \
          --pg-restart-cmd "docker restart $PG" --addr "$RECOVERY_ADDR" \
          --events-dir "$EVIDENCE/recovery" --out "$EVIDENCE/recovery/lane.json"
        ;;
      soak)
        port_free 18081 || { echo "campaign: $SOAK_ADDR is occupied" >&2; exit 1; }
        DATABASE_URL="$HOST_DB_URL" "$TOOLS/qualify" soak \
          --instantd "$CANDIDATE_BIN" --soaksetup "$TOOLS/soaksetup" --soak-bin "$TOOLS/soak" \
          --addr "$SOAK_ADDR" --out "$EVIDENCE/soak/lane.json"
        ;;
    esac
    ;;
esac

# ---- cleanup verification: no prefixed residue, then mark complete ----
verify_cleanup() {
  local leftover=0
  local c
  while IFS= read -r c; do
    [[ -z $c ]] && continue
    echo "campaign: leftover container $c" >&2
    leftover=1
  done < <(docker ps -aq --filter "name=${PREFIX}" 2>/dev/null || true)
  if docker network ls -q --filter "name=${NET}" 2>/dev/null | grep -q .; then
    echo "campaign: leftover network $NET" >&2
    leftover=1
  fi
  local v
  while IFS= read -r v; do
    [[ -z $v ]] && continue
    echo "campaign: leftover volume $v" >&2
    leftover=1
  done < <(docker volume ls -q 2>/dev/null | prefixed_only "$PREFIX" || true)
  return $leftover
}

# Trap removes prefixed resources first; verify nothing remains.
cleanup
trap - EXIT
if verify_cleanup; then
  mkdir -p "$EVIDENCE/$LANE"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$EVIDENCE/$LANE/cleanup.complete"
  echo "campaign: lane $LANE complete, cleanup verified"
else
  echo "campaign: cleanup verification failed" >&2
  exit 1
fi
