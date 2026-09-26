#!/usr/bin/env bash
# QH-001 evidence assembly: turns the native/recovery/soak lane results of one
# campaign into gate-schema records and the release manifest. Runs on the
# campaign host after all lanes; uses the image-built host `qualify` binary.
#
# Every artifact path is relative to <workdir>/evidence (the gate's manifest
# root). A lane without its verified cleanup marker is refused.
set -euo pipefail

usage() {
  echo "usage: assemble.sh --campaign ID --candidate SHA --workdir DIR --host-id NAME --handoffs FILE --campaign-started-at RFC3339" >&2
  exit 2
}

CAMPAIGN="" CANDIDATE="" WORKDIR="" HOST_ID="" HANDOFFS="" STARTED=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --campaign) CAMPAIGN=${2:-}; shift 2;;
    --candidate) CANDIDATE=${2:-}; shift 2;;
    --workdir) WORKDIR=${2:-}; shift 2;;
    --host-id) HOST_ID=${2:-}; shift 2;;
    --handoffs) HANDOFFS=${2:-}; shift 2;;
    --campaign-started-at) STARTED=${2:-}; shift 2;;
    *) usage;;
  esac
done
[[ $CAMPAIGN =~ ^[a-z0-9][a-z0-9-]{0,40}$ ]] || { echo "assemble: bad --campaign" >&2; exit 2; }
[[ $CANDIDATE =~ ^[0-9a-f]{40}$ ]] || { echo "assemble: bad --candidate" >&2; exit 2; }
[[ -n $WORKDIR && -d $WORKDIR/evidence ]] || { echo "assemble: --workdir has no evidence/" >&2; exit 2; }
[[ -n $HOST_ID ]] || { echo "assemble: --host-id is required" >&2; exit 2; }
[[ -f $HANDOFFS ]] || { echo "assemble: --handoffs file missing" >&2; exit 2; }
[[ $STARTED =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || { echo "assemble: --campaign-started-at must be RFC3339 UTC" >&2; exit 2; }

EV=$WORKDIR/evidence
QUALIFY=$WORKDIR/tools/qualify
[[ -x $QUALIFY ]] || { echo "assemble: $QUALIFY missing (run a lane first)" >&2; exit 1; }

spec() { # path relative to $EV -> path:size:sha256
  local rel=$1
  [[ -f $EV/$rel && ! -L $EV/$rel ]] || { echo "assemble: missing artifact $rel" >&2; return 1; }
  printf '%s:%s:%s' "$rel" "$(stat -c %s "$EV/$rel")" "$(sha256sum "$EV/$rel" | awk '{print $1}')"
}

# Candidate identity: binary from the build lane, non-secret config copied
# under the evidence root so the gate can hash it.
BIN_SHA=$(cat "$EV/candidate/binary.sha256")
[[ $(sha256sum "$EV/candidate/instantd" | awk '{print $1}') == "$BIN_SHA" ]] || { echo "assemble: candidate binary sha mismatch" >&2; exit 1; }
install -m 0600 "$WORKDIR/qualify/instantd.env" "$EV/candidate/instantd.env"
CFG_SHA=$(sha256sum "$EV/candidate/instantd.env" | awk '{print $1}')
IMAGE_DIGEST=$(cat "$EV/candidate/image.digest")
KERNEL=$(uname -r)
FIXTURE="instant_bench_qh001_$(printf '%s' "$CAMPAIGN" | tr -c 'a-z0-9' '_')"

mkdir -p "$EV/records"
for lane in native recovery soak; do
  [[ -f $EV/$lane/cleanup.complete ]] || { echo "assemble: lane $lane has no verified cleanup" >&2; exit 1; }
  [[ -f $EV/$lane/lane.json ]] || { echo "assemble: lane $lane has no lane.json" >&2; exit 1; }
  case $lane in
    native) runtime="go1.25.14 in qualification image ${IMAGE_DIGEST:7:12}";;
    *) runtime="candidate binary on host, docker $(docker version --format '{{.Server.Version}}')";;
  esac
  args=()
  while IFS= read -r f; do
    args+=(--artifact "$(spec "${f#"$EV"/}")")
  done < <(find "$EV/$lane" -type f | sort)
  "$QUALIFY" record --lane-result "$EV/$lane/lane.json" \
    --campaign "$CAMPAIGN" --candidate "$CANDIDATE" --binary-sha "$BIN_SHA" --config-sha "$CFG_SHA" \
    --fixture "$FIXTURE" --host-id "$HOST_ID" --host-os linux --host-kernel "$KERNEL" \
    --host-arch amd64 --host-runtime "$runtime" --cleanup complete \
    "${args[@]}" --out "$EV/records/$lane.json"
  jq -e '.result == "PASS"' "$EV/records/$lane.json" >/dev/null || { echo "assemble: $lane record is not PASS" >&2; exit 1; }
done

# Handoffs: coordinator-authored packet/state/ledger_ref list, stamped now.
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq --arg now "$NOW" 'map(. + {finished_at: $now})' "$HANDOFFS" > "$EV/handoffs-input.json"

"$QUALIFY" manifest --evidence-root "$EV" --campaign "$CAMPAIGN" --candidate "$CANDIDATE" \
  --binary "$(spec candidate/instantd)" --configuration "$(spec candidate/instantd.env)" \
  --campaign-started-at "$STARTED" --native-record records/native.json \
  --recovery-record records/recovery.json --soak-record records/soak.json \
  --handoffs "$EV/handoffs-input.json" --out manifest.json
echo "assemble: manifest $EV/manifest.json"
