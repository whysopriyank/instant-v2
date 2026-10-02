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

CAMPAIGN="" CANDIDATE="" WORKDIR="" HOST_ID="" HANDOFFS="" STARTED="" PROFILE=single-node-alpha FIXTURES="" DISTRIBUTION_IMAGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE=${2:-}; shift 2;;
    --fixtures) FIXTURES=${2:-}; shift 2;;
    --image-digest) DISTRIBUTION_IMAGE=${2:-}; shift 2;;
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

case "$PROFILE" in single-node-alpha|single-node-public-alpha) ;; *) echo "assemble: unknown profile" >&2; exit 2;; esac
if [[ $PROFILE == single-node-public-alpha ]]; then
  [[ -f $FIXTURES && $DISTRIBUTION_IMAGE =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "assemble: public profile requires --fixtures and distribution --image-digest" >&2;exit 2; }
fi

WORKDIR=$(cd "$WORKDIR" && pwd -P)
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
lanes=(native recovery soak)
manifest_args=()
if [[ $PROFILE == single-node-public-alpha ]]; then
  policy=$WORKDIR/src/docs/plans/next-release/qualification-policy.json
  performance=$(jq -er '.performance | select(.=="not_selected" or .=="artifact")' "$policy")
  external_v1=$(jq -er '.external_v1 | select(.=="not_selected" or .=="run")' "$policy")
  lanes+=(container restore)
  manifest_args+=(--policy "$policy" --fixtures "$FIXTURES" --image-digest "$DISTRIBUTION_IMAGE" --container-record records/container.json --restore-record records/restore.json)
  if [[ $external_v1 == run ]]; then
    lanes+=(v1_differential)
    manifest_args+=(--v1-differential-record records/v1_differential.json)
  else
    jq -e '.external_v1_approval=="EXCLUDED_APPROVED" and .external_v1_scope=="Document explicit compatibility limits; no v1 parity claim"' "$policy" >/dev/null || { echo "assemble: unapproved v1 exclusion" >&2; exit 1; }
    [[ ! -e $EV/records/v1_differential.json ]] || { echo "assemble: unselected v1 record supplied" >&2; exit 1; }
  fi
  if [[ $performance == artifact ]]; then lanes+=(performance);manifest_args+=(--performance-record records/performance.json);fi
fi
for lane in "${lanes[@]}"; do
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
  identity_args=()
  fixture=$FIXTURE
  if [[ $PROFILE == single-node-public-alpha ]]; then
    name=$lane; [[ $name != native ]] || name=native_linux
    fixture=$(jq -er --arg name "$name" '.[$name].id' "$FIXTURES")
    fixture_sha=$(jq -er --arg name "$name" '.[$name].sha256' "$FIXTURES")
    identity_args+=(--image-digest "$DISTRIBUTION_IMAGE" --fixture-sha "$fixture_sha")
  fi
  "$QUALIFY" record --profile "$PROFILE" "${identity_args[@]}" --lane-result "$EV/$lane/lane.json" \
    --campaign "$CAMPAIGN" --candidate "$CANDIDATE" --binary-sha "$BIN_SHA" --config-sha "$CFG_SHA" \
    --fixture "$fixture" --host-id "$HOST_ID" --host-os linux --host-kernel "$KERNEL" \
    --host-arch amd64 --host-runtime "$runtime" --cleanup complete \
    "${args[@]}" --out "$EV/records/$lane.json"
  jq -e '.result == "PASS"' "$EV/records/$lane.json" >/dev/null || { echo "assemble: $lane record is not PASS" >&2; exit 1; }
done

# Handoffs: coordinator-authored packet/state/ledger_ref list, stamped now.
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq --arg now "$NOW" 'map(. + {finished_at: $now})' "$HANDOFFS" > "$EV/handoffs-input.json"

manifest_cmd=("$QUALIFY" manifest --profile "$PROFILE" "${manifest_args[@]}" --evidence-root "$EV" --campaign "$CAMPAIGN" --candidate "$CANDIDATE" \
  --binary "$(spec candidate/instantd)" --configuration "$(spec candidate/instantd.env)" \
  --campaign-started-at "$STARTED" --native-record records/native.json \
  --recovery-record records/recovery.json --soak-record records/soak.json \
  --handoffs "$EV/handoffs-input.json" --out manifest.json)
if [[ $PROFILE == single-node-public-alpha ]]; then
  (cd "$WORKDIR/src" && "${manifest_cmd[@]}")
else
  "${manifest_cmd[@]}"
fi
echo "assemble: manifest $EV/manifest.json"
