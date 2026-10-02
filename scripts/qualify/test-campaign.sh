#!/usr/bin/env bash
# Hermetic contract checks for scripts/qualify orchestration.
# Shellcheck-clean, no docker required: covers dry-run argument validation
# and prefix-safety (refuses empty campaign id, refuses unprefixed names).
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CAMPAIGN="$SCRIPT_DIR/campaign.sh"
GATE="$SCRIPT_DIR/gate.sh"

pass=0
fail=0
ok() { local name=$1 out; shift; if out=$("$@" 2>&1); then echo "PASS: $name"; pass=$((pass+1)); else echo "FAIL: $name ($out)" >&2; fail=$((fail+1)); fi; }
bad() { local name=$1 expected=$2; shift 2; local out status=0; out=$("$@" 2>&1) || status=$?; if [[ $status -ne 0 && $out == *"$expected"* ]]; then echo "PASS: $name"; pass=$((pass+1)); else echo "FAIL: $name ($status: $out)" >&2; fail=$((fail+1)); fi; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
touch "$tmp/bundle.git"
mkdir -p "$tmp/work"

SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

# --- dry-run happy path (no docker touched) ---
ok "dry-run build" bash "$CAMPAIGN" --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane build --host-id host-1 --dry-run
ok "dry-run native" bash "$CAMPAIGN" --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane native --host-id host-1 --dry-run
ok "dry-run recovery" bash "$CAMPAIGN" --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane recovery --host-id host-1 --dry-run
ok "dry-run soak" bash "$CAMPAIGN" --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane soak --host-id host-1 --dry-run

ok "public profile dry-run build" bash "$CAMPAIGN" --profile single-node-public-alpha --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane build --host-id host-1 --dry-run
bad "unknown profile refused" "unknown profile" bash "$CAMPAIGN" --profile unknown --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane build --host-id host-1 --dry-run

# --- argument validation ---
bad "empty campaign refused" "--campaign must be non-empty" bash "$CAMPAIGN" --campaign "" --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane build --host-id host-1 --dry-run
bad "uppercase campaign refused" "--campaign must be non-empty" bash "$CAMPAIGN" --campaign "Alpha" --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane build --host-id host-1 --dry-run
bad "bad candidate refused" "--candidate must be 40" bash "$CAMPAIGN" --campaign alpha-1 --candidate deadbeef --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane build --host-id host-1 --dry-run
bad "missing bundle refused" "--bundle must be" bash "$CAMPAIGN" --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/nope.git" --workdir "$tmp/work" --lane build --host-id host-1 --dry-run
bad "bad lane refused" "--lane must be one of" bash "$CAMPAIGN" --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane destroy --host-id host-1 --dry-run
bad "missing host refused" "--host-id is required" bash "$CAMPAIGN" --campaign alpha-1 --candidate "$SHA" --bundle "$tmp/bundle.git" --workdir "$tmp/work" --lane build --host-id "" --dry-run
bad "gate bad candidate refused" "--candidate must be 40" bash "$GATE" --campaign alpha-1 --candidate deadbeef --workdir "$tmp/work"
bad "gate empty campaign refused" "--campaign must be non-empty" bash "$GATE" --campaign "" --candidate "$SHA" --workdir "$tmp/work"

# --- prefix safety: require_prefix refuses names without the campaign prefix ---
prefix_case() {
  local campaign=$1 name=$2
  PREFIX="iv2q-${campaign}-" bash -c '
    PREFIX="iv2q-'"$campaign"'-"
    require_prefix() { local name=$1; [[ -n $name && $name == "$PREFIX"* ]] || { echo "campaign: refusing unprefixed resource '\''$name'\'' (want prefix '\''$PREFIX'\'')" >&2; return 1; }; }
    require_prefix "'"$name"'"
  '
}
bad "empty name refused" "refusing unprefixed resource" prefix_case alpha-1 ""
bad "unowned container refused" "refusing unprefixed resource" prefix_case alpha-1 "ci-pg"
bad "other campaign refused" "refusing unprefixed resource" prefix_case alpha-1 "iv2q-other-pg"
bad "prefix substring refused" "refusing unprefixed resource" prefix_case alpha-1 "xiv2q-alpha-1-pg"
if prefix_case alpha-1 "iv2q-alpha-1-pg" >/dev/null 2>&1; then echo "PASS: prefixed name accepted"; pass=$((pass+1)); else echo "FAIL: prefixed name accepted" >&2; fail=$((fail+1)); fi

# --- the scripts source require_prefix/prefixed_only from campaign.sh itself ---
if grep -q "require_prefix \"\$PG\"" "$CAMPAIGN" && grep -q "prefixed_only \"\$PREFIX\"" "$CAMPAIGN"; then
  echo "PASS: cleanup paths go through prefix gates"; pass=$((pass+1))
else
  echo "FAIL: cleanup paths go through prefix gates" >&2; fail=$((fail+1))
fi
if grep -q '127.0.0.1' "$CAMPAIGN" && ! grep -Eq '\-p [0-9]+:5432|\-p \$\{?PG_HOST_PORT\}?:5432' "$CAMPAIGN"; then
  echo "PASS: no non-loopback port publish"; pass=$((pass+1))
else
  echo "FAIL: no non-loopback port publish" >&2; fail=$((fail+1))
fi

# --- QH-001 R1: build lane writes through a read-write /src mount; every
# other image mount stays read-only; recovery/soak execute the make-built
# candidate (evidence/candidate/instantd), never a separately built binary.
rw_mounts=$(grep -cE '\$SRC:/src"' "$CAMPAIGN" || true)
ro_mounts=$(grep -cE '\$SRC:/src:ro"' "$CAMPAIGN" || true)
if [[ $rw_mounts == 2 && $ro_mounts -ge 2 ]]; then
  echo "PASS: build lane mounts /src read-write (2 builds), other lanes stay :ro ($ro_mounts)"; pass=$((pass+1))
else
  echo "FAIL: /src mount modes (rw=$rw_mounts want 2, ro=$ro_mounts want >=2)" >&2; fail=$((fail+1))
fi
if grep -q -- '--instantd "$CANDIDATE_BIN"' "$CAMPAIGN" && ! grep -q -- '--instantd "$TOOLS/instantd"' "$CAMPAIGN" && grep -q 'install -m 0755 "$SRC/bin/instantd" "$EVIDENCE/candidate/instantd"' "$CAMPAIGN"; then
  echo "PASS: recovery/soak execute the make-built candidate binary"; pass=$((pass+1))
else
  echo "FAIL: recovery/soak execute the make-built candidate binary" >&2; fail=$((fail+1))
fi
if grep -q 'RUN git config --system --add safe.directory' "$SCRIPT_DIR/Dockerfile" && ! grep -Eq 'go build[^&|;]*-buildvcs' "$SCRIPT_DIR/Dockerfile" "$CAMPAIGN"; then
  echo "PASS: image trusts checkouts via git safe.directory (no -buildvcs=false)"; pass=$((pass+1))
else
  echo "FAIL: image trusts checkouts via git safe.directory (no -buildvcs=false)" >&2; fail=$((fail+1))
fi
if ! grep -q 'createdb -U instant postgres' "$CAMPAIGN" "$GATE" && grep -q 'createdb -U instant "\$DB_NAME"' "$CAMPAIGN"; then
  echo "PASS: owned database created with correct createdb syntax"; pass=$((pass+1))
else
  echo "FAIL: owned database created with correct createdb syntax" >&2; fail=$((fail+1))
fi
if ! grep -q 'INSTANT_V2_INSECURE_DEV_SECRETS=1' "$CAMPAIGN"; then
  echo "PASS: no insecure dev secrets in campaign orchestration"; pass=$((pass+1))
else
  echo "FAIL: no insecure dev secrets in campaign orchestration" >&2; fail=$((fail+1))
fi
if grep -q 'INSTANT_V2_STORAGE_SECRET=' "$CAMPAIGN" && grep -q 'INSTANT_OAUTH_GOOGLE_CLIENT_ID=' "$CAMPAIGN" && ! grep -q 'INSTANT_V2_STORAGE_SECRET.*>>.*instantd.env' "$CAMPAIGN"; then
  echo "PASS: per-campaign secrets generated env-only (never written to env file)"; pass=$((pass+1))
else
  echo "FAIL: per-campaign secrets generated env-only (never written to env file)" >&2; fail=$((fail+1))
fi

echo "$pass passed, $fail failed"
(( fail == 0 ))
