#!/usr/bin/env bash
# Same load, correctness, lag, and RSS budgets as the existing PR soak gate.
# The caller owns provisioning/startup; this gate never chooses a database.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/soak-process-identity.sh
source "$SCRIPT_DIR/soak-process-identity.sh"

: "${SOAK_APP:?soak-gate requires SOAK_APP from the seeded fixture}"
: "${SOAK_ATTR:?soak-gate requires SOAK_ATTR from the seeded fixture}"
: "${SOAK_SERVER_PID:?soak-gate requires SOAK_SERVER_PID for the RSS check}"
: "${SOAK_BIN:?soak-gate requires SOAK_BIN}"
: "${SOAK_URL:?soak-gate requires SOAK_URL}"
: "${SOAK_EVENTS:?soak-gate requires SOAK_EVENTS}"
: "${SOAK_LOG:?soak-gate requires SOAK_LOG}"
case "$SOAK_SERVER_PID" in *[!0-9]*|'') echo 'SOAK_SERVER_PID must be a positive PID' >&2; exit 2;; esac
test "$SOAK_SERVER_PID" -gt 0
test -x "$SOAK_BIN"

# EV-003: Process identity binding
# Resolve expected server binary, candidate SHA, endpoint, and configuration digest
SOAK_EXPECTED_BIN="${SOAK_EXPECTED_BIN:-${SOAK_SERVER_BIN:-bin/instantd}}"
if [[ ! -f "$SOAK_EXPECTED_BIN" && ! -x "$SOAK_EXPECTED_BIN" ]]; then
  echo "quality-soak: expected server binary '$SOAK_EXPECTED_BIN' not found; failing closed" >&2
  exit 1
fi

SOAK_CANDIDATE_SHA="${SOAK_CANDIDATE_SHA:-${SOAK_CANDIDATE_DIGEST:-}}"
SOAK_CONFIG_DIGEST="${SOAK_CONFIG_DIGEST:-${SOAK_CONFIGURATION_DIGEST:-}}"
SOAK_ENDPOINT="${SOAK_EXPECTED_ENDPOINT:-$(soak_parse_url_endpoint "$SOAK_URL")}"

# Sample 1: Initial process identity check before soak; kill -0 alone is insufficient
initial_sample=$(soak_sample_process_identity "$SOAK_SERVER_PID" "$SOAK_EXPECTED_BIN" "$SOAK_CANDIDATE_SHA" "$SOAK_ENDPOINT" "$SOAK_CONFIG_DIGEST" "pre_soak") || exit $?

soak_status=0
"$SOAK_BIN" \
  -url "$SOAK_URL" -app "$SOAK_APP" -attr "$SOAK_ATTR" \
  -sessions 150 -ramp 10s -settle 30s -duration 100s \
  -global-tx-rate 8 -tx-interval 2s \
  -max-p99-lag 10s -events "$SOAK_EVENTS" 2>&1 | tee "$SOAK_LOG" || soak_status=$?

# Sample 2: Verify post-soak process identity; detect replacement, restarted process, or altered state
post_sample=$(soak_sample_process_identity "$SOAK_SERVER_PID" "$SOAK_EXPECTED_BIN" "$SOAK_CANDIDATE_SHA" "$SOAK_ENDPOINT" "$SOAK_CONFIG_DIGEST" "post_soak") || exit $?
soak_verify_process_sample "$initial_sample" "$post_sample" || exit 1

# Even a failed soak must still check the server's retained memory.
rss_kb=$(ps -o rss= -p "$SOAK_SERVER_PID" | tr -d ' ')
case "$rss_kb" in *[!0-9]*|'') echo 'cannot read server RSS' >&2; exit 1;; esac
echo "instantd RSS: $((rss_kb / 1024)) MiB"
if test "$rss_kb" -gt 1048576; then
  echo 'RSS budget exceeded' >&2
  exit 1
fi
exit "$soak_status"
