#!/usr/bin/env bash
# Same load, correctness, lag, and RSS budgets as the existing PR soak gate.
# The caller owns provisioning/startup; this gate never chooses a database.
set -euo pipefail
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
kill -0 "$SOAK_SERVER_PID"

soak_status=0
"$SOAK_BIN" \
  -url "$SOAK_URL" -app "$SOAK_APP" -attr "$SOAK_ATTR" \
  -sessions 150 -ramp 10s -settle 30s -duration 100s \
  -global-tx-rate 8 -tx-interval 2s \
  -max-p99-lag 10s -events "$SOAK_EVENTS" 2>&1 | tee "$SOAK_LOG" || soak_status=$?

# Even a failed soak must still check the server's retained memory.
rss_kb=$(ps -o rss= -p "$SOAK_SERVER_PID" | tr -d ' ')
case "$rss_kb" in *[!0-9]*|'') echo 'cannot read server RSS' >&2; exit 1;; esac
echo "instantd RSS: $((rss_kb / 1024)) MiB"
if test "$rss_kb" -gt 1048576; then
  echo 'RSS budget exceeded' >&2
  exit 1
fi
exit "$soak_status"
