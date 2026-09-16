#!/usr/bin/env bash
# Test suite for EV-003: soak process identity contract in scripts/quality-soak.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
QUALITY_SOAK="$REPO_ROOT/scripts/quality-soak.sh"

TEST_TMPDIR="$(mktemp -d)"
CLEANUP_PIDS=()
cleanup() {
  for p in "${CLEANUP_PIDS[@]}"; do
    kill -9 "$p" 2>/dev/null || true
    wait "$p" 2>/dev/null || true
  done
  rm -rf "$TEST_TMPDIR"
}
trap cleanup EXIT

passed=0
failed=0

assert_fail_with_msg() {
  local test_name="$1"
  local expected_msg="$2"
  shift 2
  local out_file="$TEST_TMPDIR/out.$$.$RANDOM"
  local err_file="$TEST_TMPDIR/err.$$.$RANDOM"
  local status=0
  "$@" >"$out_file" 2>"$err_file" || status=$?
  local combined
  combined="$(cat "$out_file" "$err_file" 2>/dev/null || true)"
  if [[ "$status" -eq 0 ]]; then
    echo "FAIL: $test_name (expected failure, but exited 0)" >&2
    echo "Output: $combined" >&2
    failed=$((failed + 1))
    return 1
  fi
  if [[ -n "$expected_msg" ]] && ! grep -Fq "$expected_msg" <<< "$combined"; then
    echo "FAIL: $test_name (expected message '$expected_msg' not found in output)" >&2
    echo "Output: $combined" >&2
    failed=$((failed + 1))
    return 1
  fi
  echo "PASS: $test_name"
  passed=$((passed + 1))
  return 0
}

assert_pass() {
  local test_name="$1"
  shift
  local out_file="$TEST_TMPDIR/out.$$.$RANDOM"
  local err_file="$TEST_TMPDIR/err.$$.$RANDOM"
  local status=0
  "$@" >"$out_file" 2>"$err_file" || status=$?
  local combined
  combined="$(cat "$out_file" "$err_file" 2>/dev/null || true)"
  if [[ "$status" -ne 0 ]]; then
    echo "FAIL: $test_name (expected exit 0, got $status)" >&2
    echo "Output: $combined" >&2
    failed=$((failed + 1))
    return 1
  fi
  echo "PASS: $test_name"
  passed=$((passed + 1))
  return 0
}

assert_exit_code() {
  local test_name="$1"
  local expected_code="$2"
  shift 2
  local out_file="$TEST_TMPDIR/out.$$.$RANDOM"
  local err_file="$TEST_TMPDIR/err.$$.$RANDOM"
  local status=0
  "$@" >"$out_file" 2>"$err_file" || status=$?
  if [[ "$status" -ne "$expected_code" ]]; then
    local combined
    combined="$(cat "$out_file" "$err_file" 2>/dev/null || true)"
    echo "FAIL: $test_name (expected exit $expected_code, got $status)" >&2
    echo "Output: $combined" >&2
    failed=$((failed + 1))
    return 1
  fi
  echo "PASS: $test_name"
  passed=$((passed + 1))
  return 0
}

# Create mock soak binary
MOCK_SOAK_BIN="$TEST_TMPDIR/mock-soak"
cat << 'EOF' > "$MOCK_SOAK_BIN"
#!/usr/bin/env bash
if [[ -n "${MOCK_SOAK_ACTION:-}" ]]; then
  eval "$MOCK_SOAK_ACTION"
fi
exit "${MOCK_SOAK_EXIT:-0}"
EOF
chmod +x "$MOCK_SOAK_BIN"

# Helper to compile a small Go daemon listening on a specified port
COMPILED_DAEMON="$TEST_TMPDIR/mock-daemon"
cat << 'EOF' > "$TEST_TMPDIR/daemon.go"
package main
import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)
func main() {
	port := flag.Int("port", 19988, "port to listen on")
	flag.Parse()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen error: %v\n", err)
		os.Exit(1)
	}
	defer l.Close()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-sigCh:
	case <-time.After(120 * time.Second):
	}
}
EOF
go build -o "$COMPILED_DAEMON" "$TEST_TMPDIR/daemon.go"

# Compute candidate SHA of COMPILED_DAEMON
DAEMON_SHA="$(shasum -a 256 "$COMPILED_DAEMON" 2>/dev/null | awk '{print $1}' || sha256sum "$COMPILED_DAEMON" | awk '{print $1}')"

# Test 1: Unsupported platform must fail closed
test_unsupported_platform() {
  "$COMPILED_DAEMON" -port 19901 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_fail_with_msg "unsupported platform fails closed" "unsupported on platform 'SunOS'" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19901/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_OS_OVERRIDE="SunOS" \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 2: Stale PID (nonexistent process) must fail closed
test_stale_pid_nonexistent() {
  local stale_pid=65530
  if kill -0 "$stale_pid" 2>/dev/null; then
    stale_pid=65531
  fi
  assert_fail_with_msg "stale nonexistent PID fails closed" "" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$stale_pid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19902/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        bash "$QUALITY_SOAK"
}

# Test 3: Stale PID (process that exited before sampling) must fail closed
test_stale_pid_just_exited() {
  "$COMPILED_DAEMON" -port 19903 >/dev/null 2>&1 &
  local dpid=$!
  sleep 0.2
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
  assert_fail_with_msg "stale just-exited PID fails closed" "stale PID" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19903/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        bash "$QUALITY_SOAK"
}

# Test 4: Wrong executable must fail closed
test_wrong_executable() {
  sleep 30 >/dev/null 2>&1 &
  local sleep_pid=$!
  CLEANUP_PIDS+=("$sleep_pid")
  assert_fail_with_msg "wrong executable fails closed" "executable mismatch" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$sleep_pid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19904/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        bash "$QUALITY_SOAK"
  kill -9 "$sleep_pid" 2>/dev/null || true
  wait "$sleep_pid" 2>/dev/null || true
}

# Test 5: Candidate SHA mismatch must fail closed
test_candidate_sha_mismatch() {
  "$COMPILED_DAEMON" -port 19905 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_fail_with_msg "candidate SHA mismatch fails closed" "candidate SHA mismatch" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19905/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_CANDIDATE_SHA="0000000000000000000000000000000000000000000000000000000000000000" \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 6: Endpoint mismatch must fail closed
test_endpoint_mismatch() {
  "$COMPILED_DAEMON" -port 19906 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  # Server is listening on 19906, but SOAK_URL points to 19907
  assert_fail_with_msg "endpoint mismatch fails closed" "endpoint mismatch" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19907/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_CANDIDATE_SHA="$DAEMON_SHA" \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 7: Configuration digest mismatch must fail closed
test_config_digest_mismatch() {
  "$COMPILED_DAEMON" -port 19908 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_fail_with_msg "config digest mismatch fails closed" "configuration digest mismatch" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19908/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_CANDIDATE_SHA="$DAEMON_SHA" \
        SOAK_CONFIG_DIGEST="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 8: Replacement detected (process replaced during soak) must fail closed
test_replacement_detected() {
  "$COMPILED_DAEMON" -port 19909 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_fail_with_msg "replacement during soak fails closed" "replacement detected" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19909/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_CANDIDATE_SHA="$DAEMON_SHA" \
        SOAK_SEAM_INJECT_REPLACEMENT=1 \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 9: Untrustworthy identity fails closed
test_untrustworthy_identity() {
  "$COMPILED_DAEMON" -port 19910 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_fail_with_msg "untrustworthy identity fails closed" "untrustworthy" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19910/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_SEAM_FORCE_UNTRUSTWORTHY=1 \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 10: Successful fully-bound soak passes
test_successful_bound_soak_pass() {
  "$COMPILED_DAEMON" -port 19911 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_pass "fully bound soak succeeds" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19911/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_CANDIDATE_SHA="$DAEMON_SHA" \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 11: Real instantd binary binding
test_real_instantd_binary() {
  if [[ ! -x "$REPO_ROOT/bin/instantd" ]]; then
    echo "SKIP: real instantd binary test (bin/instantd not built)"
    return 0
  fi
  local port=19922
  INSTANT_V2_INSECURE_DEV_SECRETS=1 INSTANT_V2_HTTP_PORT="$port" "$REPO_ROOT/bin/instantd" >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.5
  assert_pass "real instantd binary binds successfully" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:$port/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 12: Soak failure status code is preserved even when process identity passes
test_soak_status_preserved() {
  "$COMPILED_DAEMON" -port 19912 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_exit_code "soak exit code is preserved on failure" 42 \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19912/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        SOAK_CANDIDATE_SHA="$DAEMON_SHA" \
        MOCK_SOAK_EXIT=42 \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

# Test 13: Invalid PID format fails with exit 2
test_invalid_pid_format() {
  assert_exit_code "invalid PID format fails with exit 2" 2 \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="abc" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19913/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" \
        bash "$QUALITY_SOAK"
}

# Test 14: A wrong listening address on the expected port fails closed.
test_endpoint_address_mismatch() {
  "$COMPILED_DAEMON" -port 19914 >/dev/null 2>&1 &
  local dpid=$!
  CLEANUP_PIDS+=("$dpid")
  sleep 0.3
  assert_fail_with_msg "endpoint address mismatch fails closed" "endpoint mismatch" \
    env SOAK_APP="test-app" SOAK_ATTR="00000000-0000-0000-0000-000000000001" \
        SOAK_SERVER_PID="$dpid" SOAK_BIN="$MOCK_SOAK_BIN" \
        SOAK_URL="ws://127.0.0.1:19914/runtime/session" \
        SOAK_EVENTS="$TEST_TMPDIR/events.jsonl" SOAK_LOG="$TEST_TMPDIR/soak.log" \
        SOAK_EXPECTED_BIN="$COMPILED_DAEMON" SOAK_CANDIDATE_SHA="$DAEMON_SHA" \
        SOAK_SEAM_PROCESS_ENDPOINTS="127.0.0.2:19914" \
        bash "$QUALITY_SOAK"
  kill -9 "$dpid" 2>/dev/null || true
  wait "$dpid" 2>/dev/null || true
}

echo "=== Running EV-003 tests against $QUALITY_SOAK ==="
test_unsupported_platform || true
test_stale_pid_nonexistent || true
test_stale_pid_just_exited || true
test_wrong_executable || true
test_candidate_sha_mismatch || true
test_endpoint_mismatch || true
test_config_digest_mismatch || true
test_replacement_detected || true
test_untrustworthy_identity || true
test_successful_bound_soak_pass || true
test_real_instantd_binary || true
test_soak_status_preserved || true
test_invalid_pid_format || true
test_endpoint_address_mismatch || true

echo "=== Results: $passed passed, $failed failed ==="
if [[ "$failed" -gt 0 ]]; then
  exit 1
fi
exit 0
