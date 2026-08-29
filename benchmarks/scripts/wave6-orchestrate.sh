#!/usr/bin/env bash
set -euo pipefail

# Wave 6 is intentionally a controller, not a benchmark implementation. It
# snapshots every executable/input it invokes, performs fail-closed preflight,
# and launches one process group for the immutable seven-block bundle.
readonly TOTAL_ATTEMPTS=21
readonly FIRST_CHECK_SECONDS=16200
readonly MIN_FOLLOWUP_SECONDS=7200
readonly MAX_FOLLOWUP_SECONDS=36000

usage() {
  cat <<'EOF'
Usage:
  WAVE6_CONFIG=/abs/live.json WAVE6_STATE_DIR=/abs/state \
  WAVE6_BENCHSMOKE=/abs/benchsmoke WAVE6_BENCHRUN=/abs/benchrun \
    wave6-orchestrate.sh start

  WAVE6_STATE_DIR=/abs/state wave6-orchestrate.sh status [--json]

start must run inside the signed, dedicated loopback-only Linux network
namespace. It runs one V1 diagnostic, one exact triad diagnostic, and then one
detached seven-block/21-attempt benchmark process. status is read-only and
never approves, restarts, cleans, or mutates the benchmark bundle.
EOF
}

die() {
  printf 'wave6-orchestrate: %s\n' "$*" >&2
  exit 1
}

current_uid() { id -u; }

stat_uid() {
  local path=$1
  stat -c %u -- "$path" 2>/dev/null || stat -f %u -- "$path"
}

require_owned() {
  local label=$1 path=$2 owner uid
  owner=$(stat_uid "$path") || die "cannot inspect $label ownership"
  uid=$(current_uid)
  [[ "$owner" == "0" || "$owner" == "$uid" ]] || die "$label is not root/task owned"
}

# Reject . and .. as well as symlinks in every existing path component. Missing
# components are allowed so the caller can create a fresh state/output dir.
check_path_components() {
  local label=$1 path=$2 rest part current
  [[ "$path" == /* ]] || die "$label must be an absolute path"
  [[ "$path" != "/" ]] || die "$label must not be filesystem root"
  rest=${path#/}
  current=/
  while [[ "$rest" == */* ]]; do
    part=${rest%%/*}
    rest=${rest#*/}
    [[ -n "$part" && "$part" != "." && "$part" != ".." ]] || die "$label contains an unsafe path component"
    current="${current%/}/$part"
    [[ ! -L "$current" ]] || die "$label crosses a symlink: $current"
    if [[ -e "$current" && ! -d "$current" ]]; then
      die "$label has a non-directory parent: $current"
    fi
  done
  part=$rest
  [[ -n "$part" && "$part" != "." && "$part" != ".." ]] || die "$label contains an unsafe path component"
  current="${current%/}/$part"
  [[ ! -L "$current" ]] || die "$label is a symlink: $current"
}

require_regular_input() {
  local label=$1 path=$2
  check_path_components "$label" "$path"
  [[ -f "$path" ]] || die "$label is not a regular file: $path"
  [[ ! -L "$path" ]] || die "$label is a symlink: $path"
  require_owned "$label" "$path"
}

require_directory() {
  local label=$1 path=$2
  check_path_components "$label" "$path"
  [[ -d "$path" ]] || die "$label is not a directory: $path"
  [[ ! -L "$path" ]] || die "$label is a symlink: $path"
  require_owned "$label" "$path"
}

canonical_directory() {
  local path=$1
  require_directory directory "$path"
  (cd -- "$path" && pwd -P)
}

atomic_write() {
  local destination=$1 value=$2 directory temporary
  directory=$(dirname -- "$destination")
  [[ -d "$directory" && ! -L "$directory" ]] || die "atomic state parent is invalid"
  [[ ! -L "$destination" ]] || die "atomic state destination is a symlink"
  umask 077
  temporary=$(mktemp "$directory/.wave6-atomic.XXXXXX") || die "cannot create atomic state temporary"
  if ! { printf '%s\n' "$value" >"$temporary" && chmod 600 "$temporary" && mv -f -- "$temporary" "$destination"; }; then
    rm -f -- "$temporary"
    die "atomic state write failed"
  fi
}

atomic_identity_write() {
  local destination=$1
  shift
  local directory temporary
  directory=$(dirname -- "$destination")
  umask 077
  temporary=$(mktemp "$directory/.wave6-identity.XXXXXX") || die "cannot create identity temporary"
  if ! { "$@" >"$temporary" && chmod 600 "$temporary" && mv -f -- "$temporary" "$destination"; }; then
    rm -f -- "$temporary"
    die "identity write failed"
  fi
}

output_is_empty() {
  local output=$1
  require_directory full_output "$output"
  if find -P -- "$output" -mindepth 1 -print -quit 2>/dev/null | grep -q .; then
    die "full output is not empty: $output"
  fi
}

snapshot_input() {
  local label=$1 source=$2 destination=$3 mode=$4 before after copied helper
  require_regular_input "$label" "$source"
  check_path_components "$label snapshot" "$destination"
  [[ ! -e "$destination" && ! -L "$destination" ]] || die "$label snapshot already exists"
  helper=${SNAPSHOT_HELPER:-}
  [[ -f "$helper" && ! -L "$helper" ]] || die "stable snapshot helper is missing"
  before=$(sha256sum -- "$source" | awk '{print $1}') || die "cannot hash $label source"
  copied=$("$helper" -snapshot-source "$source" -snapshot-mode "$mode" -output "$destination") || die "cannot snapshot $label"
  [[ "$copied" =~ ^[0-9a-fA-F]{64}$ ]] || die "$label snapshot helper returned an invalid digest"
  after=$(sha256sum -- "$source" | awk '{print $1}') || die "cannot rehash $label source"
  [[ "$before" == "$after" ]] || die "$label changed while being snapshotted"
  require_owned "$label snapshot" "$destination"
  after=$(sha256sum -- "$destination" | awk '{print $1}') || die "cannot hash $label snapshot"
  [[ "$copied" == "$after" ]] || die "$label snapshot digest mismatch"
  printf '%s\n' "$copied"
}

verify_snapshot_hash() {
  local label=$1 path=$2 expected=$3 actual
  [[ -f "$path" && ! -L "$path" ]] || die "$label snapshot is missing"
  actual=$(sha256sum -- "$path" | awk '{print $1}') || die "cannot hash $label snapshot"
  [[ "$actual" == "$expected" ]] || die "$label snapshot hash changed"
}

snapshot_hash_matches() {
  local path=$1 expected=$2 actual
  [[ -f "$path" && ! -L "$path" ]] || return 1
  actual=$(sha256sum -- "$path" 2>/dev/null | awk '{print $1}') || return 1
  [[ "$actual" == "$expected" ]]
}

validate_config_shape() {
  local config=$1 self
  jq -e '
    .family == "H-append" and .scale == 300 and
    (.pair_id | type == "string" and length > 0) and
    (.seed | type == "number" and floor == . and . > 0) and
    .ramp_seconds == 30 and .settle_seconds == 30 and
    .warmup_seconds == 60 and .warmup_mutations == 0 and
    .measure_seconds == 180 and .grace_seconds == 30 and
    (.output | type == "string" and startswith("/")) and
    ([.targets[].id] | sort == ["v1","v2_current","v2_reference"]) and
    ([.targets[].role] | sort == ["v1","v2_current","v2_reference"]) and
    ([.targets[].kind] | sort == ["v1","v2","v2"]) and
    (.targets | length == 3) and
    (.targets | all(.[]; .id == .role and (.revision | type == "string" and length > 0))) and
    (.v1_sha == ([.targets[] | select(.id == "v1") | .revision][0])) and
    (.v2_reference_sha == ([.targets[] | select(.id == "v2_reference") | .revision][0])) and
    (.v2_sha == ([.targets[] | select(.id == "v2_current") | .revision][0]))
  ' "$config" >/dev/null || die "config must be the canonical authorized H300 triad"
  self=$(readlink /proc/self/ns/net 2>/dev/null || true)
  [[ "$self" == net:\[*\] ]] || die "start must run on Linux inside a readable network namespace"
}

run_bounded() {
  local stdout_path=$1 stderr_path=$2
  shift 2
  set +e
  "$@" >("$SNAPSHOT_SMOKES" -redact-stream -output "$stdout_path") 2>("$SNAPSHOT_SMOKES" -redact-stream -output "$stderr_path")
  local rc=$?
  wait || true
  set -e
  return "$rc"
}

record_identity() {
  local state=$1 config_hash=$2 smoke_hash=$3 run_hash=$4 config=$5 output=$6 lock_path=$7 namespace seed ns_v1 ns_ref ns_cur initial
  namespace=$(readlink /proc/self/ns/net)
  seed=$(jq -er '.seed' "$config")
  ns_v1=$(jq -er '.targets[] | select(.id == "v1") | .network_namespace_id' "$config")
  ns_ref=$(jq -er '.targets[] | select(.id == "v2_reference") | .network_namespace_id' "$config")
  ns_cur=$(jq -er '.targets[] | select(.id == "v2_current") | .network_namespace_id' "$config")
  initial=$(jq -er '.targets[] | select(.id == "v1") | .initial_network_namespace_id' "$config")
  atomic_identity_write "$state/identity.txt" bash -c '
    printf "state_root=%s\n" "$1"
    printf "config_snapshot=%s\n" "$2"
    printf "config_sha256=%s\n" "$3"
    printf "benchsmoke_snapshot=%s\n" "$4"
    printf "benchsmoke_sha256=%s\n" "$5"
    printf "benchrun_snapshot=%s\n" "$6"
    printf "benchrun_sha256=%s\n" "$7"
    printf "collector_namespace=%s\n" "$8"
    printf "full_output=%s\n" "$9"
    printf "output_lock_path=%s\n" "${10}"
    printf "contract_family=H-append\ncontract_scale=300\ncontract_seed=%s\n" "${11}"
    printf "target_v1_namespace=%s\ntarget_v2_reference_namespace=%s\ntarget_v2_current_namespace=%s\ninitial_namespace=%s\n" "${12}" "${13}" "${14}" "${15}"
    printf "first_checkpoint_seconds=%s\nmaximum_followup_window_seconds=%s\n" "${16}" "${17}"
  ' _ "$state" "$config" "$config_hash" "$SNAPSHOT_SMOKES" "$smoke_hash" "$SNAPSHOT_RUN" "$run_hash" "$namespace" "$output" "$lock_path" "$seed" "$ns_v1" "$ns_ref" "$ns_cur" "$initial" "$FIRST_CHECK_SECONDS" "$MAX_FOLLOWUP_SECONDS"
}

wait_for_full_start() {
  local state=$1 i
  for i in {1..100}; do
    if [[ -s "$state/full.started_at_epoch" && -s "$state/full.wrapper.pid" && -s "$state/full.wrapper.start_ticks" ]]; then
      return 0
    fi
    sleep 0.1
  done
  die "detached full process did not publish its start identity"
}

run_full_background() {
  local state=$1 config=$2 benchrun=$3 lock_fd=$4 child=0 rc=1 signal_seen=0
  local wrapper_exe wrapper_ticks started
  [[ "$lock_fd" =~ ^[0-9]+$ ]] || exit 125
  flock -n "$lock_fd" || exit 125
  SNAPSHOT_SMOKES="$state/input/benchsmoke"
  wrapper_exe=$(readlink /proc/$$/exe) || exit 1
  wrapper_ticks=$(awk '{print $22}' "/proc/$$/stat") || exit 1
  started=$(date +%s)
  atomic_write "$state/full.wrapper.pid" "$$"
  atomic_write "$state/full.launcher.pid" "$$"
  atomic_write "$state/full.wrapper.start_ticks" "$wrapper_ticks"
  atomic_write "$state/full.wrapper.exe" "$wrapper_exe"
  atomic_write "$state/full.wrapper.exe_sha256" "$(sha256sum -- "$wrapper_exe" | awk '{print $1}')"
  atomic_write "$state/full.started_at_epoch" "$started"
  local expected_config_hash expected_run_hash expected_smoke_hash
  expected_config_hash=$(read_identity_value config_sha256 "$state")
  expected_run_hash=$(read_identity_value benchrun_sha256 "$state")
  expected_smoke_hash=$(read_identity_value benchsmoke_sha256 "$state")
  if ! snapshot_hash_matches "$config" "$expected_config_hash" || ! snapshot_hash_matches "$benchrun" "$expected_run_hash" || ! snapshot_hash_matches "$SNAPSHOT_SMOKES" "$expected_smoke_hash"; then
    atomic_write "$state/full.finished_at_epoch" "$(date +%s)"
    atomic_write "$state/full.exit_code" 125
    exit 125
  fi

  forward_signal() {
    signal_seen=1
    if [[ "$child" =~ ^[0-9]+$ ]] && (( child > 1 )); then
      kill -TERM -- "-$child" 2>/dev/null || kill -TERM "$child" 2>/dev/null || true
    fi
  }
  trap forward_signal TERM INT HUP
  set +e
  setsid "$benchrun" -config "$config" \
    >("$SNAPSHOT_SMOKES" -redact-stream -output "$state/full.stdout") \
    2>("$SNAPSHOT_SMOKES" -redact-stream -output "$state/full.stderr") &
  child=$!
  atomic_write "$state/full.child.pid" "$child"
  atomic_write "$state/full.child.start_ticks" "$(awk '{print $22}' "/proc/$child/stat" 2>/dev/null || true)"
  atomic_write "$state/full.child.pgid" "$(ps -o pgid= -p "$child" | tr -d ' ' 2>/dev/null || true)"
  wait "$child"
  rc=$?
  while kill -0 "$child" 2>/dev/null; do
    wait "$child"
    rc=$?
  done
  wait || true
  set -e
  trap - TERM INT HUP
  if (( signal_seen != 0 )) && (( rc == 0 )); then
    rc=143
  fi
  atomic_write "$state/full.finished_at_epoch" "$(date +%s)"
  # Publish the exit code last. Status therefore never treats a finish epoch
  # without its paired exit record as a completed process.
  atomic_write "$state/full.exit_code" "$rc"
  exit "$rc"
}

start_locked() {
  local state=$1 config=$2 smoke=$3 run=$4 config_hash=$5 smoke_hash=$6 run_hash=$7 output=$8
  local lock_path="${output}.wave6-controller.lock"
  [[ $# == 8 && "${WAVE6_LOCK_FD:-}" =~ ^[0-9]+$ ]] || die "locked start invocation is incomplete"
  OUTPUT_LOCK_FD=$WAVE6_LOCK_FD
  flock -n "$OUTPUT_LOCK_FD" || die "benchmark output lock is no longer held"
  STATE_DIR=$state
  SNAPSHOT_SMOKES=$smoke
  SNAPSHOT_RUN=$run
  SNAPSHOT_HELPER=$smoke
  output_is_empty "$output"

  verify_snapshot_hash config "$config" "$config_hash"
  verify_snapshot_hash benchsmoke "$smoke" "$smoke_hash"
  if ! "$smoke" -validate-namespace -config "$config" >/dev/null 2>("$smoke" -redact-stream -output "$state/namespace.stderr"); then
    die "dedicated namespace proof failed"
  fi
  record_identity "$state" "$config_hash" "$smoke_hash" "$run_hash" "$config" "$output" "$lock_path"
  atomic_write "$state/contract.json" "{\"family\":\"H-append\",\"scale\":300,\"timings\":{\"ramp\":30,\"settle\":30,\"warmup\":60,\"measure\":180,\"grace\":30},\"warmup_mutations\":0,\"total_attempts\":21}"
  mkdir -- "$state/.start-lock" || die "state directory is already active"
  trap 'rmdir "$state/.start-lock" 2>/dev/null || true' EXIT

  if ! run_bounded "$state/v1-smoke.stdout" "$state/v1-smoke.stderr" \
      "$smoke" -config "$config" -targets v1 -measure-seconds 1 -output "$state/v1-smoke.json"; then
    die "V1 diagnostic smoke failed"
  fi
  jq -e '.schema_version == "bench-v1" and .diagnostic_only == true and .passed == true and .family == "H-append" and .scale == 300 and (.results | length == 1) and .results[0].target_id == "v1" and .results[0].target_revision != "" and .results[0].measured_window_valid == true and .results[0].ledger_rows == .results[0].expected_ledger_rows and .results[0].protocol_errors == 0 and .results[0].behavior_errors == 0' "$state/v1-smoke.json" >/dev/null || die "V1 smoke did not pass its diagnostic contract"
  verify_snapshot_hash config "$config" "$config_hash"
  verify_snapshot_hash benchsmoke "$smoke" "$smoke_hash"
  atomic_write "$state/v1-smoke.passed" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  if ! run_bounded "$state/triad-smoke.stdout" "$state/triad-smoke.stderr" \
      "$smoke" -config "$config" -targets all -measure-seconds 1 -output "$state/triad-smoke.json"; then
    die "three-target diagnostic smoke failed"
  fi
  jq -e '.schema_version == "bench-v1" and .diagnostic_only == true and .passed == true and .family == "H-append" and .scale == 300 and ([.results[].target_id] | sort == ["v1","v2_current","v2_reference"]) and all(.results[]; .passed == true and .target_revision != "" and .measured_window_valid == true and .ledger_rows == .expected_ledger_rows and .protocol_errors == 0 and .behavior_errors == 0)' "$state/triad-smoke.json" >/dev/null || die "triad smoke did not pass its diagnostic contract"
  verify_snapshot_hash config "$config" "$config_hash"
  verify_snapshot_hash benchsmoke "$smoke" "$smoke_hash"
  verify_snapshot_hash benchrun "$run" "$run_hash"
  atomic_write "$state/triad-smoke.passed" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # Recheck the root while the same lock descriptor is still held. This closes
  # the race between diagnostic completion and benchmark launch.
  output_is_empty "$output"
  nohup setsid "$0" _run-full "$state" "$config" "$run" \
    "$OUTPUT_LOCK_FD" \
    >("$smoke" -redact-stream -output "$state/controller.stdout") \
    2>("$smoke" -redact-stream -output "$state/controller.stderr") &
  wait_for_full_start "$state"
  exec {OUTPUT_LOCK_FD}>&-
  rmdir "$state/.start-lock" || die "cannot release start lock"
  trap - EXIT
  printf '%s\n' "$state"
}

start() {
  [[ "$(uname -s 2>/dev/null || true)" == "Linux" ]] || die "remote benchmark execution requires Linux dirfd/O_NOFOLLOW support"
  command -v jq >/dev/null || die "jq is required"
  command -v sha256sum >/dev/null || die "sha256sum is required"
  command -v setsid >/dev/null || die "setsid is required for process-group isolation"
  command -v flock >/dev/null || die "flock is required for output ownership"
  local source_config=${WAVE6_CONFIG:-} requested_state=${WAVE6_STATE_DIR:-}
  local source_smoke=${WAVE6_BENCHSMOKE:-} source_run=${WAVE6_BENCHRUN:-}
  [[ "$requested_state" == /* ]] || die "WAVE6_STATE_DIR must be an absolute path"
  check_path_components WAVE6_STATE_DIR "$requested_state"
  [[ ! -e "$requested_state" && ! -L "$requested_state" ]] || die "state directory must be a new unique path"
  mkdir -p -- "$requested_state" || die "cannot create state directory"
  chmod 700 -- "$requested_state"
  local state_dir
  state_dir=$(canonical_directory "$requested_state")
  [[ "$state_dir" != "/" ]] || die "state directory is invalid"
  mkdir -- "$state_dir/input" || die "cannot create immutable input directory"
  chmod 700 -- "$state_dir/input"
  STATE_DIR="$state_dir"
  trap 'rm -f -- "$STATE_DIR/input"/*.tmp.* 2>/dev/null || true' EXIT

  require_regular_input WAVE6_CONFIG "$source_config"
  require_regular_input WAVE6_BENCHSMOKE "$source_smoke"
  require_regular_input WAVE6_BENCHRUN "$source_run"
  SNAPSHOT_SMOKES="$STATE_DIR/input/benchsmoke"
  SNAPSHOT_RUN="$STATE_DIR/input/benchrun"
  SNAPSHOT_HELPER="$source_smoke"
  local snapshot_config="$STATE_DIR/input/live.json"
  local config_hash smoke_hash run_hash
  config_hash=$(snapshot_input config "$source_config" "$snapshot_config" 400)
  smoke_hash=$(snapshot_input benchsmoke "$source_smoke" "$SNAPSHOT_SMOKES" 500)
  run_hash=$(snapshot_input benchrun "$source_run" "$SNAPSHOT_RUN" 500)
  validate_config_shape "$snapshot_config"

  local output raw_output
  raw_output=$(jq -er '.output' "$snapshot_config") || die "config output is missing"
  check_path_components config.output "$raw_output"
  if [[ -e "$raw_output" ]]; then
    [[ -d "$raw_output" && ! -L "$raw_output" ]] || die "config output must be a non-symlink directory"
    require_owned config.output "$raw_output"
  else
    mkdir -p -- "$raw_output" || die "cannot create benchmark output directory"
  fi
  output=$(canonical_directory "$raw_output")
  [[ "$output" != "$STATE_DIR" && "$output" != "$STATE_DIR/"* ]] || die "benchmark output must not be the controller state root"
  exec "$SNAPSHOT_SMOKES" -lock-output "${output}.wave6-controller.lock" -- "$0" _start-locked \
    "$STATE_DIR" "$snapshot_config" "$SNAPSHOT_SMOKES" "$SNAPSHOT_RUN" "$config_hash" "$smoke_hash" "$run_hash" "$output"
}

read_identity_value() {
  local key=$1 state=$2
  awk -F= -v want="$key" '$1 == want {sub(/^[^=]*=/, ""); print; exit}' "$state/identity.txt"
}

process_identity_ok() {
  local state=$1 pid ticks exe expected_exe expected_hash actual_hash
  pid=$(cat "$state/full.wrapper.pid" 2>/dev/null || true)
  ticks=$(cat "$state/full.wrapper.start_ticks" 2>/dev/null || true)
  expected_exe=$(cat "$state/full.wrapper.exe" 2>/dev/null || true)
  expected_hash=$(cat "$state/full.wrapper.exe_sha256" 2>/dev/null || true)
  [[ "$pid" =~ ^[0-9]+$ && "$pid" -gt 1 && "$ticks" =~ ^[0-9]+$ && -n "$expected_exe" && -n "$expected_hash" ]] || return 1
  [[ -r "/proc/$pid/stat" && -r "/proc/$pid/exe" ]] || return 1
  [[ "$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true)" != "Z" ]] || return 1
  [[ "$(awk '{print $22}' "/proc/$pid/stat" 2>/dev/null || true)" == "$ticks" ]] || return 1
  [[ "$(readlink "/proc/$pid/exe" 2>/dev/null || true)" == "$expected_exe" ]] || return 1
  actual_hash=$(sha256sum -- "/proc/$pid/exe" 2>/dev/null | awk '{print $1}' || true)
  [[ "$actual_hash" == "$expected_hash" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

child_identity_ok() {
  local state=$1 pid ticks expected_exe expected_hash actual_hash pgid
  pid=$(cat "$state/full.child.pid" 2>/dev/null || true)
  ticks=$(cat "$state/full.child.start_ticks" 2>/dev/null || true)
  expected_exe=$(read_identity_value benchrun_snapshot "$state")
  expected_hash=$(read_identity_value benchrun_sha256 "$state")
  pgid=$(cat "$state/full.child.pgid" 2>/dev/null || true)
  [[ "$pid" =~ ^[0-9]+$ && "$pid" -gt 1 && "$ticks" =~ ^[0-9]+$ && "$pgid" == "$pid" && -n "$expected_exe" && -n "$expected_hash" ]] || return 1
  [[ -r "/proc/$pid/stat" && -r "/proc/$pid/exe" ]] || return 1
  [[ "$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true)" != "Z" ]] || return 1
  [[ "$(awk '{print $22}' "/proc/$pid/stat" 2>/dev/null || true)" == "$ticks" ]] || return 1
  [[ "$(readlink "/proc/$pid/exe" 2>/dev/null || true)" == "$expected_exe" ]] || return 1
  actual_hash=$(sha256sum -- "/proc/$pid/exe" 2>/dev/null | awk '{print $1}' || true)
  [[ "$actual_hash" == "$expected_hash" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

status_json() {
  local requested_state=${WAVE6_STATE_DIR:-} state identity output lock_path config smoke config_hash smoke_hash progress_raw progress_rc
  local started finished now elapsed phase running wrapper_running child_running exit_code completed failed eta avg next failure warning transient state_error
  local -a progress_args
  [[ "$(uname -s 2>/dev/null || true)" == "Linux" ]] || die "status requires Linux descriptor-safe artifact reads"
  [[ "$requested_state" == /* ]] || die "WAVE6_STATE_DIR must be an absolute path"
  check_path_components WAVE6_STATE_DIR "$requested_state"
  state=$(canonical_directory "$requested_state")
  [[ -f "$state/identity.txt" && ! -L "$state/identity.txt" ]] || die "state identity is missing: $state"
  identity=$(read_identity_value state_root "$state")
  [[ "$identity" == "$state" ]] || die "state identity root mismatch"
  output=$(read_identity_value full_output "$state")
  lock_path=$(read_identity_value output_lock_path "$state")
  config=$(read_identity_value config_snapshot "$state")
  smoke=$(read_identity_value benchsmoke_snapshot "$state")
  config_hash=$(read_identity_value config_sha256 "$state")
  smoke_hash=$(read_identity_value benchsmoke_sha256 "$state")
  [[ -n "$output" && -n "$lock_path" && -n "$config" && -n "$smoke" ]] || die "state identity is incomplete"
  [[ "$config" == "$state/input/live.json" ]] || die "state config snapshot is outside the immutable input root"
  [[ "$smoke" == "$state/input/benchsmoke" ]] || die "state benchsmoke snapshot is outside the immutable input root"
  local namespace_value namespace_key
  for namespace_key in collector_namespace target_v1_namespace target_v2_reference_namespace target_v2_current_namespace initial_namespace; do
    namespace_value=$(read_identity_value "$namespace_key" "$state")
    [[ "$namespace_value" =~ ^net:\[[0-9]+\]$ ]] || die "state namespace identity is malformed"
  done
  check_path_components full_output "$output"
  require_directory full_output "$output"
  [[ "$output" != "$state" && "$output" != "$state/"* ]] || die "state output is inside the controller root"
  check_path_components output_lock_path "$lock_path"
  [[ "$lock_path" == "${output}.wave6-controller.lock" ]] || die "state output lock identity mismatch"
  [[ -f "$lock_path" && ! -L "$lock_path" ]] || die "state output lock is missing or not a regular file"
  require_owned output_lock_path "$lock_path"
  check_path_components config_snapshot "$config"
  require_regular_input config_snapshot "$config"
  check_path_components benchsmoke_snapshot "$smoke"
  require_regular_input benchsmoke_snapshot "$smoke"
  verify_snapshot_hash config "$config" "$config_hash"
  verify_snapshot_hash benchsmoke "$smoke" "$smoke_hash"

  started=$(cat "$state/full.started_at_epoch" 2>/dev/null || printf '0')
  now=$(date +%s)
  finished=$(cat "$state/full.finished_at_epoch" 2>/dev/null || true)
  state_error=''
  if [[ -f "$state/full.exit_code" && ! -f "$state/full.finished_at_epoch" ]]; then
    state_error="full process finish record is missing"
  elif [[ -f "$state/full.finished_at_epoch" && ! "$finished" =~ ^[0-9]+$ ]]; then
    state_error="full process finish record is malformed"
  elif [[ -f "$state/full.finished_at_epoch" && "$started" =~ ^[0-9]+$ && "$finished" -lt "$started" ]]; then
    state_error="full process finish record predates its start"
  fi
  if [[ "$started" =~ ^[0-9]+$ && "$started" -gt 0 ]]; then
    if [[ "$finished" =~ ^[0-9]+$ && "$finished" -ge "$started" ]]; then
      elapsed=$((finished-started))
    else
      elapsed=$((now-started))
    fi
    (( elapsed < 0 )) && elapsed=0
  else
    elapsed=0
  fi
  running=false
  wrapper_running=false
  child_running=false
  if [[ ! -f "$state/full.exit_code" ]] && process_identity_ok "$state"; then
    wrapper_running=true
    # The wrapper remains the authority during child publication: while the
    # child PID/start identity is being published and while it publishes the
    # child-exit/finish records.
    # Those windows must not be misreported as an interrupted run.
    if child_identity_ok "$state"; then
      child_running=true
    fi
  fi
  running=$wrapper_running

  progress_raw='{}'
  progress_rc=0
  progress_args=(-config "$config" -validate-progress "$output")
  [[ "$running" == true ]] && progress_args+=(-allow-partial-progress)
  set +e
  progress_raw=$("$smoke" "${progress_args[@]}" 2>/dev/null)
  progress_rc=$?
  set -e
  if ! jq -e . >/dev/null 2>&1 <<<"$progress_raw"; then
    progress_raw='{"valid":false,"pending_evidence":false,"transient":false,"completed_attempts":0,"failed_attempts":0,"total_attempts":21,"error":"progress validator did not return JSON"}'
    progress_rc=1
  fi
  completed=$(jq -er '.completed_attempts // 0' <<<"$progress_raw")
  failed=$(jq -er '.failed_attempts // 0' <<<"$progress_raw")
  [[ "$completed" =~ ^[0-9]+$ ]] || completed=0
  [[ "$failed" =~ ^[0-9]+$ ]] || failed=0
  failure=$(jq -r '.error // empty' <<<"$progress_raw" | tr '\n' ' ' | cut -c1-512)
  warning=$(jq -r '.warning // empty' <<<"$progress_raw" | tr '\n' ' ' | cut -c1-512)
  transient=$(jq -r '.transient // false' <<<"$progress_raw")
  exit_code=''
  [[ -f "$state/full.exit_code" ]] && exit_code=$(cat "$state/full.exit_code")
  if [[ "$exit_code" =~ ^-?[0-9]+$ ]]; then
    if [[ -z "$state_error" && "$exit_code" == 0 && "$progress_rc" == 0 && "$completed" == "$TOTAL_ATTEMPTS" && "$failed" == 0 && $(jq -r '.valid // false' <<<"$progress_raw") == true ]]; then
      phase=completed
    else
      phase=failed
      [[ -n "$failure" ]] || failure="${state_error:-full benchmark exited without a valid canonical 21-attempt bundle}"
    fi
  elif [[ -f "$state/full.exit_code" ]]; then
    phase=failed
    failure="full process exit record is malformed"
  elif [[ "$running" == true ]]; then
    phase=running
    # A running wrapper/child owns the output lock and has a verified PID
    # identity. An atomically published artifact is authoritative; a malformed
    # partial file observed during publication is only a warning until the next
    # check or the atomic child exit record.
    if [[ "$progress_rc" != 0 && "$transient" != true ]]; then
      phase=failed
    fi
    [[ -n "$warning" ]] || warning="$failure"
  elif [[ "$progress_rc" != 0 && -n "$failure" ]]; then
    phase=failed
  elif [[ -f "$state/full.launched_at" || -f "$state/full.started_at_epoch" ]]; then
    phase=interrupted
    [[ -n "$failure" ]] || failure="full process is no longer live and has no atomic exit record"
  else
    phase=not_started
  fi
  avg=0
  eta=0
  if (( completed > 0 && elapsed > 0 )); then
    avg=$((elapsed/completed))
    eta=$((avg*(TOTAL_ATTEMPTS-completed)))
    (( eta < 0 )) && eta=0
  fi
  next=0
  if [[ "$phase" == running ]]; then
    if (( elapsed < FIRST_CHECK_SECONDS )); then
      next=$((FIRST_CHECK_SECONDS-elapsed))
    elif (( eta > 0 )); then
      next=$eta
      (( next < MIN_FOLLOWUP_SECONDS )) && next=$MIN_FOLLOWUP_SECONDS
      (( next > MAX_FOLLOWUP_SECONDS )) && next=$MAX_FOLLOWUP_SECONDS
    else
      next=$MIN_FOLLOWUP_SECONDS
    fi
  fi
  jq -n \
    --arg phase "$phase" --arg output "$output" --arg failure "$failure" --arg warning "$warning" --argjson running "$running" \
    --argjson completed "$completed" --argjson failed "$failed" --argjson elapsed "$elapsed" \
    --argjson avg "$avg" --argjson eta "$eta" --argjson next "$next" --arg exit "$exit_code" \
    '{phase:$phase,running:$running,completed_attempts:$completed,total_attempts:21,
      failed_attempts:$failed,elapsed_seconds:$elapsed,average_attempt_seconds:$avg,
      estimated_remaining_seconds:$eta,next_recommended_check_seconds:$next,
      exit_code:(if $exit=="" then null else ($exit|tonumber) end),
      failure:(if $failure=="" then null else $failure end),
      warning:(if $warning=="" then null else $warning end),output:$output}'
}

status() {
  local json
  json=$(status_json)
  if [[ ${1:-} == --json ]]; then
    printf '%s\n' "$json"
  else
    jq -r '"phase=\(.phase) attempts=\(.completed_attempts)/\(.total_attempts) failed=\(.failed_attempts) elapsed_s=\(.elapsed_seconds) eta_s=\(.estimated_remaining_seconds) output=\(.output)"' <<<"$json"
  fi
}

case ${1:-} in
  start)
    shift
    [[ $# == 0 ]] || die "start takes no positional arguments"
    start
    ;;
  status)
    shift
    [[ $# == 0 || $# == 1 && "$1" == "--json" ]] || die "status accepts only --json"
    status "${1:-}"
    ;;
  _start-locked)
    [[ $# == 9 ]] || die "invalid locked start invocation"
    start_locked "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9"
    ;;
  _run-full)
    [[ $# == 5 ]] || die "invalid internal invocation"
    run_full_background "$2" "$3" "$4" "$5"
    ;;
  -h|--help|help|'')
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
