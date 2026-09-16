#!/usr/bin/env bash
# scripts/soak-process-identity.sh
# EV-003: Process identity and provenance binding helper for quality-soak.
# Binds process samples to expected executable, candidate SHA/digest,
# configuration digest, endpoint, PID, and trustworthy start instance.
set -euo pipefail

soak_platform_check() {
  local os_type="${SOAK_OS_OVERRIDE:-$(uname -s)}"
  case "$os_type" in
    Darwin|Linux)
      return 0
      ;;
    *)
      echo "quality-soak: process identity unsupported on platform '$os_type'; failing closed" >&2
      return 1
      ;;
  esac
}

soak_hash_file() {
  local path="$1"
  if [[ -n "${SOAK_SEAM_FILE_HASH:-}" ]]; then
    echo "$SOAK_SEAM_FILE_HASH"
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" 2>/dev/null | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" 2>/dev/null | awk '{print $1}'
  else
    echo "quality-soak: untrustworthy environment: no sha256 tool available" >&2
    return 1
  fi
}

soak_hash_string() {
  local str="$1"
  if command -v shasum >/dev/null 2>&1; then
    printf "%s" "$str" | shasum -a 256 | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    printf "%s" "$str" | sha256sum | awk '{print $1}'
  else
    echo "quality-soak: untrustworthy environment: no sha256 tool available" >&2
    return 1
  fi
}

soak_canonical_path() {
  local p="$1"
  if command -v realpath >/dev/null 2>&1; then
    realpath "$p" 2>/dev/null || true
  elif command -v readlink >/dev/null 2>&1; then
    readlink -f "$p" 2>/dev/null || true
  else
    local dir base
    dir="$(cd "$(dirname "$p")" 2>/dev/null && pwd || true)"
    base="$(basename "$p")"
    if [[ -n "$dir" ]]; then
      echo "$dir/$base"
    fi
  fi
}

soak_get_process_start_instance() {
  local pid="$1"
  if [[ -n "${SOAK_SEAM_FORCE_UNTRUSTWORTHY:-}" ]]; then
    echo "quality-soak: untrustworthy start instance evidence forced by seam" >&2
    return 1
  fi
  if [[ -n "${SOAK_SEAM_START_INSTANCE:-}" ]]; then
    echo "$SOAK_SEAM_START_INSTANCE"
    return 0
  fi
  local os_type="${SOAK_OS_OVERRIDE:-$(uname -s)}"
  local token=""
  if [[ "$os_type" == "Linux" && -f "/proc/$pid/stat" ]]; then
    local stat_line
    stat_line="$(cat "/proc/$pid/stat" 2>/dev/null || true)"
    if [[ -n "$stat_line" ]]; then
      local suffix="${stat_line##*) }"
      token="$(awk '{print $20}' <<< "$suffix")"
    fi
  fi
  if [[ -z "$token" ]]; then
    token="$(ps -p "$pid" -o lstart= 2>/dev/null | tr -s ' ' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' || true)"
  fi
  if [[ -z "$token" ]]; then
    echo "quality-soak: cannot read trustworthy start instance for PID $pid (stale or unreadable)" >&2
    return 1
  fi
  echo "$token"
}

soak_get_process_executable() {
  local pid="$1"
  if [[ -n "${SOAK_SEAM_FORCE_UNTRUSTWORTHY:-}" ]]; then
    echo "quality-soak: untrustworthy executable evidence forced by seam" >&2
    return 1
  fi
  if [[ -n "${SOAK_SEAM_PROCESS_EXE:-}" ]]; then
    echo "$SOAK_SEAM_PROCESS_EXE"
    return 0
  fi
  local os_type="${SOAK_OS_OVERRIDE:-$(uname -s)}"
  local exe=""
  if [[ "$os_type" == "Linux" && -e "/proc/$pid/exe" ]]; then
    exe="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
    if [[ "$exe" == *"(deleted)"* ]]; then
      echo "quality-soak: process $pid executable was unlinked/replaced on disk: $exe" >&2
      return 1
    fi
  fi
  if [[ -z "$exe" ]]; then
    if command -v lsof >/dev/null 2>&1; then
      exe="$(lsof -p "$pid" -a -d txt -Fn 2>/dev/null | sed -n 's/^n//p' | grep -v -E '(dyld|dylib)$' | head -n 1 || true)"
    fi
  fi
  if [[ -z "$exe" ]]; then
    echo "quality-soak: cannot determine trustworthy executable path for PID $pid" >&2
    return 1
  fi
  local canonical
  canonical="$(soak_canonical_path "$exe")"
  if [[ -z "$canonical" ]]; then
    canonical="$exe"
  fi
  echo "$canonical"
}

soak_parse_url_endpoint() {
  local url="$1"
  local without_proto="${url#*://}"
  local hostport="${without_proto%%/*}"
  echo "$hostport"
}

soak_verify_endpoint() {
  local pid="$1"
  local expected_endpoint="$2"
  if [[ -n "${SOAK_SEAM_FORCE_UNTRUSTWORTHY:-}" ]]; then
    echo "quality-soak: untrustworthy endpoint evidence forced by seam" >&2
    return 1
  fi
  local expected_host="${expected_endpoint%:*}"
  local expected_port="${expected_endpoint##*:}"
  if [[ -z "$expected_port" || "$expected_port" == "$expected_endpoint" || -z "$expected_host" ]]; then
    echo "quality-soak: endpoint mismatch: expected endpoint must include host and port: $expected_endpoint" >&2
    return 1
  fi

  local listening_endpoints=""
  if [[ -n "${SOAK_SEAM_PROCESS_ENDPOINTS:-}" ]]; then
    listening_endpoints="$SOAK_SEAM_PROCESS_ENDPOINTS"
  elif [[ -n "${SOAK_SEAM_PROCESS_PORTS:-}" ]]; then
    # Legacy test seam: a port-only observation is treated as an all-address
    # listener, never as proof of a specific host binding.
    for port in $SOAK_SEAM_PROCESS_PORTS; do
      listening_endpoints+=" *:${port}"
    done
  fi
  if [[ -z "$listening_endpoints" ]] && command -v lsof >/dev/null 2>&1; then
    listening_endpoints="$(lsof -n -P -a -p "$pid" -iTCP -sTCP:LISTEN -Fn 2>/dev/null | sed -n 's/^n//p' | tr '\n' ' ')"
  fi
  if [[ -z "$listening_endpoints" ]]; then
    echo "quality-soak: endpoint mismatch: process $pid has no active TCP listening ports for expected $expected_endpoint" >&2
    return 1
  fi
  for actual in $listening_endpoints; do
    local actual_host="${actual%:*}"
    local actual_port="${actual##*:}"
    if [[ "$actual_port" != "$expected_port" ]]; then
      continue
    fi
    case "$actual_host" in
      '*'|0.0.0.0|'[::]'|::)
      return 0
        ;;
      "$expected_host")
        return 0
        ;;
    esac
  done
  echo "quality-soak: endpoint mismatch: process $pid is listening on [${listening_endpoints}], expected endpoint $expected_endpoint" >&2
  return 1
}

soak_compute_config_digest() {
  local pid="$1"
  local endpoint="$2"
  if [[ -n "${SOAK_SEAM_CONFIG_DIGEST:-}" ]]; then
    echo "$SOAK_SEAM_CONFIG_DIGEST"
    return 0
  fi
  local cmdline=""
  local os_type="${SOAK_OS_OVERRIDE:-$(uname -s)}"
  if [[ "$os_type" == "Linux" && -f "/proc/$pid/cmdline" ]]; then
    cmdline="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
  fi
  if [[ -z "$cmdline" ]]; then
    cmdline="$(ps -p "$pid" -o command= 2>/dev/null | tr -s ' ' || true)"
  fi
  if [[ -z "$cmdline" ]]; then
    echo "quality-soak: cannot read command line for PID $pid" >&2
    return 1
  fi
  local raw_config="endpoint=${endpoint}|cmdline=${cmdline}"
  soak_hash_string "$raw_config"
}

soak_sample_process_identity() {
  local pid="$1"
  local expected_bin="$2"
  local expected_sha="$3"
  local expected_endpoint="$4"
  local expected_config_digest="$5"
  local sample_phase="${6:-sample}"

  # 1. Platform support check
  soak_platform_check || return 1

  # 2. Verify PID is numeric and alive
  case "$pid" in
    *[!0-9]*|'')
      echo "quality-soak: SOAK_SERVER_PID must be a positive PID" >&2
      return 2
      ;;
  esac
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "quality-soak: process PID $pid is not running (stale PID)" >&2
    return 1
  fi

  # 3. Kernel start instance
  local start_instance
  start_instance="$(soak_get_process_start_instance "$pid")" || return 1

  # Check seam for injected replacement
  if [[ "$sample_phase" == "post_soak" && -n "${SOAK_SEAM_INJECT_REPLACEMENT:-}" ]]; then
    start_instance="injected-replacement-$RANDOM"
  fi

  # 4. Executable binding
  local running_exe
  running_exe="$(soak_get_process_executable "$pid")" || return 1

  local expected_exe_canonical
  expected_exe_canonical="$(soak_canonical_path "$expected_bin")"
  if [[ -z "$expected_exe_canonical" ]]; then
    expected_exe_canonical="$expected_bin"
  fi

  if [[ "$running_exe" != "$expected_exe_canonical" ]]; then
    echo "quality-soak: executable mismatch for PID $pid: expected '$expected_exe_canonical', got '$running_exe'" >&2
    return 1
  fi

  # 5. Candidate SHA / digest
  local actual_sha
  actual_sha="$(soak_hash_file "$running_exe")" || return 1
  if [[ -n "$expected_sha" && "$actual_sha" != "$expected_sha" ]]; then
    echo "quality-soak: candidate SHA mismatch for PID $pid: expected '$expected_sha', got '$actual_sha'" >&2
    return 1
  fi

  # 6. Endpoint binding
  soak_verify_endpoint "$pid" "$expected_endpoint" || return 1

  # 7. Configuration digest binding
  local actual_config_digest
  actual_config_digest="$(soak_compute_config_digest "$pid" "$expected_endpoint")" || return 1
  if [[ -n "$expected_config_digest" && "$actual_config_digest" != "$expected_config_digest" ]]; then
    echo "quality-soak: configuration digest mismatch for PID $pid: expected '$expected_config_digest', got '$actual_config_digest'" >&2
    return 1
  fi

  echo "quality-soak: bound process sample for PID $pid [start: $start_instance, exe: $running_exe, sha: $actual_sha, endpoint: $expected_endpoint, config: $actual_config_digest]" >&2
  echo "pid=${pid}|start=${start_instance}|exe=${running_exe}|sha=${actual_sha}|endpoint=${expected_endpoint}|config=${actual_config_digest}"
}

soak_verify_process_sample() {
  local initial="$1"
  local current="$2"

  if [[ "$initial" == "$current" ]]; then
    return 0
  fi

  # Identify exact difference for explicit diagnosis
  local init_pid init_start init_exe init_sha init_endpoint init_config
  init_pid="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^pid=/) print substr($i,5)}' <<< "$initial")"
  init_start="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^start=/) print substr($i,7)}' <<< "$initial")"
  init_exe="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^exe=/) print substr($i,5)}' <<< "$initial")"
  init_sha="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^sha=/) print substr($i,5)}' <<< "$initial")"
  init_endpoint="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^endpoint=/) print substr($i,10)}' <<< "$initial")"
  init_config="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^config=/) print substr($i,8)}' <<< "$initial")"

  local curr_pid curr_start curr_exe curr_sha curr_endpoint curr_config
  curr_pid="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^pid=/) print substr($i,5)}' <<< "$current")"
  curr_start="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^start=/) print substr($i,7)}' <<< "$current")"
  curr_exe="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^exe=/) print substr($i,5)}' <<< "$current")"
  curr_sha="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^sha=/) print substr($i,5)}' <<< "$current")"
  curr_endpoint="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^endpoint=/) print substr($i,10)}' <<< "$current")"
  curr_config="$(awk -F'|' '{for(i=1;i<=NF;i++) if($i~/^config=/) print substr($i,8)}' <<< "$current")"

  if [[ "$init_pid" != "$curr_pid" ]]; then
    echo "quality-soak: process replacement detected: PID changed from $init_pid to $curr_pid" >&2
  elif [[ "$init_start" != "$curr_start" ]]; then
    echo "quality-soak: process replacement detected: start instance changed from '$init_start' to '$curr_start'" >&2
  elif [[ "$init_exe" != "$curr_exe" ]]; then
    echo "quality-soak: process replacement detected: executable changed from '$init_exe' to '$curr_exe'" >&2
  elif [[ "$init_sha" != "$curr_sha" ]]; then
    echo "quality-soak: process replacement detected: candidate SHA changed from '$init_sha' to '$curr_sha'" >&2
  elif [[ "$init_endpoint" != "$curr_endpoint" ]]; then
    echo "quality-soak: process replacement detected: endpoint changed from '$init_endpoint' to '$curr_endpoint'" >&2
  elif [[ "$init_config" != "$curr_config" ]]; then
    echo "quality-soak: process replacement detected: config digest changed from '$init_config' to '$curr_config'" >&2
  else
    echo "quality-soak: process replacement detected: process sample mismatch" >&2
  fi
  return 1
}
