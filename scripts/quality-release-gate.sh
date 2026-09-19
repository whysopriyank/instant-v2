#!/usr/bin/env bash
# DEC-001 single-node-alpha release gate. It validates evidence; it never provisions it.
set -euo pipefail

die() { echo "release-gate: $*" >&2; exit 2; }

for tool in git jq shasum make; do
  command -v "$tool" >/dev/null || die "$tool is required"
done

[[ -z ${RELEASE_GATE_TESTING:-}${RELEASE_GATE_REPO_ROOT:-}${RELEASE_GATE_NOW_UTC:-} ]] || die "test seams are not accepted by the production gate"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
manifest=${RELEASE_GATE_MANIFEST:-}
candidate=${RELEASE_CANDIDATE_SHA:-}
campaign=${RELEASE_CAMPAIGN_ID:-}
database_url=${DATABASE_URL:-}

[[ $candidate =~ ^[0-9a-f]{40}$ ]] || die "RELEASE_CANDIDATE_SHA must be 40 lowercase hex characters"
[[ -n $campaign ]] || die "RELEASE_CAMPAIGN_ID is required"
[[ -n $database_url ]] || die "DATABASE_URL is required for the owned-DB lane"
[[ -n $manifest && -f $manifest && ! -L $manifest ]] || die "RELEASE_GATE_MANIFEST must be a regular non-symlink file"
source_manifest=$(cd "$(dirname "$manifest")" && pwd -P)/$(basename "$manifest")
source_root=$(dirname "$source_manifest")
snapshot=$(mktemp -d "${TMPDIR:-/tmp}/instant-release-evidence.XXXXXX")
chmod 700 "$snapshot"
trap 'rm -rf "$snapshot"' EXIT
cp -R "$source_root/." "$snapshot/"
[[ -z $(find -P "$snapshot" -type l -print -quit) ]] || die "release evidence may not contain symlinks"
manifest="$snapshot/$(basename "$source_manifest")"
manifest_root=$snapshot

head_sha=$(git -C "$repo_root" rev-parse HEAD) || die "cannot resolve candidate HEAD"
[[ $head_sha == "$candidate" ]] || die "candidate SHA mismatch"
[[ -z $(git -C "$repo_root" status --porcelain --untracked-files=all) ]] || die "candidate tree is dirty"

expected_decision=DEC-001-single-node-alpha-20260905
expected_profile=single-node-alpha
expected_lanes='{"artifact":"validate","container":"not_selected","corpus":"run","external_v1":"not_selected","hermetic":"run","owned_db":"run","performance":"not_selected","recovery":"artifact","soak":"artifact"}'
expected_packets='["CF-002","CF-003","DA-001","DA-002","DA-003","DA-004","DA-004V","DA-005","DA-006A","DA-007","DA-008A","EV-001","EV-002","EV-003","EV-004","EV-005","EV-006","F-001","F-002","FR-001","OP-003","OP-005","QR-001","QR-003","QR-005","RT-001","RT-002","RT-003"]'

jq -e --arg sha "$candidate" --arg campaign "$campaign" --arg decision "$expected_decision" \
  --arg profile "$expected_profile" --argjson lanes "$expected_lanes" --argjson packets "$expected_packets" '
  def text: type == "string" and length > 0;
  def hex64: type == "string" and test("^[0-9a-f]{64}$");
  def uint: type == "number" and . >= 0 and floor == .;
  def artifact:
    type == "object" and keys == ["path","sha256","size_bytes"] and
    (.path|text) and (.sha256|hex64) and (.size_bytes|uint) and .size_bytes > 0;
  keys == ["campaign_id","campaign_max_age_seconds","campaign_started_at","candidate","decision_id","external_records","handoffs","lanes","profile","provider_evidence","schema_version"] and
  .schema_version == 1 and .decision_id == $decision and .profile == $profile and
  .campaign_id == $campaign and .provider_evidence == "not_selected" and
  (.campaign_started_at|text) and (.campaign_max_age_seconds|uint) and .campaign_max_age_seconds > 0 and .campaign_max_age_seconds <= 604800 and
  (.candidate | type == "object" and keys == ["binary","configuration","endpoint_sha256","sha"]) and
  .candidate.sha == $sha and (.candidate.endpoint_sha256|hex64) and
  (.candidate.binary|artifact) and (.candidate.configuration|artifact) and
  .lanes == $lanes and
  (.external_records | keys == ["native_linux","recovery","soak"]) and
  (.handoffs|type == "array") and
  all(.handoffs[];
    type == "object" and keys == ["campaign_id","candidate_sha","finished_at","packet","path","sha256","size_bytes","state"] and
    (.packet|text) and (.state|text) and (.campaign_id|text) and (.candidate_sha|text) and (.finished_at|text) and
    ({path:.path,size_bytes:.size_bytes,sha256:.sha256}|artifact)) and
  ([.handoffs[].packet] | length == (unique | length)) and
  ([.handoffs[].packet] | sort) == $packets
  ' "$manifest" >/dev/null || die "manifest schema, selection, identity, or handoff inventory is invalid"

campaign_epoch=$(jq -er '.campaign_started_at | fromdateiso8601' "$manifest") || die "campaign_started_at must be RFC3339 UTC"
commit_epoch=$(git -C "$repo_root" show -s --format=%ct "$candidate") || die "cannot read candidate commit time"
now_epoch=$(date -u +%s)
max_age=$(jq -r '.campaign_max_age_seconds' "$manifest")
(( campaign_epoch >= commit_epoch && campaign_epoch <= now_epoch && now_epoch - campaign_epoch <= max_age )) || die "campaign timestamp is stale or in the future"

binary_sha=$(jq -r '.candidate.binary.sha256' "$manifest")
config_sha=$(jq -r '.candidate.configuration.sha256' "$manifest")
endpoint_sha=$(jq -r '.candidate.endpoint_sha256' "$manifest")
evidence_paths=()

safe_relative_file() {
  local rel=$1 base=$2 full part current
  [[ -n $rel && $rel != /* && $rel != *'..'* && ! $rel =~ [[:cntrl:]] ]] || return 1
  full="$base/$rel"
  [[ -f $full && ! -L $full ]] || return 1
  current=$base
  IFS='/' read -r -a parts <<< "$rel"
  for part in "${parts[@]}"; do
    [[ -n $part && $part != . && $part != .. ]] || return 1
    current="$current/$part"
    [[ ! -L $current ]] || return 1
  done
}

verify_artifact() {
  local owner=$1 rel=$2 size=$3 sha=$4 full
  safe_relative_file "$rel" "$manifest_root" || die "$owner artifact path is unsafe or missing: $rel"
  full="$manifest_root/$rel"
  [[ $size =~ ^[1-9][0-9]*$ && $(wc -c < "$full" | tr -d ' ') == "$size" ]] || die "$owner artifact size mismatch: $rel"
  [[ $sha =~ ^[0-9a-f]{64}$ && $(shasum -a 256 "$full" | awk '{print $1}') == "$sha" ]] || die "$owner artifact hash mismatch: $rel"
  evidence_paths+=("$rel")
}

verify_artifact candidate-binary "$(jq -r '.candidate.binary.path' "$manifest")" "$(jq -r '.candidate.binary.size_bytes' "$manifest")" "$binary_sha"
verify_artifact candidate-configuration "$(jq -r '.candidate.configuration.path' "$manifest")" "$(jq -r '.candidate.configuration.size_bytes' "$manifest")" "$config_sha"

for packet in $(jq -r '.handoffs[].packet' "$manifest"); do
  row=$(jq -c --arg packet "$packet" '.handoffs[] | select(.packet==$packet)' "$manifest")
  state=$(jq -r '.state' <<<"$row")
  [[ $state == GREEN || ( $packet == DA-004V && $state == ACCEPTED_EXCEPTION ) ]] || die "$packet handoff is not accepted"
  [[ $(jq -r '.candidate_sha' <<<"$row") == "$candidate" ]] || die "$packet handoff candidate mismatch"
  [[ $(jq -r '.campaign_id' <<<"$row") == "$campaign" ]] || die "$packet handoff campaign mismatch"
  handoff_epoch=$(jq -er '.finished_at|fromdateiso8601' <<<"$row") || die "$packet handoff timestamp is invalid"
  (( handoff_epoch >= campaign_epoch && handoff_epoch <= now_epoch )) || die "$packet handoff timestamp is stale or future"
  verify_artifact "handoff $packet" "$(jq -r '.path' <<<"$row")" "$(jq -r '.size_bytes' <<<"$row")" "$(jq -r '.sha256' <<<"$row")"
done

verified_record=
verify_record() {
  local name=$1 packet=$2 rel record started finished rows
  rel=$(jq -er --arg name "$name" '.external_records[$name]' "$manifest") || die "$name record is missing"
  safe_relative_file "$rel" "$manifest_root" || die "$name record path is unsafe or missing"
  record="$manifest_root/$rel"
  evidence_paths+=("$rel")
  jq -e --arg packet "$packet" --arg campaign "$campaign" --arg sha "$candidate" --arg binary "$binary_sha" --arg config "$config_sha" --arg endpoint "$endpoint_sha" '
    def text: type == "string" and length > 0;
    def hex64: type == "string" and test("^[0-9a-f]{64}$");
    def uint: type == "number" and . >= 0 and floor == .;
    def artifact: type == "object" and keys == ["path","sha256","size_bytes"] and (.path|text) and (.sha256|hex64) and (.size_bytes|uint) and .size_bytes > 0;
    keys == ["artifacts","binary_sha256","campaign_id","candidate_sha","cleanup","configuration_sha256","details","endpoint_sha256","finished_at","fixture_id","host","packet","result","schema_version","selected_count","skipped_count","started_at"] and
    .schema_version == 1 and .packet == $packet and .campaign_id == $campaign and
    .candidate_sha == $sha and .binary_sha256 == $binary and .configuration_sha256 == $config and
    .endpoint_sha256 == $endpoint and (.fixture_id|text) and (.started_at|text) and (.finished_at|text) and
    .result == "PASS" and (.selected_count|uint) and .selected_count > 0 and (.skipped_count|uint) and .skipped_count == 0 and .cleanup == "complete" and
    (.host | type == "object" and keys == ["arch","id","kernel","os","runtime"]) and
    (.host.id|text) and (.host.os|text) and (.host.kernel|text) and (.host.arch|text) and (.host.runtime|text) and
    (.artifacts|type == "array") and (.artifacts|length>0) and all(.artifacts[]; artifact)
  ' "$record" >/dev/null || die "$name record failed its common contract"
  started=$(jq -er '.started_at|fromdateiso8601' "$record") || die "$name started_at is invalid"
  finished=$(jq -er '.finished_at|fromdateiso8601' "$record") || die "$name finished_at is invalid"
  (( started >= campaign_epoch && finished >= started && finished <= now_epoch )) || die "$name record timestamps are stale, reversed, or future"
  rows=$(jq -er '.artifacts|map([.path,.size_bytes,.sha256]|@tsv)|join("\n")' "$record") || die "$name artifact inventory is invalid"
  while IFS=$'\t' read -r path size sha; do verify_artifact "$name" "$path" "$size" "$sha"; done <<<"$rows"
  verified_record=$record
}

verify_record native_linux OP-003
linux_record=$verified_record
jq -e '.host.os == "linux" and (.details | type == "object" and keys == ["native","platform_checks"] and .native == true and (.platform_checks|type=="number" and floor==. and .>0))' "$linux_record" >/dev/null || die "native Linux evidence is insufficient"

verify_record recovery OP-005
recovery_record=$verified_record
jq -e '
  .details | type == "object" and keys == ["max_drain_seconds","max_rto_seconds","outcomes"] and
  (.max_rto_seconds|type=="number" and floor==. and .>=0 and .<=3600) and
  (.max_drain_seconds|type=="number" and floor==. and .>=0 and .<=30) and
  (.outcomes|type=="array" and length==7) and all(.outcomes[]; type=="object" and keys==["id","result"] and (.id|type=="string" and length>0) and .result=="PASS") and
  ([.outcomes[].id] | sort) == ["crash-after-commit","crash-before-commit","crash-during-publication","drain-idle","drain-moderate","drain-saturated","postgres-restart"] and
  all(.outcomes[]; .result == "PASS")
' "$recovery_record" >/dev/null || die "recovery evidence is insufficient"

verify_record soak QR-001
soak_record=$verified_record
jq -e '
  def uint: type=="number" and floor==. and .>=0;
  .details | type == "object" and keys == ["acknowledged_transactions","active_seconds","committed_transactions","dropped_transactions","refreshed_transactions","sessions","unresolved_transactions"] and
  all(.[]; uint) and .sessions >= 500 and .active_seconds >= 900 and
  .committed_transactions > 0 and
  .acknowledged_transactions == .committed_transactions and
  .refreshed_transactions == .committed_transactions and
  .dropped_transactions == 0 and .unresolved_transactions == 0
' "$soak_record" >/dev/null || die "qualified soak evidence is insufficient"

run_target() {
  local target=$1
  make -C "$repo_root" "$target"
}

# DEC-001 lane selections (mirrors Makefile; Makefile itself is coordinator-owned):
#  test-unit:        INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./... -race -count=1 -short (hermetic)
#  bench-acceptance: go test <BENCH_PACKAGES> -race -count=1 -short -p 1 (hermetic)
#  test-integration: INSTANT_TEST_INTEGRATION=1 go test ./... -race -count=1 (owned DB, no -short)
#  test-contract:    corpus validate + short subset (hermetic part) + INSTANT_TEST_INTEGRATION=1 go test ./internal/corpus -run '^TestCorpusReplayIntegration$' (owned DB)
# Outside-lane skips allowed only in hermetic parts (test-unit, bench-acceptance,
# and the short subset merged into test-contract):
#  - "integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL" (testkit.NewPostgres gate)
#  - "integration test: set DATABASE_URL" (permission-eval gate)
#  - "in short mode" (testing.Short gate: memory-cap, reporting)
# Any other skip is a skipped REQUIRED test and fails closed. Owned-DB lanes
# (test-integration fully; test-contract for its required integration test)
# allow no outside-lane skips.
# test-contract required identities (TestCorpusReplayIntegration parent and
# TestCorpusReplayIntegration/<scenario> subtests) fail closed on any final
# skip regardless of the allowlisted substrings above: the short subset runs
# with -short where replay subtests skip, so only the owned-DB run producing
# scenario passes proves the required execution.
lane_disallowed_skips() {
  local log=$1
  jq -Rsr '
    [split("\n")[] | fromjson? | select((.Test? // "") != "")]
    | group_by(.Package, .Test)
    | map({final: (.[-1].Action), outputs: ([.[].Output? // empty] | join(""))})
    | map(select(.final == "skip"))
    | map(select(
        (.outputs | contains("integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL") | not) and
        (.outputs | contains("integration test: set DATABASE_URL") | not) and
        (.outputs | contains("in short mode") | not)
      ))
    | length
  ' "$log"
}

lane_required_contract_parent_passed() {
  local log=$1
  jq -Rsr '[split("\n")[] | fromjson? | select((.Test? // "") != "")] | group_by(.Package,.Test) | map(.[-1]) | map(select(.Action=="pass" and .Package=="github.com/instant-v2/instant-v2/internal/corpus" and .Test=="TestCorpusReplayIntegration")) | length' "$log"
}

lane_required_contract_scenario_passed() {
  local log=$1
  jq -Rsr '[split("\n")[] | fromjson? | select((.Test? // "") != "")] | group_by(.Package,.Test) | map(.[-1]) | map(select(.Action=="pass" and .Package=="github.com/instant-v2/instant-v2/internal/corpus" and (.Test | test("^TestCorpusReplayIntegration/")))) | length' "$log"
}

# Required identities fail closed even when their skip output carries an
# otherwise allowlisted outside-lane substring. All three contract-identity
# helpers are scoped to the exact required package
# (github.com/instant-v2/instant-v2/internal/corpus): a same-named parent or
# scenario passing in any other package is a decoy and must not satisfy the
# owned-DB execution proof.
lane_required_contract_skipped() {
  local log=$1
  jq -Rsr '[split("\n")[] | fromjson? | select((.Test? // "") != "")] | group_by(.Package,.Test) | map(.[-1]) | map(select(.Action=="skip" and .Package=="github.com/instant-v2/instant-v2/internal/corpus" and (.Test | test("^TestCorpusReplayIntegration")))) | length' "$log"
}

run_test_target() {
  local target=$1 log status selected skipped disallowed required_parent required_scenario required_skipped
  log=$(mktemp "${TMPDIR:-/tmp}/instant-release-${target}.XXXXXX")
  status=0
  GOFLAGS=-json make -C "$repo_root" "$target" >"$log" 2>&1 || status=$?
  cat "$log"
  (( status == 0 )) || { rm -f "$log"; return "$status"; }
  selected=$(jq -Rsr '[split("\n")[] | fromjson? | select((.Test? // "") != "")] | group_by(.Package,.Test) | map(.[-1]) | map(select(.Action=="pass")) | length' "$log")
  skipped=$(jq -Rsr '[split("\n")[] | fromjson? | select((.Test? // "") != "")] | group_by(.Package,.Test) | map(.[-1]) | map(select(.Action=="skip")) | length' "$log")
  case "$target" in
    test-unit|bench-acceptance)
      disallowed=$(lane_disallowed_skips "$log")
      rm -f "$log"
      (( selected > 0 )) || die "$target selected zero tests"
      (( disallowed == 0 )) || die "$target skipped required tests"
      ;;
    test-integration)
      rm -f "$log"
      (( skipped == 0 )) || die "$target skipped selected tests"
      (( selected > 0 )) || die "$target selected zero tests"
      ;;
    test-contract)
      disallowed=$(lane_disallowed_skips "$log")
      required_parent=$(lane_required_contract_parent_passed "$log")
      required_scenario=$(lane_required_contract_scenario_passed "$log")
      required_skipped=$(lane_required_contract_skipped "$log")
      rm -f "$log"
      (( selected > 0 )) || die "$target selected zero tests"
      (( required_parent > 0 )) || die "$target did not select required TestCorpusReplayIntegration"
      (( required_scenario > 0 )) || die "$target missing required owned-DB TestCorpusReplayIntegration execution"
      (( required_skipped == 0 )) || die "$target skipped required tests"
      (( disallowed == 0 )) || die "$target skipped required tests"
      ;;
    *)
      rm -f "$log"
      die "unknown test lane: $target"
      ;;
  esac
}

run_target validate-release
run_target lint
run_target vet
run_target check-generated
run_target build
[[ -f "$repo_root/bin/instantd" && $(shasum -a 256 "$repo_root/bin/instantd" | awk '{print $1}') == "$binary_sha" ]] || die "built candidate binary does not match qualified evidence"
run_test_target test-unit
run_test_target bench-acceptance
run_test_target test-integration
run_test_target test-contract

[[ $(git -C "$repo_root" rev-parse HEAD) == "$candidate" ]] || die "candidate changed during release gate"
[[ -z $(git -C "$repo_root" status --porcelain --untracked-files=all) ]] || die "candidate tree changed during release gate"
for rel in "${evidence_paths[@]}"; do
  safe_relative_file "$rel" "$source_root" || die "source evidence changed or became unsafe: $rel"
  cmp "$source_root/$rel" "$snapshot/$rel" >/dev/null || die "source evidence changed during release gate: $rel"
done
cmp "$source_manifest" "$manifest" >/dev/null || die "release manifest changed during release gate"
echo "release-gate: selected single-node-alpha checks passed for $candidate"
