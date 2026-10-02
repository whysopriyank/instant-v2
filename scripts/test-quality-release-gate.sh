#!/usr/bin/env bash
# Hermetic contract checks for QR-003 preflight and failure propagation.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source_gate="$script_dir/quality-release-gate.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

repo="$tmp/repo"
evidence="$tmp/evidence"
fakebin="$tmp/bin"
mkdir -p "$repo/scripts" "$repo/bin" "$evidence/records" "$evidence/handoffs" "$evidence/raw" "$evidence/candidate" "$fakebin"
cp "$source_gate" "$repo/scripts/quality-release-gate.sh"
gate="$repo/scripts/quality-release-gate.sh"
git -C "$repo" init -q
git -C "$repo" config user.email test@example.invalid
git -C "$repo" config user.name test
printf 'candidate\n' >"$repo/README"
printf 'bin/\n' >"$repo/.gitignore"
git -C "$repo" add README .gitignore scripts/quality-release-gate.sh
GIT_AUTHOR_DATE=2026-01-01T00:00:00Z GIT_COMMITTER_DATE=2026-01-01T00:00:00Z git -C "$repo" commit -qm initial
sha=$(git -C "$repo" rev-parse HEAD)
hex=$(printf x | shasum -a 256 | awk '{print $1}')
campaign_started=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
printf raw >"$evidence/raw/result"
raw_size=$(wc -c <"$evidence/raw/result" | tr -d ' ')
raw_sha=$(shasum -a 256 "$evidence/raw/result" | awk '{print $1}')
printf candidate-binary >"$evidence/candidate/instantd"
printf canonical-config >"$evidence/candidate/config.json"
binary_size=$(wc -c <"$evidence/candidate/instantd" | tr -d ' ')
binary_sha=$(shasum -a 256 "$evidence/candidate/instantd" | awk '{print $1}')
config_size=$(wc -c <"$evidence/candidate/config.json" | tr -d ' ')
config_sha=$(shasum -a 256 "$evidence/candidate/config.json" | awk '{print $1}')

packets=(CF-002 CF-003 DA-001 DA-002 DA-003 DA-004 DA-004V DA-005 DA-006A DA-007 DA-008A EV-001 EV-002 EV-003 EV-004 EV-005 EV-006 F-001 F-002 FR-001 OP-003 OP-005 QR-001 QR-003 QR-005 RT-001 RT-002 RT-003)
handoffs='[]'
for packet in "${packets[@]}"; do
  path="handoffs/$packet"
  printf '%s\n' "$packet" >"$evidence/$path"
  size=$(wc -c <"$evidence/$path" | tr -d ' ')
  digest=$(shasum -a 256 "$evidence/$path" | awk '{print $1}')
  handoffs=$(jq -c --arg p "$packet" --arg sha "$sha" --arg path "$path" --arg digest "$digest" --arg at "$campaign_started" --argjson size "$size" '. + [{packet:$p,state:(if $p=="DA-004V" then "ACCEPTED_EXCEPTION" else "GREEN" end),campaign_id:"campaign-1",candidate_sha:$sha,finished_at:$at,path:$path,size_bytes:$size,sha256:$digest}]' <<<"$handoffs")
done

record() {
  local name=$1 packet=$2 details=$3
  jq -n --arg packet "$packet" --arg sha "$sha" --arg binary "$binary_sha" --arg config "$config_sha" --arg endpoint "$hex" --arg at "$campaign_started" --argjson details "$details" --argjson size "$raw_size" --arg rawsha "$raw_sha" '{schema_version:1,packet:$packet,campaign_id:"campaign-1",candidate_sha:$sha,binary_sha256:$binary,configuration_sha256:$config,endpoint_sha256:$endpoint,fixture_id:"fixture-1",started_at:$at,finished_at:$at,host:{id:"host-1",os:(if $packet=="OP-003" then "linux" else "test" end),kernel:"kernel",arch:"amd64",runtime:"go"},result:"PASS",selected_count:1,skipped_count:0,cleanup:"complete",artifacts:[{path:"raw/result",size_bytes:$size,sha256:$rawsha}],details:$details}' >"$evidence/records/$name.json"
}
record native-linux OP-003 '{"native":true,"platform_checks":1}'
record soak QR-001 '{"sessions":500,"active_seconds":900,"committed_transactions":1,"acknowledged_transactions":1,"refreshed_transactions":1,"dropped_transactions":0,"unresolved_transactions":0}'
record recovery OP-005 '{"max_rto_seconds":3600,"max_drain_seconds":30,"outcomes":[{"id":"crash-after-commit","result":"PASS"},{"id":"crash-before-commit","result":"PASS"},{"id":"crash-during-publication","result":"PASS"},{"id":"drain-idle","result":"PASS"},{"id":"drain-moderate","result":"PASS"},{"id":"drain-saturated","result":"PASS"},{"id":"postgres-restart","result":"PASS"}]}'

jq -n --arg sha "$sha" --arg endpoint "$hex" --arg at "$campaign_started" --arg binary "$binary_sha" --arg config "$config_sha" --argjson binary_size "$binary_size" --argjson config_size "$config_size" --argjson handoffs "$handoffs" '{schema_version:1,decision_id:"DEC-001-single-node-alpha-20260905",profile:"single-node-alpha",campaign_id:"campaign-1",campaign_started_at:$at,campaign_max_age_seconds:86400,candidate:{sha:$sha,endpoint_sha256:$endpoint,binary:{path:"candidate/instantd",size_bytes:$binary_size,sha256:$binary},configuration:{path:"candidate/config.json",size_bytes:$config_size,sha256:$config}},lanes:{hermetic:"run",owned_db:"run",corpus:"run",container:"not_selected",soak:"artifact",recovery:"artifact",external_v1:"not_selected",performance:"not_selected",artifact:"validate"},provider_evidence:"not_selected",external_records:{native_linux:"records/native-linux.json",soak:"records/soak.json",recovery:"records/recovery.json"},handoffs:$handoffs}' >"$evidence/manifest.json"

cat >"$fakebin/make" <<'EOF'
#!/usr/bin/env bash
target=${@: -1}
echo "$target" >>"$RELEASE_TEST_CALLS"
if [[ ${RELEASE_TEST_FAIL_TARGET:-} == "$target" ]]; then exit 7; fi
if [[ ${RELEASE_TEST_MUTATE_EVIDENCE_TARGET:-} == "$target" ]]; then printf changed >>"$RELEASE_TEST_MUTATE_EVIDENCE_PATH"; fi
if [[ $target == build ]]; then cp "$RELEASE_TEST_BINARY" "${@: -2:1}/bin/instantd"; fi
case "$target" in test-unit|bench-acceptance|test-integration|test-contract)
  if [[ ${RELEASE_TEST_ZERO_TARGET:-} == "$target" ]]; then exit 0; fi
  if [[ ${RELEASE_TEST_REPLAY_LOG_TARGET:-} == "$target" ]]; then
    cat "${RELEASE_TEST_REPLAY_LOG_PATH:?missing replay log}"
    exit 0
  fi
  if [[ ${RELEASE_TEST_SKIP_TARGET:-} == "$target" ]]; then
    printf '{"Action":"skip","Package":"example/corpus","Test":"TestSelected"}\n'
    exit 0
  fi
  if [[ ${RELEASE_TEST_REQUIRED_SKIP_TARGET:-} == "$target" ]]; then
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
    printf '{"Action":"skip","Package":"example/corpus","Test":"TestRequiredSkip"}\n'
    if [[ $target == test-contract ]]; then
      printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration"}\n'
      printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/00-smoke"}\n'
    fi
    exit 0
  fi
  if [[ ${RELEASE_TEST_ALLOWED_SKIP_TARGET:-} == "$target" ]]; then
    printf '{"Action":"output","Package":"example/corpus","Test":"TestOutsideLane","Output":"    foo_test.go:1: integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL\\n"}\n'
    printf '{"Action":"skip","Package":"example/corpus","Test":"TestOutsideLane"}\n'
    printf '{"Action":"output","Package":"example/corpus","Test":"TestShortOutside","Output":"    bar_test.go:1: reporting test skipped in short mode\\n"}\n'
    printf '{"Action":"skip","Package":"example/corpus","Test":"TestShortOutside"}\n'
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
    if [[ $target == test-contract ]]; then
      printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration"}\n'
      printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/00-smoke"}\n'
    fi
    exit 0
  fi
  if [[ ${RELEASE_TEST_MISSING_REQUIRED_TARGET:-} == "$target" ]]; then
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
    exit 0
  fi
  if [[ ${RELEASE_TEST_SHORT_ONLY_TARGET:-} == "$target" ]]; then
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
    printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration"}\n'
    printf '{"Action":"output","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/00-smoke","Output":"    integration_test.go:43: integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL\\n"}\n'
    printf '{"Action":"skip","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/00-smoke"}\n'
    exit 0
  fi
  if [[ ${RELEASE_TEST_REQUIRED_ALLOWLISTED_SKIP_TARGET:-} == "$target" ]]; then
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
    printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration"}\n'
    printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/00-smoke"}\n'
    printf '{"Action":"output","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/01-transact-refresh","Output":"    integration_test.go:43: integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL\\n"}\n'
    printf '{"Action":"skip","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/01-transact-refresh"}\n'
    exit 0
  fi
  if [[ ${RELEASE_TEST_DECOY_PACKAGE_TARGET:-} == "$target" ]]; then
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
    printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration"}\n'
    printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/cmd/corpusctl","Test":"TestCorpusReplayIntegration/00-smoke"}\n'
    exit 0
  fi
  if [[ $target == test-contract ]]; then
    printf '{"Action":"skip","Package":"example/corpus","Test":"TestSelected"}\n'
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
    printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration"}\n'
    printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/00-smoke"}\n'
  else
    printf '{"Action":"pass","Package":"example/corpus","Test":"TestSelected"}\n'
  fi
esac
EOF
chmod +x "$fakebin/make"

calls="$tmp/calls"
# This contract test runs inside the real gate, which exports these; every
# case below must see only the values it sets explicitly.
unset DATABASE_URL RELEASE_GATE_MANIFEST RELEASE_CANDIDATE_SHA RELEASE_CAMPAIGN_ID
base=(env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=postgres://redacted RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" bash "$gate")

pass=0
fail=0
ok() { local name=$1 out; shift; if out=$("$@" 2>&1); then echo "PASS: $name"; pass=$((pass+1)); else echo "FAIL: $name ($out)" >&2; fail=$((fail+1)); fi; }
bad() { local name=$1 expected=$2; shift 2; local out status=0; out=$("$@" 2>&1) || status=$?; if [[ $status -ne 0 && $out == *"$expected"* ]]; then echo "PASS: $name"; pass=$((pass+1)); else echo "FAIL: $name ($status: $out)" >&2; fail=$((fail+1)); fi; }
status_is() { local name=$1 expected=$2; shift 2; local status=0; "$@" >/dev/null 2>&1 || status=$?; if [[ $status -eq $expected ]]; then echo "PASS: $name"; pass=$((pass+1)); else echo "FAIL: $name (got $status, want $expected)" >&2; fail=$((fail+1)); fi; }

: >"$calls"
ok "happy path" "${base[@]}"
expected=$'validate-release\nlint\nvet\ncheck-generated\nbuild\ntest-unit\nbench-acceptance\ntest-integration\ntest-contract'
[[ $(cat "$calls") == "$expected" ]] || { echo "FAIL: command order" >&2; fail=$((fail+1)); }
if ! grep -Eq 'container-verify|soak-gate|differential|bench-verify|publish' "$calls"; then echo "PASS: forbidden targets absent"; pass=$((pass+1)); else fail=$((fail+1)); fi

bad "missing campaign" "RELEASE_CAMPAIGN_ID is required" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" DATABASE_URL=x bash "$gate"
bad "missing database" "DATABASE_URL is required" env -u DATABASE_URL PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 bash "$gate"
bad "missing manifest" "RELEASE_GATE_MANIFEST" env PATH="$fakebin:$PATH" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x bash "$gate"
bad "candidate mismatch" "candidate SHA mismatch" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA=0000000000000000000000000000000000000000 RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x bash "$gate"
printf dirty >"$repo/dirty"
bad "dirty tree" "candidate tree is dirty" "${base[@]}"
rm "$repo/dirty"

cp "$evidence/manifest.json" "$evidence/bad.json"
jq '.lanes.container="run"' "$evidence/bad.json" >"$evidence/bad.tmp" && mv "$evidence/bad.tmp" "$evidence/bad.json"
bad "wrong lane selection" "manifest schema" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/bad.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x bash "$gate"

status_is "child failure propagated" 7 env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_FAIL_TARGET=vet bash "$gate"
for lane in test-unit bench-acceptance test-integration test-contract; do
  status_is "child failure propagates for $lane" 7 env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_FAIL_TARGET="$lane" bash "$gate"
done
bad "zero selected tests rejected" "test-unit selected zero tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_ZERO_TARGET=test-unit bash "$gate"
for lane in bench-acceptance test-integration test-contract; do
  bad "zero selected tests rejected for $lane" "$lane selected zero tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_ZERO_TARGET="$lane" bash "$gate"
done
bad "selected test skip rejected" "test-integration skipped selected tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_SKIP_TARGET=test-integration bash "$gate"
for lane in test-unit bench-acceptance test-contract; do
  bad "skipped required test rejected for $lane" "$lane skipped required tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_REQUIRED_SKIP_TARGET="$lane" bash "$gate"
done
bad "skipped required test rejected for test-integration" "test-integration skipped selected tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_REQUIRED_SKIP_TARGET=test-integration bash "$gate"
for lane in test-unit bench-acceptance test-contract; do
  ok "outside-lane skips allowed for $lane" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_ALLOWED_SKIP_TARGET="$lane" bash "$gate"
done
bad "outside-lane skips still fail owned-DB lane" "test-integration skipped selected tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_ALLOWED_SKIP_TARGET=test-integration bash "$gate"
bad "missing required contract test rejected" "did not select required TestCorpusReplayIntegration" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_MISSING_REQUIRED_TARGET=test-contract bash "$gate"
bad "short-only contract without owned-DB execution rejected" "missing required owned-DB TestCorpusReplayIntegration execution" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_SHORT_ONLY_TARGET=test-contract bash "$gate"
bad "required scenario skip with allowlisted message rejected" "test-contract skipped required tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_REQUIRED_ALLOWLISTED_SKIP_TARGET=test-contract bash "$gate"
bad "decoy-package scenario without corpus execution rejected" "missing required owned-DB TestCorpusReplayIntegration execution" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_DECOY_PACKAGE_TARGET=test-contract bash "$gate"
bad "built binary mismatch rejected" "built candidate binary does not match" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/raw/result" bash "$gate"
cp "$evidence/records/soak.json" "$evidence/records/soak.source-saved"
bad "source evidence mutation rejected" "source evidence changed during release gate: records/soak.json" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_MUTATE_EVIDENCE_TARGET=lint RELEASE_TEST_MUTATE_EVIDENCE_PATH="$evidence/records/soak.json" bash "$gate"
mv "$evidence/records/soak.source-saved" "$evidence/records/soak.json"

cp "$evidence/records/soak.json" "$evidence/records/soak.saved"
jq '.selected_count="1"' "$evidence/records/soak.saved" >"$evidence/records/soak.json"
bad "wrong record type rejected" "soak record failed its common contract" "${base[@]}"
jq '.artifacts=["not-an-artifact"]' "$evidence/records/soak.saved" >"$evidence/records/soak.json"
bad "malformed artifact inventory rejected" "soak record failed its common contract" "${base[@]}"
mv "$evidence/records/soak.saved" "$evidence/records/soak.json"

cp "$evidence/records/native-linux.json" "$evidence/records/native-linux.saved"
jq '.host.os="darwin"' "$evidence/records/native-linux.saved" >"$evidence/records/native-linux.json"
bad "non-Linux native evidence rejected" "native Linux evidence is insufficient" "${base[@]}"
mv "$evidence/records/native-linux.saved" "$evidence/records/native-linux.json"

jq '(.handoffs[] | select(.packet=="RT-003") | .candidate_sha)="0000000000000000000000000000000000000000"' "$evidence/manifest.json" >"$evidence/bad.json"
bad "handoff candidate mismatch rejected" "RT-003 handoff candidate mismatch" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/bad.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x bash "$gate"
jq '(.handoffs[] | select(.packet=="RT-003") | .state)="PARTIAL"' "$evidence/manifest.json" >"$evidence/bad.json"
bad "handoff state rejected" "RT-003 handoff is not accepted" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/bad.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x bash "$gate"
jq '.candidate.binary.path="../outside"' "$evidence/manifest.json" >"$evidence/bad.json"
bad "unsafe artifact path rejected" "candidate-binary artifact path is unsafe" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/bad.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x bash "$gate"
ln -s raw/result "$evidence/evidence-link"
bad "evidence symlink rejected" "release evidence may not contain symlinks" "${base[@]}"
rm "$evidence/evidence-link"

bad "production rejects test seams" "test seams are not accepted" env PATH="$fakebin:$PATH" RELEASE_GATE_TESTING=1 RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x bash "$gate"

# R4/R6: real-suite pin for the hermetic skip policy. Captures the real
# ./internal/reactive selection (hermetic: INTEGRATION=0, -short) and feeds it
# through the ACTUAL copied production gate ($gate) via the fake-make replay
# harness, so the policy under test is the gate itself, not a mirrored jq
# expression. Runs from the real repo root (not the fake $repo above).
real_log="$tmp/real-hermetic.json"
if ! (cd "$script_dir/.." && INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./internal/reactive -count=1 -short -json >"$real_log" 2>&1); then
  cat "$real_log" >&2
  echo "FAIL: real hermetic selection did not run" >&2; fail=$((fail+1))
else
  if ! grep -q "integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL" "$real_log" || ! grep -q "in short mode" "$real_log"; then
    echo "FAIL: real hermetic fixture missing allowlisted reasons" >&2; fail=$((fail+1))
  else
    echo "PASS: real hermetic fixture contains both allowlisted reasons"; pass=$((pass+1))
  fi
  ok "real reactive log accepted by production gate" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_REPLAY_LOG_TARGET=test-unit RELEASE_TEST_REPLAY_LOG_PATH="$real_log" bash "$gate"
  cp "$real_log" "$tmp/real-injected.json"
  printf '{"Action":"skip","Package":"example/corpus","Test":"TestRequiredSkip"}\n' >>"$tmp/real-injected.json"
  bad "real reactive log with injected required skip rejected" "skipped required tests" env PATH="$fakebin:$PATH" RELEASE_GATE_MANIFEST="$evidence/manifest.json" RELEASE_CANDIDATE_SHA="$sha" RELEASE_CAMPAIGN_ID=campaign-1 DATABASE_URL=x RELEASE_TEST_CALLS="$calls" RELEASE_TEST_BINARY="$evidence/candidate/instantd" RELEASE_TEST_REPLAY_LOG_TARGET=test-unit RELEASE_TEST_REPLAY_LOG_PATH="$tmp/real-injected.json" bash "$gate"
  # The pre-allowlist production rule (skipped==0) would reject this real log;
  # run that OLD check inline as an assertion without modifying the gate.
  old_skipped=$(jq -Rsr '[split("\n")[] | fromjson? | select((.Test? // "") != "")] | group_by(.Package,.Test) | map(.[-1]) | map(select(.Action=="skip")) | length' "$real_log")
  if [[ $old_skipped -gt 0 ]]; then echo "PASS: old reject-every-skip policy would reject real hermetic lane (skipped=$old_skipped)"; pass=$((pass+1)); else echo "FAIL: real hermetic lane has no skips to prove R4" >&2; fail=$((fail+1)); fi
fi

# Public binaries must use the same real Git tag as publication; an ldflag alone
# does not pin the main-module version embedded by Go.
build_repo="$tmp/build-repo"
mkdir -p "$build_repo/docs/plans/next-release" "$build_repo/scripts/qualify" "$tmp/build-bin"
cp "$script_dir/qualify/build-candidate.sh" "$build_repo/scripts/qualify/"
cp "$script_dir/../docs/plans/next-release/qualification-policy.json" "$build_repo/docs/plans/next-release/"
git -C "$build_repo" init -q
git -C "$build_repo" config user.email test@example.invalid
git -C "$build_repo" config user.name test
git -C "$build_repo" add .
git -C "$build_repo" commit -qm candidate
build_sha=$(git -C "$build_repo" rev-parse HEAD)
cat >"$tmp/build-bin/go" <<'EOF'
#!/usr/bin/env bash
touch "$BUILD_TEST_GO_CALLED"
EOF
chmod +x "$tmp/build-bin/go"
build_candidate() {
  (cd "$build_repo" && PATH="$tmp/build-bin:$PATH" BUILD_TEST_GO_CALLED="$tmp/go-called" bash scripts/qualify/build-candidate.sh single-node-public-alpha "$tmp/build-output/instantd")
}
bad "public build without release tag rejected" "release tag must identify candidate HEAD" build_candidate
[[ ! -f $tmp/go-called ]] || { echo "FAIL: untagged build invoked Go" >&2; fail=$((fail+1)); }
rm -f "$tmp/go-called"
git -C "$build_repo" tag v0.1.0-alpha.1 "$build_sha"
git -C "$build_repo" commit --allow-empty -qm different-candidate
bad "public build with tag on another commit rejected" "release tag must identify candidate HEAD" build_candidate
[[ ! -f $tmp/go-called ]] || { echo "FAIL: mismatched tag build invoked Go" >&2; fail=$((fail+1)); }
git -C "$build_repo" checkout -q --detach "$build_sha"
ok "public build with exact candidate tag accepted" build_candidate

# Public schema uses the actual candidate verifier and production shell. These
# fixtures are hermetic contract checks, never retained live qualification.
if (cd "$script_dir/.." && go test ./cmd/qualify -run '^TestPublic' -count=1 >"$tmp/public-contract.log" 2>&1); then
  echo "PASS: public profile actual verifier and production shell contracts";pass=$((pass+1))
else
  cat "$tmp/public-contract.log" >&2
  echo "FAIL: public profile contracts" >&2;fail=$((fail+1))
fi

echo "$pass passed, $fail failed"
(( fail == 0 ))
