#!/usr/bin/env bash
# Hermetic contract checks for the offline QR-005 preflight.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source_preflight="$script_dir/quality-supply-chain-preflight.sh"
repo_root=$(cd "$script_dir/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
pass=0
fail=0

ok() {
  local name=$1
  shift
  if "$@" >/dev/null 2>&1; then
    printf 'PASS: %s\n' "$name"
    pass=$((pass + 1))
  else
    printf 'FAIL: %s\n' "$name" >&2
    fail=$((fail + 1))
  fi
}

bad() {
  local name=$1 expected=$2 output status=0
  shift 2
  output=$("$@" 2>/dev/null) || status=$?
  if [[ "$status" -ne 0 ]] && jq -e --arg expected "$expected" 'any(.findings[]?; .code == $expected)' <<<"$output" >/dev/null; then
    printf 'PASS: %s\n' "$name"
    pass=$((pass + 1))
  else
    printf 'FAIL: %s (status=%s)\n' "$name" "$status" >&2
    printf '%s\n' "$output" >&2
    fail=$((fail + 1))
  fi
}

write_base_repo() {
  local repo=$1
  mkdir -p "$repo/.github/workflows" "$repo/cmd/schemagen" "$repo/internal/protocol/schema" "$repo/scripts"
  cp "$source_preflight" "$repo/scripts/quality-supply-chain-preflight.sh"
  chmod +x "$repo/scripts/quality-supply-chain-preflight.sh"
  printf '%s\n' \
    'name: ci' 'jobs:' '  build:' '    runs-on: ubuntu-24.04' '    steps:' \
    '      - uses: actions/checkout@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
    '      - uses: actions/setup-go@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' \
    '      - run: docker run --rm postgres@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' \
    '      - run: curl -fsSLo tool https://example.invalid/tool && printf "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee  tool\\n" | sha256sum -c -' \
    >"$repo/.github/workflows/ci.yml"
  printf '%s\n' '# syntax=docker/dockerfile:1' 'FROM golang@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' >"$repo/Dockerfile"
  printf '%s\n' 'module example.invalid/qr005' 'go 1.25.0' >"$repo/go.mod"
  printf '%s\n' 'example.invalid/placeholder v1.0.0 h1:placeholder' >"$repo/go.sum"
  printf '%s\n' '{"definitions":{"wsOp":{"enum":["init"]}}}' >"$repo/internal/protocol/schema/protocol.schema.json"
  printf '%s\n' 'package main' >"$repo/cmd/schemagen/main.go"
  printf '%s\n' 'package protocol' >"$repo/internal/protocol/protocol.go"
  printf '%s\n' 'package protocol' >"$repo/internal/protocol/generated.go"
  printf '%s\n' 'export type WsOp = "init";' >"$repo/internal/protocol/protocol.d.ts"
  printf '%s\n' 'GO ?= go' >"$repo/Makefile"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$repo/scripts/quality-generated.sh"
  git -C "$repo" init -q
  git -C "$repo" config user.email test@example.invalid
  git -C "$repo" config user.name test
  git -C "$repo" add .
  git -C "$repo" commit -qm initial
}

clone_base() {
  local repo="$tmp/$1"
  mkdir -p "$repo"
  write_base_repo "$repo"
  printf '%s' "$repo"
}

current_before=$(git -C "$repo_root" status --porcelain --untracked-files=all)
current_status=0
current_output=$(bash "$source_preflight") || current_status=$?
ok "current tree pins are accepted" test "$current_status" -eq 0
ok "current report is valid JSON" jq -e '.status == "PASS" and (.findings|length == 0)' <<<"$current_output"
ok "current classes have no unresolved inputs" jq -e 'all(.actions[]?; .status == "resolved") and all(.images[]?; .status == "resolved") and all(.runner_labels[]?; .status == "resolved") and all(.runtime_tool_acquisitions[]?; .status == "resolved")' <<<"$current_output"
current_after=$(git -C "$repo_root" status --porcelain --untracked-files=all)
ok "current-tree scan does not mutate the worktree" test "$current_before" = "$current_after"

base=$(clone_base base)
first=$(bash "$base/scripts/quality-supply-chain-preflight.sh")
second=$(bash "$base/scripts/quality-supply-chain-preflight.sh")
ok "synthetic all-pinned success" test "$?" -eq 0
ok "synthetic report has all classified input families" jq -e '.status == "PASS" and (.actions|length > 0) and (.images|length > 0) and (.runner_labels|length > 0) and any(.runtime_tool_acquisitions[]?; .classification == "content_bound") and (.aggregate_root|test("^[0-9a-f]{64}$"))' <<<"$first"
ok "deterministic output and aggregate root" test "$first" = "$second"

changed=$(clone_base changed)
printf '%s\n' changed >>"$changed/go.sum"
changed_output=$(bash "$changed/scripts/quality-supply-chain-preflight.sh")
ok "changed immutable input changes its hash and root" jq -e --arg root "$(jq -r .aggregate_root <<<"$first")" --arg original "$(jq -r '.inputs[] | select(.path == "go.sum") | .sha256' <<<"$first")" '(.aggregate_root != $root) and any(.inputs[]; .path == "go.sum" and .sha256 != $original)' <<<"$changed_output"

mutable_action=$(clone_base mutable-action)
sed -i.bak 's#actions/checkout@[a-f0-9]*#actions/checkout@v4#' "$mutable_action/.github/workflows/ci.yml"
rm -f "$mutable_action/.github/workflows/ci.yml.bak"
bad "mutable action tag" mutable_action_tag bash "$mutable_action/scripts/quality-supply-chain-preflight.sh"

mutable_image=$(clone_base mutable-image)
sed -i.bak 's#postgres@sha256:[^ ]*#postgres:17#' "$mutable_image/.github/workflows/ci.yml"
rm -f "$mutable_image/.github/workflows/ci.yml.bak"
bad "tag-only image" tag_only_image bash "$mutable_image/scripts/quality-supply-chain-preflight.sh"

mutable_runner=$(clone_base mutable-runner)
sed -i.bak 's#ubuntu-24.04#ubuntu-latest#' "$mutable_runner/.github/workflows/ci.yml"
rm -f "$mutable_runner/.github/workflows/ci.yml.bak"
bad "latest runner label" mutable_runner_label bash "$mutable_runner/scripts/quality-supply-chain-preflight.sh"

mutable_tool=$(clone_base mutable-tool)
printf '%s\n' '      - run: go install example.invalid/tool@v1.2.3' >>"$mutable_tool/.github/workflows/ci.yml"
bad "non-content-bound tool install" non_content_bound_tool bash "$mutable_tool/scripts/quality-supply-chain-preflight.sh"

installer_spoof=$(clone_base installer-spoof)
printf '%s\n' '      - run: go install example.invalid/tool@v1.2.3 && printf "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff  other\\n" | sha256sum -c -' >>"$installer_spoof/.github/workflows/ci.yml"
bad "installer cannot spoof checksum binding" non_content_bound_tool bash "$installer_spoof/scripts/quality-supply-chain-preflight.sh"

multiline_image=$(clone_base multiline-image)
printf '%s\n' '      - run: |' '          docker run --rm \' '            --name qr005 \' '            -p 6379:6379 \' '            redis:7' >>"$multiline_image/.github/workflows/ci.yml"
bad "multiline tag-only image" tag_only_image bash "$multiline_image/scripts/quality-supply-chain-preflight.sh"
multiline_output=$(bash "$multiline_image/scripts/quality-supply-chain-preflight.sh" || true)
ok "docker option values are not images" jq -e 'any(.images[]; .value == "redis:7") and all(.images[]; .value != "6379:6379")' <<<"$multiline_output"

runner_array=$(clone_base runner-array)
sed -i.bak 's#runs-on: ubuntu-24.04#runs-on: [self-hosted, ubuntu-latest]#' "$runner_array/.github/workflows/ci.yml"
rm -f "$runner_array/.github/workflows/ci.yml.bak"
bad "runner array latest label" mutable_runner_label bash "$runner_array/scripts/quality-supply-chain-preflight.sh"

local_action=$(clone_base local-action)
printf '%s\n' '      - uses: ./actions/build' >>"$local_action/.github/workflows/ci.yml"
bad "unhashed local action" unhashed_local_action bash "$local_action/scripts/quality-supply-chain-preflight.sh"

missing=$(clone_base missing)
git -C "$missing" rm -q go.sum
bad "missing input" missing_input bash "$missing/scripts/quality-supply-chain-preflight.sh"

symlinked=$(clone_base symlinked)
git -C "$symlinked" rm -q Dockerfile
ln -s go.mod "$symlinked/Dockerfile"
git -C "$symlinked" add Dockerfile
bad "symlinked input" symlinked_input bash "$symlinked/scripts/quality-supply-chain-preflight.sh"

unexpected=$(clone_base unexpected)
printf '%s\n' notes >"$unexpected/.github/workflows/notes.txt"
git -C "$unexpected" add .github/workflows/notes.txt
bad "unexpected tracked workflow input" unexpected_input bash "$unexpected/scripts/quality-supply-chain-preflight.sh"

empty=$(clone_base empty)
git -C "$empty" rm -qr .github/workflows
bad "empty scan" empty_scan bash "$empty/scripts/quality-supply-chain-preflight.sh"

unclassified=$(clone_base unclassified)
printf '%s\n' '      - run: mystery fetch tool' >>"$unclassified/.github/workflows/ci.yml"
bad "unclassified acquisition syntax" unclassified_acquisition bash "$unclassified/scripts/quality-supply-chain-preflight.sh"

printf '%s passed, %s failed\n' "$pass" "$fail"
((fail == 0))
