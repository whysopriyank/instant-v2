#!/usr/bin/env bash
# Compare fresh generator output without modifying tracked artifacts.
set -euo pipefail
generated_dir=$(mktemp -d "${TMPDIR:-/tmp}/instant-quality-generated.XXXXXX")
cleanup() {
  rm -f "$generated_dir/generated.go" "$generated_dir/protocol.d.ts"
  rmdir "$generated_dir"
}
trap cleanup EXIT
"${GO:-go}" run ./cmd/schemagen --schema internal/protocol/schema/protocol.schema.json --out "$generated_dir"
cmp internal/protocol/generated.go "$generated_dir/generated.go"
cmp internal/protocol/protocol.d.ts "$generated_dir/protocol.d.ts"
