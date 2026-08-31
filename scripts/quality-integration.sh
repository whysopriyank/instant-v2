#!/usr/bin/env bash
# Refuse the legacy shared-database test setup before enabling live tests.
set -euo pipefail
command -v rg >/dev/null || { echo 'test-integration fixture audit requires ripgrep (rg)' >&2; exit 2; }
status=0
rg -n --glob '*_test.go' --glob '!internal/testkit/**' \
  'DROP SCHEMA public|os\.Getenv\("(TEST_)?DATABASE_URL"\)' internal || status=$?
if test "$status" = 0; then
  echo 'live tests must use isolated testkit fixtures, not direct DATABASE_URL lookups or public schema resets' >&2
  exit 1
fi
if test "$status" != 1; then exit "$status"; fi
