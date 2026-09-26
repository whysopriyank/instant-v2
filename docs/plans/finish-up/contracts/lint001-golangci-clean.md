# Contract — LINT-001 make `make lint` green (behaviour-preserving)

## Why

The release gate runs `make lint`. On GNU make 4.x (every Linux host, CI
ubuntu runners) the Makefile's `LINT ?= golangci-lint` never took effect
because GNU make predefines `LINT=lint`; `make lint` therefore failed with
"lint: command not found" and the linter has never actually run. The
coordinator fixed the Makefile (uncommitted change to `Makefile` in this tree
— keep it). Running the pinned golangci-lint v2.13.1 with the repo's
`.golangci.yml` on Linux now reports **69 issues**; the full list is in
`docs/plans/finish-up/contracts/lint001-baseline.txt`.

## Goal

`golangci-lint run ./...` → 0 issues with the existing `.golangci.yml`,
with **no behaviour change**.

## Rules

1. Do not edit `.golangci.yml`, do not add `//nolint` except where a
   finding is provably a false positive — each `//nolint` needs the linter
   name and a one-line reason, and you must list every one in the handoff.
   Target: zero `//nolint`.
2. errcheck (46): handle the error the way the surrounding code does. In
   tests: `t.Fatal`/`t.Error` when the operation matters to the assertion,
   or `_ =` / `defer func() { _ = x.Close() }()` for best-effort cleanup.
   In production code (`internal/**`, non-test `cmd/**`): propagate or log
   consistently with the file's existing idiom — never silently change a
   control-flow outcome. List every production-code errcheck change
   separately in the handoff with a one-line justification.
3. unused (11): delete dead code. Before deleting any unused *production*
   function/type (e.g. `internal/reactive/reactive.go` `refreshOne`,
   `internal/sync/frame.go` `mustRaw`,
   `internal/benchrun/network_provenance_linux.go` `certifyWildcardListener`),
   grep the whole repo (including build-tagged files and scripts) to confirm
   no reference; note each deletion in the handoff. Note: some files are
   Linux-only (`_linux.go`) — check build tags so you don't delete something
   used on another GOOS.
4. ineffassign / staticcheck (12): minimal mechanical fixes (simple channel
   receive for S1000, De Morgan rewrite, merge declaration, drop ineffectual
   assignment). `internal/instaql/pagination.go:122` (`slice = entities`) is a
   named-return default overwritten on every path — fix by removing the dead
   assignment only if every return path still sets `slice`; otherwise keep
   semantics exactly.
5. Do not touch `cmd/qualify/**`, `scripts/qualify/**` (another packet),
   `corpus/**`, docs other than the ledger.

## Verification (paste raw)

Local macOS cannot link some packages with cgo; use `CGO_ENABLED=0`.
golangci-lint is not installed locally; install v2.13.1 for darwin-arm64 into
a temp dir if you can (verify its published sha256 from the release's
checksums file), else say it was not run — the coordinator re-runs lint on
Linux.

```
gofmt -l .
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=0 INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./... -race -count=1 -short 2>&1 | grep -v '^ok' | tail -40
golangci-lint run ./...   # if available
git diff --stat
```

Append a handoff section to `docs/plans/finish-up/execution-ledger.md`
(files touched, per-linter counts fixed, production-code changes listed
individually, `//nolint` list). Do NOT commit.
