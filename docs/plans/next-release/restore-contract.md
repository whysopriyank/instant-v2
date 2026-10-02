# DA-009 / DA-010 restore contract

Scope: `internal/backup/**`; daemon/storage integration belongs to the coordinator.
Baseline: `421d69f5ab4c1ed4ff524b7d8c6a51873e50a610`; existing untracked
`docs/plans/readiness-review-20261001.md` is unrelated and preserved.
Precision-contract-build high-risk persistence checks apply.

| ID | Desired invariant and real path | Planned evidence | Red expectation | Goal / status |
|---|---|---|---|---|
| DA-010a | HTTP/native v2 `Import` rejects attrs, triples, rules or journal rows before mutation; app shell/token allowed | `TestRestoreRejectsNonemptyTarget`, `TestRestoreRejectsNonemptyComponents`, `TestRestoreAllowsEmptyShellWithAdminToken`, HTTP409 regression | Original owned-DB test returned nil | FIX / GREEN; final review pending |
| DA-010b | HTTP/native v1 ZIP restore rejects the same nonempty target | `TestV1RestoreRejectsNonemptyTarget`, component cases, exact export equality | Original owned-DB ZIP restore returned nil and changed target | FIX / GREEN; final review pending |
| DA-010c | Admission cannot race ordinary writers into the target | `TestRestoreAdmissionBlocksConcurrentWriter` pauses after admission, verifies SQL55P03 for concurrent rules insert | Temporary no-lock overlay returned nil and failed intended assertion | FIX / GREEN; final review pending |
| DA-009a | ZIP bytes become durable objects keyed by matching file entity ID and reopen exactly | HTTP `TestV1RestoreFiles` checks every metadata field and reopened bytes; ZIP source byte equality | Original missing-store test silently skipped bytes; skip-blob-write overlay fails at missing reopened object | FIX / GREEN; final review pending |
| DA-009b | Missing store/blob, orphan/duplicate blob, malformed UUID/size, corrupt/oversized archive are refused before blob writes | `TestV1RestoreRejectsUnwiredBlobs`, five invalid-file cases, five bounds/CRC cases; exact SQL/source equality and zero object writes | Original unwired-store test accepted archive and changed target | FIX / GREEN; final review pending |
| DA-009c | Known precommit/commit rejection removes only new objects; unknown commit or cleanup failure requires reconciliation | `TestV1RestoreFileFailurePreservesTarget` four cases, real deferred-trigger `TestV1RestoreCommitRejectionCleansFiles` | Skip-blob-write sensitivity test plus controlled fault regressions | FIX / GREEN; final review pending |
| DA-009d | A failed object creation with uncertain cleanup returns HTTP503/reconciliation; its ownership-uncertain key is not deleted by restore | `TestV1RestoreUploadCleanupUncertainty` injects the storage sentinel after a real write, checks exact SQL/source equality and retained reopened bytes | Existing handler reported HTTP400; focused regression failed its intended status assertion | FIX / GREEN; final review pending |
| DA-010d | Checksum/tail errors and commit rejection rollback exact SQL/sequence state, with no backwards sequence movement | Existing package regressions; `TestImportReadErrorAfterChecksumRollsBack`, `TestImportCommitFailureRollsBackSequence`, `TestImportNeverMovesSequenceBackward` | Original read-after-checksum changed sequence1/false to1/true | FIX / GREEN; final review pending |

Verification: `CGO_ENABLED=0 go test -short -count=1 ./internal/backup` with
`INSTANT_TEST_INTEGRATION=1` and an explicitly owned testkit PostgreSQL admin DSN.
Focused high-risk green must run twice with independently created fixtures.
Final review must inspect code, tests, actual command outcomes and reopened state.

## Reviewed commit boundary

Reuse `storageapi.ObjectStore.PutIfAbsent/Open/Delete`, with handler injection.
SQL commits only after archive validation and durable blob writes. Ordinary
precommit failures roll SQL back and compensate only newly created objects.
Simple SQL/filesystem compensation does not prove crash atomicity or resolve an
ambiguous SQL commit acknowledgement. Cleanup must sync object directories.
Coordinator relayed nonauthor architecture acceptance on 2026-10-02, before
blob implementation: known rejection compensates; unknown commit must retain
objects and return typed `ErrCommitOutcomeUnknown`; explicit cleanup failure
returns `ErrRestoreCleanup`. SQL rows commit together.
A `PutIfAbsent` failure carrying `storageapi.ErrUploadCleanup`
also returns `ErrRestoreCleanup`/HTTP503: that object's creation/cleanup durability
is uncertain, so restore does not delete its ownership-uncertain key. SQL still
rolls back; operator reconciliation is required before retry. Process crash can leave
unreferenced blobs, which require operator reconciliation. No crash-atomic
SQL/filesystem claim is made. Contextless ObjectStore methods cannot bound one
filesystem call; SQL rollback uses a fresh10s context even after request cancel.
Global table locks make this an offline/maintenance restore operation for alpha.
Native NDJSON is unchanged and contains database records, not file bytes.
HTTP restore bodies and ZIP aggregate expansion are capped at1GiB; schema-only
ZIP restores still work without an injected file store.
No new restore/recovery/performance release acceptance is claimed by this ledger.

## Executed evidence (2026-10-02)

Owned PostgreSQL17.11 at loopback55471; only random testkit fixture databases.
Original focused run: exit1, five top-level tests and six component cases
failed at intended assertions, not setup/build. After implementation, focused
checks passed; full backup package run passed in10.152s. A fresh full run passed
in9.52s:76 passing test/subtest outcomes,0 failures,1 short-mode memory-cap skip.
Raw second-run JSON: `/tmp/instant-v2-restore-green-20261002.jsonl`.
Two temporary Go-overlay sensitivity probes each exited1 at the intended
missing-object and missing-lock assertions; repository source was unchanged.
`CGO_ENABLED=0 go vet ./internal/backup` and scoped `git diff --check` exited0.
Local golangci-lint is unavailable on PATH; coordinator owns strict lint.
Final rerun after post-commit temp-file cleanup passed in9.647s:47 top-level
tests,76 passing test/subtest outcomes,0 failures, the same1 memory-cap skip.
Raw final JSON: `/tmp/instant-v2-restore-final-20261002.jsonl`.
Final scoped vet and diff checks exited0. Nonauthor completion review and the
coordinator's assembled daemon/storage checks remain required.

Nonauthor review found an uncertain upload-cleanup error was mapped to ordinary
HTTP400. The focused `TestV1RestoreUploadCleanupUncertainty` failed at that
assertion before the fix, then passed twice with fresh owned-DB fixtures. It
checks SQL/source equality, zero Delete calls and the real retained object's
reopened bytes. After joining the exported storage sentinel with
`ErrRestoreCleanup`, the full package passed in17.843s:77 passing test/subtest
outcomes,0 failures,1 short-memory skip. Raw results:
`/tmp/instant-v2-restore-cleanup-review-20261002.jsonl`. Scoped vet/diff checks
also exited0. Independent completion review remains required.
