# 10 — Wave 0 Retract-Integrity Review Record

Status: **COMPLETE** · Date: 2026-08-25 · Scope: P0 data-integrity fixes for
batched triple deletes and transaction-boundary discipline.

## Contract

| ID | Invariant | Evidence | Result |
|---|---|---|---|
| R1 | Batched deletes correlate entity/attr/value_md5 **per input row**; `{(e1,a1,v1),(e2,a2,v2)}` never touches `(e1,a1,v2)` | `TestDeleteExactMatchMultiTuple` | PROVEN_RED (`deleted 3 rows`) → GREEN |
| R2 | Retract effects participate in `Transact`'s single tx; failed tx leaves retracted values intact; no journal row survives a failed batch | `TestRetractAtomicWithFailedStep` | PROVEN_RED (`rows=[]` after failed tx) → GREEN |
| R1b | JSON-nil delete matches stored `'null'` via `md5('null')` | `TestDeleteExactMatchJSONNil` | GREEN |
| R2b | Multi-tuple retract batch inside failed tx rolls back entirely | `TestRetractBatchAtomicWithFailedStep` | GREEN |
| R3 | Pool-variant `DeleteTriples` reserved for standalone authn/oauth cleanups; no regressions | caller inventory + full suite | GREEN |

## Implementation

- `internal/storage/storage.go`: shared `deleteTriplesExec` (executes on
  `pgx.Tx` or pool); per-row-correlated DELETE; new `DeleteTx`; doc note on
  exact-md5 numeric-scale semantics (a retract of `1` will not match a row
  stored as `1.0` — v1-parity; only externally-restored rows can diverge).
- `internal/transact/apply.go`: `applyRetract` now executes via
  `db.DeleteTx(ctx, tx, …)` inside the caller's transaction, mirroring
  `applyDeleteEntity`.

## Skeptical review record (fresh-context, adversarial)

Independent reviewer verdict: **ACCEPT**. Method: re-derived old-vs-new SQL
semantics against live Postgres in rollback-only temp tables (old predicate
over-deletes 3, new deletes exactly 2); verified md5 canonicalization parity
across insert (`enhancedRowsCTE`), COPY bulk-load, and delete paths; verified
`md5('null') == triple.JSONNullMD5`; traced every remaining `DeleteTriples`
caller to standalone HTTP-auth flows unreachable from `transact.Transact`;
re-ran focused tests and the full sequential suite.

Reviewer findings adopted:
1. JSON-nil delete path untested → closed (`TestDeleteExactMatchJSONNil`).
   First draft placed a null on a *ref* attr and was correctly rejected by
   `ref_values_are_uuid`; redesigned onto blob attrs.
2. Piecewise-only coverage of multi-tuple × atomicity → closed
   (`TestRetractBatchAtomicWithFailedStep`).
3. Numeric-scale md5 divergence → documented on `deleteTriplesExec`.

Residual known limits (accepted, non-blocking):
- `DeleteTx` does not verify the supplied `pgx.Tx` belongs to the same
  `*DB`/pool; misuse would compile and run off-transaction.
- No deterministic concurrency test for retract-vs-add interleavings on the
  same triple (single-writer-per-app serialization is enforced upstream by
  design, docs/02 §5 invariant 1).
- Live-PG tests `t.Skip` without `DATABASE_URL`; CI must keep provisioning one
  (see below).

## Verification environment notes

- Full-suite gate must run packages **sequentially** (`go test ./... -p 1`)
  when packages share one `DATABASE_URL`: each test binary resets the public
  schema, so parallel package execution cross-triggers nondeterministic
  failures in unrelated tests. CI uses an isolated service-container database
  plus `-p 1`.
- `internal/waltail` live tests require logical-replication privileges and an
  output-plugin allowlist that includes `pgoutput`; hosts restricting plugins
  (e.g. local Homebrew Postgres with `output_plugin_libraries=wal2json`) fail
  at slot creation regardless of application code.
