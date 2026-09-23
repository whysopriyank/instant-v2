# Code-quality scorecard

Historical (baseline `5f78ca0877c1a8c04b173e5c502e8c948ecfb965`, 2026-08-31) —
superseded by the finish-up program's execution ledger; not current-candidate
status.

Snapshot: 2026-08-31, working tree based on `5f78ca0877c1a8c04b173e5c502e8c948ecfb965`.
This records the approved quality work and remaining limits, not a release or
production-readiness certification. Changes were still uncommitted when measured.
See the [execution ledger](../plans/quality-execution.md) for work ownership and
the [corpus contracts](../../corpus/README.md) for exact replay boundaries.

Later functional repairs and current status are tracked in
[implementation pass 1](../plans/implementation-pass-1.md). The sizes and findings
below remain the original snapshot, not a remeasurement after those fixes.

## Evidence and measurement

- **VERIFIED**: an executed check and inspected result; scope is named below.
- **PROVEN_RED**: an executed desired-behavior assertion failed as intended;
  this is evidence of an unresolved defect, not a passing gate.
- **INSPECTED**: supported by current source, without a dedicated runtime
  reproduction in this scorecard task.
- **UNVERIFIED**: required runtime, compatibility or failure-path evidence is absent.
  Worker-reported results are identified separately from the editor's own checks.

`go list ./...` returned **35 packages: 9 commands and 26 internal packages**.
Sizes below count physical lines, including comments and blanks, in hand-written
non-test `.go` files in each package directory. Platform-specific files are
included even when inactive on this host. Generated-header files are excluded:
`internal/protocol/generated.go` adds 7 generated lines; `cmd/schemagen/main.go`
is hand-written despite containing a generated-header string. Size is an
inspection aid, not a quality grade or a reason to split a cohesive function.

## Whole-package scorecard

The responsibility column describes the current boundary, not an assertion that
every package changed or has complete test coverage. Risk IDs refer to the next
section. Small cohesive packages deliberately remain small single-file packages.

| Package | Files / lines | Largest file | Responsibility outcome / remaining focus |
|---|---:|---|---|
| `cmd/benchreport` | 1 / 32 | `main.go` 32 | Thin report entrypoint; analysis belongs to `benchrun`. |
| `cmd/benchrun` | 1 / 281 | `main.go` 281 | CLI validation and runner invocation; benchmark policy remains in the library. |
| `cmd/benchsmoke` | 15 / 2115 | `namespace.go` 308 | Historical diagnostic code only (unsupported acceptance entrypoint per DEC-001 / EV-006); preserved for non-claimable live diagnostic probing. Supported soak/benchmark workflows map to cmd/soaksetup, cmd/soak, cmd/benchrun. |
| `cmd/chaos` | 10 / 1100 | `run.go` 349 | Process, PostgreSQL, WAL observations and reporting separated; C1/C2 remain consequential. |
| `cmd/corpusctl` | 1 / 220 | `main.go` 220 | Mode/flag dispatch over shared corpus mechanics; Q3 limits parity claims. |
| `cmd/instantd` | 10 / 971 | `routes.go` 196 | Runtime assembly, routes, middleware, health, invalidation and serving separated; lifecycle retained. |
| `cmd/schemagen` | 1 / 175 | `main.go` 175 | Schema-to-Go generation remains one cohesive command. |
| `cmd/soak` | 1 / 425 | `main.go` 425 | Soak orchestration remains local to the command; no new long-duration evidence claimed. |
| `cmd/soaksetup` | 1 / 110 | `main.go` 110 | Bounded fixture setup; no abstraction added for size alone. |
| `internal/adminapi` | 11 / 1344 | `users_store.go` 221 | Handler/auth, query/transact, permission probes, token routes, user query/store and schema separated. |
| `internal/authn` | 9 / 2292 | `authn.go` 776 | Typed OAuth records and provider/storage/state/HTTP boundaries extracted; A1/A2 unresolved. |
| `internal/backup` | 12 / 1718 | `http.go` 238 | HTTP/import/export, object storage, legacy archive/schema decoding and batching separated. |
| `internal/benchharness` | 23 / 6050 | `runner.go` 666 | Target runs, readiness, qualification, receivers, wire projection and evidence budget have named owners. |
| `internal/benchrun` | 32 / 7362 | `runner.go` 742 | Report loading/provenance/evidence/rendering and artifact policy separated; large orchestration remains. |
| `internal/bus` | 1 / 227 | `bus.go` 227 | PostgreSQL notification transport remains cohesive; reconnect behavior needs its own runtime evidence. |
| `internal/config` | 1 / 304 | `config.go` 304 | Environment/default/range parsing remains cohesive; G1 documents configuration/comment drift. |
| `internal/corpus` | 4 / 793 | `manifest.go` 283 | Manifest, scenario loading, canonicalization and replay separated; Q1/Q3 are explicit gaps. |
| `internal/datalog` | 1 / 202 | `datalog.go` 202 | SQL predicate/entity-set planning stays below query execution. |
| `internal/httpjson` | 1 / 45 | `httpjson.go` 45 | Small shared decode/write mechanics; callers still own envelopes and HTML escaping policy. |
| `internal/instaql` | 2 / 941 | `query.go` 676 | Coercion and execution separated; Q1/Q2 correctness work takes priority over another extraction. |
| `internal/metrics` | 1 / 153 | `metrics.go` 153 | Registry/server/scrape callbacks remain compact; M1 exposes registration lifecycle debt. |
| `internal/perms` | 3 / 511 | `check.go` 202 | Rule representation, evaluation and checks separated; dynamic query filtering remains constrained. |
| `internal/platform` | 5 / 965 | `attrs.go` 469 | Attribute catalog, cache and migrations remain distinct; cache concurrency warrants focused checks. |
| `internal/protocol` | 1 / 188 | `protocol.go` 188 | Wire definitions remain compact; generated schema output counted separately. |
| `internal/ratelimit` | 1 / 378 | `ratelimit.go` 378 | Sharded token buckets, admission and HTTP mapping remain cohesive; cap fail-open policy retained. |
| `internal/reactive` | 5 / 1542 | `incremental.go` 623 | Notification/coalescing, cache, materialized updates and refresh separated; R1 retry behavior remains. |
| `internal/runtimeapi` | 4 / 361 | `contracts.go` 117 | Route dispatch, request contracts, auth-token handlers and framework-query handling separated. |
| `internal/storage` | 2 / 594 | `storage.go` 451 | Regular storage and bulk COPY paths remain explicit; S1 must be resolved before COPY adoption. |
| `internal/storageapi` | 9 / 894 | `storageapi.go` 148 | Request/auth contracts, object operations, signing and backend boundaries separated. |
| `internal/sync` | 13 / 2758 | `sse.go` 474 | Session operations, transports, rooms and node-list projection separated; one canonical encoder retained. |
| `internal/testkit` | 1 / 140 | `postgres.go` 140 | Unique integration databases with cleanup; fixture owner still applies migrations. |
| `internal/tracing` | 1 / 65 | `tracing.go` 65 | Small tracing setup/shutdown boundary; no framework added. |
| `internal/transact` | 17 / 2601 | `txstep.go` 367 | Parse/resolve/expand/check and transactional dispatch/merge/delete/required-field responsibilities separated. |
| `internal/triple` | 1 / 120 | `triple.go` 120 | Canonical triple/value representation remains compact. |
| `internal/waltail` | 1 / 407 | `waltail.go` 407 | WAL stream/checkpoint coordination remains cohesive; crash/restart claims require live fault evidence. |

Concrete before/after anchors: `adminapi.go` fell from 1,232 to 112 lines;
`instantd/main.go` from 843 to 71, and `run` from 470 to 45. The meaningful gains
are one commit-notification fallback, a canonical paginated user store query,
and visible runtime resource ownership—not merely smaller files. Admin user
ordering/projection/null behavior was characterized against a live database.
The server keeps ordinary defers: metrics close, publisher release, read/write
pool close, SQL close, tracing shutdown, signal stop. WebSocket drain still
precedes HTTP shutdown. No intentional route/auth/status/lifecycle change was
part of that extraction.

Sync's canonical generic encoder removes duplicate projection logic but costs
more allocations than the removed typed default. The
[wire-fixture and allocation evidence](../../internal/sync/testdata/README.md)
records that explicit tradeoff; it does not establish an end-to-end speedup.

## Prioritized unresolved risks

These are follow-ups, not fixes delivered by the decomposition. Priority is
based on consequences and exposure, not file length. No new product fix or
destructive harness run was performed while writing this scorecard.

| ID / priority | Evidence | Finding and required decision/evidence |
|---|---|---|
| C1 / before any chaos run | INSPECTED | `run` passes user-controlled `--pg-data` to `mustRm` before initialization; [`mustRm`](../../cmd/chaos/postgres.go) directly calls `os.RemoveAll` without ownership/path validation. Require a validated disposable-directory contract before running this harness; cleanup repeats the same operation. No deletion reproduction was attempted. |
| A1 / high | INSPECTED | [`OAuthToken`](../../internal/authn/oauth.go) does not enforce `oauthCodeTTL` at redemption. Fetch/check/delete are separate, and deletion count is not used to establish a single winner. Redirect state also is consumed separately from validation; callback state expiry is checked after provider exchange. Add deterministic expiry/concurrent-consumption evidence and define an atomic redemption boundary; no exploit test is claimed. |
| A2 / high | INSPECTED | Built-in [`resolveProvider`](../../internal/authn/oauth_provider.go) copies credentials/auth URL/scope but not built-in token/user-info URLs into `ResolvedProvider`; exchange uses the latter fields. Injected-provider tests do not prove configured built-in provider operation. Apple additionally lacks its intended ID-token path. Characterize supported built-ins before claiming OAuth readiness. |
| Q1 / high | PROVEN_RED, corpus-worker reported | Opt-in `after-cursor` repeats the first page; `forward-relation` drops the reference and emits an empty aliased child. Desired-behavior fixtures are outside default discovery. The exact separate commands and assertions are in [corpus/README](../../corpus/README.md); default skipping is not a pass. Query-engine fixes were explicitly deferred. |
| Q2 / high | INSPECTED | [`coerceOpMap`](../../internal/instaql/query.go) returns from the first encountered map operator, so multiple predicates are not combined and selection is unordered. `paginateWrap` applies entity-ID LIMIT/OFFSET before `runForm` applies requested field ordering in memory. Characterize multi-operator conjunction and globally ordered pagination before changing semantics. |
| C2 / high for evidence acceptance | INSPECTED | [`chaos` build fallback](../../cmd/chaos/run.go) can test an exported `HEAD` instead of the dirty tree. [`runCorpusReplay`](../../cmd/chaos/process.go) embeds subprocess failure in printed text, not an error returned by `run`. A successful command exit therefore does not establish successful corpus replay or tested working-tree provenance. Require explicit provenance and failure propagation before accepting a chaos result. |
| S1 / before production COPY use | INSPECTED | [`CopyTriples`](../../internal/storage/copy.go) maps nil to the string `"null"` before JSON encoding; cardinality-one staging dedup has no input-order tie-break, unlike regular last-write-wins intent. Fresh caller inspection found only two storage-test calls, not an in-tree production caller. Add regular-vs-COPY null/duplicate parity tests before adoption; current impact is bounded by that caller inventory. |
| R1 / medium-high | INSPECTED | [`refreshOne`](../../internal/reactive/reactive.go) immediately re-enqueues a failed refresh while `Run` drains in an inner loop. Persistent failures can retry without delay/backoff; cancellation responsiveness under that condition is unverified. Use bounded failure injection before deciding retry/backoff policy. |
| M1 / medium | PROVEN_RED, editor executed | `go test ./internal/metrics -run '^TestRegisterGaugeServesScrapeTimeValue$' -count=2` fails on the second iteration: `metrics handler status = 500`. Process-global [`scrapeFns`](../../internal/metrics/metrics.go) accumulates duplicate registrations; no unregister/reset lifecycle exists. The comment that duplicate names panic during registration does not match this path. One-shot package success does not establish repeated registration safety. |
| G1 / medium | INSPECTED | [`config.Load`](../../internal/config/config.go) uses `envOr` for the metrics address, so an empty environment value selects the default rather than the documented disable mode. The empty read-URL comment says “share the write pool”; [`runDatabase`](../../cmd/instantd/runtime.go) actually creates separate read/write pools against the same DSN and budgets. Align intended configuration contract, comments and tests before changing behavior. |
| Q3 / release-evidence gap | UNVERIFIED | No designated pinned-v1 differential run or equivalent v1 fixture bootstrap was executed. Default corpus scenarios are authored v2 expectations, not v1 oracles. Cursor/relations, full cardinality/merge/cascade, dynamic permissions, token auth, multi-client rooms, delta, SSE and HTTP/SDK coverage remain incomplete. Recursive field masking and absence of a post-final-frame observation window are comparison blind spots. See [exact corpus limits](../../corpus/README.md). |
| Q4 / numeric-boundary follow-up | INSPECTED | Transaction lookup normalization still uses default float64 JSON decoding; exact numeric lookup distinctions above 2^53 need a dedicated real lookup regression. Corpus scenario loading also validates via float64 and therefore rejects huge exponents that its exact canonicalizer can represent. Current replay raw bytes and the 16 passing scenarios are unaffected by the latter limitation. |
| B1 / benchmark-evidence follow-up | INSPECTED | Triad report verification does not share all pair-only process/live-resource checks; `mustJSON` also ignores marshal errors on its broad input type. These predate the extraction and were not silently changed as benchmark-policy cleanup. Require narrowly scoped output/failure contracts before extending acceptance claims. |

`config`, `metrics` and `ratelimit` were intentionally not restructured: their
304/153/378-line scopes are understandable together. The editor ran
`go test ./internal/config ./internal/metrics ./internal/ratelimit -race -count=1`
successfully, then found M1 with the focused repeated run. Rate limiting has
burst/refill/isolation/admission/sweep/concurrency checks; its documented
fail-open behavior for new buckets at the capacity cap remains a policy choice,
not a silently fixed defect. These findings authorize no additional changes.

## Retained files above 500 lines

There are nine hand-written non-test files above 500 lines and none above 1,000.
The following boundaries are retained rather than split to satisfy a threshold.

| File(s) | Why retained / next useful boundary |
|---|---|
| `authn/authn.go` 776 | Core attributes, guest/magic-code/token/user service flow. Resolve A1/A2 with security-sensitive lifecycle characterization before further movement. |
| `benchharness/runner.go` 666; `session.go` 536 | Run scheduling/final evidence and session acknowledgement/waiter/transport lifecycle. Extract only a genuine independently testable policy or transport boundary. |
| `benchrun/runner.go` 742; `live_config.go` 722 | Multi-target orchestration/budgets and validated live setup/identity configuration. Preserve benchmark constraints and setup ordering in any later extraction. |
| `benchrun/collectors_live.go` 615; `benchharness_adapter.go` 608 | Provenance/process/runtime/database collectors and driver-to-evidence adaptation. Remaining breadth warrants review, but file movement alone would not simplify policy. |
| `instaql/query.go` 676 | Planning, fetch, relations, ordering and projection share the query contract. Q1/Q2 need desired-behavior tests before rearranging execution stages. |
| `reactive/incremental.go` 623 | Materialization, probes and rendering mirror full-query behavior. Keep oracle fallback explicit; contract drift is more important than a smaller file. |

Paths in this table are relative to `internal/`. This is a retention rationale,
not a waiver from correctness, readability or future focused decomposition.

## Bounded verification and remaining gates

The editor's completed implementation checks included:

- Admin API: dedicated-database `go test ./internal/adminapi -race -count=2 -v`
  passed (28 top-level executions, 80 subtest executions; no skips/failures),
  plus package unit/build/vet and formatting checks.
- Server assembly: dedicated-database `go test ./cmd/instantd -race -count=2 -v`
  passed (26 top-level executions, 62 subtest executions; no skips/failures),
  package unit/build/vet, and `golangci-lint run --allow-serial-runners ./cmd/instantd`
  reported zero issues. Independent read-only review also passed its live
  race run, lint and vet, and checked route/defer equivalence.
- Documentation: current `go list`/source inventory, package-table completeness,
  local link targets and whitespace were checked. This task did not rerun a
  repository-wide test suite or alter product code.

Those live package runs used private testkit databases on the dedicated local
PostgreSQL cluster, with owner-applied migrations. Earlier fixture DSN wiring
briefly invoked migrations against the default local `postgres` database before
the corrected isolation helper and dedicated-cluster reruns; the actual delta
to that database was not measured. Do not infer “no database side effects” from
the final passing runs; the coordinator's execution record owns that incident.

Full-process crash/drain, new long-duration soak, deployed built-in OAuth,
and pinned-v1 differential behavior are not established by the checks above.
Known-red tests remain failures, source-inspected risks remain unproven at
runtime, and package size reductions do not close those gates.
