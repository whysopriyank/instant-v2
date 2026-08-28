# Audit Remediation Plan

Status: proposed implementation plan based on the 2026-08-28 committed-code,
working-tree, architecture, conformance, and performance audit.

## Objectives

1. Remove correctness and concurrency blockers from the current working tree.
2. Close committed authorization, validation, and pagination gaps.
3. Restore a trustworthy live-Postgres verification baseline.
4. Reconcile plans and architecture with the implemented system.
5. Replace the current smoke-soak evidence with a reproducible performance gate.

## Execution rules

- Keep concurrent write ownership disjoint. At most three subagents run at once.
- The main agent owns requirements, integration, final verification, and changes
  to shared assembly or public behavior.
- Use Luna workers for bounded implementation and Luna test workers for focused
  verification. Escalate to Terra after two failed focused attempts or when
  integration evidence conflicts. Use Sol only for unresolved security or
  public-compatibility decisions.
- Do not combine performance tuning with correctness fixes in one patch. First
  establish semantic parity, then measure and optimize.
- Each workstream must leave a focused regression test and report the exact
  commands run. No worker commits, pushes, or deploys unless separately asked.

## Wave 0 — Preserve the baseline

Owner: main agent. No parallel product edits yet.

1. Record `git status`, HEAD, and the dirty diff without modifying user work.
2. Preserve minimal reproductions for:
   - rate-limiter lock inversion and cap oversubscription;
   - deep-merge scalar/map transition divergence;
   - single-flight double-close panic;
   - malformed deep-merge live tests.
3. Provision a dedicated disposable Postgres database and record which suites
   require `DATABASE_URL` or logical-decoding configuration.

Gate: every known blocker has a deterministic failing test or a documented
environmental prerequisite.

## Wave 1 — Parallel correctness blockers

Run these three workstreams concurrently. Their production-file ownership is
disjoint.

### W1-A — Rate limiter concurrency

Ownership: `internal/ratelimit/ratelimit.go` and its tests only.

- Replace cross-shard eviction while holding a shard lock with a lock-order-safe
  design.
- Make the global bucket reservation/cap invariant atomic.
- Preserve the existing behavior that cap pressure does not become a denial of
  service lever.
- Add high-contention tests with small caps, many keys, timeouts, and a strict
  `tracked <= maxBuckets` assertion; run under `-race`.

Gate: no deadlock, no cap overshoot, existing rate/refill/eviction behavior
unchanged.

### W1-B — Deep-merge semantic parity

Ownership: `internal/transact/apply.go`, pair-fetch support in
`internal/storage/storage.go`, and deep-merge tests only.

- Fix malformed test construction first.
- Preserve exact sequential semantics for repeated `(entity, attr)` operations,
  including map→map, map→scalar, scalar→map, null transitions, and multiple
  entity/attribute pairs.
- Retain batched reads/writes only where equivalence is proven.
- Add query-count instrumentation or a narrow benchmark showing the database
  round-trip reduction without using it as a V1/V2 claim.

Gate: old sequential implementation and batched implementation produce equal
final values for a transition matrix; live-Postgres tests pass under `-race`.

### W1-C — Single-flight tests and error-path semantics

Ownership: `internal/sync/groups.go`, `singleflight*_test.go`, and no other sync
files.

- Remove the double-close panic using idempotent release ownership.
- Prove one successful refresh for concurrent cold subscribers.
- Prove failure propagation, retry behavior, context cancellation, and no
  overlapping refreshes when a failed leader races a new caller.
- Preserve per-session acknowledgement behavior.

Gate: focused sync tests pass repeatedly (`-count=20`) and under `-race` with a
live Postgres database where required.

## Wave 2 — Parallel committed-code gaps

Start only after Wave 1 is integrated and green.

### W2-A — Permission projection fails closed

Ownership: transaction permission projection and its tests.

- Change entity projection to return `(map[string]any, error)`.
- Propagate storage errors through every create/update/delete permission path.
- Add injected query-failure tests proving CEL is never evaluated with an empty
  synthetic projection after a storage failure.
- Review admin bypass behavior and preserve it explicitly.

Gate: all projection errors abort the transaction; authorization regression
tests pass.

Model note: implementation is Luna-sized, but the main agent performs a bounded
security review before integration.

### W2-B — Required-attribute decision and implementation

Ownership: attribute model/migrations and required validation tests. This task
must not overlap W2-A in `apply.go`; sequence the small integration edit through
the main agent.

- First determine the exact V1 source-of-truth representation and when
  validation runs.
- If the current schema carries the signal, implement batched validation for
  touched entities.
- If the signal is absent, write an ADR and migration/compatibility plan before
  changing storage.
- Cover create, update, delete, null, cardinality, and same-transaction attr
  creation cases.

Gate: explicit V1 parity evidence or an approved documented deviation.

Model note: use Terra for the initial contract if schema evidence is ambiguous;
return bounded implementation to Luna.

### W2-C — Pagination boundaries

Ownership: `internal/instaql/query.go` pagination logic and pagination tests.

- Use `limit+1` or an existence probe to derive `hasNextPage`.
- Cover zero, under-limit, exact-limit, and over-limit datasets, plus before,
  after, first, last, and stable ordering.
- Confirm cursor wire compatibility.

Gate: live-Postgres boundary tests pass and existing corpus envelopes do not
change except where the old flag was incorrect.

### W2-D — Configuration overflow

Ownership: config parser and config tests only.

- Parse pool minima at 32-bit width or reject values above `MaxInt32`.
- Cover negative, zero, boundary, overflow, and min-greater-than-max behavior.

Gate: invalid values fail at startup with actionable errors.

## Wave 3 — Integration verification

Owner: main agent, assisted by a Luna test worker.

1. Run formatting and `git diff --check`.
2. Run `go vet ./...`.
3. Run `go test ./... -count=1` without database dependencies.
4. Run `go test ./... -race -count=1 -short -p 1` against a dedicated Postgres
   database configured for the required logical-decoding tests.
5. Run focused stress tests repeatedly for rate limiting, deep merge, and
   single-flight.
6. Review the integrated diff for ownership leakage, unnecessary API changes,
   and performance/correctness coupling.

Gate: all mandated checks pass. Environmental skips are reported, not counted
as passes.

## Wave 4 — Documentation and architectural reconciliation

This can run in parallel with the performance-harness design after Wave 3.

Ownership: `README.md`, `tasks/`, architecture/conformance docs, and ADRs only.

- Replace “implementation not started” with a capability/status matrix.
- Mark completed, partial, blocked, and deliberately deferred task items using
  code/test evidence.
- Reconcile direct post-commit notification with the documented WAL-tail
  acknowledgement invariant.
- Record room/presence multi-node limits, admin presence status, Apple OAuth
  status, required-attribute decision, and corpus limitations.
- Separate measured performance facts from inferred improvements.

Gate: no plan or README claim contradicts current code or verification output.

## Wave 5 — Benchmark harness hardening

Ownership: `tools/soak`, benchmark result schema/scripts, CI performance job,
and performance documentation. Keep product hot-path code out of this wave.

1. Add a transaction-ID/semantic-state delivery ledger per expected recipient.
2. Measure commit-to-receipt and convergence latency separately from raw frame
   count; classify V1 coalescing rather than calling it dropped data.
3. Collect target-process CPU, RSS, GC, database, pool, and network samples.
4. Store raw results with V1/V2 SHAs, dirty-state hashes, toolchain/database
   versions, command lines, host settings, schema hashes, and run order.
5. Run at least five randomized paired repetitions and report median, spread,
   and confidence intervals.
6. Add workload shapes for homogeneous and heterogeneous queries, append/update/
   retract/reorder, cold/warm data, slow readers, reconnects, and 300/1k/2k
   subscribers.
7. Keep CI's short soak as a correctness/resource guard; add a separate,
   scheduled or explicitly invoked performance suite with tighter budgets.

Gate: the harness can reproduce a checked-in results bundle and distinguish
delivery, convergence, latency, throughput, CPU, memory, and wire cost.

## Wave 6 — Remeasure before further tuning

- Establish a clean V2 baseline after correctness integration.
- Compare committed HEAD, corrected optimized V2, and pinned V1.
- Use profiles to select the next optimization. Candidate work—catalog indexes,
  JSON encoding, CTE planning, or topic routing—must be justified by measured
  share of CPU/allocations.
- Do not state a percentage gain for the current dirty work until this wave.

Gate: publish only measured ratios with raw artifacts; label architectural and
microbenchmark conclusions separately.

## Suggested agent allocation

| Workstream | Preferred agent/model | Reason |
|---|---|---|
| W1-A rate limiter | `trial_luna_deep_worker` (GPT-5.6 Luna, high/xhigh role setting) | Bounded but concurrency-sensitive |
| W1-B deep merge | `trial_luna_worker` (GPT-5.6 Luna, high) | Clear semantic matrix and isolated ownership |
| W1-C single-flight | `trial_luna_worker` (GPT-5.6 Luna, high) | Small sync primitive plus tests |
| W2-A permissions | `trial_luna_worker` (GPT-5.6 Luna, high) + main security review | Mechanically small, security-sensitive integration |
| W2-B required attrs | `trial_terra_analyst` first if ambiguous, then Luna | May require schema/public-compatibility contract |
| W2-C pagination | `trial_luna_worker` (GPT-5.6 Luna, high) | Bounded query/test change |
| W2-D config | `trial_luna_worker` (GPT-5.6 Luna, high) | Very small parser/test task |
| Integration tests | `trial_luna_test_worker` | Focused execution and log reduction |
| Documentation | `trial_luna_worker` or main agent | Evidence-driven, no architecture decision authority |
| Benchmark design | `trial_terra_analyst`; Luna implements accepted contract | Cross-version methodology needs moderate analysis |

Sol is not required for routine implementation. Use `trial_sol_security` only
if the permission/required-attribute work exposes an unresolved authorization
or data-integrity decision, and `trial_sol_architect` only if direct-notify versus
WAL-tail semantics require a public compatibility change.
