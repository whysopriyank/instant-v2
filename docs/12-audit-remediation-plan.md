# Audit Remediation Plan

Status: Waves 0–3 complete; Wave 4 benchmark contract complete; Wave 5
implementation and Wave 6 measurement remain open. Based on the 2026-08-28
committed-code, working-tree, architecture, conformance, and performance audit.

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

## Wave 0 — Preserve the baseline — COMPLETE

Owner: main agent. No parallel product edits yet.

1. Record `git status`, HEAD, and the dirty diff without modifying user work.
2. Preserve minimal reproductions for:
   - rate-limiter lock inversion and cap oversubscription;
   - deep-merge scalar/map transition divergence;
   - single-flight double-close panic;
   - malformed deep-merge live tests.
3. Provision a dedicated disposable Postgres database and record which suites
   require `DATABASE_URL` or logical-decoding configuration.

Evidence: baseline state/diff were recorded, deterministic reproductions were
preserved, and the dedicated logical-decoding Postgres prerequisite was
documented and verified.

Gate: complete.

## Wave 1 — Parallel correctness blockers — COMPLETE

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

## Wave 2 — Parallel committed-code gaps — COMPLETE

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

Evidence: permission projection now fails closed, required attributes and
backup compatibility are implemented, pagination uses the corrected boundary
semantics, and configuration overflow is rejected. Focused tests, race tests,
and the integrated verification record are in the completed commits; any
remaining environmental prerequisite is reported rather than treated as a
pass.

## Wave 3 — Integration verification — COMPLETE

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

Evidence: formatting/diff checks, `go vet ./...`, the no-database suite, the
serialized live-Postgres race suite, repeated focused stress suites, and the
deterministic SSE overflow regression all passed. The disposable PostgreSQL
cluster was stopped and removed.

Gate: complete.

## Wave 4 — Documentation, architecture, and benchmark contract — COMPLETE

The status and architecture reconciliation is complete. The implementation-ready
comparative benchmark contract is [docs/13-benchmark-contract.md](13-benchmark-contract.md).

Ownership: `README.md`, `tasks/`, architecture/conformance docs, and ADRs only.

- Replace “implementation not started” with a capability/status matrix.
- Mark completed, partial, blocked, and deliberately deferred task items using
  code/test evidence.
- Reconcile direct post-commit notification with the documented WAL-tail
  acknowledgement invariant.
- Record room/presence multi-node limits, admin presence status, Apple OAuth
  status, required-attribute decision, and corpus limitations.
- Separate measured performance facts from inferred improvements.

Evidence: current capability claims distinguish complete, partial, blocked, and
deferred work; direct post-commit notification versus independently verified
WAL-tail behavior is explicit; historical performance observations are labeled
as such; the accepted benchmark contract freezes the semantic oracle, timing,
matrix, provenance, statistics, safety, and Wave 5 ownership boundaries.

Gate: complete. Wave 5 must implement docs/13 before Wave 6 measurement starts.

## Wave 5 — Benchmark harness hardening

Exact disjoint Luna work packages and their ownership are defined in
[docs/13-benchmark-contract.md](13-benchmark-contract.md) §12:
`WP5-A` (`internal/benchharness`, `tools/soak`, `tools/soaksetup`), `WP5-B`
(`internal/benchrun`, `tools/benchrun`, `tools/benchreport`,
`benchmarks/schema`), and `WP5-C` (`.github/workflows/ci.yml`,
`.github/workflows/performance.yml`, `Makefile`, `benchmarks/README.md`,
`docs/14-benchmark-running.md`).
Keep product hot-path code out of this wave.

1. Add the dedicated-writer/client-event/server-tx ledger and prefix oracle per
   expected query and recipient.
2. Measure submit-to-cover and acknowledgement-bounded commit-to-cover
   intervals, retaining refresh-before-ack evidence; do not fabricate an exact
   commit timestamp. Classify V1 coalescing rather than calling it dropped data.
3. Collect target-process CPU, RSS, GC, database, pool, and network samples.
4. Store raw results with V1/V2 SHAs, dirty-state hashes, toolchain/database
   versions, command lines, host settings, schema hashes, and run order.
5. Run seven balanced seeded AB/BA paired repetitions for each fixed workload
   family H/X/M/O/S/R/C/T at 300/1k/2k subscribers; five is preliminary and
   fewer than five is smoke evidence. Do not repeat until only valid pairs
   remain; retain target failures and poor performance.
6. Add the fixed workload families and keep V2 full-vs-delta as a separate
   benchmark.
7. Keep CI's short soak as a correctness/resource guard; add a separate,
   scheduled or explicitly invoked performance suite with tighter budgets.

Gate: the harness can reproduce a checked-in results bundle and distinguish
delivery, convergence, latency, throughput, CPU, memory, and wire cost, while
passing the Luna test-worker acceptance in docs/13 §13. Wave 5 is not complete
until that evidence exists.

## Wave 6 — Remeasure before further tuning — MEASUREMENT ONLY

- Establish clean pinned V1 and V2 baselines after correctness integration.
- Execute seven balanced seeded AB/BA pairs for every H/X/M/O/S/R/C/T family at
  300/1k/2k using the accepted intervals, prefix oracle, and no-regression
  budgets in [docs/13-benchmark-contract.md](13-benchmark-contract.md).
- Compute paired log-ratio confidence intervals with at least 10,000 bootstrap
  resamples and publish the paired sign result and claim-gate decision.
- Use profiles to select the next optimization. Candidate work—catalog indexes,
  JSON encoding, CTE planning, or topic routing—must be justified by measured
  share of CPU/allocations.
- Do not state a percentage gain for the current dirty work until this wave.

Gate: publish only measured ratios with raw artifacts; label architectural and
microbenchmark conclusions separately. Wave 6 is not complete and does not
authorize concurrent optimization while the comparison is running.

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
| Wave 4 benchmark contract | Sol contract, main agent/docs owner | Accepted methodology is frozen in docs/13; no worker may improvise interfaces |
| WP5-A harness | Luna worker | `internal/benchharness`, `tools/soak`, `tools/soaksetup` only |
| WP5-B run/report | Luna worker | `internal/benchrun`, `tools/benchrun`, `tools/benchreport`, `benchmarks/schema` only |
| WP5-C CI/docs | Luna worker | `.github/workflows/ci.yml`, `.github/workflows/performance.yml`, `Makefile`, `benchmarks/README.md`, `docs/14-benchmark-running.md` only |
| Benchmark acceptance | Luna test worker | Offline replay, synthetic classifications, safety/provenance checks, and isolated short smoke |

Sol's Wave 4 contract is the authority for benchmark methodology. Sol is not
required for routine Wave 5 implementation. Use a higher-level review only if
Wave 5 exposes a security, data-integrity, or public-compatibility decision;
Wave 6 remains a measurement gate, not an optimization authorization.
