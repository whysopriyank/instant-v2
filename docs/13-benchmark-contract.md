# Wave 4 — Comparative Benchmark Contract

Status: Wave 5 harness implemented and accepted; Wave 6 measurement has not
run.

This document is the binding contract for the hardened V1/V2 benchmark. It is
deliberately more demanding than the historical smoke and soak runs. A worker
may implement the harness only within the interfaces and ownership boundaries
below. A result is not a performance claim unless it satisfies this contract.

## 1. Audit conclusion and non-negotiable decisions

The existing measurements are useful diagnostic evidence, but they do not
support one headline such as “V2 is N times faster.” The main defects in the
old evidence were:

- V1 was once run without the required `wal2json` output-plugin setting, making
  its local fanout appear inert.
- V1 coalesces invalidation waves. A lower frame count can therefore mean
  semantic coalescing, not dropped data.
- The old driver counted frames and writes but did not maintain an expected
  recipient-by-transaction semantic ledger.
- Warm-up, schema, exact server/database revisions, run order, and raw artifact
  provenance were not all pinned in one bundle.
- A single short run cannot characterize tail latency, resource cost, or
  variance across workload shapes.

The following decisions are frozen for Wave 5:

1. Semantic delivery and final convergence are the correctness oracle. Frame
   count is a diagnostic metric only.
2. The driver has a dedicated writer, a client event id, and a benchmark
   transaction key. If a protocol exposes a server transaction id, it is
   recorded; the harness must not require a new public protocol field.
3. The harness records submit-to-cover and an acknowledgement-bounded
   commit-to-cover interval. It must retain refresh-before-ack evidence and
   must not fabricate an exact commit timestamp or pretend that an observation
   after the acknowledgement is a database commit timestamp.
4. V1 and V2 use separate databases with the same PostgreSQL version, settings,
   seed state, workload, host, and run protocol. The production V2 invalidation
   mode is recorded explicitly and is not described as pgoutput-driven merely
   because pgoutput is tested independently. V1 must qualify with healthy
   `wal2json` fanout.
5. The fixed comparative matrix is eight workload families, `H/X/M/O/S/R/C/T`,
   each at 300, 1,000, and 2,000 subscribers. These are named families, not a
   Cartesian product of topology and behavior dimensions. V2 full-vs-delta is
   a separate benchmark family.
6. Seven balanced, seeded AB/BA paired repetitions are publishable for each
   workload. Five pairs are preliminary/interrupted evidence; fewer than five
   is smoke evidence. Run exactly seven attempts; replacement attempts are
   forbidden. Target failures remain in the
   result set and are visible in the claim gate.
7. Raw artifacts are immutable inputs to aggregation. A report must be
   reproducible from the raw bundle without contacting either server.
8. Correctness failures stop a workload's claim, while target crash, timeout,
   resource exhaustion, semantic failure, and poor performance remain recorded
   outcomes rather than exclusion criteria.
9. Wave 5 changes the harness and reporting only. It does not tune product hot
   paths or change protocol behavior.

## 2. Terminology and semantic oracle

- **Run**: one server revision, database, workload cell, and process lifetime.
- **Pair**: two runs of one cell, one V1 and one V2, with the same seed and
  randomized server order.
- **Mutation**: one logical change submitted by the dedicated writer. A
  transaction containing several low-level operations has one client event id
  and one expected post-transaction state.
- **Recipient**: a subscriber in the expected-recipient set for the mutation.
  The writer records the expected query set and recipient set before submit;
  the harness does not infer either from the number of received frames.
- **Receipt**: a parsed WS/SSE message accepted by the client protocol adapter.
- **Convergence**: the recipient's canonical semantic state equals the expected
  state for the latest committed mutation applicable to that subscription.
- **Coalescing**: one receipt proves multiple mutations, or an intermediate
  overwritten state is not separately observed, while the recipient still
  converges to the required latest state. Coalescing is not a drop.
- **Drop**: a submitted mutation with a successful commit acknowledgement for
  which an expected recipient neither receives a receipt proving the required
  semantic transition nor converges by the cell deadline. A target crash,
  timeout, or resource failure is retained as that target failure as well;
  neither category is silently converted into a drop or removed from results.
- **Protocol error**: malformed, unauthorized, out-of-order, or otherwise
  unparseable server behavior that the adapter cannot classify as a valid
  receipt.

The ledger is keyed by `(pair_id, run_id, writer_id, recipient_id,
client_event_id)`. Each row contains at least:

```text
pair_id, run_id, writer_id, recipient_id
client_event_id, server_transaction_id (nullable)
expected_query_set, expected_recipient_set
submitted_at, acknowledgement_at, cover_at (nullable)
processed_transaction_id (nullable)
expected_materialized_digest, observed_materialized_digest (nullable)
expected_state_version, observed_state_version (nullable)
coverage: exact | coalesced | converged_without_intermediate | none
refresh_before_ack, buffered_snapshot_ahead
converged_at (nullable), error_class (nullable), evidence_ref
```

The dedicated writer creates a deterministic client event id (for example,
`bench/<run-id>/<sequence>`) and writes a unique marker into the benchmark
fixture's dedicated value. The marker is never treated as a transport-level
transaction id. The server transaction id is recorded when observable, but it
is nullable and cannot be fabricated. The client protocol adapter records the
processed transaction id when the target exposes it.

The adapter canonicalizes each refresh by sorting entities and attributes
according to the protocol's stable ordering, removing only documented volatile
fields, and hashing the resulting materialized query tree. It stores the digest
and a reference to the redacted event evidence, not secret-bearing raw full
frames. The prefix oracle replays the ordered mutation log and computes the
expected materialized digest for every expected query. If a snapshot arrives
ahead of the processed transaction id, the adapter buffers that snapshot and
reconciles it against the prefix oracle once the corresponding event is known;
it must not mark the event lost merely because delivery and acknowledgement
were reordered.

For append/retract workloads, each mutation has a durable marker and can be
proved independently. For update/reorder workloads, the expected state is the
post-transaction state. A receipt covering the exact prefix is `exact`; a later
receipt whose materialized digest covers multiple expected events is
`coalesced`; a final snapshot that proves the expected prefix after buffering is
`converged_without_intermediate`. The report shows mutation count, exact and
coalesced coverage, and final convergence separately.

## 3. Timing interval and lifecycle

All durations use a monotonic clock in the driver or recipient process. Wall
clock timestamps are included only for correlation. The run lifecycle is:

```text
qualify → provision → seed → ramp subscribers → settle → warm-up
→ measured mutations → drain/convergence grace → final snapshot → teardown
```

The writer records `submitted_at` immediately before submission and
`acknowledgement_at` when the protocol acknowledgement arrives. It records
`cover_at` when the first valid receipt or converged materialized snapshot
proves the event for that recipient. The harness does not claim an exact commit
instant. The primary intervals are:

- **Submit-to-cover**: `cover_at - submitted_at`.
- **Acknowledgement-bounded commit-to-cover**: the interval
  `[max(0, cover_at - acknowledgement_at), cover_at - submitted_at]`, with
  the ordering evidence retained. If a refresh covers the event before the
  acknowledgement arrives, set `refresh_before_ack=true` and retain both
  timestamps; do not rewrite the observation to make acknowledgement precede
  refresh.
- **Commit-to-convergence**: reported only as that bounded interval, not as an
  exact timestamp, to the first canonical state equal to the expected state,
  including a receipt that coalesces prior mutations.
- **Receipt processing time**: frame arrival to canonicalization and ledger
  update; this is reported separately and never subtracted from delivery
  latency.
- **Reconnect recovery**: reconnect initiation to a valid converged snapshot;
  it is a separate scenario metric, not mixed into steady-state latency.

Initial snapshot delivery, ramp-up, warm-up, teardown, failed qualification,
and writes issued before the settle barrier are excluded from steady-state
latency distributions. The artifact still records their counts and durations.

Default timing controls for Wave 5 are explicit command-line/configuration
values, not hidden constants: 30 s subscriber ramp, 60 s warm-up, 180 s
measured phase, and 30 s convergence grace after the last committed mutation.
The cold-process family has no warm-up and measures readiness, initial
convergence, and then 60 s at 8 tx/s. The saturation family has a ten-minute
hard limit. Any override is recorded.

## 4. Required workload families

The required matrix is the fixed set of eight workload families
`H`, `X`, `M`, `O`, `S`, `R`, `C`, and `T`, each run at exactly 300, 1,000,
and 2,000 subscribers. These are named scenarios, not a Cartesian product of
independent dimensions; the family definition is part of the contract and may
not be recombined by a worker.

| Family | Fixed scenario |
|---|---|
| `H-append` | Empty seed; identical match-all query; unique entity append. |
| `X-heterogeneous` | 8,192 entities; 64 deterministic bucket cohorts; append rotating across buckets. |
| `M-mixed` | 4,096 entities; 40% update, 20% append, 20% retract/delete, 20% reorder. |
| `O-reorder` | 4,096 entities; ordered top-50 window; rank changes move entities into/out of the window. |
| `S-slow-reader` | 1,024 entities; match-all update workload; deterministic 10% of clients pause application reads for 2 s every 10 s. |
| `R-reconnect` | 1,024 entities; match-all update workload; deterministic 10% cohort reconnects every 30 s with recorded 0.5–1.5 s backoff. |
| `C-process-cold` | 8,192 entities; 64 cohorts; fresh target process, no warm-up; measure readiness, initial convergence, then 60 s at 8 tx/s. |
| `T-saturation` | 8,192 entities; commuting updates; 4,096 fixed operations through eight closed-loop writers; ten-minute hard limit. |

Families H/X/M/O/S/R use the declared global writer rate (8 scheduled tx/s)
through a blocking scheduler that records schedule slip. C uses the same rate
only after its cold readiness/initial-convergence phase. T is intentionally
not rate-controlled: its eight closed-loop writers execute 4,096 fixed
operations to measure saturation throughput. Every family records its exact
query assignment, mutation sequence,
subscriber scale, timing controls, and seed. `C` and `T` differ only in the
declared cold/warm lifecycle; they are not silently merged into another family.
A family can be marked `not_applicable` only with a recorded reason approved by
the main agent.

V2 full-refresh versus delta-refresh is a separate benchmark family with its
own fixture, oracle, and report. Its byte/reconciliation result must not be
combined with the eight comparative V1/V2 sync families.

The fixture contains a stable set of entities, attributes, and query groups.
Seed counts, entity ids, attribute ids, query definitions, and initial semantic
hashes are part of the artifact. No cell may add an unrecorded random fixture
or select a different query after qualification.

The historical 5,000-session/30-minute V2-only soak remains a resource guard,
not a required comparative V1/V2 cell. It cannot establish a comparative
speedup.

## 5. Target qualification

Before a pair starts, the harness must write a qualification record and fail
closed if any required check fails:

- V1 and V2 revisions are resolved commits. The V1 default is the pinned
  `a4d2ef33` unless Wave 6 explicitly records another reviewed ref. V2 records
  the exact commit and dirty-state hash; dirty product trees are rejected for
  the claim-producing run.
- Both servers pass health/readiness checks, expose the expected protocol
  endpoints, and complete a four-subscriber live-refresh probe. The primary
  endpoints are WS `GET /runtime/session`, SSE `GET /runtime/sse` plus
  `POST /runtime/sse`, and the admin control paths `POST /admin/transact`,
  `POST /admin/query`, and `POST /admin/subscribe-query` as applicable to the
  family. The adapter records which endpoint is under test.
- V1's `wal2json` output plugin is available and its replication path is
  healthy. V2's production invalidation mode is recorded explicitly (currently
  post-commit notification/direct invalidation, with optional peer bus); an
  independently verified `pgoutput` tailer must not be described as the
  production driver. A plugin or slot failure is a target/qualification
  failure, not zero delivery.
- Each side uses a newly provisioned database with the same PostgreSQL major/
  minor version and logically equivalent benchmark schema, fixture, and seed.
  Each target's distinct migration-history hash and physical-schema hash are
  recorded separately; they are not required to match. Extension differences
  required by the target and relevant settings (`wal_level`, output-plugin
  allowlist, connection limits, timeouts, replication settings, and locale/time
  zone) are recorded.
- The host, OS/kernel, CPU model/count, memory, power/performance mode, swap
  state, container/runtime limits, Go/JVM/toolchain versions, and environment
  variables are recorded. No unrelated load is permitted.
- File-descriptor and process limits, available disk, and database pool sizes
  meet the cell's declared requirements. Clocks are monotonic and the host has
  no wall-clock jump during a run.
- The client driver, protocol adapter version, command line, random seed,
  run order, and configuration file hash are recorded.

Qualification output is retained even when it fails. A failed pair remains in
the attempted-pair result set and is never repeated or removed to improve a
headline. A metric with no observation has no median; the claim gate fails
rather than substituting a value.

### Target roles

| Role | Meaning |
|---|---|
| `v1` | Healthy pinned V1 at `a4d2ef33`, qualified through its `wal2json` path. |
| `v2_current` | Clean pinned Wave 6 HEAD. |
| `v2_reference` | Separately pinned pre-optimization V2 commit, only if it passes the same semantic preflight; `6f5b715` is a candidate to qualify, not an assumed baseline. |
| `v2_candidate` | Optional future optimization commit, qualified through the same gate. |
| Dedicated writer | The only mutation issuer; owns client event ids and expected prefix-log state. |
| Readers | Deterministic subscriber clients that record protocol events and materialized digests. |
| Collectors | Read-only process, database, network, and artifact observers. |

V1 full versus V2 full is the primary compatibility comparison. V2 delta is
compared only with V2 full at the same SHA. Targets with incompatible failure
or retry behavior are reported rather than normalized away.

### Three-target reference mode

When a historical V2 reference is configured, the canonical target identities
are `v1`, `v2_reference`, and `v2_current`. The harness runs seven blocks with
each target exactly once per block. The schedule contains all six target
permutations once plus one deterministic seed-selected repeat, so every target
occupies each execution position two or three times. It produces 21 target run
artifacts and two independent seven-observation comparisons:
`v1-v2_current` and `v2_reference-v2_current`. The reference is never used as
the V1 baseline. A reference qualification failure remains visible in every
affected block and makes both the global and relevant comparison gates
ineligible.

## 6. Metrics and instrumentation

The harness records raw observations before aggregation.

Correctness and semantic metrics:

- submitted mutations, acknowledgement outcomes, and post-run durable-state
  audit outcomes;
- expected recipient count;
- valid receipts by recipient;
- delivered, coalesced, converged-without-intermediate, dropped, protocol-error,
  and infrastructure-error counts;
- convergence deadline and final per-recipient semantic hash;
- reconnect success, resnapshot correctness, and duplicate/out-of-order events.

Latency and throughput metrics:

- submit-to-cover p50, p90, p95, p99, p99.9, maximum, and sample count;
- acknowledgement-bounded commit-to-cover lower/upper interval distributions;
- receipt-processing latency;
- committed tx/s, semantic receipts/s, converged recipients/s;
- frame count, payload bytes, semantic bytes, and bytes per converged recipient;
- write acknowledgement latency, initial snapshot latency, reconnect recovery,
  and queue/backpressure durations.

Resource metrics are sampled at a fixed recorded interval (default 1 s) and
include target CPU, RSS, heap/GC where available, goroutine/thread count,
database CPU/RSS, active/idle pool connections, transaction latency, WAL/slot
lag, network bytes, and process file descriptors. Samples include source and
collection errors. A missing optional sample is `unavailable`, never zero.

Mandatory target samples include PID/start time, executable and child-process
hashes, cumulative user/system CPU, current/peak RSS, thread and file-descriptor
counts, exits, and signals. Go runtime samples include allocation bytes, live
heap/goal, GC count, and pause total; JVM samples use JMX/JFR or `jstat` for
heap and GC. Database before/after deltas include `pg_stat_database`,
`pg_stat_wal`, connections, block hits/reads, temp bytes/files, commits and
rollbacks, tuple reads/writes, and `pg_stat_statements` when installed. V2
Prometheus deltas include sessions, refresh frames/outcomes/bytes, queue depth,
sheds, transaction histograms, bus events/errors, pool states, and rate-limit
rejections. Application payload bytes are mandatory; transport bytes require
an isolated network namespace/cgroup or are `unsupported`. Profile runs are
diagnostic and excluded from headline pairs.

## 7. Artifact schema and reproducibility

Each bundle writes this shape:

```text
benchmarks/results/<bundle-id>/
  manifest.json
  plan.json
  environment.json
  run-order.json
  targets/{v1,v2-current,v2-reference}.json
  runs/<run-id>/
    run.json
    ledger.jsonl.zst
    frames.jsonl.zst
    process.jsonl.zst
    runtime-metrics.jsonl.zst
    db-before.json
    db-after.json
    stdout.log
    stderr.log
  summary.json
  report.md
  checksums.sha256
  raw-index.json
```

`manifest.json` must include schema version, pair/run/family/scale identifiers,
V1/V2 SHAs and dirty hashes, source tree paths, executable hashes, command
lines, random seeds, AB/BA order, host and database identifiers,
PostgreSQL/toolchain versions, schema/fixture/config hashes, start/end times,
and artifact hashes. Three-target manifests additionally record the seven
`target_order` blocks, exact `target_revisions`, and explicit `comparisons`.
The raw bundle is an immutable, content-addressed bundle
of metadata, event ids, timing observations, semantic digests, and references;
it is not a dump of full frames. Secrets, access tokens, cookies, DSNs
containing credentials, and user data must not be stored. Redaction is
performed before writing the bundle and is itself recorded. Any optional
debug payload must be a separately approved, fixture-only redacted fragment;
the default and claim-producing artifact contains metadata and digests only.

The result schema distinguishes missing, zero, not-applicable, and failed
measurements. `summary.json` records the exact aggregation code version and
input file hashes. A report generator must reject an artifact with missing
required provenance, mismatched hashes, mixed schema versions, or a failed
correctness gate.

## 8. Run protocol, randomization, and repetitions

For each family/scale, create one fixture and one isolated database per target.
Two-target mode runs exactly seven paired repetitions; three-target reference
mode runs exactly seven three-target blocks and derives two independent
pairwise comparisons from those same blocks. A recorded seed
balances the treatment order across the seven pairs (AB/BA); the same seed also
controls subscriber assignment and mutation ordering. Five pairs are
preliminary/interrupted evidence, and fewer than five is smoke evidence. Run
exactly seven attempts; replacement attempts are forbidden. Target crash,
timeout,
resource exhaustion, semantic failure, and harness failure remain in the
attempted-pair result set and in the report.

Re-provision and reseed between sides; do not reuse connections, replication
slots, caches, or a dirty database. The report shows every attempted pair,
including failures and missing metrics, rather than excluding outcomes that
make a comparison look worse.

Within a run:

1. Validate the target and fixture hash.
2. Start server and collectors; wait for readiness.
3. Connect subscribers with deterministic ids and query assignments.
4. Verify all initial snapshots and wait for the settle barrier.
5. Execute warm-up mutations using a separate sequence range; drain and reset
   the ledger before measurement.
6. Execute measured mutations at the resolved global rate. Record every write
   attempt, client event id, response, server transaction id when observable,
   and expected query/recipient sets.
7. Allow the full convergence grace period. Query the database for the final
   expected state and compare every recipient.
8. Stop collectors only after flush acknowledgement, validate artifact hashes,
   and tear down the isolated target.

The pair runner must avoid alternating V1/V2 within one server process. In
three-target mode, all three targets are run once per block and the target
permutation is the only randomized treatment variable. CPU affinity, server
flags, database settings, and client counts remain fixed across the block.

## 9. Statistics and claim thresholds

Primary endpoints are p99 upper bounds for recipient and global semantic
convergence, CPU core-ms per 1,000 affected-recipient coverages, peak RSS per
active subscriber and absolute peak RSS, application wire bytes per affected-
recipient coverage, and committed throughput for `T-saturation`. In
three-target mode report separate paired `v2_current/v1` and
`v2_current/v2_reference` ratios plus absolute values,
sample counts, every pair value, and distribution spread. Ratios use log-ratio
aggregation. Compute the 95% confidence interval with at least 10,000 seeded
bootstrap resamples over pair-level observations, and report the paired
sign-test result alongside the interval. The bootstrap seed and method are
part of the artifact.

The harness may publish “faster”, “lower”, or “improved” only when all of these
hold:

- exactly seven balanced paired repetitions exist for the family/scale, with
  the AB/BA seed and all seven attempted outcomes visible;
- no semantic correctness gate is violated in the result being claimed;
- qualification and provenance are complete and raw artifacts are available;
- the claimed metric has a reported 95% confidence interval and its unit,
  denominator, and sample count are explicit;
- the paired sign result and effect direction are reported. Target failures,
  missing observations, and poor performance block a claim rather than being
  replaced or excluded.
- the median paired ratio is in the claimed direction, the 95% paired interval
  excludes 1.0, no scale cell contradicts it, and no workload/outlier was
  selected post hoc.

“Faster” must name the metric and scenario. “Lower frame count” is never a
delivery claim. A ratio is not promoted to a release/capacity claim unless
Wave 6 explicitly accepts the target, matrix weighting, and baseline revisions.
Microbenchmarks and V2-only soaks remain separate evidence classes.

## 10. Budgets and failure taxonomy

Budgets are explicit per invocation and stored in `config.json`. The
The short CI gate uses 150 subscribers, 10 s ramp, 30 s warm-up, 60 s
measurement at 8 tx/s, homogeneous append, final convergence within 20 s, at
least 95% of scheduled writes submitted, all submitted writes resolved, zero
semantic/protocol failures, p99 convergence upper bound at most 10 s, and peak
server RSS at most 1 GiB. It is a correctness/resource guard, not comparative
evidence. Once a candidate baseline exists, convergence, CPU/coverage,
RSS/session, and wire/coverage median ratios must be no worse than 1.05 with a
paired 95% upper bound no worse than 1.15; saturation throughput median must be
at least 0.95 with a paired 95% lower bound at least 0.85. V1/V2 remains
descriptive, not a release gate.

Every run has exactly one primary class:

```text
pass
setup_invalid
harness_defect
target_semantic_failure
target_protocol_failure
target_resource_exhaustion
target_crash
target_timeout
infrastructure_noise
operator_abort
```

Budget failures and detailed causes are secondary fields. Infrastructure noise
requires preserved independent evidence; high target CPU, GC, memory, or
latency is target performance, not infrastructure noise.

The summary preserves the first failure and all subsequent observations.
Every target crash, timeout, resource exhaustion, semantic failure, and poor
performance remains in the attempted result set. No failure is excluded or
replaced to obtain seven attractive pairs; a claim gate fails when its required
evidence is unavailable.

## 11. Destructive and security guards

The harness binds to loopback by default and uses only disposable databases
whose names begin with `instant_bench_`. It requires an explicit benchmark
marker before any reset and verifies the resolved database identity and marker
before dropping or truncating anything. It must refuse production-looking DSNs,
must not reset a shared/public database, and must not alter shared Postgres
configuration. Database reset is allowed only for the explicitly provisioned
benchmark instance.

Artifact output must remain under the resolved bundle root; path traversal,
symlink escape, and writes outside that root are rejected. Decompression is
bounded by declared compressed and expanded-byte limits. Logs are bounded and
redact DSNs, cookies, auth headers, tokens, and payload fields outside the
dedicated fixture markers. Credentials are supplied through the environment or
secret manager, never command-line arguments or artifacts. Resource,
connection, duration, and log limits prevent malformed configuration from
creating an unbounded process, connection, or disk load. A failed safety check
aborts before provisioning.

## 12. Wave 5 implementation contract

Wave 5 is harness hardening, not product optimization. The three Luna work
packages are exact and disjoint:

| Package | Exclusive write scope | Required result |
|---|---|---|
| `WP5-A` | `internal/benchharness/**`, `tools/soak/**`, `tools/soaksetup/**` | Pair runner, protocol adapters, deterministic H/X/M/O/S/R/C/T workloads, semantic ledger/prefix oracle collection, qualification, timing, resource sampling, and safety guards. |
| `WP5-B` | `internal/benchrun/**`, `tools/benchrun/**`, `tools/benchreport/**`, `benchmarks/schema/**` | Versioned result/ledger types, artifact validation, canonicalization, log-ratio aggregation, 10,000-bootstrap CI, paired sign result, and offline report generation. |
| `WP5-C` | `.github/workflows/ci.yml`, `.github/workflows/performance.yml`, `Makefile`, `benchmarks/README.md`, `docs/14-benchmark-running.md` | CI/no-regression guard, explicit benchmark commands, scheduled/explicit performance entry point, and operator documentation. |

No two workers may edit a path in another package. The main agent owns
integration and any required shared assembly decision. No worker may add a
product protocol field or alter `internal/reactive`, `internal/sync`,
`internal/transact`, storage, or server behavior. A cross-scope need is
escalated before editing.

The public worker handoff must state the exact resolved configuration and
artifact paths. Workers stop after two focused failed attempts and return the
failure taxonomy and evidence rather than weakening a gate.

## 13. Luna test-worker acceptance

After the implementation workers finish, a Luna test worker runs read-only
acceptance. It may add no product code and owns no overlapping implementation
files. It must:

1. run unit/property tests for `internal/benchharness`, `internal/benchrun`,
   `tools/soak`, `tools/soaksetup`, `tools/benchrun`, and `tools/benchreport`
   under `-race` where supported;
2. run a tiny two-target synthetic pair with deterministic fake receipts and
   prove delivered, coalesced, dropped, protocol-error, and infrastructure
   classifications;
3. run artifact replay: delete access to both targets, regenerate the report
   from raw artifacts, and compare the report hash;
4. test malformed manifests, mixed schema versions, missing provenance,
   secret-redaction failures, hash mismatches, unsafe DSNs, and budget failures;
5. execute one short live V2 smoke only if an isolated database is available;
6. return commands, pass/fail status, artifact hashes, and residual risks.

The main agent then runs the repository-wide verification and performs a
read-only review for ownership leakage, public-interface changes, and claims
that exceed this contract.

## 14. Wave 6 gate and deferred decisions

Wave 6 is measurement only. It begins only after Wave 5 passes its harness and
test-worker acceptance. It must:

1. freeze the V1 reference, V2 baseline, optional V2 candidate, fixture, schema,
   host, database settings, and artifact schema;
2. qualify both targets, including V1 `wal2json` preflight and the explicitly
   recorded production V2 invalidation mode;
3. run seven balanced seeded AB/BA pairs for every H/X/M/O/S/R/C/T family at
   300, 1,000, and 2,000 subscribers;
4. retain all attempted outcomes, compute prefix-oracle coverage, submit-to-
   cover and bounded commit-to-cover intervals, resource metrics, and wire
   metrics from the raw bundle;
5. aggregate paired log ratios with at least 10,000 seeded bootstrap resamples,
   report the confidence interval and paired sign result, and apply the claim
   gate; and
6. review the raw bundle, no-regression budgets, and deferred decisions before
   publishing any headline. It may not silently tune code while measuring.

The following three decisions remain intentionally deferred until Wave 6:

1. Which dedicated benchmark host to use and whether isolated Linux cgroup/
   network-namespace transport measurement is available.
2. Whether to run the complete 24-cell parity matrix immediately or publish a
   smaller predeclared core subset first.
3. Whether historical pre-dedupe V2 commit `6f5b715` should be qualified as
   `v2_reference`; the harness must reject it if it fails semantic preflight.

### Superseded interpretations

The following historical statements remain useful context but are superseded as
claim methodology:

- “V1 delivered zero live refreshes” was an output-plugin environment failure,
  not healthy V1 behavior.
- “V1 delivered fewer refreshes” is not automatically “V1 dropped data”; its
  invalidation aggregator coalesces waves and must be judged by convergence.
- The historical post-dedupe figures (47× more V2 refreshes, roughly 94% lower
  V2 CPU/RSS in that run, and roughly 44× lower per-delivered-update CPU) are
  single historical smoke comparisons, not hardened estimates.
- The approximately 4,285× delta-refresh wire reduction is a focused V2
  microbenchmark, not a server throughput or V1/V2 speedup.
- The 5,000-session, 30-minute V2-only soak proves only the recorded V2 guard;
  it is not a comparative capacity result.

Wave 6 may confirm, narrow, or reject these observations. Until then, reports
must call them historical or diagnostic evidence.
