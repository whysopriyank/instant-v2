# Product performance headroom

Status: implementation decision record, 2026-08-29. This document covers
product-runtime performance only. It does not change the benchmark contract,
claim eligibility, artifact handling, or the frozen client protocol.

The purpose of this document is to separate improvements that are reasonable
to implement now from architectural changes whose potential gain is larger but
whose correctness, rendering, authorization, or operational risk needs a
separate design and rollout.

## 1. Executive decision

Work is divided into two tracks:

1. **Track A — bounded product optimization:** no new security boundary and
   low-to-moderate correctness or operational risk. Track A contains both
   simple direct improvements and higher-gain changes that can be protected by
   byte parity, full-refresh fallback, or configuration rollback. This is the
   active implementation track.
2. **Track B — architectural optimization:** potentially large gains, but the
   change can affect query equivalence, rendering, transaction visibility,
   authorization, delivery ordering, or recovery semantics. Track B is
   documented but deferred until separately approved.

The most valuable Track A sequence is:

1. Correct coalesced queue accounting.
2. Reduce invalidation-routing allocations.
3. Collapse the two triple-write statements into one database-authoritative
   statement.
4. Measure fused incremental membership and entity loading; retain it only if
   the one-statement plan beats the existing two-query path. Current evidence
   rejects it.
5. Replace generic node-list map construction with a typed, parity-pinned
   encoder.
6. Measure immutable query-preparation work and adopt a compiled-plan cache
   only if it reaches 5% of refresh CPU or a meaningful allocation share. The
   current A2 evidence rejects the cache at roughly 0.18% of refresh time.
7. Consider persistent/adaptive refresh workers only after the preceding stage
   timings identify scheduler or queueing pressure.

The highest theoretical gain remains patch-first materialization, but it stays
in Track B because it changes the state and recovery model of query results.

## 2. Evidence and claim language

Three evidence classes are used throughout this document:

- **Measured:** observed directly in a named benchmark or profile.
- **Inferred:** code-path evidence indicates a cost, but the proposed change
  has not been benchmarked.
- **Plausible:** an engineering estimate used to prioritize experiments. It is
  neither an acceptance target nor a performance claim.

The plausible ranges below are stage-local unless explicitly described as
end-to-end. A 30% faster rendering stage does not make the server 30% faster
when PostgreSQL or network delivery dominates the workload.

### 2.1 Current measured headroom

Identical product microbenchmarks were run three times on the same Apple M4 Pro
against historical V2 `f23bb787` and current product revision `2e64b1b7`:

| Product path | Historical V2 median | Current V2 median | Current change |
|---|---:|---:|---:|
| Full match-all refresh | 11.46 ms | 8.26 ms | about 28% faster |
| Incremental match-all append | 7.26 ms | 6.53 ms | about 10% faster |
| Delta generation over 10k results | 25.71 ms | 25.98 ms | about 1% slower; effectively parity |
| Shared dispatch to 100 members | 357.8 us | 390.7 us | about 9% slower |
| Shared-dispatch allocation | about 326 KB | about 397 KB | about 22% higher |
| Large-frame rendering | 170.2 us | 178.3 us | about 5% slower |

The current incremental benchmark still allocates approximately 5.8 MB and
57,000 objects per operation. Delta generation over 10,000 results allocates
approximately 13.7 MB and 80,000 objects. Allocation profiles identify
full-result cloning, JSON reconstruction, node-list construction, frame
assembly, and byte cloning as the dominant removable costs.

The live H300 comparison has only one attempt per V2 revision. Current V2 was
about 2% worse in global convergence p99 and 0.36% worse in throughput, while
using about 2.2% less CPU and 7.2% more peak RSS. That result is useful as a
diagnostic observation, but cannot distinguish a regression from run-order or
host variance. It must not be used to promise or reject an optimization.

### 2.2 A1 checkpoint (2026-08-29)

A bounded before/after checkpoint compared baseline `5f78ca0` with the A1
working tree on the same Apple M4 Pro. Routing benchmarks used identical
in-process stores with 300 subscriptions and five 500 ms samples per case.
Storage benchmarks used an isolated loopback PostgreSQL 17.11 cluster and
alternated baseline/treatment blocks, with six samples per revision.

| Stage | Baseline median | A1 median | Change |
|---|---:|---:|---:|
| Coalesced routing, duplicate topics, 300 subscriptions | 90.85 us, 36.25 KB, 25 allocs | 62.94 us, 9.52 KB, 13 allocs | 30.7% faster; 73.7% fewer bytes; 48% fewer allocations |
| Coalesced routing, unique topics, 300 subscriptions | 76.15 us, 36.25 KB, 25 allocs | 62.62 us, 9.43 KB, 12 allocs | 17.8% faster; about 74% fewer bytes; 52% fewer allocations |
| Single cardinality-one triple write | 204.29 us, 2.94 KB, 69 allocs | 174.45 us, 3.17 KB, 47 allocs | 14.6% faster; 31.9% fewer allocations; about 7.8% more Go bytes |
| Full match-all refresh | 7.67 ms | 7.66 ms | parity within noise |
| Incremental match-all append | 6.45 ms | 6.40 ms | parity within noise |

The full live `internal/storage` package passed against the isolated database,
covering overwrite counts, many-cardinality deduplication, unique violations,
nulls, deletes, statement limits, copy, and transaction journal behavior.

This checkpoint verifies stage-local A1 movement and no obvious match-all
regression. It does not establish an end-to-end convergence, throughput, CPU,
RSS, or wire improvement; those remain for the post-A2 comparative run.

### 2.3 A2 checkpoint (2026-08-30)

One evidence-backed A2 product change is implemented in the working tree; two
other candidates were measured and rejected:

- Fused incremental membership-plus-payload loading is rejected. The original
  inlined CTE plan took about 304 ms because PostgreSQL rescanned the membership
  side 1,800 times. A materialized CTE still measured about 18.7 ms. The best
  one-statement rewrite fixed the SQL plan but measured 4.27 ms end to end
  versus 3.46 ms for the existing two-query path, while allocating more. The
  production path therefore remains `Members` followed by `Entities`; no fused
  code or deterministic-order behavior change is retained.
- Node-list construction now has a typed common path with the original generic
  encoder retained as the compatibility oracle and fallback. On an Apple M4
  Pro, five repeated ASCII-safe 300-entity samples measured a median of about
  1.23 ms, 1.12 MB, and 15,223 allocations for the generic encoder versus
  1.09 ms, 0.93 MB, and 9,029 allocations for the typed path: about 11% faster,
  17% fewer allocated bytes, and 41% fewer allocations. This is a component result,
  not an end-to-end convergence claim. Seventy-two fixed-seed bounded
  differential cases pin byte parity for numeric spellings, nulls, nested
  values, refs, multiple etypes, unknown attrs, page/aggregate data, and
  fallback-triggering strings/depth.

An uncontended bracketed regression check compared baseline `5f78ca0` with the
combined A1+A2 working tree on the same Apple M4 Pro. Baseline medians were
about 9.17 ms for full match-all refresh and 6.81 ms for incremental append.
The working tree measured 8.88/9.24 ms in the two surrounding full-refresh
blocks (parity within run variance) and 6.52/6.65 ms for incremental append
(about 2–4% faster). This is a regression guard for the combined working tree,
not an end-to-end performance claim.

The proposed compiled-plan cache is rejected for A2 by its measured adoption
gate. Five Apple M4 Pro samples of an intentionally complex eight-predicate
eligible query measured about 16.5 us, 17.3 KB, and 301 allocations for the
complete removable preparation path: JSON decode, query coercion, condition
construction, datalog-plan construction, and SQL generation. Against the
measured full refresh this is approximately 0.18% of time and 0.23% of allocated
bytes; incremental refresh repeats only the roughly 7.6 us condition/SQL half.
This is more than 25 times below the documented 5% adoption threshold before
paying for cache keys, locking, lookup, and eviction.

The current catalog cache also has no generation token, and an invalidation can
race with an in-flight catalog load that later republishes stale state. A safe
shared plan cache would therefore require a cross-component catalog-generation
contract for less than 0.25% theoretical headroom. No cache code is included in
A2. `BenchmarkQueryPreparationHeadroom` remains as regression evidence; revisit
the decision only if a production profile shows preparation at or above 5%.

### 2.4 Gains already realized

Historical V2 already contains the largest obvious asymptotic wins:

- batched entity loading instead of database N+1 reads;
- one logical query group for identical subscriptions;
- one render and encode per group generation;
- shared immutable bytes fanned to group members;
- bounded concurrent refreshes;
- incremental maintenance for eligible flat queries;
- delta delivery for clients that negotiate it.

The previous V1-to-historical-V2 order-of-magnitude result came from removing
subscriber-count and query-count multipliers. Further gains of the same order
require another asymptotic change, not a collection of small allocation
optimizations.

## 3. Risk model

Each item is classified across five dimensions:

| Dimension | Question |
|---|---|
| Correctness | Can results, ordering, pagination, counts, or transaction watermarks become wrong? |
| Rendering/UX | Can the frozen SDK render differently, flicker, reconnect, or remain stale? |
| Security | Can authorization, tenant isolation, or transaction visibility change? |
| Operations | Can the change overload PostgreSQL, increase memory, or reduce slow-client isolation? |
| Rollback | Can the optimization be disabled without migrating persistent data or changing the wire protocol? |

Severity meanings:

- **Low:** local implementation risk; existing semantics remain authoritative;
  focused tests and a simple revert or configuration rollback are sufficient.
- **Moderate:** incorrect implementation can affect latency or output, but an
  existing oracle/fallback can prevent exposure.
- **High:** the optimization changes a correctness, authorization, ordering,
  recovery, or persistent-state boundary. Shadow comparison alone is not a
  sufficient rollout argument.

## 4. Track A — bounded product optimization

Track A changes do not intentionally alter client-visible semantics or create
a new authorization boundary. They are the recommended implementation scope.

### 4.1 A1: simple direct improvements

| Item | Plausible gain | Correctness | Rendering/UX | Security | Operations | Overall |
|---|---|---|---|---|---|---|
| Correct coalesced queue accounting | 0–15% burst throughput; no expected steady-state gain | Low | Low | None | Low | **Low** |
| Deduplicate invalidation topics before store lookup | 5–20% routing CPU for duplicate-heavy commits | Low | Low | None | Low | **Low** |
| Reuse bounded routing scratch storage | 5–15% routing allocation reduction | Low | Low | None | Moderate memory-retention guard | **Low** |
| Collapse cardinality write paths into one DB-authoritative statement | one avoided DB round trip; plausibly 5–30% mutation latency for write batches | Low–moderate | None | None | Low | **Low–moderate** |
| Tune existing WebSocket compression by configuration | plausibly 3–10x fewer full-frame wire bytes | Low | Low compatibility risk | None | CPU tradeoff | **Low–moderate** |

#### A1.1 Queue accounting

`Notifier.enqueue` currently increments the queue gauge for every qualifying
notification, including a subscription already represented in `pending`.
`drainPass` subtracts only the number of unique pending subscriptions. Under a
coalesced burst the gauge can therefore exceed real queued work and activate
the shedding gate prematurely.

Implementation contract:

- increment the gauge only when a subscription first enters `pending`;
- preserve the newest transaction watermark and change-set merging;
- retain a non-negative exact unique-pending gauge across enqueue/drain races;
- add a burst regression covering repeated notification of the same group.

This is the best first change because it corrects product accounting and can
improve overloaded behavior without touching query or wire semantics.

#### A1.2 Invalidation routing

`NotifyChanges` flattens attribute IDs and `SubsForTopics` constructs a seen
map and sorted subscription slice for each notification. Repeated attribute
IDs and overlapping topic memberships amplify this work.

Implementation contract:

- deduplicate attribute IDs before taking the Store read lock;
- preserve deterministic subscription ordering;
- keep scratch buffers bounded and clear references before reuse;
- demonstrate reduced allocations at equal routed-subscription cardinality.

#### A1.3 Single-round-trip triple batches

The storage path historically executed separate cardinality-one and
remaining-value statements for every insert batch. Pre-partitioning from the
in-memory catalog is not safe because a catalog can briefly be stale after an
attribute-cardinality change while PostgreSQL remains authoritative. The safe
optimization is one statement with a shared input/enhanced CTE, disjoint
cardinality-one and remaining-value data-modifying CTEs, and a final count of
both results.

Implementation contract:

- derive cardinality routing from the authoritative `attrs` row in the same
  statement, not from the catalog cache;
- preserve input index semantics for duplicate cardinality-one triples;
- pre-encode every input value before execution so validation remains
  all-or-nothing;
- keep one-cardinality, many-cardinality, and mixed batches row-equivalent;
- measure database statement count and transaction duration, not only Go CPU.

#### A1.4 Compression is a deployment optimization

WebSocket compression is already implemented and disabled by default. It can
substantially reduce full-frame bytes, but it exchanges network cost for CPU.
It should be enabled as a per-environment experiment before adding new product
code. Delta-capable traffic must be measured separately because its payloads
may be too small to benefit.

### 4.2 A2: higher-gain, bounded-risk improvements

| Item | Plausible gain | Correctness | Rendering/UX | Security | Operations | Overall |
|---|---|---|---|---|---|---|
| Fuse membership and entity payload read | 15–40% incremental refresh latency when DB round trips dominate | Moderate | Low with full-refresh fallback | None | Query-plan risk | **Moderate** |
| Typed node-list encoder | 1.5–3x render stage; 30–60% fewer dispatch allocations | Moderate | Moderate, parity-pinned | None | Low | **Moderate** |
| Cache compiled query/incremental plan per group | 5–20% refresh CPU for complex forms | Moderate invalidation risk | Low | Low if rule identity remains part of key | Bounded-cache requirement | **Moderate** |
| Persistent bounded refresh workers | 5–20% at many dirty groups; negligible for one shared group | Low | Low | None | Moderate DB/fairness risk | **Moderate** |

#### A2.1 Fused incremental read

The incremental path currently performs a membership query followed by an
entity-payload query for every touched entity type. One SQL statement can
return candidate membership plus the projected triples needed for current
members, under one statement snapshot.

Safety boundary:

- the fused result must match the current two-query outcome for committed
  states;
- uncertainty or unsupported query forms must fall back to the full refresh;
- the existing full executor remains the correctness oracle;
- no authorization decision moves into this optimization.

This is a product latency optimization, not a security redesign.

Decision: **rejected for A2**. Three bounded SQL formulations were measured on
an H300-like fixture. The original and materialized CTEs produced pathological
nested-loop plans; the best independent-branch statement was still about 23.5%
slower than the existing two-query path and allocated materially more. The
legacy path remains authoritative and the experimental fused code was removed.

#### A2.2 Typed node-list encoding

`BuildNodeList` now uses a bounded typed structural encoder for its common path,
keeping values as `json.RawMessage` while writing the fixed node-list shape
directly. This avoids the generic map/interface output tree that dominated the
old construction stage. The original generic encoder remains the byte-level
oracle and compatibility fallback.

The accepted implementation is not the earlier naive raw-value string-splicing
experiment. That experiment repeatedly copied and marshaled individual values
and was slower. The implemented path performs typed bounded traversal and one
amortized output build, falling back whenever exact `encoding/json` string
rewriting would be required.

Safety boundary:

- frozen-SDK node-list shape remains unchanged;
- integer and decimal literals remain exact;
- `null`, empty arrays, cardinality-many values, refs, implicit ID triples,
  page information, and aggregate placement remain equivalent;
- the retained generic encoder is the byte-level oracle in deterministic
  fixture and bounded property differentials;
- unsupported escaping or nesting falls back to the generic encoder, and the
  source-level rollback is the small isolated `BuildNodeList` change rather
  than a permanent runtime branch.

Performance boundary: the measured fast-path gain applies to the ASCII-safe
H300-like fixture. Non-ASCII, escaped, control, and HTML-sensitive strings use
the generic fallback; correctness remains identical, but workloads dominated by
those values may receive little or no encoder speedup.

#### A2.3 Compiled plan cache — measured no-go

Query classification, condition compilation, and incremental plan construction
are repeated for stable query groups. Cache only immutable compiled structure;
bind app values and candidate IDs per execution.

Adoption requires preparation to consume at least 5% of refresh CPU or a
meaningful allocation share. The A2 checkpoint measured only about 0.18% of
full-refresh time and 0.23% of allocated bytes in an intentionally complex
eight-predicate case, so no cache is implemented. If future production profiles
cross the gate, the cache key must include every semantic input, including
query wire class, admin/public mode, catalog generation, and rule identity;
entries then need hard count/byte bounds and explicit generation invalidation.

#### A2.4 Persistent/adaptive workers

`drainPass` creates a goroutine per dirty group and uses a semaphore with a
fixed concurrency of 16. A persistent worker pool can bound scheduler churn,
but increasing concurrency without evidence can make PostgreSQL contention and
p99 worse.

Adoption gate:

- measure at multiple distinct-query cardinalities;
- report queue delay separately from query execution;
- cap workers below available read-pool capacity;
- preserve per-subscription transaction ordering and fairness;
- reject the change if it only moves time from the queue into database waits.

## 5. Track B — deferred higher-risk architecture

Track B contains improvements with material correctness, rendering,
authorization, or ordering exposure. Gain size does not make them suitable for
the initial direct implementation wave.

| Item | Plausible gain | Primary risk | Security severity | Overall risk | Decision |
|---|---|---|---|---|---|
| Patch-first materialized state with lazy full rendering | 3–10x delta-path CPU/allocation for large result/small change workloads | stale/wrong order, pagination or aggregate divergence, recovery model | Low directly | **High** | Deferred |
| Post-image invalidation payloads | 2x+ incremental gain across many distinct groups | commit visibility, event ordering, data exposure | **High** | **High** | Deferred |
| Semantic query canonicalization/group merging | multiplier equal to eliminated duplicate groups | incorrectly treating different queries or authorization contexts as equal | **High** if isolation key is incomplete | **High** | Deferred |
| Asynchronous or sharded fanout completion | 20–60% tail improvement when delivery blocks refresh | out-of-order frames, unbounded queues, stale slow clients | Low directly | **High** | Deferred |
| Batched permission projections/validation | 20–50% transaction stage for large permissioned batches | allow/deny semantic divergence | **High** | **High** | Deferred |
| Structural sharing of persistent query state | 30–70% less refresh allocation in favorable shapes | retained memory, reader lifetime, rollback/recovery complexity | Low directly | **High** | Deferred |

### 5.1 Why patch-first remains deferred

The current incremental engine produces a complete envelope and retains the
full executor as an obvious oracle. A patch-first engine changes the primary
state representation and can avoid full-result cloning, rendering, and
old-versus-new diffing. That is the closest remaining opportunity to another
order-of-magnitude gain.

It also introduces difficult cases:

- custom ordering after an ordering-key mutation;
- `limit`, offset, cursors, and page-boundary repair;
- aggregate count changes;
- nested and reverse links;
- permission-dependent membership;
- dropped or out-of-order invalidations;
- reconnect and full-snapshot recovery;
- mixed groups containing delta and full-frame clients.

An eligibility-limited version could eventually begin with flat,
ID-ordered, non-paginated, non-aggregate, non-nested queries. It still needs a
separate state-machine design, differential model, and rollout plan.

### 5.2 Why post-image invalidations remain deferred

Including committed entity post-images in invalidations could remove database
reads from many query-group refreshes. It also moves data across a new event
boundary. The design must prove:

- the payload is emitted only after commit;
- every consumer applies transaction watermarks monotonically;
- unauthorized fields cannot reach a public group;
- oversized or partial events degrade safely;
- reconnect/replay obtains an authoritative state;
- multi-node delivery cannot apply a newer post-image and then an older one.

Those are security and data-integrity concerns, not routine performance work.

### 5.3 Why permission batching remains deferred

Set-based permission evaluation may materially shorten large write
transactions, but a subtle mismatch can authorize a forbidden mutation or
deny a valid one. It should not be combined with ordinary hot-path tuning.
Any future implementation requires an explicit equivalence model and a
security-focused review.

## 6. User rendering and UX contract

Track A must not intentionally change anything visible to current clients.
The following invariants are mandatory:

- identical query semantics and membership;
- stable entity ordering and pagination fields;
- exact integer and decimal preservation;
- equivalent `null`, empty-list, ref, and cardinality-many behavior;
- correct implicit ID triples and frozen node-list structure;
- monotonically advancing processed transaction IDs;
- no update delivered after a newer update for the same subscription;
- slow clients reconnect or recover explicitly rather than silently missing
  state;
- full-refresh fallback remains available whenever optimized execution is
  uncertain.

A violation can appear to users as list flicker, missing entities, wrong
counts, reordered pages, numeric corruption, repeated reconnects, or a UI that
remains stale. Performance acceptance therefore requires semantic evidence,
not only lower wall time.

## 7. Track A implementation waves

### Wave A1 — accounting and direct waste removal

1. Correct unique-pending queue accounting.
2. Deduplicate topic IDs and reduce routing allocation.
3. Collapse triple insert branches into one database-authoritative statement.
4. Measure optional WebSocket compression using existing configuration.

Acceptance:

- focused race tests for notifier and storage paths;
- no protocol fixture change;
- exact queue gauge under coalesced bursts;
- equal affected-subscription and database-row outcomes;
- before/after allocation, statement-count, and latency evidence.

### Wave A2 — bounded higher-gain changes

1. Measure fused incremental membership and payload reads; reject if the
   one-statement path does not beat legacy. Current evidence rejects it.
2. Implement typed node-list encoding under parity comparison.
3. Measure compiled-plan headroom; add a bounded cache only at or above the 5%
   adoption gate. Current evidence rejects it.
4. Evaluate a persistent worker pool only if profiles still show scheduling
   or queue delay.

Acceptance:

- full executor and current encoder remain test or runtime oracles;
- unsupported cases fall back instead of approximating;
- frozen node-list bytes remain equivalent to the existing generic encoder,
  which is the server-side input consumed by the unchanged SDK rendering path;
- no new authorization or tenant-isolation decision is introduced;
- each change is measured independently before combination;
- a change is removed if its end-to-end gain is lost to CPU, RSS, database
  contention, or delivery latency elsewhere.

## 8. Measurement matrix

Every Track A change should report the smallest relevant matrix:

| Dimension | Required values |
|---|---|
| Result size | small, H300-like, and at least one large-result shape |
| Subscribers | 1, 300, and a higher scale where locally practical |
| Query diversity | one shared group and many distinct groups |
| Mutation shape | homogeneous append, update, retract, and mixed batch where relevant |
| Client mode | full frame; delta when supported; compressed full frame where tested |
| Metrics | convergence p50/p99, transaction latency, throughput, CPU, allocation, RSS, SQL statements/time, wire bytes, failures |

Microbenchmarks establish stage-local movement. A paired product run establishes
end-to-end movement. Neither should be substituted for the other.

## 9. Decision summary

- Start with Track A1. Its changes are direct, reversible, and do not alter
  query or client contracts.
- Continue to Track A2 only one optimization at a time, using full-refresh or
  old-encoder parity as the safety boundary.
- Do not mix permission changes, post-image events, semantic group merging, or
  patch-first state into the low-risk implementation wave.
- Treat every percentage in this document as measured only where explicitly
  labeled; all proposed gains remain hypotheses until a before/after run
  validates them.
