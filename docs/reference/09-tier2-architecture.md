# Tier 2 Architecture Spec — scale-out, incremental results, plane split

Goal: break v2's single-node ceiling and the remaining recompute-per-write
cost, so capacity scales with node count instead of box size. Builds directly
on Tier 1 (docs/08): query groups render once per generation; the drain loop,
checkpoint coalescing, and group cap accounting are the substrate this spec
modifies.

Baseline for all claims: post-T1.1 rerun (docs/archive/01-state.md) — 2000 sessions ×
8 tx/s at ~31% CPU / ~156 MB RSS. Tier 2 targets: N-node session capacity with
zero cross-node state beyond Postgres, and O(change) refresh cost for hot
match-all queries.

Non-goals: wire-format changes visible to frozen SDKs; Postgres physical
layout changes (partitioning stays behind an ADR — see T2.5).

## Reconciled implementation status (2026-08-28)

The Tier 2 mechanisms listed here are now present in the current checkout:

| Item | Status | Evidence / boundary |
|---|---|---|
| T2.1 backpressure | Implemented | `Notifier.Gate`, transact/HTTP wiring, queue-depth health, and gate tests. |
| T2.2 WebSocket compression | Implemented | Configurable compression modes and handshake tests; default remains disabled. |
| T2.3 read/write pools | Implemented | `cmd/instantd` separates pools; defaults are 32 max per pool and 8 warm per pool, so one DSN can account for 64 max / 16 warm connections. |
| T2.4 invalidation bus | Partial | `internal/bus` LISTEN/NOTIFY publisher, supervised listener, payload degradation, and raw bus tests exist; the two-Store end-to-end peer-refresh acceptance test is still missing. |
| T2.5 incremental maintenance | Implemented for bounded scope | Top-level eligible queries splice known changes; uncertain/nested/paginated/aggregate cases fall back to full refresh; randomized and live-DB differential tests exist. |

These are implementation statuses, not capacity claims. The optional bus
replicates invalidations, not writes, ordering, rooms, or presence. Comparative performance
figures elsewhere in this document are historical smoke/soak evidence and
must be replaced or confirmed by the Wave 4 contract and Wave 6 paired run.

---

## T2.1 Backpressure enforcement

### Problem (resolved in the current implementation)

Historically, `Notifier.QueueDepth()` existed but nothing consumed it. One pathological
publisher (1000 tx/s into a 2000-member query) melts the drain loop before
the per-class rate limiter notices — the limiter counts requests, not
outstanding refresh work.

### Design

- `Notifier.Gate(maxDepth int64) func(appID string) error` — hysteresis gate:
  opens (deny) at `>= maxDepth`, closes (allow) below `maxDepth/2`. Denial is
  `*ShedError{RetryAfter}` (250 ms fixed). Atomic loads only; no locks on the
  transact path.
- `sync.Deps.TransactGate func(appID string) error` — nil means always allow.
  `handleTransact` consults it first; denial produces the same 429-shaped
  error frame the rate-limit gate already emits (`gates_test.go` shapes).
- HTTP `POST /runtime/transact` consults the same gate → 429 + Retry-After.
- `/health` gains `"queue-depth"` (and `"queue-max"` when gated) so overload
  is observable from the load balancer.

Config: `INSTANT_V2_MAX_QUEUE_DEPTH` (default `0` = unlimited = today's
behavior).

### Acceptance

Test: flood N Notify into a stalled group until depth > max → gate denies;
drain below low-water → gate reopens. Health JSON shows depth.

---

## T2.2 Wire compression

### Problem (resolved in the current implementation)

Refresh envelopes are large repeated-structure JSON (460 KB match-all shape).
Per-frame gzip would help; WebSocket permessage-deflate with context takeover
beats it because the LZ77 window persists across frames of the same query.

### Design

`INSTANT_V2_WS_COMPRESSION` ∈ {`disabled`, `no-context-takeover`,
`context-takeover`} (default `disabled` — byte-identical behavior for every
existing client and proxy). Maps onto coder/websocket's `CompressionMode`.
Applies to `/runtime/session` only; SSE bodies are short-lived admin frames.

Ordering vs deltas: delta-refresh (already shipped behind the ≥0.23.0 gate)
reduces bytes ~4000× where negotiated; deflate covers everyone else. They
compose — deflate after delta still wins on highly-repetitive trees.

### Acceptance

Handshake test: client offering `permessage-deflate` against
`context-takeover` mode gets the extension accepted; `disabled` mode never
negotiates it.

## T2.3 Read/write plane separation

### Problem (resolved in the current implementation)

Instaql reads and the transactor share one pool (`main.go`, MaxConns≥32).
Soak evidence: snapshot queries queue behind invalidation-driven refreshes;
a slow read stalls the write path's connection acquisition.

### Design

Two pgx pools in assembly:

- **Write pool** (`DATABASE_URL`): storage.DB, transact, authn writes,
  checkpoint persistence, and the CatalogCache — attr-id identity must not
  lag the transactor behind a replica, and catalogs change rarely (the cache
  absorbs steady-state reads either way).
- **Read pool** (`INSTANT_V2_READ_URL`, defaulting to `DATABASE_URL`):
  instaql Executor refreshes — the hot read plane.

Pool sizes: `INSTANT_V2_WRITE_POOL_MAXCONNS` /
`INSTANT_V2_READ_POOL_MAXCONNS` default to 32 each. `INSTANT_V2_POOL_MINCONNS`
defaults to 8 for each pool. With both URLs pointing at one Postgres, the
defaults therefore permit up to 64 pooled connections and warm up to 16;
operators must size `max_connections` for the combined total (plus migrations,
LISTEN, and other clients).

With `INSTANT_V2_READ_URL` unset this changes nothing except pool accounting;
with a replica DSN it routes reads off the writer immediately.

Explicitly deferred: partitioning triples by app_id hash. It breaks the
v1-parity layout that makes corpus replay and v1-dump import trivial, and no
fleet exists yet that needs it. First parity-breaking change gets its own ADR.

### Acceptance

Assembly test/config unit tests; soak unchanged with knobs defaulted.

---

## T2.4 Horizontal scale-out via invalidation bus

### Problem (implementation present; acceptance incomplete)

v2 is single-node by construction: post-commit notifier notifies *its own*
store only. A second node serving the same app never hears about writes
committed elsewhere.

### Design decision — symmetric bus, not lease-sharded slots

The audit sketched app_id-hash sharding with advisory-lock slot ownership.
That design assumes WAL-slot-based invalidation; v2's production path is the
direct post-commit notifier (the tailer is not wired into main), and sessions
are self-contained by deliberate design (02 §5.3). Given those facts:

- **Write correctness remains an explicit boundary.** Postgres allocates
  transaction identities and enforces row constraints, but the current code
  does not provide an app-specific distributed mutex or ordering protocol.
  LISTEN/NOTIFY only propagates invalidation events; it cannot make arbitrary
  node writes equivalent to serialized writes.
- **Fanout scales symmetrically**: each node refreshes only its local
  subscriptions; cost per node is proportional to local members.
- A future app-level lock/lease may provide ordering, but needs fencing and
  crash-recovery tests before it can be used as a correctness claim.

So the current scale-out mechanism is a **symmetric invalidation bus**: every
node may carry local subscriptions for every app, but the bus is only an event
transport. Stickiness is useful operationally; it is not a correctness proof
for cross-node writes.
Lease-based shard ownership becomes interesting only if/when the WAL-tailer
path replaces direct notify (it dedupes replay work across nodes); recorded
as follow-up, not built now.

### Mechanism

Postgres LISTEN/NOTIFY (no new infra; NATS/Redis stay swappable later behind
`internal/bus`'s interface):

- New package `internal/bus`: `Publisher.PublishInvalidation(ctx, appID,
  attrIDs, txID)` and `Run(ctx, conn, channel, onEvent)` (self-echo included;
  delivery is idempotent because refreshes are watermark-deduped).
- Payload `{app-id, attr-ids, tx-id}`; NOTIFY caps at 8000 bytes, so payloads
  over ~7.4 KiB degrade to attr-ids `null` = "invalidate all local subs of
  app" (boring, correct, rare).
- Assembly (`main.go`) when `INSTANT_V2_INVALIDATION_BUS=postgres`:
  - `OnCommit` bridges publish to the bus **and** notify locally (local path
    stays zero-latency);
  - a listener goroutine feeds received events into `notifier.Notify`;
  - dedicated connection (LISTEN state dies with the connection — pgxpool
    must NOT be used for LISTEN).
- Rooms/presence remain node-local (documented limitation); sticky LB by
  app-id keeps room participants colocated.

Config: `INSTANT_V2_INVALIDATION_BUS=none|postgres` (default none),
`INSTANT_V2_NODE_ID` (default hostname, appears in logs/health for ops).

### Acceptance

Raw payload and truncation behavior are covered by unit tests. The planned
integration test (skips without TEST_DATABASE_URL, same pattern as waltail)
would use two Stores over one DB and prove commit on A → B subscriber refresh;
that two-Store peer-refresh acceptance test is not present yet. Even after it
lands, it would prove event delivery, not cross-node write serialization.

---

## T2.5 Incremental result maintenance

### Problem

Every invalidation recomputes the full InstaQL result. For match-all queries
under append-heavy workloads the result barely changes per write: cost is
O(result) where O(change) would do. This is the ceiling-raiser that makes the
485%-CPU class of workload collapse even without client negotiation.

### Design

Incremental engine **inside reactive, behind the existing Refresh seam**
(instaql stays the oracle):

- Change records flow with invalidations: `Change{Etype, EntityID string,
  AttrIDs []string}`. Sources: WS `transact` (resolved steps know entity +
  attr), HTTP transact handler, admin bridge (same info). New
  `Notifier.NotifyChanges(ctx, appID, changes, txID)`; legacy `Notify`
  delegates with unknown changes → full recompute (unchanged behavior).
- `Subscription` gains optional materialized state per top-level form:
  ordered entity-id list + cached field maps. Engine applies a ChangeSet:
  - **update-in-place** (entity known member): batch-fetch changed entities'
    rows (reuse instaql's batched loader), splice, re-render;
  - **create/delete**: targeted membership check (run the form's WHERE for
    that one entity) before insert/remove;
  - **bail-outs to full refresh**: nested forms, pagination cursors,
    aggregates, order ops, unknown changes, engine uncertainty of any kind.
- Emission is identical either way: the rendered envelope feeds the same
  group dispatch path, so clients cannot tell which path produced bytes.
- Correctness invariant: full refresh remains the oracle. Property test runs
  randomized workloads through incremental-only and full-refresh-only paths
  and diffs envelopes byte-wise; any bail-out divergence is a bug.

Scope guard: v1 targets the dominant economics case (top-level forms, no
cursor/aggregate/order). Everything else silently takes today's path.

### Acceptance

Fuzz differential (incremental == full) plus benchmark: match-all group under
8 tx/s append load — target ≥80% fewer instaql CTE executions vs post-T1.1.

---

## Sequencing

T2.1 → T2.2 → T2.3 → T2.4 are independent of T2.5 and land first (days);
T2.5 lands last (weeks-scale review surface). Every step lands green under
`go test -race ./...`; final verification reruns the comparative soak and the
groups fanout benchmark.

## Summary table

| # | Item | Effort | Payoff |
|---|------|--------|--------|
| T2.1 | Backpressure gate + health gauge | ~1 d | Overload survival |
| T2.2 | permessage-deflate knob | ~½ d | ~10× wire on big envelopes |
| T2.3 | Read/write pools (+replica URL) | ~1 d | Writes isolated from read storms |
| T2.4 | Invalidation bus (LISTEN/NOTIFY) | ~2–3 d | Cross-node invalidation propagation; write ordering remains open |
| T2.5 | Incremental maintenance | 2–3 wk | O(change) refresh cost |

## T3 — Production readiness (landed this phase)

### T3.1 Observability

`internal/metrics` owns a dedicated Prometheus registry served on
`INSTANT_V2_METRICS_ADDR` (default `127.0.0.1:9465`; empty disables). Series:
`ws_sessions_active`, `ws_refresh_frames_total{kind=full|delta}`,
`fanout_bytes_total`, `refreshes_total{outcome=spliced|full|error}`,
`notifier_queue_depth`, `notifier_sheds_total`, `transact_duration_seconds{plane=ws|runtime|admin}`,
`ratelimit_rejections_total{class}`, `bus_publish_errors_total`,
`bus_events_received_total`, `bus_malformed_total`, `db_pool_conns{pool,state}`.
Gauges over live state (queue depth, pools, session count) are scrape-time
callbacks — no polling goroutines. OpenTelemetry spans cover the
transact→commit→fanout chain (`transact.{ws,runtime,admin}` →
`notifier.refresh`, which spans the synchronous group fanout); export is
opt-in via `OTEL_EXPORTER_OTLP_ENDPOINT`, otherwise the no-op provider costs
nothing.

### T3.2 Security pass

- `CheckAdminToken` now fetches the app's tokens and compares constant-time
  in Go (`subtle.ConstantTimeCompare` over normalized hex); the SQL-side
  `WHERE token=$1` compare leaked match timing through B-tree descent.
- `/backup/*` is mounted (was unreachable dead code) behind the same admin
  token check; object keys are force-namespaced `<app-id>/<key>` — the old
  raw-key form let app A read/overwrite app B's dumps; zip entries are capped
  (1 GiB uncompressed) against decompression bombs.
- Known gap (documented, not fixed): v2 has no rules persistence, so the WS
  transact plane passes a nil rule doc and perms are enforced only by the
  `perms-check` dry-run endpoints. Wiring the `rules` table is a follow-up.

### T3.3 Conformance + differential

- `corpusctl --mode differential` runs v2 and v1 side-by-side and diffs the
  canonicalized frame sequences. Environment-volatile fields (trace-id,
  server-hostname/port, isn) are dropped; tx watermarks masked; attrs masked
  (v1 always sends them, v2 skips for skip-attrs clients). The driver spaces
  c2s frames 120 ms apart — v1's grouped-queue races back-to-back frames
  against session init.
- Result against a healthy local v1 (sunset commit `a4d2ef33`, booted via its
  self-host compose; the historical "v1 delivers 0 live refreshes" baseline
  was environmental misconfiguration — v1's push works when
  `wal_level=logical` + `WAL_HISTORY_STORAGE=pg` are set):
  `EQUAL` on smoke and transact→refresh scenarios, including the pushed
  refresh carrying the new triple.
- Server fixes the differential/visual pass forced: real `add-attr` handling
  (create + adopt-by-ident with step rewriting — the TS SDK mints attr ids
  client-side and references them in-batch), `<etype>/id` triples emitted in
  join-rows (clients assemble entities from the primary-key triple),
  `primary?` flag on id attrs, deterministic `WireAttrs` ordering, node data
  without v1-absent `etype`/`k` keys.

### T3.4 Soak + comparative performance

300-session / 3-minute soak against a single instantd: 390 000 refreshes
delivered (3 000/s), 1 299 WS transacts (10/s), 54.8 GB fanned out,
99.9% of drains served by incremental splices, 0 sheds / 0 drops / 0 bus
errors. Head-to-head (200 admin writes each, same box, both pushes verified):

| server | tx p50 | tx p95 | push p50 | push p95 |
|--------|--------|--------|----------|----------|
| v1     | 5.05ms | 6.42ms | 3.88ms   | 5.47ms   |
| v2     | 0.82ms | 1.13ms | 0.04ms   | 0.06ms   |

v2's push is the direct post-commit notify; v1 routes through its WAL
replication slot (~4 ms — respectable for a WAL roundtrip).

### Follow-ups

- Corpus driver: multi-connection/phase grammar + value capture for
  reconnect-resume and cross-session presence scenarios (links / perms-check
  / presence / resume scenarios are sketched in this phase's notes but not
  landed).
- Rules persistence (`rules` table) wired into the WS + runtime transact
  planes.
