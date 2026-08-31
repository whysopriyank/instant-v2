# 01 — V1 State Assessment

Source of truth: `instantdb/instant` @ `a4d2ef33` ("Instant sunset"), local checkout
`/Users/priyank/Developer/sideproj/instant`. All paths below are relative to that repo.

This document mixes the original v1 assessment with dated v2 experiments. The
inventory and historical measurements below are retained as audit history; they
are not a statement that every listed v1 surface or benchmark gate is complete
in the current checkout. Current implementation status is reconciled in
`README.md`, `docs/plans/04-roadmap.md`, and `docs/guides/05-conformance.md`.

## 1. Inventory

| Layer | Size | Language |
|---|---|---|
| Sync server (`server/src/instant`) | 64,728 LOC / 212 files | Clojure |
| Hand-written JVM helpers (`server/src/java`) | 694 LOC / 8 files | Java |
| Client core (`client/packages/core`) | ~13.4k LOC | TS (untyped `Reactor.js`: 2,836 lines) |
| Framework bindings (react/rn/svelte/vue/solid) | ~1–1.5k each | TS — thin shims over core |

## 2. Server subsystem map (with v2 disposition)

| Subsystem | Key namespaces (LOC) | V2 disposition |
|---|---|---|
| **Datalog→SQL engine** | `db/datalog.clj` (3,500), `db/model/triple.clj` (1,661), `db/model/attr*` (~2,260) | **PORT** — core. Patterns must name an index (`:ea :eav :av :ave :vae`); compiles to one CTE per query via honeysql. |
| **InstaQL front-end** | `db/instaql.clj` (2,326), `util/instaql`, `db/instaql_topic` | **PORT** — parse/coerce, forms→patterns, pagination cursors, aggregates (`:count` only, admin-only at instaql.clj:1190). Dual client impl in `instaql.ts` must stay semantically identical. |
| **Transactor** | `db/transaction.clj` (716), `db/permissioned_transaction.clj` (769), `db/model/transaction`, `model/schema` | **PORT** — tx-steps grammar is frozen public API. |
| **Permissions (CEL)** | `db/cel.clj` (1,907), `db/cel_builder`, `db/proto`, `model/rule.clj` (447) | **PORT** — cel-java → cel-go. Includes AST-walking attr-pattern extraction, custom functions, protobuf request context (`ip`/`origin`/`time`). |
| **WS sync layer** | `reactive/session.clj` (1,584), `reactive/store.clj` (1,835), `reactive/invalidator.clj` (1,026), `reactive/aggregator` (649), `reactive/ephemeral` (658) | **REDESIGN+PORT** — replace DataScript session state with plain structures; keep op semantics byte-compatible. |
| **WAL tailing** | `jdbc/wal.clj` (959), `src/java/instant/jdbc/*` (pgoutput row decoding), `jdbc/failover.clj` (800) | **PORT core / DROP failover** — pglogrepl replaces PgJDBC replication API + hand-written Java decoders. Aurora socket-level writer detection is dropped. |
| **Triple storage** | `jdbc/sql.clj` (684), `jdbc/copy`, `custodian.clj` (605) | **PORT** — pgx; same physical layout initially. |
| **Blob storage (S3)** | `storage/*` (~995), `util/s3`, `util/aws_signature` | **PORT** — aws-sdk-go-v2; signed-URL routes are SDK-coupled (frozen). |
| **Auth** | `auth/oauth` (462), `auth/jwt`, `runtime/magic_code_auth`, `model/app_user` | **PORT** — JWKS RS256/ES256, opaque hashed refresh tokens (verified via InstaQL on `$userRefreshTokens.hashedToken`, model/app_user.clj:155). |
| **Admin/platform API** | `admin/routes` (822), `runtime/routes` (776), `dash/routes.clj` (2,903, 137 routes) | **PORT admin+runtime; DROP dash plane** — only the CLI couples to dash routes. |
| **Cluster plumbing** | Hazelcast membership, `grpc*.clj` (Nippy records, no .proto), `loadbalancer` (SNS/SQS), `work_queue`/`grouped_queue` | **DROP** — single-node v2. Reintroduce as optional HA later behind an interface. |
| **Billing/webhooks/dashboard** | `stripe*`, `webhook_*`, `dash/*` (3,341) | **DROP or defer** — webhooks deferred to Phase 5+. |
| **Ops tooling** | `intern/metrics`, `gauges`, backup/restore (1,118+), flags | **SIMPLIFY** — slog + OTel export; backup/restore deferred Phase 6. |

**Effective port target ≈ 20k LOC of hard logic + ~8k LOC of mechanical API surface;
~45k LOC is droppable for a solo-maintained v2.**

## 3. Hot path (measured shape)

```
write:  WS transact → validate tx-steps → CEL permission check
        → INSERT triples + wal_logs row (one PG txn)
        → WAL tailer sees commit (replication slot, pgoutput)
        → invalidator maps changed attrs → affected subscriptions
        → per-session novelty diff vs cached results → refresh-ok frames

read:   add-query → InstaQL coerce → datalog patterns → one SQL CTE
        → result cached per subscription in reactive store
        → later invalidations re-run CTE and diff against cache
```

Consequence: server CPU is dominated by JSON encode/decode + diffing, not query execution.
Postgres is the throughput ceiling for both v1 and v2.

## 4. Known v1 debts a v2 may repay

1. No formal protocol schema — stringly-typed `{op}` JSON split between `Reactor.js`
   (`_handleReceive`, lines 641–929) and `reactive/session.clj` (dispatch ~984).
2. Full-result `refresh-ok` envelopes (no wire-level delta patching for queries).
3. Untyped 2,836-line `Reactor.js` god object.
4. Cluster mesh complexity (Hazelcast/Nippy-gRPC/SNS) unsuitable for self-hosting.
5. Legacy compat weight in client: `weakHashLegacy`, `StoreJsonVersion0`.
6. Aurora coupling throughout `jdbc/`.

Items 1 and 2 are v2 workstreams; 3–6 are drops/simplifications. The frozen surface in
`docs/reference/03-protocol.md` bounds all of it.

## 5. Feature gates observed in v1 (must replicate)

`reactive/session.clj:73-107` gates by client semver:
`skip-attrs ≥ 0.20.4`, `patch-presence ≥ 0.17.5`, `batch-messages ≥ 0.22.75`.
The `init` op carries a `versions` map keyed by SDK name.


## Historical capacity baseline: v1 vs v2 (2026-08-24)

Identical workload via `cmd/soak` against both servers on the same machine
(Apple M4 Pro), same Postgres 17 instance, separate databases:

    2000 concurrent WS sessions · 60 s ramp · 10 min hold · global write rate 8 tx/s
    (single-entity add-triple into one etype; match-all live query per session)

| metric                        | v1 (a4d2ef33, local boot)     | v2 (2fbdf88+fix)              |
|-------------------------------|-------------------------------|-------------------------------|
| sessions held                 | 2000 / 2000                   | 2000 / 2000                   |
| writes committed (durable)    | 4079 / 4079                   | 4079 / 4079                   |
| drops / protocol errors       | 0                             | 0                             |
| initial snapshots delivered   | 2000                          | 2000                          |
| LIVE refreshes delivered      | **0** (fanout inert locally)  | **65,313** (~124/s peak)      |
| peak server RSS               | ~2.98 GB (JVM)                | ~2.50 GB                      |
| avg server CPU during run     | ~7.5%                         | ~485% (≈5 cores)              |

Notes, stated plainly:

- RESOLVED (2026-08-25): the "inert local fanout" above was environmental, not a
  sunset regression. Postgres 17.11 gates logical-decoding output plugins behind
  `output_plugin_libraries`; v1's `wal2json` slot creation failed with
  `library "wal2json" may not be used as an output plugin` until
  `ALTER SYSTEM SET output_plugin_libraries = 'wal2json'` + reload. After that,
  v1's live fanout works locally (full delivery verified in a 4-session probe).
  The pre-T1.1 v1 column above therefore understates healthy-v1 fanout behavior.
- v2's CPU cost is the price of actually doing the fanout: every write
  invalidates the match-all query held by all 2000 sessions, each getting a
  full recompute + ~460 KB envelope. Coalescing bounds it to ~124 refreshes/s
  aggregate here; per-session coalescing means larger fleets converge toward
  `writes × sessions` only when results keep changing.
- The comparison surfaced a real v2 defect: InstaQL fetched entities ONE
  round-trip PER ENTITY (`loadEntity` N+1). A 4079-entity snapshot took
  unbounded time under pool pressure; fixed with batched `= ANY(...)` loading
  (single query, 64 ms for the full snapshot post-fix).

Verdict (pre-T1.1): at equal connection-hold and durable-write capacity,
memory is comparable; v2 delivers the reactive layer v1's boot could not, at
the cost of proportional CPU. Full 5000×30-min v2-only soak numbers stand
from Phase 4.

### Post Tier-1 rerun (same workload, commit da65259+; historical)

Query-group dedupe changed the picture materially:

| metric                  | v2 baseline | v2 post-T1.1   | delta            |
|-------------------------|-------------|----------------|------------------|
| sessions                | 2000        | 2000           | —                |
| writes committed        | 4079        | 3619           | —                |
| drops                   | 0           | 0              | —                |
| LIVE refreshes delivered| 65,313      | **3,101,127**  | **47× more**     |
| peak server RSS         | ~2.50 GB    | **~156 MB**    | **−94%**         |
| avg server CPU          | ~485%       | **~31%**       | **−94%**         |

The refresh explosion is the story: pre-dedupe, every write demanded 2000
full recomputes (~16k instaql queries/s); the notifier drowned, coalesced
away ~97% of deliveries, and burned ~5 cores doing it. Post-dedupe, a write
costs ONE recompute shared by all subscribers — the drain keeps up, and
clients finally receive the live updates they subscribed for (~857 per
client over the run vs ~32 before). Lower CPU, lower memory, and strictly
more correct delivery.

### Head-to-head smoke soak (2026-08-25, both servers fully functional; historical)

Same machine (M4 Pro), same Postgres 17 (`wal_level=logical`,
`output_plugin_libraries=wal2json`), separate databases, identical workload
shape via `cmd/soak`: 300 WS sessions · 15 s ramp · 3.5 min hold · 8 tx/s
global · match-all live query per session · empty start state.
Three runs per side; every run PASSED with 300/300 sessions held and durably
committed writes of 1,319 of 1,319 sent (SQL-verified).

| metric (write phase, 165 s) | v1 @ a4d2ef33 | v2 @ b276627 |
|---|---|---|
| live refreshes delivered    | ~281k–291k (~72% of ideal 396k; aggregator coalesces waves) | **396,000 / 396,000 (100%, all runs)** |
| steady refresh rate         | ~1,500–2,200/s, uneven | **2,400/s metronomic (= 8 × 300)** |
| avg server CPU under load   | **4.16 cores** | **0.11 cores** |
| peak server RSS             | **~5.2 GB** (ZGC JVM) | **50 MB** |
| cold start to serving       | ~30 s | **~2 s** |

Caveat: v1's lower delivery count is its aggregator coalescing invalidation
waves by design (clients still converge); it is not dropped data. Per-delivery
CPU cost nonetheless differs ~48× (v2 ≈0.3 ms-core vs v1 ≈14.3 ms-core).
Interop note: provisioning a plain blob attr with reverse_etype/reverse_label
set (as v2's `GetOrCreateAttr` parity shape does) makes v1 classify the value
slot as a link and reject string writes — keep reverse columns NULL for blobs
when seeding v1 databases.

### Head-to-head smoke soak II (2026-08-25, v2 @ f23bb78, broader scope; historical)

Same methodology as above, two workload shapes, one paired run each side.
New in this pass: `cmd/soak` now reports write→refresh **delivery lag**
(p50/p99/max) and fails on a p99 budget (`-max-p99-lag`). All runs PASSED;
all 1,319 writes per run SQL-verified durable on both sides.

| metric | Workload A: 300 sess · 8 tx/s · 3.5 min | Workload B: 1000 sess · 8 tx/s · 3.5 min |
|---|---|---|
| v2 refreshes / ideal            | **396,000 / 396,000 (100%)** | **1,320,000 / 1,320,000 (100%)** |
| v1 refreshes / ideal            | 319,377 (80.6%)              | 428,732 (**32.5%**) |
| v2 refresh rate                 | 2,400/s metronomic           | 8,000/s metronomic |
| v1 refresh rate                 | ~1,700–1,800/s uneven        | ~2,850/s uneven |
| v2 CPU under load / peak RSS    | 0.11 cores / 53 MB           | 0.31 cores / 100 MB |
| v1 CPU under load / peak RSS    | 3.72 cores / 5.4 GB          | 4.33 cores / 5.8 GB |
| v2 lag p50 / p99 / max          | 12 / 34 / 161 ms             | 26 / 71 / 84 ms |
| v1 lag p50 / p99 / max          | 81 / 369 / 671 ms            | 190 / 1,108 / 1,713 ms |

Findings: (1) The 18 commits b276627..f23bb78 (stability waves 2–4, perms
enforcement, cross-node catalog invalidation, keepalives) cost nothing on the
hot path — v2's delivery profile is byte-for-byte identical to the previous
audit and latency is now measured, not inferred. (2) v1's effective delivery
share *falls* as watcher count grows (80% → 32%) while its lag triples; v2
holds 100% with p99 <100 ms at both scales. (3) Per-delivered-update CPU at
the 1000-session scale: v2 ≈0.23 ms-core vs v1 ≈10.1 ms-core (~44×).

The preceding tables are useful historical observations, but their environments,
commits, workload drivers, and run counts do not satisfy the hardened Wave 6
methodology. In particular, they must not be converted into a single “v2 is N×
faster” headline. The accepted methodology must pin server/database commits,
schema, run order, warm-up, concurrency, delivery/convergence criteria, raw
artifacts, and statistical summaries before a new comparative claim is made.
