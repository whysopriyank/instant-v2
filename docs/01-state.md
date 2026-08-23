# 01 — V1 State Assessment

Source of truth: `instantdb/instant` @ `a4d2ef33` ("Instant sunset"), local checkout
`/Users/priyank/Developer/sideproj/instant`. All paths below are relative to that repo.

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
`docs/03-protocol.md` bounds all of it.

## 5. Feature gates observed in v1 (must replicate)

`reactive/session.clj:73-107` gates by client semver:
`skip-attrs ≥ 0.20.4`, `patch-presence ≥ 0.17.5`, `batch-messages ≥ 0.22.75`.
The `init` op carries a `versions` map keyed by SDK name.
