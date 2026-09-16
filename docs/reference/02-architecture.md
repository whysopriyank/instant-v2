# 02 — V2 Target Architecture

## 1. Process model

One static binary (`instantd`), one Postgres, optional S3-compatible blob store.

```
                    ┌───────────────── instantd ──────────────────┐
 WS /runtime/session │ session manager · per-conn goroutine        │
 SSE /runtime/sse    │ subscription registry (per app)              │
 REST /*            │  instaql-query → cached result · topics      │
                     │  processed-tx-id/isn watermarks              │
                     │                                              │
                     │ query engine   transactor    permissions     │
                     │  instaql coerce   tx-step validate  cel-go   │
                     │  datalog planner  lookup resolve   cached env│
                     │  CTE builder (pgx)  permission gate          │
                     │                                              │
                     │ wal tailer (pglogrepl/pgoutput)              │
                     │   WalRecord → topic index → invalidator      │
                     │                                              │
                     │ storage svc (S3 signed URLs)                 │
                     │ authn svc (JWT/OIDC/magic code)              │
                     └───────────────┬────────────────────────────────┘
                                     ▼
                     Postgres (triples, attrs, apps, transactions
                               + wal_logs, app_users, rules)
```

Goroutine-per-WebSocket-session mirrors v1's core.async per-session worker. The
production write path publishes a post-commit invalidation directly to the
local notifier; when enabled, Postgres LISTEN/NOTIFY carries the same event to
peer nodes. The `internal/waltail` tailer remains the logical-decoding path for
durable LSN/checkpoint behavior and isolated integration verification, but is
not the production notifier assembly today.

## 2. Package layout (Go module `github.com/instant-v2/instant-v2`)

```
cmd/
  instantd/              config, server assembly, graceful shutdown
  corpusctl/             corpus validation, external replay, differential evidence
  schemagen/             protocol schema validation and generated Go/TS artifacts
  benchrun/              benchmark execution CLI
  benchreport/           offline benchmark verification/report CLI
  benchsmoke/            historical diagnostic smoke CLI (unsupported acceptance entrypoint; DEC-001)
  soak/                  sustained-load correctness harness
  soaksetup/             explicit disposable benchmark fixture setup
  chaos/                 process-failure and recovery harness
internal/
  protocol/              wire types + op dispatch (generated from schema; see 03)
  triple/                triple value encoding, attr metadata types, value_tag codec
  storage/               Postgres access: triple CRUD and COPY batches
  datalog/               pattern IR, index selection, CTE builder
  instaql/               InstaQL parse/coerce → datalog plans; pagination; aggregates
  transact/              tx-step validate, lookup-ref resolve, cascade deletes
  perms/                 CEL env cache, rule-doc fallback chain, where-clause compile
  sync/                  session lifecycle, op dispatcher, presence/rooms/topics
  reactive/              subscription registry, novelty diffing, invalidation
  waltail/               pglogrepl consumer, LSN checkpoints, record→topic map
  authn/                 JWT/JWKS verify, OAuth flows, magic codes, refresh tokens
  adminapi/              /admin/* routes
  runtimeapi/            /runtime/* REST routes
  storageapi/            /storage/* signed upload/download routes
  platform/              apps/attrs catalog, idents, system-catalog ops
    migrations/          authoritative embedded goose SQL migrations
  backup/                import/export codecs, HTTP and object-store boundaries
  benchharness/          benchmark target/session/evidence mechanisms
  benchrun/              benchmark configuration, artifacts and reporting
  corpus/                scenario inventory, normalization and replay
  bus/                   cross-node Postgres invalidation transport
  config/                runtime environment configuration
  metrics/               Prometheus metrics
  tracing/               OpenTelemetry setup
  ratelimit/             request budgets
  httpjson/              shared HTTP JSON mechanics
  testkit/               isolated test fixtures; not imported by runtime code
```

**Dependency rule**: `protocol` and `triple` are leaf packages. `sync` depends on
everything; nothing depends on `sync`. Cross-package data crosses only via §4 interfaces.

## 3. Storage design (phase 1 = physical parity)

Keep v1's physical layout so corpus replay and data migration stay trivial:

```sql
triples(
  app_id    uuid        not null,
  entity_id uuid        not null,
  attr_id   bigint      not null,
  value     jsonb       not null,
  value_md5 text        not null,
  ea boolean, eav boolean, av boolean, ave boolean, vae boolean, -- flag cols
  primary key (app_id, entity_id, attr_id, value_md5)
) -- + partial unique indexes driven by flag cols; indexed values capped at 1024 bytes

attrs(id, app_id, etype, label, value_type blob|ref, cardinality one|many,
      forward_ident uuid, reverse_ident uuid, ... )

rules(app_id uuid primary key, code jsonb)   -- whole permission doc per app
transactions(id uuid, ...); wal_logs(...)    -- tx journal feeding the tailer
apps(id, title, admin_token, connection_string null)
```

Index-flag booleans are set from `attrs.index?`/`unique?`; queries pick indexes exactly
as `db/datalog.clj`'s `best-index`. Deviation from this layout needs an ADR.

## 4. Key internal contracts (sub-agent coordination boundary)

Any change to these needs the main orchestrator's approval (see 06).

```go
// triple
type Value struct { /* tagged union: string|number|boolean|json|ref(uuid)|blob */ }
type Triple struct { App uuid.UUID; E uuid.UUID; A int64; V Value }

// datalog / instaql
type Pattern struct { E, A, V Specifier; Index IndexKind }
func Plan(q instaql.Query, attrs AttrCatalog) (*Plan, error)
func (p *Plan) Run(ctx context.Context, db pgx.Tx) (*Result, error)
func Coerce(raw map[string]any) (Query, error)
func EvaluateLocal(store LocalStore, q Query) (map[string]any, error)

// transact
type TxStep struct { Op string; Args []any } // frozen grammar, see 03 §3
func Validate(steps []TxStep, attrs AttrCatalog) ([]ResolvedStep, error)
func Apply(ctx context.Context, tx pgx.Tx, app uuid.UUID, steps []ResolvedStep) (TxID, error)

// perms — fallback chain: etype.allow.action → etype.allow.$default
//                      → $default.allow.action → $default.allow.$default
//                      → etype.fallback.action  (model/rule.clj:286-290)
func Check(ctx context.Context, rules *RuleDoc, act Action, subj Subject) (bool, error)

// reactive
type Subscription interface {
    Query() instaql.Query
    Topics() []Topic
    Refresh(ctx context.Context) (Frame, bool, error) // re-run + novelty diff
}
```

## 5. Concurrency invariants

1. A transaction journal allocates a globally monotonic transaction identity,
   but the current code does not establish an app-specific mutex or distributed
   write serializer. `processed-tx-id` is a session watermark, not proof that
   concurrent writers were serialized.
2. A `transact-ok{tx-id,isn}` is emitted only after the database transaction has
   committed and the local post-commit invalidation has been enqueued. This is
   the current assembly contract. A future tailer-driven assembly must preserve
   the stronger v1 ordering (acknowledge the WAL record only after downstream
   refresh handling); the standalone tailer/checkpoint tests cover that seam,
   but the running service does not currently gate the client ack on it.
3. Session state is plain memory (map of subscription-id → last result + watermark), not
   a Datalog DB. Simpler and sufficient; don't reintroduce DataScript.
4. When the WAL tailer is enabled, it is a single consumer with a durable LSN
   checkpoint and restarts replay from the confirmed LSN. The current main
   assembly uses direct post-commit notification instead.
5. CEL programs cached per `(app, rule-version)`.

## 6. Configuration and ops

- Config via env+flags (12-factor). Secrets from files or env.
- Logging via `log/slog` JSON. Metrics via OTel exporter.
- Migrations via `goose`, embedded, lock-guarded on boot.
- Single container image + plain-binary self-host path.

## 7. Stack

Go `^1.24`, `jackc/pgx/v5`, `jackc/pglogrepl`, `github.com/cel-go/cel-go` (reference impl),
`coder/websocket`, `aws-sdk-go-v2` (s3), `golang-jwt/jwt/v5` + `jwks-rsa`, `chi` or stdlib mux,
`slog`, `otel-go`, `goose`.

## 8. Multi-node operation (Tier 2)

v2 can propagate invalidations symmetrically when the optional bus is enabled
(`docs/reference/09-tier2-architecture.md` §T2.4). Sessions, query caches, and rooms
remain node-local. Postgres is shared, but the current service has no explicit
per-app distributed write serializer (§5 invariant 1). The bus carries
invalidation hints only; it does not establish cross-node ordering or make
arbitrary-node writes safe for semantics that require serialization.

- **Invalidation bus**: with `INSTANT_V2_INVALIDATION_BUS=postgres`, every
  committed write is published on the `instant_v2_invalidate` NOTIFY channel
  and applied by each peer's local notifier (`internal/bus`). Delivery is
  idempotent — refreshes dedupe on the subscription watermark.
- **Write-order boundary**: the bus does not serialize writes. Until an
  app-level lock/lease or equivalent ordering protocol is implemented and
  tested, deployments requiring ordered cross-node writes must route an app's
  writes through one writer or accept that ordering is not guaranteed.
- **Stickiness**: LB app-id affinity helps connection reuse and cache warmth,
  but it is not a substitute for a write serializer and must not be described
  as arbitrary-node write safety.
- **Known limitation**: rooms/presence fan-out is in-process. Participants in
  one room must land on one node; the invalidation bus does not replicate
  ephemeral room state. Keep sticky routing for room-heavy apps until a
  dedicated ephemeral-state channel exists.

## 9. Reconciled implementation status (2026-08-28)

| Contract | Current implementation | Boundary |
|---|---|---|
| Local invalidation | `cmd/instantd/main.go` calls `reactive.Notifier.Notify` or `NotifyChanges` from post-commit hooks for WS, runtime, and admin writes. | It is not driven by `waltail.Tailer` in the main assembly. |
| Peer invalidation | `internal/bus` uses a dedicated LISTEN connection and optional `INSTANT_V2_INVALIDATION_BUS=postgres`; raw events are idempotent and degrade safely when payloads are large. | The two-Store end-to-end peer-refresh test is still missing; delivery depends on Postgres availability, and there is no NATS/Redis transport. |
| WAL/checkpoint | `internal/waltail` decodes pgoutput, checks `wal_level`, and coalesces durable checkpoint writes. | Live PG17 logical-decoding tests exist; production wiring and pcap corpus remain follow-up work. |
| Rooms/presence | `internal/sync.RoomHub` provides bounded, full-snapshot in-process fan-out. | Cross-node rooms are unsupported; sticky routing is an operational requirement for multi-node room use. |
| Admin presence | `/admin/rooms/presence` is authenticated but returns an empty object because `adminapi` cannot import `sync.RoomHub`. | Wiring a read-only presence projection is still open. |
| Auth | Guest and bounded local Google/GitHub authorization-code flows are assembled; PKCE, one-time state/code, Google nonce/JWKS verification, provider timeout, and configuration failure are covered locally. | Real-provider acceptance is not established; direct ID-token exchange and Apple/custom providers are unsupported; magic-code delivery is excluded for the alpha. |
| Required attributes | Catalog persistence, wire `required?`, tx validation, update-attr required-only patch, and backup compatibility are implemented. | Broader frozen corpus coverage is still incomplete. |
