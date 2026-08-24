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

Goroutine-per-WebSocket-session mirrors v1's core.async per-session worker. A single
WAL-tailer goroutine feeds a topic router; fan-out uses per-app subscriber lists.

## 2. Package layout (Go module `github.com/<you>/instant-v2`)

```
cmd/instantd/            main: config, server boot, graceful shutdown
internal/
  protocol/              wire types + op dispatch (generated from schema; see 03)
  triple/                triple value encoding, attr metadata types, value_tag codec
  storage/               Postgres access: triple CRUD, COPY batches, migrations
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
tools/
  corpusctl/             corpus recorder+replayer (drives v1 or v2; see 05)
  schemagen/             protocol schema → Go types + TS d.ts emitter
migrations/              goose-embedded SQL, starting from re-derived 01_bootstrap
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

1. Per-app serialization of writes (monotonic `processed-tx-id` per app/session).
2. A `transact-ok{tx-id,isn}` is only enqueued **after** the local tailer has observed the
   commit — otherwise refresh ordering breaks (v1 guarantees this by tailing its own writes).
3. Session state is plain memory (map of subscription-id → last result + watermark), not
   a Datalog DB. Simpler and sufficient; don't reintroduce DataScript.
4. Single WAL consumer with durable LSN checkpoint; restarts replay from confirmed LSN.
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

v2 scales out symmetrically (docs/09-tier2-architecture.md §T2.4). Sessions
remain node-local; Postgres remains the only shared state and the per-app
write serializer (§5 invariant 1).

- **Invalidation bus**: with `INSTANT_V2_INVALIDATION_BUS=postgres`, every
  committed write is published on the `instant_v2_invalidate` NOTIFY channel
  and applied by each peer's local notifier (`internal/bus`). Delivery is
  idempotent — refreshes dedupe on the subscription watermark.
- **No ownership leases**: any node may accept writes for any app. The
  audit's advisory-lock shard-ownership sketch buys replay dedup only if the
  WAL-tailer path replaces direct notify; recorded as follow-up, not built.
- **Stickiness**: LB app-id affinity is an optimization (connection reuse,
  cache warmth), never a correctness requirement.
- **Known limitation**: rooms/presence fan-out is in-process. Participants in
  one room must land on one node — keep sticky routing for room-heavy apps
  until ephemeral state gets a bus channel of its own.
