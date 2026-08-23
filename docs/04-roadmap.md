# 04 — Roadmap

**Feasibility thesis**: the v2 is worth doing only as a product-ownership play, not a
performance play (see README). Phases below are ordered so each one can ship conformance
before the next. The whole sequence is **incrementally valuable** — aborting after any
phase leaves working, corpus-tested code.

Each phase lists: entry gate, owned packages (which sub-agents may touch those packages in
parallel), key tasks, and the **exit gate** that blocks the next phase.

---

## Phase 0 — Foundations + golden corpus (must finish before any port)

**Goal**: a green repo with CI, a formal protocol schema, and a conformance harness that
exercises v1 and will exercise every later phase.

| | |
|---|---|
| **Gate in** | This repo exists. |
| **Owner** | Pure infrastructure; no ported logic yet. |
| **Packages** | `tools/corpusctl`, `tools/schemagen`, `internal/protocol`, `migrations/` boot, `cmd/instantd` skeleton, `docs/` |

### Tasks

1. **Scaffold**: `go.mod`, `golangci-lint`, `Makefile`, `Dockerfile`, GH Actions
   (lint + `go vet` + `go test`, embed migrations).
2. **Migrations**: re-derive `01_bootstrap` from v1's `01_bootstrap.up.sql` into goose SQL;
   include `rules` table (migration 04), `transactions`+`wal_logs` (95/97), auth tables,
   `apps`+`attrs`+`triples`. Tool: `goose`.
3. **Protocol schema** (`protocol.schema.json`): every `op` from 03, every error shape,
   InstaQL option/operator set, tx-step tuple grammar, attr JSON shape, REST request/response
   envelopes. Commit the JSON Schema at `internal/protocol/schema/`.
4. **`schemagen` codegen**: generate Go wire types + TS `.d.ts` from the schema.
   Both sides compile; schema is the single source through v6.
5. **`corpusctl` recorder**: proxy that sits between the published `@instantdb/core` SDK
   and a running v1 checkout (`/Users/priyank/Developer/sideproj/instant`), captures
   every WS frame + HTTP call deterministically, plus seed fixtures that mirror
   `server/test`. Output: `corpus/*.ndjson` with envelope checksums.
6. **Smoke fixture run**: spin v1's self-hosting `docker-compose.local.yml` against the
   corpus, prove deterministic replay without v2 yet.

### Exit gate

```
[ ] migrations apply cleanly into a fresh Postgres (testcontainers in CI)
[ ] schemagen output compiles (Go + d.ts) and is committed
[ ] corpus/ contains ≥ 50 scenarios covering: auth flows, permissioned reads+transacts,
    pagination cursors, aggregates, $files, presence/rooms, storage signed URLs
[ ] corpusctl replay against live v1 is byte-stable (canonicalized JSON diff == 0)
```

*No human review of protocol drift may pass after this gate without an ADR.*

---

## Phase 1 — Storage engine

**Goal**: the physical triple store works and round-trips against the corpus at SQL granularity.

| | |
|---|---|
| **Gate in** | Phase 0 exit gate green. |
| **Owners** | `internal/triple`, `internal/storage`, `internal/platform` can run in parallel on disjoint packages. |

### Tasks

1. **Value codec** (`triple`): canonical `Value` tagged union, jsonb encode/decode parity
   with `db/model/triple.clj`, `value_md5` derivation, 1024-byte indexed-value cap,
   ref-values-must-be-uuid check, composite PG type parsing parity.
2. **Attrs catalog** (`platform`): apps/attrs/idents CRUD, boolean flag-column derivation
   (`index?`/`unique?` → `ea/eav/av/ave/vae` flags), `idents UNIQUE(app_id,etype,label)`.
   BYOP hook stubbed behind an interface.
3. **Triple CRUD** (`storage`): `InsertMany`/`DeleteByPredicate`/`FetchByPatterns` via `pgx`,
   `COPY`-path bulk writes, `next.jdbc` error-code → PG error mapping parity
   (`pgerrors`), statement timeout handling.
4. **Journal** (`storage`): `transactions`+`wal_logs` writer that v1's `custodian`+`wal` tailer
   expect; per-app tx-id/isn sequence generation.

### Exit gate

```
[ ] triple/value round-trip tests (fuzz + table-driven) match v1 edge cases
[ ] attrs prefix resolution matches v1 (etype/label → attr id)
[ ] storage/*_test.go runs corpus DB-granularity fixtures against a real Postgres
[ ] no package reaches into another's tables directly
```

---

## Phase 2 — Transactor + permissions

**Goal**: authenticated permissioned writes, verified against both the corpus and direct
CEL property tests. This is where existing apps would first write through v2.

| | |
|---|---|
| **Gate in** | Phase 1 exit gate. |
| **Owners** | `internal/transact` and `internal/perms` run in parallel against the interface in 02 §4. |

### Tasks

1. **Tx-step validation** (`transact`): grammar from 03 §4, lookup-ref `[attrId,value]`
   resolution to entity ids, `create`/`update` mode required-checks, `delete-entity`
   cascade-delete generation, `add-attr`/`delete-attr`/`restore-attr` lifecycle
   (including missing-attr synthesis like v1's `createMissingAttrs`).
2. **Permissioned transact** (`transact` + `perms`): per-step `Check()` before commit,
   `rule-params` propagation, rate-limit bucket `limit(key)` builtin.
3. **CEL bridge** (`perms`): `cel-go` environment per (app, rule-version), protobuf
   `modifiedFields`/`time`/`ip`/`origin` bindings (parity with `db/proto.clj`), custom
   `rateLimit.bucket.limit`, attribute-pattern extraction for pre-filtering, `bind`
   topological sort with cycle detection, fallback chain
   `etype.allow.action → … → etype.fallback.action`.
4. **Rule docs** (`perms`/`platform`): JSONB persistence, versions, cache invalidation
   on `POST /rules`, reserved namespaces, permissive-fallback when `rules.code` is null.

### Exit gate

```
[ ] corpus transact scenarios replay through v2's transactor and match v1 outcomes
    (including permission denials, cascades, missing-attr synthesis)
[ ] cel-go custom-function coverage matches v1's ~1.9k-LOC CEL surface
[ ] permissioned read where-clause generation tested (see next phase)
[ ] Go Report Card: `perms` package has ≥ 95% branch coverage on fallback-chain logic
```

---

## Phase 3 — Query engine

**Goal**: InstaQL→datalog→one-CTE execution with pagination/aggregates, proven against
a dual-impl harness (server and JS optimistic path must agree).

| | |
|---|---|
| **Gate in** | Phase 2 exit gate. |
| **Owners** | `internal/datalog` and `internal/instaql` (split at the IR boundary in 02 §4). |

### Tasks

1. **Datalog IR** (`datalog`): pattern types, index selection (`best-index`), symbol-map
   binding-path semantics, nested `or`/`and` groups, topic extraction, `query`/`query-nested`
   distinction, honeysql → pgx CTE emission (lines `datalog.clj:3260-3414`).
2. **InstaQL front-end** (`instaql`): `Coerce`/`parseWhere`/`makeJoin`/`extendObjects`,
   option map (`order/limit/first/last/offset/before/after+Inclusive/aggregate/fields`),
   where operators (`$in`+legacy `in`, `$not`/`$ne`, `$isNull`, `$gt`/`$gte`/`$lt`/`$lte`,
   `$like`/`$ilike`, `$entityIdStartsWith`), aggregates (`:count`, admin-only raise at
   instaql.clj:1190), page-info cursor construction, `$$ruleParams` namespace,
   `$files` URL rewriting.
3. **EvaluateLocal parity** (`instaql`): must byte-match `client/packages/core/src/instaql.ts`
   for the corpus's optimistic-update scenarios (reuse `datalog` IR).
4. **Permissioned reads**: CEL `view` rules compiled into `where` clauses via `perms`,
   per-entity field filters.

### Exit gate

```
[ ] every corpus query replays through v2's engine and diffs 0 vs v1's envelopes
[ ] a JS harness runs `instaql.ts` + server engine on identical fixtures and diffs 0
[ ] EXPLAIN cost sanity: CTEs hit the intended partial indexes (ea/eav/av/ave/vae)
[ ] pagination cursors are stable across savepoints (tested)
```

---

## Phase 4 — Reactive sync layer (the release)

**Goal**: a live WebSocket service that replaces the original for the SDK.

| | |
|---|---|
| **Gate in** | Phase 3 exit gate. |
| **Owners** | `internal/sync`, `internal/reactive`, `internal/waltail` — three disjoint packages. `internal/authn` in parallel for signin flows. |

### Tasks

1. **WAL tailer** (`waltail`): `pglogrepl` replication slot lifecycle, `pgoutput` record decode
   (Relation/Insert/Update/Delete/Truncate), LSN acknowledgement (only after downstream
   refresh acknowledged — invariant 02 §5.2), singleton + aggregator roles simplified to
   one-consumer, checkpoint persistence.
2. **Subscription store + invalidator** (`reactive`): per-app map of
   `subscription.id → {query, cachedResult, topics, lastTxId}`, topic index over attrs/etypes,
   novelty computation, coalesced refresh scheduling, `batch-messages` envelope when negotiated.
3. **Session manager** (`sync`): `handle-init!` handshake (session-id mint, auth resolution,
   `init-ok{attrs,app-status}` with `skip-attrs` gating), op dispatch table from the schema,
   `client-event-id` correlation, pending-handler RPC bookkeeping, SSE transport mirroring WS
   semantics, presence/rooms fan-out (no Hazelcast pubsub — in-process fan-out + optional
   redis/nats interface stub).
4. **Authn** (`authn`): JWT verify (JWKS), OIDC/GitHub OAuth flows, magic-code email,
   hashed opaque refresh-token lifecycle, Apple client-secret minting.

### Exit gate (the service is shippable here)

```
[ ] corpus full-session replay (open → auth → add-query → transact → refresh → rooms)
    diffs 0 vs v1 at frame granularity
[ ] soak test: ≥ 5k concurrent sessions (test harness, not users) for 30 min, no LSN drift,
    no memory leak (pprof), no dropped invalidations under burst transacts
[ ] feature-gate matrix green: old SDKs (pre-0.17.5 and pre-0.20.4) still pass
[ ] graceful shutdown: in-flight transacts drained, LSN checkpointed, WS closed with code
```

Post-exit: tag `v0.1.0-alpha`, publish self-host compose (derived from `self-hosting/*`).

---

## Phase 5 — Platform surfaces

**Goal**: complete the surfaces SDKs and CLIs couple to.

| | |
|---|---|
| **Gate in** | Phase 4 release tagged. |
| **Owners** | `internal/adminapi`, `internal/runtimeapi`, `internal/storageapi` in parallel. |

### Tasks

1. **Admin API**: `POST /admin/{query,transact,query_perms_check,transact_perms_check,subscribe-query,sse}`,
   `/admin/users`, `/admin/schema`, `/admin/rooms/presence`, token auth.
2. **Runtime REST**: `/runtime/{auth/*, framework/query, signout, oauth/*, openid-configuration}`,
   HTTP query path parity with WS query engine.
3. **Storage**: S3-compatible signed upload/download URLs, `PUT /storage/upload` passthrough,
   `/storage/files` delete, file→triple linkage, MIME via `Content-Type` (v1 uses Tika; stdlib is fine).

### Exit gate

```
[ ] corpus HTTP surface replay passes (WireMock-style recorded/restored cases)
[ ] every storage/ route is hit by at least one SDK example (`examples/*` from v1) against v2
[ ] admin permissions parity: `__admin-token` bypasses all checks exactly as in v1
```

---

## Phase 6 — Hardening + release polish

**Goal**: make self-hosting the default and close the loop on the product.

- **Delta sync** (optional, additive): wire-level patches for `refresh-ok` behind a new feature flag;
  fall back to full envelopes for old SDKs. Largest user-visible win after the port.
- **Backup/restore** (parity with `backup.clj` 1,118 LOC): export/import tooling for self-host migrations.
- **Performance**: pprof-guided tuning of JSON encode, topic index, CTE planning; connection-limit and
  per-app rate-limit policy (Bucket4j-equivalent, local-first).
- **Docs + examples**: port `client/www/docs` narrative for the new deploy model; verify every
  `examples/*` app against v2.
- **Release**: `v1.0.0`, published container image, upgrade guide from v1 self-host.

### Exit gate

```
[ ] load profile: steady-state throughput within 30% of v1's Postgres-bound ceiling
    (language is not the bottleneck; this gate guards regressions)
[ ] chaos run: Postgres bounce mid-stream → sessions reconnect, LSN resumes without phantom reads
[ ] signed artifacts + SBOM + upgrade doc are published
```

---

## Overall ordering constraints

- Phase 0 blocks everything (no ported logic until schema+corpus green).
- Phases 1 and 2 may overlap only at the interface boundary defined in 02 §4 — types first, logic second.
- Phase 3 depends on transactor already validating lookups that `Plan` intersects.
- Phase 4 is the only phase allowed to touch `sync/waltail/reactive/authn` together; keep it behind the prior gates so invalidator bugs have nowhere to hide.
