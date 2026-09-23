# Phase 4 — Reactive sync layer (release gate)

**Read first**: `docs/reference/02-architecture.md` §5 (invariants), §1 (process model),
`docs/reference/03-protocol.md` §1–5 (op tables + feature flags), `docs/guides/05-conformance.md` row "WAL / invalidation".

This is the first phase where the SDK talks to v2 as a real server.
Three packages run in parallel on disjoint paths: `waltail` / `reactive` / `sync`;
`authn` runs alongside on `internal/authn`. The main orchestrator owns the
`cmd/instantd` wire-up that composes them (so agents don't contend on `main.go`).

## Current status (2026-08-28)

Partial. WS/SSE sessions, query groups, invalidation, delta/incremental
refresh, room/presence fan-out, and optional peer invalidation are implemented.
Magic-code/guest auth, injected-provider/direct id-token paths, JWKS, and
builtin Google/GitHub auth-code exchange with nonce handling are implemented
(DA-006A); real-provider acceptance is `BLOCKED` (DA-006B). The
logical-decoding package and checkpoint behavior have live PG17 verification,
but production assembly currently uses post-commit notify; full-session corpus
replay, the 5k×30-minute release soak, stream subprotocols, Apple end-to-end
OAuth (explicitly excluded for this alpha), and cross-node write ordering
remain open.

## 4A — `internal/waltail` (owner: `waltail`)

Replaces `jdbc/wal.clj` (959 LOC) + the 8 Java files at `server/src/java/instant/jdbc/`
that decode `pgoutput` rows (Relation/Insert/Update/Delete/Truncate/Type/Commit).

- [x] `slot.go` — replication slot lifecycle (`jackc/pglogrepl`): create-if-missing, `START_REPLICATION`
      with `pgoutput` (`proto_version`, `publication_names`, `messages`, `streaming`),
      standby restarts from confirmed `LSN`, DDL for `PUBLICATION`/`SLOT` idempotently.
- [x] `decode.go` — `pgoutput` record decode into typed `WalRecord{LSN, XID, Op, Relation, Row}`.
      Null/TOAST handling matching v1's `WalColumnVectorDeserializer` behaviour.
- [x] `checkpoint.go` — durable LSN checkpoint (derived from `wal_logs`), acknowledged only
      **after** the downstream `volatile/sync-exclusive` section has confirmed refresh fan-out
      (invariant 02 §5.2). WAL retention correctness depends on this ordering.
- [x] `topics.go` — `WalRecord → []Topic` mapping used by `internal/reactive`.
- [x] `run.go` — `Tail(ctx, SlotConfig, chan<- WalRecord)` supervisor: reconnect w/ backoff,
      keepalive handling, single-consumer guarantee (v1's singleton+aggregator roles collapsed to one
      goroutine — the Hazelcast leader election is replaced by "single process owns the slot").
- [~] Tests: live pgoutput verification against PG17 wal_level=logical (insert→decoded
      record); golden pcap fixtures + checkpoint fuzz deferred to Phase 6.

Acceptance: unit decode tests diff 0 vs v1's pcap; `TestCheckpointRestart` proves LSN monotonicity
across crash replay.

## 4B — `internal/reactive` (owner: `reactive`)

Replaces `reactive/store.clj` (1,835 LOC, DataScript-backed session state), `reactive/invalidator.clj`
(1,026 LOC), `reactive/aggregator` (649), `reactive/ephemeral` (658).

**Design shift**: no DataScript. Plain `map[subscriptionID]*entry` with `bytes` payloads.

- [x] `store.go` — per-app `query → {cachedResult, topics, lastTxID, lastISN, attrsVersion}` map;
      add/remove query lifecycle; per-subscription `processed-tx-id/isn` watermarks.
- [x] `topics.go` — `Topic` type (attr id / etype); `TopicIndex` inverted index
      `topic → []*entry` for O(k) invalidation (k = entries touching that topic).
- [x] `novelty.go` — `Refresh(ctx, entry) (Frame,bool,error)`: re-run the CTE via `internal/datalog`,
      novelty-diff against `cachedResult`, store new cache on change.
      Batch coalescing when `batch-messages` negotiated (one `refresh-ok` for N invald queries).
- [x] `invalidator.go` — consumer of `waltail.Chan`: fan-out of `WalRecord.topics` through `TopicIndex`
      to per-app invalidators; debounced refresh scheduling; unbounded bursts coalesced.

  - [x] Tests: topic indexing, coalescing, stale suppression, app isolation;
        parallel drain under -race.
  - [~] Determinism (`TestReactiveDeterminism` — same write order → same refresh order);
      topic-index correctness; batch coalescing behind `batch-messages` flag.

## 4C — `internal/sync` (owner: `sync`)

Replaces `reactive/session.clj` (1,584 LOC op dispatcher) + `lib/ring/websocket` (518).

- [x] `session.go` — `handleInit(versions) → negotiatedFeatureSet`; `init-ok{session-id,auth,attrs,app-status}`
      with `skip-attrs` gating; session-id lifecycle; `reconnect` semantics.
- [x] `dispatch.go` — op table from the JSON Schema: `init/add-query/remove-query/transact/error`
      plus `join-room`/`leave-room`/`set-presence`/`client-broadcast`/`start-sync`/`start-stream` ….
      Every op carries `client-event-id` correlation. Unknown `op`s are logged and ignored (forward-compat lever).
- [x] pending correlation via client-event-id echo (dedicated map pending Phase 6 delta-sync) — `pending-handler` RPC map for synchronous `refresh-ok`/`transact-ok` correlation.
- [x] `rooms.go` — in-process pubsub; full-snapshot presence (patch-presence editscript optimization documented as deviation) — rooms/presence fan-out (in-process pubsub; optional `nats`/`redis` interface stub for later HA).
- [x] `ws.go` + graceful `drain.go`
- [ ] `sse.go` (SSE fallback) — `coder/websocket` handler + SSE fallback that mirrors WS frame semantics;
      single `http.ServeMux` with undertow-equivalent graceful shutdown, connector stats hook.
- [ ] `topics_streams.go` — `start-sync`/`start-stream` subprotocols behind the same dispatcher.
- [x] Feature-gate matrix (12 boundary versions + garbage), unknown-op forward-compat,
      live WS session flow test.
- [ ] Frame-granularity corpus replay vs v1 (needs corpus recorder, Phase 5).

## 4D — `internal/authn` (owner: `authn`, no shared files with 4A–4C)

- [x] No user JWTs exist in v1 (scout finding): credentials are opaque bearer UUIDs.
      Direct/injected-provider id_token JWKS verification is implemented and
      tested; builtin Google/GitHub auth-code provider configuration and nonce
      handling are implemented (DA-006A); real-provider acceptance is `BLOCKED`
      (DA-006B).
- [x] `magiccode.go` — opaque hashed refresh-tokens looked up by InstaQL query on `$userRefreshTokens.hashedToken`
      (`model/app_user.clj:155-176`), magic-code email flows, guest sign-in.
- [x] `oauth.go` — authorization-code scaffolding, PKCE/state/one-time-code
      helpers, and builtin Google/GitHub provider token/userinfo URLs with
      nonce generation/validation are implemented and configured in the main
      assembly (DA-006A). Apple's end-to-end exchange remains explicitly
      excluded for this alpha.
- [x] `admin_token.go` (CatalogCache.CheckAdminToken) — `__admin-token` bypass (skips all permission checks on WS `init`).

## Phase 4 final assembly (main orchestrator; no sub-agent writes `cmd/instantd`)

- [x] Wire-up complete incl. OnCommit invalidation bridge; shutdown order: stop accept →
      drain WS (1001) → close server (single-node: no WAL lag to flush).
- [x] pprof via cmd/soak harness endpoint; OTel wiring Phase 6.

## Phase 4 exit gate — **the service is shippable here**

```
corpus full-session suites replay (open → auth → add-query → transact → refresh → rooms) diff 0 vs v1 (frame granularity)
soak: ≥ 5k concurrent sessions (test harness) for 30 min: no LSN drift, no leak (pprof delta), no dropped refresh under burst
feature-gate matrix: old SDKs (pre-0.17.5, pre-0.20.4) green
graceful-shutdown test: in-flight transact drained, LSN checkpointed, WS code correct
tag: v0.1.0-alpha + published self-host compose + upgrade doc
```
