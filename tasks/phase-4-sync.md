# Phase 4 — Reactive sync layer (release gate)

**Read first**: `docs/02-architecture.md` §5 (invariants), §1 (process model),
`docs/03-protocol.md` §1–5 (op tables + feature flags), `docs/05-conformance.md` row "WAL / invalidation".

This is the first phase where the SDK talks to v2 as a real server.
Three packages run in parallel on disjoint paths: `waltail` / `reactive` / `sync`;
`authn` runs alongside on `internal/authn`. The main orchestrator owns the
`cmd/instantd` wire-up that composes them (so agents don't contend on `main.go`).

## 4A — `internal/waltail` (owner: `waltail`)

Replaces `jdbc/wal.clj` (959 LOC) + the 8 Java files at `server/src/java/instant/jdbc/`
that decode `pgoutput` rows (Relation/Insert/Update/Delete/Truncate/Type/Commit).

- [ ] `slot.go` — replication slot lifecycle (`jackc/pglogrepl`): create-if-missing, `START_REPLICATION`
      with `pgoutput` (`proto_version`, `publication_names`, `messages`, `streaming`),
      standby restarts from confirmed `LSN`, DDL for `PUBLICATION`/`SLOT` idempotently.
- [ ] `decode.go` — `pgoutput` record decode into typed `WalRecord{LSN, XID, Op, Relation, Row}`.
      Null/TOAST handling matching v1's `WalColumnVectorDeserializer` behaviour.
- [ ] `checkpoint.go` — durable LSN checkpoint (derived from `wal_logs`), acknowledged only
      **after** the downstream `volatile/sync-exclusive` section has confirmed refresh fan-out
      (invariant 02 §5.2). WAL retention correctness depends on this ordering.
- [ ] `topics.go` — `WalRecord → []Topic` mapping used by `internal/reactive`.
- [ ] `run.go` — `Tail(ctx, SlotConfig, chan<- WalRecord)` supervisor: reconnect w/ backoff,
      keepalive handling, single-consumer guarantee (v1's singleton+aggregator roles collapsed to one
      goroutine — the Hazelcast leader election is replaced by "single process owns the slot").
- [ ] Tests: record decode golden fixtures from v1's Java decoder (recorded pcap/hex); checkpoint fuzz
      (crash at every ack boundary → no lost or phantom refresh under replay).

Acceptance: unit decode tests diff 0 vs v1's pcap; `TestCheckpointRestart` proves LSN monotonicity
across crash replay.

## 4B — `internal/reactive` (owner: `reactive`)

Replaces `reactive/store.clj` (1,835 LOC, DataScript-backed session state), `reactive/invalidator.clj`
(1,026 LOC), `reactive/aggregator` (649), `reactive/ephemeral` (658).

**Design shift**: no DataScript. Plain `map[subscriptionID]*entry` with `bytes` payloads.

- [ ] `store.go` — per-app `query → {cachedResult, topics, lastTxID, lastISN, attrsVersion}` map;
      add/remove query lifecycle; per-subscription `processed-tx-id/isn` watermarks.
- [ ] `topics.go` — `Topic` type (attr id / etype); `TopicIndex` inverted index
      `topic → []*entry` for O(k) invalidation (k = entries touching that topic).
- [ ] `novelty.go` — `Refresh(ctx, entry) (Frame,bool,error)`: re-run the CTE via `internal/datalog`,
      novelty-diff against `cachedResult`, store new cache on change.
      Batch coalescing when `batch-messages` negotiated (one `refresh-ok` for N invald queries).
- [ ] `invalidator.go` — consumer of `waltail.Chan`: fan-out of `WalRecord.topics` through `TopicIndex`
      to per-app invalidators; debounced refresh scheduling; unbounded bursts coalesced.
- [ ] Tests: determinism (`TestReactiveDeterminism` — same write order → same refresh order);
      topic-index correctness; batch coalescing behind `batch-messages` flag.

## 4C — `internal/sync` (owner: `sync`)

Replaces `reactive/session.clj` (1,584 LOC op dispatcher) + `lib/ring/websocket` (518).

- [ ] `session.go` — `handleInit(versions) → negotiatedFeatureSet`; `init-ok{session-id,auth,attrs,app-status}`
      with `skip-attrs` gating; session-id lifecycle; `reconnect` semantics.
- [ ] `dispatch.go` — op table from the JSON Schema: `init/add-query/remove-query/transact/error`
      plus `join-room`/`leave-room`/`set-presence`/`client-broadcast`/`start-sync`/`start-stream` ….
      Every op carries `client-event-id` correlation. Unknown `op`s are logged and ignored (forward-compat lever).
- [ ] `pending.go` — `pending-handler` RPC map for synchronous `refresh-ok`/`transact-ok` correlation.
- [ ] `presence.go` — rooms/presence fan-out (in-process pubsub; optional `nats`/`redis` interface stub for later HA).
- [ ] `ws.go` / `sse.go` — `coder/websocket` handler + SSE fallback that mirrors WS frame semantics;
      single `http.ServeMux` with undertow-equivalent graceful shutdown, connector stats hook.
- [ ] `topics_streams.go` — `start-sync`/`start-stream` subprotocols behind the same dispatcher.
- [ ] Tests: frame-granularity corpus replay per scope (`sync/session` suites), feature-gate matrix
      (pre-0.17.5, pre-0.20.4 SDKs still pass), unknown-op forward-compat.

## 4D — `internal/authn` (owner: `authn`, no shared files with 4A–4C)

- [ ] `jwt.go` — RS256/ES256 via JWKS discovery + cache (parity with `auth/jwt.clj`), Google-certs warmup.
- [ ] `magiccode.go` — opaque hashed refresh-tokens looked up by InstaQL query on `$userRefreshTokens.hashedToken`
      (`model/app_user.clj:155-176`), magic-code email flows, guest sign-in.
- [ ] `oauth.go` — OIDC-discovery providers, bespoke GitHub flow, Apple client-secret minting, nonce quirks
      (Google skips, Apple accepts `sha256(nonce)` — `auth/oauth.clj`), PKCE.
- [ ] `admin_token.go` — `__admin-token` bypass (skips all permission checks on WS `init`).

## Phase 4 final assembly (main orchestrator; no sub-agent writes `cmd/instantd`)

- [ ] Wire `waltail → reactive → sync` in `cmd/instantd` with graceful-shutdown order:
      stop accepting new WS → drain in-flight `transact`s → checkpoint LSN → close WS with correct code.
- [ ] pprof endpoints + OTel trace wiring.

## Phase 4 exit gate — **the service is shippable here**

```
corpus full-session suites replay (open → auth → add-query → transact → refresh → rooms) diff 0 vs v1 (frame granularity)
soak: ≥ 5k concurrent sessions (test harness) for 30 min: no LSN drift, no leak (pprof delta), no dropped refresh under burst
feature-gate matrix: old SDKs (pre-0.17.5, pre-0.20.4) green
graceful-shutdown test: in-flight transact drained, LSN checkpointed, WS code correct
tag: v0.1.0-alpha + published self-host compose + upgrade doc
```
