# FU-01 — Multi-client stream capture contract (candidate side, preparatory slice)

**Status:** contract defined as a preparatory slice; replay legs and
manifest flips remain gated (see "Remains gated" below). This document
satisfies FU-01 acceptance criteria 1–3 **as definition only**: it states
the raw-evidence contract whose existence would let a future packet flip
`rooms-fanout-positive`, `query-concurrency-gap`, and
`refresh-convergence-concurrency` by replay. It authorizes no recorder
change, no capture run, no NDJSON leg, and no manifest flip.

**Parent packet:** `docs/plans/finish-up/followup-packets.md` FU-01
(proposed). That file is untouched by this slice; all four FU packets
stay proposed there and every terminal row
(`program-manifest.md`, `corpus/manifest.json`, `execution-ledger.md`
verdicts) is preserved.

**Shape proof:** `cmd/instantd/runtime_fu01_capture_contract_test.go`
(`TestFU01CaptureContractQueryConcurrency`,
`TestFU01CaptureContractRoomFanout`) exercises this contract's
retention/derivation/oracle shape over the production-mounted SSE path
against owned fixtures with exact whole-result oracles. It is a
deterministic harness proving the contract shape, not a recorder and not
a replay leg: replay here means re-asserting the exact oracle from the
retained per-subscriber logs alone, in the same process.

## 1. Scope and transports

- Transports: SSE multi-client only, over the production-mounted
  `GET /runtime/sse` (stream) + `POST /runtime/sse` (messages) path.
  HTTP single-flow and WS are explicitly out. WS capture stays excluded
  by enforcement (`TestCF002WSRecordCreatesNoArtifact` fails closed with
  no artifact); no WS NDJSON leg may be authored or accepted under this
  contract.
- Assembled legs this contract must reproduce via future capture +
  replay (all accepted on `main`, none flipped here):
  - `rooms-fanout-positive`:
    `TestCF003AssembledRoomFanoutSSE` (ordered presence fanout, resync
    convergence, set-presence fanout, peer-only broadcast with exact
    sender session, leave → exact one-member snapshot;
    lengths-before-elements DeepEqual).
  - `query-concurrency-gap` + `refresh-convergence-concurrency`:
    `TestCF003AssembledSSEQueryConcurrencyOrderedSnapshot` (two SSE
    subscribers through a concurrent barrier observe one identical
    ordered whole-result snapshot; exact float watermark compare; one
    committed admin change converges both to identical refresh frames at
    the exact tx with exact final titles).
- Oracle standard: "exact" per `followup-packets.md` — whole-result
  `reflect.DeepEqual` (lengths before elements), exact float
  `processed-tx-id` compare, exact status/body strings, exact
  session/presence snapshots. Subset/ID-only checks do not satisfy it.

## 2. Capture topology and ordering rules

### 2a. Shared-query topology (query legs)

- Subscriber count: exactly 2 concurrent subscribers (A, B) plus one
  late joiner (C) admitted after the triggering transaction (see §4).
- Shared topology: A and B attach the same query (`{"todos": {}}`)
  against the same seeded entities; C attaches the identical query
  later. All three use distinct SSE sessions/tokens (assert pairwise
  distinct, else abort the capture).
- Barrier rule: A and B `add-query` POSTs race through a start barrier
  (both goroutines released by closing one channel). The barrier
  encourages but does not force DB-level overlap; the contract therefore
  requires the convergence oracle (§3), not a schedule proof. A future
  fault-scheduling campaign is explicitly out of scope (FU-01
  non-goal).
- Ordering rule: per subscriber, frames are retained in arrival order
  with a per-subscriber sequence number (0, 1, 2, …). Cross-subscriber
  comparison is by oracle equality (DeepEqual of whole snapshots), never
  by assuming identical arrival interleavings.

### 2b. Shared-room topology (room leg)

- Subscriber count: exactly 2 SSE subscribers (A, B) in one room
  (`cf003-room` in the harness; the contract names the room explicitly
  per capture).
- Shared topology: A joins first (solo snapshot), B joins second (late
  join, §4); both converge to the exact two-member snapshot; B resyncs
  explicitly (`refresh-presence`); A updates presence; A broadcasts
  (peer-only); B leaves; A converges to the exact one-member snapshot.
- Ordering rule: an actor's ack and its caused presence frame may arrive
  in either order; the capture accepts exactly the set
  {ack, presence} with no other frame interleaved (unknown ops fail the
  capture). Sender isolation is proven by ordering: the sender's next
  frame after its broadcast ack must be the leave-driven presence, not a
  `server-broadcast` echo.

## 3. Per-subscriber raw retention, canonical derivation, replay oracle

- Retention: every post-handshake `data:` frame per subscriber is
  retained verbatim (decoded JSON map, deep-copied at capture) with
  `(subscriber, seq)`. The transport-level handshake hello is excluded
  from retention; session/token distinctness is asserted at open time
  instead. Retention is in-memory in the shape-proof harness; a future
  capture packet must persist it as append-only per-subscriber NDJSON
  with the same `(subscriber, seq)` keys before any manifest flip.
- Canonical derivation:
  - Query legs: the canonical initial snapshot is `snapshots[0]` iff
    `reflect.DeepEqual(snapshots[0], snapshots[1])`; the canonical
    refresh is `refreshes[0]` iff
    `reflect.DeepEqual(refreshes[0], refreshes[1])`. Any divergence
    fails the capture — there is no quorum or majority vote.
  - Room leg: the canonical presence at each step is the survivor's (or
    each peer's, where both must converge) exact snapshot frame; every
    step asserts lengths-before-elements then DeepEqual.
- Replay oracle (from retention alone): the replay entry point takes
  only the retained logs plus the external trigger facts recorded at
  capture time (triggering `tx-id`(s), attr names, expected titles /
  presence maps) and re-asserts:
  - each captured refresh carries its exact triggering transaction ID:
    `processed-tx-id` (float64) `== float64(txID)` exactly;
  - each refresh reproduces the identical ordered whole-result snapshot
    (query legs) or exact presence snapshot (room leg) via DeepEqual
    with lengths checked before elements;
  - the initial query snapshot carries `processed-tx-id == 0` exactly;
  - refresh frames carry no `delta` key (full-snapshot convergence).
- Absence of evidence is never evidence: a missing frame fails the
  capture; only a bounded quiet period (§4) proves quiescence.

## 4. Late-join, resync, leave, quiescence

- Late-join (query): subscriber C opens a fresh SSE session after the
  triggering transaction commits, attaches the same query, and must
  converge to the identical final whole-result snapshot at the exact
  trigger `tx-id` with the exact final titles. A fresh subscriber's
  first answer uses the init-query object-tree envelope (not the
  transactional node-list envelope the peers' refreshes carry); both
  envelopes carry exact oracles and the harness pins each to its own
  shape. C's log is retained and replayed under the same oracle.
- Late-join (room): B joins after A holds the solo snapshot; both must
  converge to the exact two-member snapshot. Join order is part of the
  captured facts, not an accident of scheduling.
- Resync: B issues explicit `refresh-presence` after joining; the
  returned snapshot must DeepEqual the converged two-member snapshot.
  Resync proves the joiner's mounted session sees the same room state;
  it is a captured frame, not a test-only read.
- Leave: B `leave-room` → exact ack; the survivor A must converge to
  the exact one-member snapshot as its next presence frame.
- Quiescence: after the final expected frame on each subscriber, the
  capture observes a bounded quiet period (250 ms in the harness: a
  reader goroutine + `select` timeout; any frame arriving inside the
  window fails the capture). Quiescence is therefore distinguished from
  absence of evidence by an explicit bounded wait, never an unbounded
  one. A future NDJSON capture must record the quiet-window bound as a
  capture fact.

## 5. CF-002 provenance/security compatibility

This contract is compatible with the CF-002
`ACCEPTED_CANDIDATE_BOUND_LOCAL_CAPTURE` invariants by construction:

- Candidate-bound: all capture runs use the production-mounted routes
  of the recorded candidate only; no override binary, no shim, no
  alternate build participates.
- Owned fixtures only: integration runs require
  `INSTANT_TEST_INTEGRATION=1` with `DATABASE_URL` pointing at a role
  with `CREATEDB`; each run mints a private randomly-named database and
  drops only that database on cleanup. No developer default database is
  assumed disposable.
- Fresh private redacted atomic write-once output: the future NDJSON
  persistence must follow the managed-record output discipline (fresh
  directory, redaction, atomic publish); this shape-proof harness keeps
  retention in memory and writes no artifact, so it cannot leak secrets.
- No secret or database URL in manifests, docs, or committed logs: this
  document and the harness contain no tokens, no DSN, no session IDs;
  session/token values are asserted distinct but never printed.
- WS exclusion preserved: this contract defines SSE capture only; the
  `record --transport ws` fail-closed enforcement is unaffected and is
  re-verified as the CF-002 guard.

## 6. Remains gated (not done here)

- A `corpusctl` managed-record multi-client capture path (recorder
  implementation) — would additionally need the FU-02 scope decision
  for any manifest effect; no recorder code is written or authorized
  here.
- Checked-in per-subscriber NDJSON legs + corpus replay tests for the
  three gap rows — the actual future capture runs.
- Manifest flips of `rooms-fanout-positive`,
  `query-concurrency-gap`, `refresh-convergence-concurrency` from `gap`
  to covered — require the NDJSON legs + replay above; explicitly not
  made here (`corpus/manifest.json` untouched).
- v1 parity / external evidence (FU-04): untouched and unclaimed.
- `expectedState` rewrite (FU-03): untouched.
- WS capture: excluded by enforcement, permanently out of this
  contract's scope.
