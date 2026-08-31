# Tier 1 Hot-Path Spec — fanout economics, replay damping

Goal: break v2's fanout cost multiplication (`writes × subscribers × payload`)
and harden restart behavior, so CPU at load reflects *deliveries served*, not
recompute waste. Baseline for all numbers: 2000 sessions × 10 min @ 8 tx/s
(docs/archive/01-state.md capacity table) — 65,313 refreshes delivered, ~5 cores busy.

Non-goals: multi-node sharding (Tier 2), incremental result maintenance
(Tier 2), wire-format changes visible to frozen SDKs (none of this changes any
byte on the wire for existing clients).

## Reconciled implementation status (2026-08-28)

Tier 1 is substantially implemented, but this design document is not itself a
benchmark result. Query-group dedupe, parallel notifier draining, the empty
store short-circuit, checkpoint coalescing, queue gates, and incremental/delta
refresh have focused tests in the current tree. The planned easyjson codegen
workstream was not adopted. The live logical-decoding component is verified
against an isolated PostgreSQL 17 cluster, while the production assembly uses
post-commit notification (and optional LISTEN/NOTIFY for peers).

The numeric baseline and target reductions below are retained as dated design
inputs. They are not a hardened v1-versus-v2 claim: run order, warm-up,
environment, raw artifacts, and convergence criteria must be pinned by Wave 4
before Wave 6 publishes a comparison.

---

## T1.1 Query-group registry (subscription dedupe)

### Problem

`subKey(sess.ID, rawQ)` keys subscriptions per-session
(`internal/sync/session.go:441`). N clients with the identical query are N
independent pipelines: N instaql recomputes + N `BuildNodeList` renders +
N `json.Marshal` **per write**, producing byte-identical frames N times.

### Design

One **QueryGroup** per `(appID, canonical-query-json, wire-class)`:

```
QueryGroup {
    key       string            // groupKey = appID | class | sha256(canonical q)
    sub       *Subscription     // exactly as today: Topics, TxID, snapshot, Delta
    class     WireClass         // nodelist | tree   (delta is per-member, see below)
    members   map[*Member]struct{}
}
Member { sess *Session }        // sess.Send used for delivery
```

- Lives in `internal/reactive` next to `Store`; `Store` gains
  `groups map[string]*QueryGroup` keyed identically to its `byID` map so the
  topic index, `Notify`, and cap accounting keep working unchanged — a group
  IS a subscription.
- `MaxSubsPerApp` semantics: counts **group members** (sockets × queries),
  i.e. real memory pressure, checked at member-attach time. Breach → same
  `*SubLimitError` → same 429 protocol path.
- Membership: attach on add-query, detach on remove-query/disconnect; group
  unregisters when last member leaves (`Cancelled=true`, Remove from store).
  No linger timer in v1 of this — reconnect storms are handled by T1.4's
  single-flight instead.

### Frame routing (heterogeneous members)

On group emit (one `reactive.Frame` from `refreshOne`):

1. Render **full envelope frame once** for the group's wire-class:
   - `nodelist`: `BuildNodeList(cat, result)` wrapped in the computations
     entry → `json.Marshal` ONCE → `[]byte`.
   - `tree`: `UnwrapTree` + result-meta once (admin SSE path).
2. If any member negotiated `delta-refresh` AND `DiffResults(prev, result)`
   succeeds → render delta frame once too.
3. Route: delta members get the delta bytes when available, else full;
   everyone else gets full. Delivery = `sess.Send(prebuiltFrame)` where the
   prebuilt frame is a `Frame` carrying pre-marshaled raw JSON (extend `Frame`
   with a `Raw []byte` escape hatch honored by `Encode()`).

Frames contain no per-member fields on async refreshes (client-event-id is
absent by design; processed-tx-id is group-wide), so one encoding serves all.

**Initial answer stays per-connection**: the ack enrichment in `ws.go` runs
the query synchronously for THIS client's ack (frozen SDK reads `result` off
the ack). Concurrent duplicate adds single-flight through a per-key mutex in
the handler: first caller computes + seeds the group snapshot; concurrent
callers reuse it if the generation already advanced past registration.

### Files touched

- `internal/reactive/reactive.go` — group registry, member attach/detach,
  dispatch hook (Emit becomes group-level).
- `internal/sync/session.go` — handleAddQuery/remove-query/teardown switch to
  group keys + membership; Session.Subs now holds group keys.
- `internal/sync/ws.go` — ack enrichment via singleflight; frame Raw path.
- `internal/sync/frame.go` (or wherever `Encode` lives) — `Raw` escape hatch.
- Tests: `TestSubscriptionCap` (member-count breach), handshake tests
  unchanged (protocol-level), new `TestGroupDedupe`: two sessions, same query
  → one recompute per write (assert via Refresh call counter), both receive
  identical frames.

### Acceptance

- BenchmarkDeltaRefresh10k-style harness extended: `BenchmarkFanout2000x8`
  measuring allocations + wall time of 8 writes/s into a 2000-member group.
  Target: ≥80% reduction in marshal calls vs today (from 16k/frame-set to
  ≤2/frame-set).
- Full suite green under `-race`; soak-v2 rerun shows same deliveries with
  materially lower server CPU (target: <50% of previous 485% at identical
  workload).

---

## T1.2 Replay damping + checkpoint hygiene

### Problem

Observed live: after an unclean restart with 4079 committed-but-unprocessed
transactions, the tailer replayed everything while zombie sessions
re-registered; per-record processing saturated the pool (32 conns) and
starved snapshots/admin queries for minutes. Also `tail_state` only advances
on confirm — ungraceful kills replay large windows.

### Design

1. **Empty-store short-circuit**: in the drain path (`Notifier.Run` batch
   loop), when `Store.Len() == 0` skip decode+dispatch for the whole batch
   and advance the watermark directly. Record decode is wasted work when no
   subscriber can match.
2. **Checkpoint coalescing**: confirm LSN to `tail_state` at most every
   `INSTANT_V2_CHECKPOINT_INTERVAL` (default 2s) OR every 500 records,
   whichever first; always flush on graceful `Stop()`. Ungraceful kill
   replay window therefore ≤ interval — bounded work, not unbounded history.
3. **Replay-mode guard**: while `cp.Last() < slotConfirmedLSNAtAttach`
   (i.e. we know we're catching up), process records but SKIP Emit renders
   for groups whose members == 0 (already implied by (1)) and log a single
   "replaying N records" line so operators see catch-up mode.

### Files touched

- `internal/reactive/reactive.go` (drain loop early-exit)
- `internal/waltail/waltail.go` (confirm coalescing, Stop flush)
- `cmd/instantd/main.go` (env knob)

### Acceptance

- New test: kill tailer mid-stream after N writes, restart against same DB
  with zero subscribers → no decode work (assert via counter hook), LSN
  converges within one interval.
- Chaos harness unchanged-green.

---

## T1.3 JSON codegen on the envelope path

### Problem

`encoding/json` reflection marshal/unmarshal dominates the render cost of
large envelopes (~460 KB each).

### Design

easyjson codegen for the hot types only:
- sync: computations entry struct, node-list shapes built by `BuildNodeList`
  (switch to generated structs instead of `map[string]any`),
- reactive: `Frame` payload assembly.

Everything else keeps stdlib. No cgo, boring tooling, additive `*_easyjson.go`
files.

### Acceptance

Benchmark A/B on `BenchmarkFanout2000x8` + `BenchmarkDeltaRefresh10k`;
adopt if ≥20% end-to-end render improvement, else drop (no sunk cost).

---

## T1.4 Backpressure enforcement (wiring, new logic minimal)

The queue-depth gauge exists (`Notifier.QueueDepth()`), and the current
assembly consumes it through the T2.1 transact gate and health endpoint.
This section records the original Tier 1 design problem; its implementation
landed with the Tier 2 backpressure work.

- Expose depth on `/runtime` health output.
- New knob `INSTANT_V2_MAX_QUEUE_DEPTH` (default: unlimited, preserving today's
  behavior): when exceeded, transact-class rate limiter returns 429 +
  Retry-After until drained — publishers shed before the invalidator melts.
- Slow-session protection stays out of scope for Tier 1 (per-send timeouts are
  a follow-up once delivery is shared-byte based and cheap).

### Acceptance

Test: flood 10k Notify into one slow group → transacts 429 until depth <
low-water mark; gauge monotonic and observable.

---

## Sequencing & verification

Order: T1.1 → T1.2 → benchmarks gate → T1.3 (conditional) → T1.4.
Every step lands behind the full `-race` suite; final verification reruns
`cmd/chaos` and the comparative soak against the recorded baseline table in
docs/archive/01-state.md. No wire-visible change for any current SDK version.
