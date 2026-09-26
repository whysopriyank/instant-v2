# Contract — RT-004 WebSocket fan-out head-of-line blocking

New bounded product packet (phase 02 realtime), opened by the QR-001 soak
rehearsal on cutiewhy (2026-09-26, candidate `c3780c5`): 500 WS sessions in
ONE query group, 8 tx/s. Refresh throughput decayed 3452 → 2666 r/s over
12 min, then dropped to exactly 0 for every session at once while writes
continued; clients hit their 20 s "refresh stream stalled" guard and
reconnected. Server logged nothing.

Root cause (code, `internal/sync/groups.go` `dispatchGroup`): WS fan-out is a
sequential loop over group members calling `mem.sess.SendRaw(b)`, which in
`internal/sync/ws.go` is a blocking `conn.Write` under `writeMu` with a 10 s
`wsWriteTimeout`. One slow reader therefore delays EVERY member of its group
by up to 10 s per generation (head-of-line blocking), contradicting the
`ws.go` design note "a wedged peer must not stall its group". SSE does not
have this problem: it enqueues into a bounded per-session `events` channel
drained by one writer, with a generation guard checked at write time
(RT-001f) and overflow → end stream so the client reconnects
(`internal/sync/sse.go`: `sseConn`, `SendRawGen`, `signalOverflow`,
`writeSSEEvent`).

Read first: `docs/plans/finish-up/00-operating-contract.md`,
`docs/plans/finish-up/02-realtime-correctness.md` (RT-001/RT-002 accepted
semantics — you must preserve them), `internal/sync/ws.go`,
`internal/sync/sse.go`, `internal/sync/groups.go`, `internal/sync/session.go`,
and the existing RT-001/RT-002 tests in `internal/sync/*_test.go` and
`cmd/instantd/*` that exercise WS fan-out, revocation, detach and retry.

## Required rows

R1 **PROVEN_RED first.** Add a hermetic test in `internal/sync` using real
   WebSocket connections (httptest server + the existing WS handler, as other
   tests do): one query group with ≥2 healthy members and 1 member whose
   client never reads; drive refreshes with frames large enough to fill the
   socket buffers. Assert every healthy member receives each refresh within a
   tight bound (≤ 1 s) regardless of the stuck member. Run it on the
   UNCHANGED code and paste the failure (it must fail for the HOL reason).

R2 **Per-session outbound queue for WS**, mirroring SSE: a bounded queue
   (justify the capacity; reuse the SSE capacity constant if suitable) drained
   by exactly one writer goroutine per connection that performs ALL frame
   writes for the session (replies, errors, refresh, delta) so per-session
   frame order is preserved exactly as before. `SendRawGen` for WS enqueues
   with the generation guard, and the writer drops superseded/cancelled
   generations at write time exactly like `writeSSEEvent` (RT-001f). Keep the
   bounded `wsWriteTimeout` on the actual socket write. Keepalive pings keep
   working (no concurrent writes on the conn).

R3 **Overflow policy:** when a session's queue is full, do not block the
   fan-out: signal overflow, close the connection with an explicit
   close status (1013 "try again later" or 1001, pick one and document it),
   and let the member be detached so the client reconnects from a full
   snapshot. Log overflow at Warn with the session id (not Debug), and add a
   Prometheus counter consistent with `internal/metrics` naming.

R4 **Preserve accepted semantics** (RT-001 bounded rebinding, RT-002
   delivery/watermarks/withhold-and-retry): the certification meaning of
   "served" for queued transports must match what SSE already does; show in
   the handoff, citing test names, that each RT-001/RT-002 guarantee still
   holds for WS. All existing tests must pass unchanged — if you believe one
   must change, stop and report why instead of editing it.

R5 **Tests** (in addition to R1 turning green): per-session ordering
   (transact-ok vs refresh-ok ordering unchanged), revocation drops a queued
   superseded envelope (mirror the SSE RT-001f test), overflow closes only
   the slow member with the chosen close status while siblings keep receiving,
   writer goroutine exits on close (no leak: use goleak if already a dep,
   otherwise a goroutine-count check), and a 20-iteration `-race -count`
   run of the new tests.

## Verification (paste raw output)

macOS here needs `CGO_ENABLED=0` for linking.

```
gofmt -l internal/sync cmd/instantd
CGO_ENABLED=0 go vet ./internal/sync/... ./cmd/instantd/...
CGO_ENABLED=0 go test ./internal/sync/... -race -count=1
CGO_ENABLED=0 go test ./internal/sync/... -race -count=20 -run '<your new tests>'
CGO_ENABLED=0 INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./... -race -count=1 -short 2>&1 | grep -v '^ok' | tail -30
git diff --stat
```

If a local PostgreSQL is available (`pg_ctl`/`initdb`), also run
`INSTANT_TEST_INTEGRATION=1 DATABASE_URL=<disposable> go test ./internal/sync/... ./cmd/instantd/... -race -count=1`
and paste it; otherwise state it was not run (the coordinator runs the
owned-DB lanes on Linux).

## Leases / limits

Write: `internal/sync/**` (ws.go, groups.go only if strictly needed, new
tests), `internal/metrics/**` (one counter). Read-only: everything else,
especially `internal/reactive/**`, `cmd/soak/**`, `cmd/qualify/**`, SSE
behaviour. Append a handoff section to
`docs/plans/finish-up/execution-ledger.md`. Do NOT commit.
