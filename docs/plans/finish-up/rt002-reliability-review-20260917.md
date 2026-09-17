# RT-002 Reliability Review — 2026-09-17

Candidate: `main` at `bbf5561f8d8e784b8ea4c1a2395c5581b760bac2` plus uncommitted RT-002 work (see Changes below).
Reviewer: independent read-only reliability review (subagent session `ses_f52b9fd13ffexxOdHNG3185f7u`), fresh, no write access.
Policy: DEC-001 explicit disconnect plus full replay (`Refresh outcome semantics (explicit disconnect + full replay)`; boundary `explicit disconnect plus full replay may be sufficient`). No durable-ack, no exactly-once. Detached members recover via fresh connection + full replay; still-attached members may receive same-transaction corrective frames via bounded retry; detached members are NOT required to receive in-place corrective frames.

## Verdict: ACCEPT

All 12 falsification attempts failed — code guards + pinning tests hold for each target under DEC-001. No unresolved blocker.

## Falsification targets (all FALSIFIED)

1. **Watermark advancement without accepted delivery** — FALSIFIED.
   Guards: `internal/reactive/reactive.go:777` progress guard; `:786-793` refresh error → retry, no publish; `:819-831` Emit error → retry, no publish; `:836-841` Publish false → retry; `internal/sync/groups.go:504-506` empty encode → error; `:486-489` render error; `:456-459` unwrap error; `:565-567` mid-fan-out Gen mismatch → superseded; `:589-596` total direct failure current → withhold, keep attached; `internal/sync/rebind.go:106-118` generation-guarded commit.
   Tests: `TestDispatchEncodeFailureServesNothing`, `TestDispatchEncodeFailureSchedulesSameTxRetry`, `TestNotifierChainAllDeliveryFailsWithholdsAndHeals`, `TestRefreshFailureDoesNotHotLoop`.
   Note: queued-only total failure publishes to a now-removed sub (orphan commit, invisible, heals via replay); all-missing-transport same shape, harmless.

2. **Snapshot/watermark disagreement** — FALSIFIED.
   Guards: publish snapshot-then-TxID `internal/sync/rebind.go:113-114`; read TxID-then-snapshot `internal/reactive/reactive.go:157-162`; fail-closed on nil snapshot `internal/sync/sse.go:485-488`, `internal/sync/admin_sse.go:206-212`, `internal/sync/ws.go:305-309`.
   Tests: `TestReconnectConvergesAfterSendFailure`, `TestNotifierChainPartialDeliveryCertifiesAndReconnects`, `TestWatermarkMatchesDeliveredGeneration`.

3. **Skipped transaction after reconnect** — FALSIFIED.
   Tests (all require missed-row presence + watermark equality): `TestReconnectConvergesAfterSendFailure`, `TestLiveReconnectConvergesAfterDrop`, `TestSSEFailureReconnectFullReplay`, `TestDeltaReconnectFullReplayCoversMissedTx`, `TestNotifierChainPartialDeliveryCertifiesAndReconnects`.

4. **Failed member still registered** — FALSIFIED.
   Guards: `internal/sync/groups.go:623-628` failMember (eager detach + Close); `:527-529` missing → detach; `:589-607` partial → detach failed; `:590-596` total-current → withhold + keep attached (RT-002c retry-OR-disconnect).
   Tests: `TestDispatchSendFailureDetachesOnlyFailedMember`, `TestDispatchMissingTransportDetachesExplicitly`, `TestNotifierChainPartialDeliveryCertifiesAndReconnects`, `TestNotifierChainAllDeliveryFailsWithholdsAndHeals` (pins keep-attached while withheld).

5. **Closed member still counted as live** — FALSIFIED (with accounting note below).
   Guards: `internal/sync/groups.go:281-307` detach accounting; `internal/sync/ws.go:161-166` deferred DetachAll; `internal/sync/sse.go:243-255` same.
   Tests: `Store.Len()==1` (chain C1/C2), `ConnCount==0` + `Store.Len()==0` after overflow, `TestSubscriptionCap`.

6. **SSE subscription leak** — FALSIFIED.
   Guards: runtime GET overflow → return → deferred delete + DetachAll `internal/sync/sse.go:297-302,243-255`; admin overflow → `defer teardownSubs` `internal/sync/admin_sse.go:174,57-60,238-253`.
   Tests: `TestSSEOverflowClosesStream`, `TestSSEFailureReconnectFullReplay`, `TestAdminSSEOverflowClosesStream` (with concurrent producer).

7. **Delta/full replay divergence** — FALSIFIED.
   Guards: `internal/reactive/reactive.go:803-811` ambiguous diff → full; `internal/sync/groups.go:508-519` delta only when eligible, fallback to full, same watermark.
   Tests: `TestDeltaRefreshNegotiation`, `TestDeltaReconnectFullReplayCoversMissedTx`, `TestWatermarkMatchesDeliveredGeneration`.

8. **Duplicate amplification** — FALSIFIED.
   Guards: one timer per sub `internal/reactive/reactive.go:591-598`; capped delays; partial fan-out returns nil → no retry.
   Tests assert singularity: chain C1 `countTx==1` + `publishCount==1`; chain C2 healthy/rejoined `==1`; encode-retry same-tx `7` with `sent==0`.

9. **Empty frame after encode failure** — FALSIFIED.
   Guards: `internal/sync/frame.go:67-71` json.Valid per value; `internal/sync/groups.go:630-639` encodeFrame nil on error; `:504-506` len==0 → error; `internal/sync/sse.go:74-84` encode before first Write/Flush.
   Tests: `TestDispatchEncodeFailureServesNothing`, `TestWriteSSEEventEncodeFailureWritesNothing/UnguardedControl`, `TestDispatchNeverEmitsEmptyFrames`, SSE reconnect stream validator (no empty/malformed/missing-op).

10. **Unbounded retry or hot loop** — FALSIFIED.
    Guards: `retryDelays` capped 5s `internal/reactive/reactive.go:410-418`; `stableRetryJitter` `[0.8,1.2]` capped `:434-446`; timer-owned only, single timer, cancel-safe `:559-711`.
    Tests: `TestRefreshFailureDoesNotHotLoop`, full `TestRetry*` matrix, `TestDispatchEncodeFailureSchedulesSameTxRetry` (bounded same-tx re-arm).

11. **Deadlock/global-progress blockage** — FALSIFIED.
    Lock order delivery-lease → groupsMu → sub/g/sess (`internal/sync/rebind.go:48-51,104-105`, `internal/sync/groups.go:145-148`); fan-out holds no locks across Send; bounded I/O (WS 10s `internal/sync/ws.go:20-21`, SSE 10s `internal/sync/sse.go:68`); SSE enqueue non-blocking.
    Tests: `TestWriteSSEEventHoldsDeliveryLeaseThroughFlush`, `TestWriteSSEEventExcludesRefreshGate` (blocked writer stalls own swap, sibling completes).

12. **Mock-client evidence substituted for real SDK evidence** — FALSIFIED.
    Real SDK: `examples/vite-vanilla/rt002-sdk-same-tx-correction.test.mjs` imports real `Reactor`/`InMemoryStorage`/`weakHash` from `@instantdb/core/dist/esm`, drives production `_handleReceive` (unconditional `querySubs[hash].result` overwrite, no txID dedupe), observes via real `subscribeQuery` callback + `dataForQuery`. Go `chainTap` mocks only `Session.SendRaw` transport boundary, makes no SDK claim. Wire-shape compatibility separately covered by `extractTriplesMirror`.

## Non-blocking notes (do not affect ACCEPT)

1. `internal/sync/groups.go:300-302` `appMembers` decremented even on no-op delete when group survives (concurrent double-detach undercounts cap; empty-group path avoids it).
2. `internal/sync/sse.go:76-77` `__raw` bypasses Encode by design; safe today (all producers post-encodeFrame non-empty), consider debug-assert `json.Valid`.
3. `internal/sync/frame.go:67-71` validation adds one O(payload) scan per generation; parity holds.
4. No dedicated Emit-failure hot-loop test (shared `scheduleRetry` timer, low risk).
5. WS fan-out sequential with 10s per-member timeout (bounded, siblings delayed ≤10s/generation; SSE non-blocking).
6. `ClearServedState` retains `TxID` with nil snapshot; all readers fail closed — correct, transient `(nil,oldTx)` possible.

## Changes reviewed (exact candidate delta)

- `internal/sync/frame.go`: `Encode` validates every value with `json.Valid` (matches stdlib Marshal rejection).
- `internal/sync/groups.go`: direct-write failures collected; nothing-served + current generation → withhold (error, members kept) for bounded same-tx retry; partial → detach failed + certify via siblings; post-regate total → superseded; queued (`SendRawGen`) path unchanged (immediate detach).
- New tests: `dispatch_encode_failure_test.go`, `sse_encode_failure_test.go`, `notifier_chain_failure_test.go`, `sse_failure_reconnect_test.go`, `delta_reconnect_test.go`, `examples/vite-vanilla/rt002-sdk-same-tx-correction.test.mjs`.
