# Phase 02 — Realtime authorization and delivery correctness

Phase gate: `REALTIME_TRUSTWORTHY`
Packets: `RT-001`, `RT-002`, `RT-003`
Required for: any production realtime profile

These packets are ordered. Permission rebinding and delivery semantics both
cross authorization/state boundaries and require fresh review. Do not combine
their implementation into one goal.

## RT-001 — Dynamic permission rebinding

**Goal objective:** `Ensure an existing realtime subscription cannot continue receiving newly denied data after persisted application rules change.`

**Class:** confirmed security/correctness defect.
**Real path:** attach in `internal/sync/groups.go` → captured `QueryGate` →
reactive refresh → daemon query executor.

### Ledger

| Row | Desired invariant | Red evidence |
|---|---|---|
| RT-001a | An allow→deny rule update prevents all subsequent protected frames on the existing subscription, within one bounded race: a generation admitted before the re-gate may deliver at most one superseded envelope to members served before the swap lands (wire bytes cannot be recalled). The server certifies nothing for it — no snapshot, no watermark advance — and the armed retry re-serves every attached member under the new gate at the same txID. | Attach under allow, persist deny, trigger refresh, observe leak before repair; race order pinned by `TestSwapMidFanOutStopsSpreadAndRefusesCommit`, healing by `TestMidFanOutSwapHealsThroughNotifierRetry`. Bound added post-approval; owner ratification pending. |
| RT-001b | A deny→allow change follows the declared reauthorization/reconnect policy without using stale state. | Controlled rule-version transition. |
| RT-001c | Concurrent rule update and data invalidation has one safe deterministic outcome. | Barriers around version read and refresh. |
| RT-001d | Shared subscription groups cannot mix members authorized under incompatible rule versions. | Two members spanning a rule transition. |
| RT-001e | Rule lookup failure fails closed and does not retain a formerly permissive gate. | Inject catalog/rule-load failure. |

### Decision and implementation boundary

DEC-001 must say whether live sessions are immediately reauthorized, detached and
required to resubscribe, or version-bound until an explicit protocol event. The
recommended safe behavior is a rules version on each group followed by
reauthorization or disconnect when the version changes.

Write lease: `internal/sync/**`; a narrow `cmd/instantd` assembly hook remains
coordinator-owned. Tests may use `internal/reactive` without altering its
semantics. Do not implement dynamic per-record SQL permission filtering or
redesign CEL rules.

### Verification and review

Run the exact deterministic transition tests, sync package race tests, affected
reactive tests, and the selected permission corpus slice. A security reviewer
must challenge cached allow decisions, group sharing, lookup failure, and
concurrent invalidation. Completion requires real-path denial with no leaked
frame, not only a cache-unit test.

## RT-002 — Refresh watermark and delivery outcome

**Goal objective:** `Define and implement refresh outcome semantics so a transaction watermark never falsely certifies a member that received neither a usable frame nor an explicit replay-triggering disconnect.`

**Class:** confirmed contract/correctness gap.

### Ledger

| Row | Desired invariant | Red evidence |
|---|---|---|
| RT-002a | Watermark meaning—computed, queued, or delivered—is explicit and consistent across WS and SSE. | Source/behavior characterization. |
| RT-002b | Render or encode failure cannot advance a client-visible processed state silently. | Inject renderer/encoder failure. |
| RT-002c | Send/backpressure failure causes a declared retry or explicit disconnect/replay outcome. | Inject member send and SSE overflow failure. |
| RT-002d | Reconnect converges to exact current state without skipping the failed transaction. | Fail delivery, reconnect, compare exact result and watermark. |
| RT-002e | No empty frame is emitted after encoding failure. | Encoder failure assertion. |

### Boundary

Architecture review first freezes the smallest acceptable policy. A durable
per-client acknowledgment protocol is not authorized unless required by the
selected contract; explicit disconnect plus full replay may be sufficient.

Write leases must be sequential: reactive owner for refresh state, then sync
owner for transport outcome. Shared interfaces are coordinator-owned. Do not
rewrite transports or promise stronger delivery durability than DEC-001.

### Verification and review

Use injected deterministic send/render failures and bounded channels, never
scheduler sleeps. Run focused tests twice under race, adjacent reactive/sync
packages, and reconnect corpus cases. Reliability reviewer attempts to produce a
watermark/frame mismatch, duplicate amplification, deadlock, or subscriber leak.

## RT-003 — Queue depth one

**Goal objective:** `Make every accepted reactive queue-depth configuration recover from pressure, or reject depth one during configuration validation.`

At depth one, the low-water threshold becomes zero and a latched gate may never
reopen. Choose one minimal behavior:

- validate queue depth as zero/unbounded or at least two; or
- special-case the low-water calculation and prove reopening.

Write lease: either `internal/config/**` or the narrow reactive gate, not both
without coordinator approval. Red evidence fills and drains a depth-one queue and
asserts recovery or startup rejection. Run focused configuration/reactive tests
and routine review.

## Phase completion

All three packets must be green for a production realtime claim. An alpha may
exclude depth one through validation. RT-001 and RT-002 may not be accepted as
exceptions while realtime subscriptions remain supported.
