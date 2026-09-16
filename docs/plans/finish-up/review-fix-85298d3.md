# Review repair contract — 85298d3

Baseline: clean `main` at `85298d365d744e3a5c4f7abea2f3fabb014e8177`.
Scope: the three findings from the candidate review. No later finish-up phases,
WS policy exception, rule-only broadcast, storage redesign, push or deployment.

Status: **COMPLETE** for the scoped A1/A2/S1 repair.

| ID | Required invariant and real path | Evidence | Status |
|---|---|---|---|
| A1 | `attachGroup`: registry/store identity, cap accounting, group and session membership are one atomic admission after any unlocked I/O | `TestAdmissionRecreatesGroupRemovedDuringLoad`; registry/store/member assertions; final resolution through both membership records is one critical section | GREEN |
| A2 | `attachGroup`: delayed rule loads cannot replace an intervening newer gate | `TestAdmissionDelayedLoadPreservesNewerGate`: block allow load, install deny, release old allow; deny gate/epoch and snapshot preserved | GREEN |
| S1 | Runtime/admin SSE: observed re-gating is serialized with dequeue generation validation and actual bounded write, per subscription | `TestWriteSSEEventExcludesRefreshGate` (write and flush); stale/cancelled-pointer drop; sibling gate progress; deadline/error tests; inspect runtime/admin/initial routing to shared helper | GREEN |

Use existing rule-loader and transport seams first. For A1 the final lock-gap
itself has no blocking hook; pair deterministic removal during unlocked I/O with
structural inspection of the single final critical section. Do not invent a
scheduler-dependent red test for that interval. A2 has a direct deterministic
red. SSE test design follows the independent lock-boundary review.

Run focused regressions before fixes, twice with race after repair, then sync /
reactive / daemon package checks, the two existing live PostgreSQL acceptance
tests, vet, and diff checks. Independent review must examine lock order and
deadline enforcement. Pending remains pending until those outputs are inspected.

## Evidence and boundary

- A2 red: `go test ./internal/sync -run '^TestAdmission(DelayedLoadPreservesNewerGate|RecreatesGroupRemovedDuringLoad)$' -count=1`
  failed at `delayed allow load replaced the newer deny gate` before the fix.
- Admission first green: the same two tests with `-race -count=2` passed.
  A1's loader-removal branch was already correct; the previously unprotected
  final resolution-to-insertion gap is verified by lock-scope inspection, not a
  fabricated deterministic reproduction.
- Reactive package after delivery-lock implementation:
  `go test -race ./internal/reactive -count=1` passed (7.643s).
- Architecture review selected a per-subscription RWMutex. All gate mutation
  waits acquire its exclusive lock **before** `groupsMu`; SSE write leases hold
  only its read lock through deadline-bounded write and flush. Publication takes
  the read lease then `groupsMu` to preserve serialization of snapshot/watermark
  pairs. Queue identity is the exact subscription pointer, not a reusable key.

An SSE write admitted before an observed re-gate may finish before that gate is
installed. Old epochs cannot begin writes after installation. This does not
promise synchronization with a database rule commit, client receipt of buffered
bytes, proactive rule-only notification, or a change to the WS exception.

## Final verification (2026-09-07 UTC)

- S1 targeted mutation red: temporarily replaced the delivery lease with the
  prior check-then-write behavior (retaining the epoch check), ran
  `go test ./internal/sync -run '^TestWriteSSEEventExcludesRefreshGate$' -count=1`.
  Both `write` and `flush` cases failed with `RefreshGate completed before
  guarded ... completed`. Restored the lease immediately afterward. This is a
  focused mutation check of the extracted real writer, not an end-to-end replay
  of the original HTTP handler.
- `go test -race ./internal/sync -run '^(TestAdmission|TestWriteSSEEvent)' -count=2 -v`:
  11 top-level tests passed twice, including both write/flush subcases.
- `go test -race ./internal/sync ./internal/reactive ./cmd/instantd -count=1`:
  all three passed (2.354s / 8.171s / 2.913s).
- `INSTANT_TEST_INTEGRATION=1 DATABASE_URL=postgres://priyank@localhost/postgres go test -race ./internal/sync -run 'Test(LiveReconnectConvergesAfterDrop|SupersededGenerationDrops)$' -v -count=1`:
  both executed and passed on isolated test databases (no skips).
- `go vet ./internal/sync ./internal/reactive ./cmd/instantd`: exit 0.

Evidence limits: the write/flush tests block in the actual helper's response
writer, invoke real `RefreshGate`, and assert sibling progress and old-epoch
rejection after the swap. Their negative exclusion check uses a bounded 25ms
observation, so it is complemented by structural inspection of the shared
RWMutex and lock ordering, not described as a scheduler-independent proof.
The separate live tests cover WS/reconnect generation behavior; they are not
live SSE/delta/SDK revocation acceptance. Runtime/admin routing and the guarded
admin initial answer are inspected directly; no new HTTP-handler test seam was
introduced.

## Independent review and handoff

Fresh-context, read-only Sol review found **no actionable blockers** in A1/A2/S1.
It inspected both gate-mutation sites, all generation-callback call sites,
runtime/admin initial and queued routing, and error/teardown paths. It found no
reverse delivery/registry lock acquisition or global lock across network I/O.
The reviewer accepted the recorded checks as proportionate, with the evidence
limits above. Cancellation is checked at lease admission; removal itself is not
serialized as a gate mutation, so already-admitted writes may finish.

Formatting check (`gofmt -l` on all nine touched Go files) and
`git diff --check` were empty. The test suites compile both changed packages and
daemon wiring; no separate release build or live SSE/SDK acceptance was run.

Compared with the clean baseline, changes comprise seven tracked Go files,
two new regression-test files, and this contract/evidence document. No unrelated
working-tree files were changed. There is no wire-format or database migration.
The generation-aware callback signature change is internal.

No remaining blocker for this scoped repair. Safest next action is reviewing
and committing this local diff when requested; this does not declare later
finish-up phases complete. No commit, push, or deployment performed.

## Current-candidate refresh (2026-09-08 UTC)

The prior handoff above is preserved as historical evidence for the earlier
working session. A fresh Sol review of the current dirty tree found two A1
atomic-admission gaps: teardown could complete while unlocked rule I/O was in
flight and a late admission could resurrect the session; and a new group's
reactive Store entry could become visible before all registry/member/session
records were committed. A2 and S1 remained sound.

The coordinator repaired those gaps in the scoped sync packet:

- `Session.closing` is set before `DetachAll` snapshots membership, and
  `attachGroup` rejects closing sessions both after unlocked rule I/O and at
  the final commit boundary.
- New group/member/session/accounting state is prepared under the existing
  registry and group locks; `Store.Add` is the final visibility step, with
  rollback on a Store cap failure. Existing-group member publication keeps
  the group lock through session ownership publication.
- `TestAdmissionRejectsClosingSessionAfterBlockedLoad` covers the teardown
  interleaving and proves no group, Store subscription, cap count, or session
  membership remains.

Current local evidence:

- `GOCACHE=/private/tmp/instant-v2-gocache go test -race ./internal/sync -run '^TestAdmission(DelayedLoadPreservesNewerGate|RecreatesGroupRemovedDuringLoad|RejectsClosingSessionAfterBlockedLoad)$' -count=2 -v`: PASS.
- `GOCACHE=/private/tmp/instant-v2-gocache go test -race ./internal/sync ./internal/reactive ./cmd/instantd -count=1`: PASS.
- `GOCACHE=/private/tmp/instant-v2-gocache go vet ./internal/sync ./internal/reactive ./cmd/instantd`: PASS.
- `gofmt -l` over all nine touched Go files: empty; `git diff --check`: PASS.

Required live PostgreSQL evidence remains blocked, not skipped or claimed:
`TestLiveReconnectConvergesAfterDrop` and `TestSupersededGenerationDrops`
were run with `INSTANT_TEST_INTEGRATION=1` and the owned test URL
`postgres://priyank@localhost/postgres`; both failed immediately because
`localhost:5432` had no response (`pg_isready` confirmed the same). No service
was started or modified.

Current scoped status: **PARTIAL** — local A1/A2/S1 implementation and package
checks are green, but the required live acceptance and any commit-time clean
candidate proof remain outstanding. No later finish-up packet was started; no
commit, push, deployment, provider traffic, or destructive database action was
performed.

Next prerequisite: make the explicitly owned PostgreSQL test service available,
then rerun the two exact live tests and refresh this handoff before any release
or final-candidate claim.
