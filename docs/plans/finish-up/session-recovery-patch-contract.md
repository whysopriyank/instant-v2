# Session recovery: remaining implementation and acceptance contract

## Purpose and execution boundary

This is the entry document for a fresh OpenCode session completing the current
finish-up patch. It is not permission to declare the entire project complete.
Complete packets H-00 through H-06 below, report the acceptance result, and only
then select the next required packet from `program-manifest.md`.

Do not depend on the previous assistant's private memory, a particular API,
model, `/goal` syntax, or registered custom agent names. Read this repository's
`AGENTS.md` and applicable instructions first. Use the harness's goal mechanism
if available; otherwise run the same bounded execution loop explicitly.

The owner previously authorized implementation, verification, and local commits.
The most recent session stopped implementation for verification and requested
this handoff. The new session must receive an execution instruction before
changing code. No push, publication, deployment, real-provider traffic, or
destructive use of an existing database is authorized by this document.

## Verified starting point

- Repository: `/Users/priyank/Developer/sideproj/instant-v2`.
- Inspected revision: `26a1caf9856110b711315aaed4c5cbeaec3bbc36` on `main`.
- Last four commits, oldest first:
  - `20433cd`: RT-001 permission re-gating.
  - `e8565ce`: RT-002/RT-003 delivery and queue behavior.
  - `b1550d1`: DA-001/DA-003 storage and backup changes.
  - `26a1caf`: status, release envelope, and self-host documentation.
- Baseline is dirty: 13 modified tracked files and the untracked
  `execution-ledger.md`, before creation of this handoff.
- Modified files: `cmd/instantd/routes.go`; `internal/reactive/reactive.go`;
  `internal/storageapi/backend.go`, `root_durability_test.go`;
  `internal/sync/groups.go`, `rebind.go`, `rebind_group_test.go`,
  `reconnect_live_test.go`, `rt001_rebind_test.go`, `session.go`;
  `docs/plans/current-candidate-truth-ledger.md`;
  `docs/plans/finish-up/02-realtime-correctness.md`;
  `docs/reference/release-envelope.md`.

Treat the current filesystem as authoritative. Reconcile any differences before
editing; another session may have advanced the patch. Preserve all existing
work. Do not reset, stash away, discard, or recreate it from this summary.

### Evidence already executed, scoped to the previous dirty tree

The following passed in the previous verification pass:

```sh
go test -race ./internal/sync ./internal/reactive ./internal/storageapi ./cmd/instantd ./internal/backup ./internal/config -count=1
go vet ./internal/sync ./internal/reactive ./internal/storageapi ./cmd/instantd ./internal/backup ./internal/config
git diff --check
```

Formatting checks were clean. The exact regression
`TestDiskBackendFailedCleanupDoesNotBypassRootSync` first failed with
`retry bypassed failed root confirmation: <nil>`; its implementation repair
then passed in the storage package race suite.

`TestMidFanOutSwapHealsThroughNotifierRetry` explicitly executed and passed.
It drives the real notifier and its retry, using a fake query result and fake
transport writers. It is evidence of server recovery, not SDK consumption.

`TestLiveReconnectConvergesAfterDrop` and `TestSupersededGenerationDrops`
explicitly SKIPPED because integration configuration was absent. Earlier
handoffs claim PostgreSQL/corpus runs; do not attribute those runs to the latest
tree without reproducing them. No new corpus execution occurred in the last
verification pass. No commit was made during that pass.

## What to preserve, rather than repair again

The tree already contains the following mechanisms, with package-level evidence:

1. `Session.Subs` reads/writes identified in the review now use `sess.mu`.
2. `Notifier.Publish` is wired to `Manager.PublishGeneration`; it coordinates
   epoch validation and snapshot/watermark publication with `groupsMu`.
3. Fan-out rejects obsolete epochs before admission and between members.
4. Render failures return errors; failed WS members detach/close; SSE initial
   refresh failures propagate to the stream-closing path.
5. Queue depth one reopens after drain.
6. `DiskBackend.confirmAppDir` serializes first initialization through
   `createMu`; root sync occurs before the first upload proceeds.
7. The backend remembers unconfirmed directories if both root sync and cleanup
   fail. A later upload retries root confirmation rather than trusting `Stat`.
8. The SSE profile correction and partial-generation exception are explicitly
   marked as pending owner ratification.

These are not blanket production-acceptance claims. Keep their regression tests
and rerun them after touching adjacent interfaces.

## Packet sequence and ownership

| Packet | Goal | Write scope | Dependencies |
|---|---|---|---|
| H-00 | Establish exact candidate and test environment | execution/evidence ledger only | none |
| H-01 | Resolve authorization publication behavior faithfully | sync/reactive/daemon assembly; coordinator owns interfaces | H-00; independent design review |
| H-02 | Prove transport/client recovery and delivery ordering | sync transport tests, reactive integration tests, selected corpus/SDK fixture | H-01 |
| H-03 | Accept bounded storage repair | storageapi backend and durability tests only if a new red requires repair | H-00; parallel with H-01 discovery |
| H-04 | Execute live integration and selected corpus | evidence; smallest fixes routed to their component owner | H-01/H-02/H-03 |
| H-05 | Reconcile decisions and canonical status | finish-up and reference docs | preceding results |
| H-06 | Independent review, commit, handoff | focused fixes, reviewed local commit | H-05 |

Do not give simultaneous writers ownership of `groups.go`, `rebind.go`, or
`reactive.go`. A useful maximum is three workers: read-only architecture/security,
storage verification, and environment discovery. After design, one coordinated
writer owns the realtime change. Run independent review after the concrete diff
and evidence exist. If the harness lacks subagents, use separate sessions and
preserve the same ownership boundaries; do not label self-review independent.

## H-00 — Baseline and environment

Goal: identify exactly what is being repaired and what can be tested.

1. Read `00-operating-contract.md`, `01-scope-and-decisions.md`,
   `02-realtime-correctness.md`, `03-data-auth-integrity.md`,
   `program-manifest.md`, `execution-ledger.md`, the release envelope, and current
   candidate ledger. Never invent a filename when a document has moved.
2. Capture HEAD, branch, full porcelain status including untracked files, diff
   summary, UTC timestamp, Go version, and OS/architecture. Record an exact diff
   fingerprint for evidence, not just a count of changed files.
3. Inspect code through the configured graph tools first. The previous graph was
   stale; check freshness and fall back to current source when necessary.
4. Identify available PostgreSQL tooling and an owned test instance. Inspect
   `internal/testkit/postgres.go` and Makefile requirements before setting DSNs.
   Never assume a developer's default database is disposable.
5. Create a per-row evidence table with ID, assertion, real path, command,
   expected red, actual result, and remaining gap.

Exit: current tree and environment recorded; no unavailable test called green.

## H-01 — Authorization publication: the remaining material issue

### Current behavior and unresolved contract

Original RT-001a required no subsequent protected frames after allow→deny.
The working document now proposes an exception: an admitted generation can
deliver a superseded envelope, then retry under the new rule at the same txID.
This is marked pending ratification, not an approved security exception.

The current code checks `Gen` before each send, but a rule transition can occur
after that check and before the write. Refusing server snapshot publication
does not erase already sent data or its wire watermark. A later empty result
does not undo disclosure. Do not solve this by changing assertions to expect
the leak and calling the original requirement complete.

### Inspect these real paths

- `internal/sync/rebind.go`: `RefreshGate`, `rebindGroupLocked`,
  `PublishGeneration`, `SpliceAuthorize`.
- `internal/sync/groups.go`: `attachGroup`, `dispatchGroup`,
  `snapshotOrRefresh`, `failMember`, membership teardown.
- `internal/reactive/reactive.go`: `refreshOneAttempt`, `refreshResult`,
  `SnapshotPair`, retry scheduling, and `Publish`.
- `internal/reactive/incremental.go`: authorization, materialized state, deltas.
- `internal/sync/ws.go`: initial response, writer serialization, cancellation.
- `internal/sync/sse.go` and `admin_sse.go`: queued frames, dequeue/write,
  overflow, initial response, stream teardown.
- `cmd/instantd/refresh.go` and `routes.go`: every production wiring.
- Persisted rule-update/catalog-invalidation paths: discover their callers and
  establish when a rule transition is considered effective.

### Required design result before implementation

Produce a short, independently reviewed design that names:

1. The revocation linearization point: database commit, observed rule epoch, or
   completed revocation barrier. Distinguish these; do not silently substitute
   one for the documented guarantee. Bytes already accepted by the network
   cannot be recalled; any unavoidable limit must be explicit.
2. How new delivery admission and revocation coordinate for one affected group
   or session, including the interval immediately before an actual write.
3. What happens to pre-existing queued SSE envelopes and an initial answer
   being rendered concurrently with revocation.
4. What happens to slow or wedged clients, with a finite cancellation/write
   bound and no process-wide rule lock held across network I/O.
5. How invalidation, snapshot, delta baseline, and watermark remain consistent
   after a partially completed operation.

Candidate mechanisms to evaluate: per-group delivery coordination, revocation
epochs carried through the transport queue, and cancel/disconnect plus full
reauthorization for affected members. Prefer existing cancellation and close
facilities. Do not assume a process-wide `groupsMu` around socket writes is the
only alternative. A per-group lock alone is insufficient unless queue flushing,
in-flight writes, bounded cancellation, and rule-writer ordering are addressed.

If satisfying the literal contract requires changing the rule-update boundary
or the owner instead selects the partial-generation exception, record the exact
decision as pending until authorized. Continue independent packets; do not
invent approval or permanently weaken the contract to unblock implementation.

### Deterministic regression matrix

Each row needs a bounded barrier/channel test on the actual owning path:

| ID | Forced schedule | Required assertion |
|---|---|---|
| H-01a | Check passes; pause before send; revoke; resume | No newly admitted protected delivery after the chosen approved revocation boundary |
| H-01b | Revoke during multi-member fan-out | Each member has the declared safe outcome; no hidden stale snapshot or indefinite attachment |
| H-01c | Revoke after fan-out but before `Publish` | Old result cannot overwrite cleared/new-generation snapshot or advance its watermark |
| H-01d | Initial-answer computation/render overlaps revoke | Old cached result/flight/fallback cannot be sent as an authorized new answer |
| H-01e | Incremental success overlaps revoke | Spliced results obey the same generation boundary as full refresh |
| H-01f | Fill bounded SSE queue with several old generations; revoke | Queue policy enforced; do not infer a global one-envelope bound from a two-member fake-writer test |
| H-01g | One writer blocked; revoke that group; refresh another group | Bounded teardown and independent progress, no manager-wide authorization stall |
| H-01h | Rule lookup fails during admission/revalidation | No permissive fallback or protected frame; declared retry/disconnect outcome |

Proposed new test names may be chosen freely; record actual names in the ledger.
Existing `TestSwapMidFanOutStopsSpreadAndRefusesCommit` and
`TestMidFanOutSwapHealsThroughNotifierRetry` characterize the partial-send
policy. Preserve that knowledge, but do not use it to prove a stronger invariant.
If policy legitimately changes, explain which expectations become obsolete.

Exit: design accepted; code and meaningful regression tests match the same
contract; no unapproved exception hidden as acceptance.

## H-02 — Recovery, watermarks, and client behavior

The real notifier retry test now exists and passes. Do not report it missing or
replace it with another manually invoked `Emit` test. Its refresh and writers
are fakes, so SDK behavior and transport queue behavior remain separate duties.

Implement tests at the WS/SSE handler boundary with real notifier publication:

1. Force a deterministic delivery failure or revocation outcome through a
   bounded transport seam. Do not rely on kernel-buffer saturation or sleeps.
2. Keep a healthy sibling subscribed; reconnect the affected client through
   normal initialization and `add-query`, not a direct members-map insertion.
3. Assert exact result and matching watermark from the actual reconnect
   response; exercise both a surviving shared group and a recreated group.
4. Assert subsequent updates converge and no subscription/capacity leak remains.
5. If same-tx corrective frames remain part of an approved policy, pin the
   selected client/SDK version and demonstrate it applies the correction.
   A slice collecting bytes is not a client implementation. If clients discard
   duplicate txIDs, use an approved reconnect/reset/full-snapshot mechanism;
   do not fabricate transaction numbers or disable deduplication blindly.
6. Cover delta-enabled members. A partial generation cannot make the next delta
   depend on a baseline that some members never received.

Keep wire semantics precise: WS write success is transport acceptance, not
application receipt; SSE enqueue success is earlier than socket delivery.

Exit: exact-state client/transport evidence for the chosen recovery policy,
including failure and follow-up liveness, without manual publication bookkeeping.

## H-03 — Storage repair acceptance

No further storage code change is automatically required. Verify the current
repair first using:

- `TestDiskBackendConcurrentFirstUploads`.
- `TestDiskBackendFailedRootSyncRollsBack`.
- `TestDiskBackendFailedCleanupDoesNotBypassRootSync`.
- `TestDiskBackendFirstUploadPersistsNamespace`.
- `TestDiskBackendPutSurvivesReopen`.

Strengthen concurrency evidence if needed: block `syncRoot` with a barrier,
start a second upload, and prove it cannot acknowledge before confirmation.
Use a bounded wait and explicit entry signals. The existing 32-writer test
proves concurrent success/readback, not by itself fsync ordering under failure.

Ownership is `internal/storageapi/backend.go` and
`internal/storageapi/root_durability_test.go`. Verify rollback failure preserves
the unconfirmed state and a successful retry restores progress. Reopening a
backend is not a power-loss test. Report external/precreated-directory and
multi-process assumptions explicitly; do not claim unsupported crash proof.

Do not expand this packet into DA-002 upload metadata/overwrite redesign or
backup journal-ID collision repair. Those belong to their own Phase 03 contracts
unless a demonstrated dependency prevents acceptance of this specific patch.

## H-04 — Verification commands and integration evidence

From the repository root, execute focused tests first and inspect their output:

```sh
go test -race ./internal/storageapi -run 'TestDiskBackend(FailedCleanupDoesNotBypassRootSync|FailedRootSyncRollsBack|ConcurrentFirstUploads|FirstUploadPersistsNamespace|PutSurvivesReopen)$' -count=2
go test -race ./internal/sync -run 'Test(SwapMidFanOutStopsSpreadAndRefusesCommit|MidFanOutSwapHealsThroughNotifierRetry)$' -count=2
```

Adapt the second selector if the accepted policy requires replacement tests.
Enumerate the test names first and retain the red/green record; zero tests is
not success. Then run the six affected packages and static checks quoted above.
Run `go build ./cmd/instantd` to an explicit temporary output path if required
by the final interface change; avoid leaving a binary in the repository.

For live acceptance, use an owned PostgreSQL instance with the permissions and
logical-WAL configuration required by testkit. Supply credentials through the
environment, never printed commands or committed artifacts. Once configured:

```sh
INSTANT_TEST_INTEGRATION=1 go test -race ./internal/sync -run 'Test(LiveReconnectConvergesAfterDrop|SupersededGenerationDrops)$' -v -count=1
```

Add the new live test names, selected rule-transition tests, and SSE cases to
this command. Check `PASS` for each named test; `SKIP` is incomplete evidence.
Do not assume successful voluntary client-close coverage proves failed fan-out.

For corpus verification, read the current Makefile, `corpus/README.md`, and
`internal/corpus` fixture routing. Select `05-permission-deny` through the
supported command/fixture mechanism; do not invent a `--suite` flag or reuse a
v1 endpoint as a v2 oracle. Record target revision, scenario selection, actual
executed count, command, and outcome. Earlier pagination failures in scenarios
11/17 were reported pre-existing; preserve them as CF work unless this patch
causes a new regression. Never delete or bless failing scenarios to pass a gate.

Exit: selected live and package tests actually run; environment limitations
recorded as missing evidence, not implementation failure or completed work.

## H-05 — Decision and status reconciliation

Update `docs/reference/release-envelope.md` and
`docs/plans/current-candidate-truth-ledger.md` only after the behavior is settled.

- Preserve the original approval record. SSE selection and the partial-delivery
  exception are separate decisions; approving one does not approve the other.
- Remove remaining contradictory proposal/approval wording consistently, not
  just one sentence. A pending correction must not look like an approved row.
- If a new decision is authorized, use the document's required predecessor,
  timestamp, owner/evidence reference, and packet selection schema.
- The canonical ledger must describe the actual candidate and dirty fingerprint;
  retain old `14e2988`/`26a1caf` evidence in explicitly historical sections.
- Distinguish source implementation, hermetic tests, DB integration, SDK/corpus
  compatibility, independent review, and release acceptance.
- Record H-01/H-02 unresolved decisions or evidence honestly. No self-approval
  of missing security evidence and no inherited green from a different tree.

Exit: a new session can determine the current state without reading chat history.

## H-06 — Review, local commit, and next-phase selection

Give an independent reviewer the requirement table, exact diff, tests, raw
outcomes, skipped tests, and draft handoff. Ask specifically for authorization
ordering, SSE queued data, partial-generation delta behavior, session lock order,
root confirmation failure, and hidden status contradictions.

Repair actionable findings with focused tests. Do not repeat all suites without
a new change or unresolved concern. No commit represents acceptance until the
review and selected verification pass. If the user requests a checkpoint while
incomplete, label it explicitly as a checkpoint, not phase completion.

When execution and commit authority are active, stage the exact reviewed files
and inspect `git diff --cached --check` and the staged diff. Realtime shared
interfaces, daemon wiring, and tests must land together. A separately accepted
storage repair may be committed separately; never split dependent components
into broken intermediate commits. Do not use `git add .` indiscriminately.

Record resulting SHA(s), clean/dirty status, exact accepted rows and remaining
ones. Acceptance evidence must identify its tree; a documentation-only final
commit does not justify rerunning unrelated runtime suites without reason.

After H-06, consult `program-manifest.md` and the approved selection table:
finish unresolved Phase 01/02 gates, then select the lowest required Phase 03
packet whose dependencies are satisfied. Later ordering is evidence tooling
(04), compatibility (05), topology/recovery (06), qualification (07), and final
acceptance (08). Phase 09 and excluded/deferred external-provider, v1, publishing,
or deployment work are not automatically selected. This handoff does not prove
any later phase complete or authorize external operations.

## Copyable prompt for the new session

> Read AGENTS.md and docs/plans/finish-up/session-recovery-patch-contract.md.
> Execute H-00 first, preserving the dirty tree. Then complete H-01 through H-06
> in order, allowing independent storage verification in parallel. Verify and
> repair the actual implementation, not merely its comments. Do not weaken the
> authorization contract or invent owner approval. Use bounded deterministic
> tests, run the required live integration against owned fixtures, and obtain
> independent review. I authorize local implementation and commits of accepted
> work; do not push or deploy. If an external dependency or genuine policy
> decision prevents a row, record it precisely and continue independent ready
> work. Return actual test outcomes, remaining gaps, and commit SHAs. Do not
> start later finish-up phases until this patch's applicable gates are accepted.
