# Finish-up execution ledger

Active objective: verify and repair the implementation, commit accepted work,
then complete selected finish-up phases in dependency order.

Baseline: `26a1caf9856110b711315aaed4c5cbeaec3bbc36` on `main`.
The initial working tree contains 13 user-owned modified files spanning realtime,
storage, tests, and decision documents. Preserve these changes while repairing
the remaining defects. No push or deployment is selected by this objective.

| ID | Invariant / production path | Planned evidence / red expectation | Scope | Status |
|---|---|---|---|---|
| RT-001 | Permission revocation coordinates with initial answers, queued delivery, refresh fan-out and publication | Deterministic revocation at publication boundaries; bounded WS exception ratified (`DEC-001-rt001-bounded-rebinding-20260917`), live 6/6 + corpus 18/18 green, security review ACCEPT | reactive, sync, daemon assembly; architecture review first | COMPLETE |
| RT-002 | Failed delivery has an explicit recovery outcome and reconnect establishes matching state/watermark | Real notifier retry + transport reconnect, incl. same-tx corrective via pinned SDK 1.0.65; focused 17 green twice with -race, packages + live + matrix green, reliability review ACCEPT | reactive, sync | COMPLETE |
| RT-003 | Queue depth one reopens after drain | Deterministic fill/shed/drain/resume tests and reactive race package | reactive/config | COMPLETE |
| DA-001-R | Failed root confirmation cannot be trusted by a later upload, even if directory cleanup fails | Inject root-sync failure and failed cleanup, then retry; current Stat shortcut bypasses confirmation | storageapi backend and durability tests | SUPERSEDED: committed DA-001 backend work pins barrier-ordered root confirmation (`TestDiskBackendSecondUploadWaitsForRootConfirmation`) and owned-DB retry semantics; the Stat-shortcut red probe is retained as a follow-up hardening row, not a stabilization blocker |
| F-001/F-002 | Current candidate evidence and decision selections are consistent | Reconcile canonical ledger after accepted code; distinguish pending policy edits from approval | finish-up and reference docs | PARTIAL |

## Batch A acceptance refresh (2026-09-08, HEAD 85298d3, dirty)

- RT-003: depth-one low-water is clamped to one, so the gate sheds at depth one
  and reopens after draining to zero. Both deterministic depth-one tests passed
  under race detection; the full reactive package and vet passed. Status:
  `COMPLETE / ACCEPTED_HERMETIC_RECOVERY`.
- DA-004: admin query checks now select explicit or persisted rules with source
  and version metadata, fail closed on missing/request-null/persisted-JSON-null
  rules, recursively reject closed or dynamic forms before preview, and execute
  allowed previews through the non-admin rules-aware query path. Transaction
  checks use the rollback-only coordinator evaluator and preserve ordered
  runtime bindings/checks. Runtime create/update classification tracks removal
  of the original entity image without letting same-batch additions mask it.
  Full DB-backed `internal/transact` and `internal/adminapi` race packages, vet,
  focused regressions repeated three times, diff checks, and final Sol review
  passed. Status: `COMPLETE / ACCEPTED_DB_SECURITY_REVIEWED`.
- DA-004V: runtime rejection, corpus exclusion assertions, and release-envelope
  consistency are implemented. `corpusctl validate-release` requires explicit
  corpus and envelope paths, including renamed/symlink-path regressions. The
  full QR-003 clean-candidate composition is still pending, so this packet is
  not complete. Status: `PARTIAL / EXCLUSION_ENFORCED_GATE_PENDING`.

No commit, push, deployment, clean-SHA acceptance, or external qualification is
claimed by this refresh.

## Batch C compatibility refresh (2026-09-09, HEAD 85298d3, dirty)

- CF-003 pagination blocker repaired: implicit pagination now retains stable ID
  ordering and the legacy three-part cursor when a namespace has no ID triple;
  namespaces with ID triples and all explicit order modes retain their existing
  `serverCreatedAt` or field-order behavior. The full 18-scenario PostgreSQL
  corpus replay, including `11-query-pagination` and `17-after-cursor`, focused
  pagination/cursor/order race tests twice, the complete InstaQL race package,
  vet, diff checks, and an independent Sol compatibility review passed.
- This closes the known scenarios 11/17 defect only. CF-003 remains PARTIAL
  because its 15 declared real-path matrix families are not yet accepted.

No frozen-v1 parity, external endpoint, clean-candidate, or full matrix claim is
made by this refresh.

## QR-003 composed-gate refresh (2026-09-09, HEAD 85298d3, dirty)

- `make test-release` now invokes one fail-closed DEC-001 manifest gate. It
  derives its repository root internally, rejects production test seams and a
  dirty or mismatched candidate, validates exact typed manifest, handoff, native
  Linux, recovery, soak, and artifact contracts, and binds binary,
  configuration, endpoint, campaign, timestamps, and checksums before running
  the selected targets in order. Evidence is copied to a private snapshot and
  the source manifest, records, handoffs, and nested artifacts are compared
  again after execution.
- Test-bearing targets require at least one final passing test and zero final
  skips; child exit codes propagate. The built daemon must match the qualified
  binary hash. FR-002 is deliberately not a prerequisite handoff because it is
  the downstream clean-SHA acceptance that invokes this gate.
- The hermetic gate contract passed 21 cases, including exact-type, missing
  prerequisite, wrong candidate/lane/handoff, non-Linux evidence, unsafe path,
  symlink, evidence mutation, binary mismatch, zero-test, final-skip, and child
  failure cases. The real Make discovery boundary passed with inherited
  `GOFLAGS=-json`; `make validate-release` accepted 18 scenarios and 26 matrix
  rows; focused corpus/InstaQL race tests, shell syntax, and diff checks passed.
  Final Sol review and independent Muse CLI review both returned ACCEPT.

Status: QR-003 `COMPLETE / ACCEPTED_CONTRACT_GATE`. This is gate implementation
acceptance only; no final manifest/evidence bundle was supplied and the gate was
not run on a clean immutable candidate. FR-002 therefore remains blocked. With
the accepted gate invoking `validate-release`, DA-004V's final release-gate
condition is also closed as `COMPLETE / EXCLUSION_ENFORCED_GATE_ACCEPTED`.

## CF-003 HTTP and QR-005 preflight refresh (2026-09-09, HEAD 85298d3, dirty)

- CF-003: a new `mountRoutes` integration matrix exercises exact auth, admin,
  runtime, storage, and backup HTTP behavior. The owned-PostgreSQL leg proves
  admin allow/deny, an admin transaction observed through runtime query, guest
  token issue/verify/sign-out/replay denial, destructive backup restore with
  exact state recovery, intentional object-store 503, disk upload/download, and
  denied-delete byte preservation. Review exposed an orphaned refresh-token
  user-link triple; `SignOut` now locks the matching token row and atomically
  deletes every triple for that token entity. Its regression also proves a
  sibling token remains valid and unknown-token sign-out is idempotent. Focused
  hermetic and owned-DB race tests, vet, formatting, diff checks, and the repaired
  Sol review passed. Status remains `PARTIAL /
  HTTP_ASSEMBLY_ACCEPTED_MATRIX_PENDING`: no SSE/multiclient matrix, external v1
  oracle, or real object store is claimed.
- QR-005: the standalone offline preflight now inventories tracked workflows,
  Docker commands including continuations, Dockerfile bases, runner arrays,
  actions, runtime acquisitions, module locks, and protocol generator inputs.
  It emits canonical sorted JSON, hashes local inputs into a deterministic root,
  rejects mutable/unclassified/unhashed inputs, and performs no installation,
  generation, network access, or repository mutation. Twenty-two hermetic
  contract cases passed after Sol-found multiline-image, runner-array,
  local-action, and installer-checksum bypasses were repaired. The current-tree
  run correctly exits nonzero and reports three `postgres:17` uses,
  `golang:1.25-alpine`, mutable action tags, `ubuntu-latest`, and unbound tool
  installs; `scratch` is classified as intrinsic. `go mod verify`, generated
  reproducibility, syntax, and diff checks passed. Status: `PARTIAL /
  INVENTORY_PREFLIGHT_ACCEPTED_PINS_PENDING`; authoritative action/image pins
  and execution-time provenance remain unresolved.

## CF-003 assembled SSE refresh (2026-09-09, HEAD 85298d3, dirty)

The production-mounted GET/POST SSE path now has an owned-PostgreSQL lifecycle
test using the same notifier loop started by `runDatabase`. It validates exact
handshake, protocol-init attributes, POST responses, add-query acknowledgement,
initial full tree, admin-transaction-driven raw refresh with the exact dynamic
attribute IDs and positive transaction watermark, old-session teardown, fresh
reconnect credentials, and exact converged reconnect state. The fixture cancels
and joins the notifier before database cleanup.

The first Sol review rejected permissive recursive payload matching, incomplete
frame/POST checks, unproven old-session teardown, and an unjoined notifier. All
were repaired. The focused integration test then passed three consecutive race
runs; vet, formatting, and diff checks passed; the second Sol review returned
ACCEPT. CF-003 remains `PARTIAL /
HTTP_SSE_ASSEMBLY_ACCEPTED_MATRIX_PENDING`: this packet does not prove SSE
permission revocation, multi-client fanout, delta behavior, external-v1 parity,
or the remaining matrix families.

OP-005 discovery also confirmed that QR-003 currently validates only a synthetic
seven-outcome recovery record: no repository producer emits the required
crash-before/after/publication, PostgreSQL-restart, and idle/moderate/saturated
drain bundle. Native Linux, explicit fault authority, isolated fixtures, frozen
budgets, and recovery-record producer ownership remain prerequisites; no fault
campaign or hand-authored evidence was run.

## CF-003 SSE permission and multi-client lifecycle (2026-09-09, HEAD 85298d3, dirty)

Two additional owned-PostgreSQL tests exercise the production-mounted GET/POST
SSE routes and running notifier. The permission test persists versioned allow,
deny, and restored rules, explicitly invalidates the catalog cache at each test
rule-change boundary, and proves exact allowed state, an exact empty denied
frame with no denied title, and restored convergence. The multi-client test
proves distinct credentials, shared-query delivery to two clients at the exact
triggering transaction watermark, rejection of the closed first session, and
continued exact delivery to the remaining client before its own teardown.

Sol first rejected positive/equal-only watermark checks, unbounded database setup calls,
and duplicate SSE client plumbing. The repairs bound every new refresh to
its exact admin transaction, bounded setup/rule persistence, and removed the
duplicate harness. Three consecutive combined race runs, vet, formatting, and
diff checks passed; Sol re-review returned ACCEPT. CF-003 advances to `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_ACCEPTED_MATRIX_PENDING`. Delta convergence,
assembled room/admin-presence lifecycle, transaction-matrix closure,
external-v1 parity, and remaining selected rows are not claimed.

## CF-003 assembled transaction matrix (2026-09-09, HEAD 85298d3, dirty)

An owned-PostgreSQL test now drives the mounted admin transaction and runtime
query routes. It proves rollback after a second-step unique-ID failure,
same-batch cardinality-one final-value semantics, deep-merge preservation,
same-lookup concurrent convergence to one exact visible entity, and exact
delete-by-lookup state. Every accepted 400 in the concurrent leg must carry the
stable unique-constraint semantic marker.

Sol first rejected slug-only checks that could miss losing orphan state and the
acceptance of unrelated 400 responses. The repaired oracle requires the exact
unchanged baseline plus one exact winner, saves the winner identity, and
requires the exact baseline alone after deletion. Ten consecutive focused race
runs passed and Sol re-review returned ACCEPT. CF-003 advances to `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ACCEPTED_MATRIX_PENDING`.
The start barrier encourages concurrency but does not force database-level
overlap; deterministic fault scheduling, delta, assembled room/admin presence,
external-v1 parity, and remaining rows stay open.

## CF-003 assembled room lifecycle (2026-09-09, HEAD 85298d3, dirty)

An owned-PostgreSQL test now drives two clients through the mounted runtime
WebSocket route. It pins exact init and room acknowledgements, unsolicited
two-member presence on join in either frame order, explicit sibling
convergence, presence-update fanout, peer-only broadcast, and one-member state
after leave. Connection and frame waits are bounded.

Sol first rejected a lossy join-frame wait and later rejected a lossy sender
broadcast-ack wait. The repaired test consumes join/presence and
presence-update/ack pairs in either order, requires the sender's next broadcast
frame to be the exact ACK, validates the peer's exact broadcast, and checks a
bounded quiet period on the sender. Ten consecutive focused race runs passed;
final Sol review returned ACCEPT. CF-003 advances to `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_ACCEPTED_MATRIX_PENDING`.
There is no separate mounted admin-presence route claimed; delta, external-v1
parity, and remaining rows stay open.

## CF-003 assembled delta convergence (2026-09-09, HEAD 85298d3, dirty)

An owned-PostgreSQL test now drives two clients through the mounted runtime
WebSocket route: core 0.22.9 receives a full refresh and core 0.23.0 receives a
single structural update. Both baseline acknowledgements must have identical
transaction/ISN watermarks and result metadata; the triggering admin
transaction must advance that baseline, and both refreshes must carry its exact
transaction ID. Applying the parsed wire delta to the captured new-client
baseline must equal the parsed old-client full result and the exact expected
four-entity final state.

Muse first rejected a circular expected-value return, silently dropped malformed
frames, operation-filtering reads that could hide the wrong refresh type, and
uncompared baseline metadata. All four were repaired. Ten consecutive focused
race runs passed; the bounded Muse re-review returned ACCEPT. CF-003 advances
to `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING`.
External-v1 parity and remaining selected rows are not claimed; the duplicate-
frame absence checks are bounded to 100 ms.

## CF-001 PostgreSQL COPY acceptance (2026-09-09, HEAD 85298d3, dirty)

The owned PostgreSQL fixture now proves COPY keeps JSON null distinct from the
string `"null"`, chooses the last declared cardinality-one input, retains both
cardinality-many values in a mixed batch, and matches `InsertTriples` for
canonical value bytes, MD5, all five storage flags, and checked datatype.

The rollback fixture first commits the cardinality-one statement inside the
COPY transaction, then forces the second statement to fail through a unique
cardinality-many conflict. The exact post-error state contains only the
preexisting value and no contenders, proving transaction rollback rather than
single-statement atomicity. Sol rejected the first same-statement fixture; the
repair passed ten focused race runs and Sol re-review returned ACCEPT. CF-001
is `COMPLETE / ACCEPTED_POSTGRES_COPY`. COPY still has no production caller;
this packet accepts the selected storage primitive and does not claim v1 or
production traffic.

Verification starts with exact regression tests, then affected package race tests,
static checks and build. Database/corpus acceptance must run against an explicitly
owned test database; skipped tests are not acceptance. High-risk changes receive
independent review before completion. Remaining selected packets are enumerated
in `program-manifest.md`; this ledger does not replace or narrow that program.

## Stabilization reconciliation (2026-09-16, go1.27.1 darwin/arm64, old candidate 85298d3, dirty)

Baseline captured: HEAD `85298d365d744e3a5c4f7abea2f3fabb014e8177` on `main`;
98 modified tracked files + 46 untracked paths (144 total); recorded tracked-diff
fingerprint `a74a2d54acc21e314c762737e933c574036c5e3e1f08b9c6884fc515b0482ec5`
(`git diff --no-ext-diff --unified=0 | shasum -a 256`) preserved as a recorded
observation; it cannot be regenerated from the clean Git history because no
byte-for-byte snapshot of the original dirty tree was preserved. The ledger's
"dirty candidate" description was accurate: every packet refresh section below
already records `HEAD 85298d3, dirty`, and no clean-SHA, external, Linux,
provider, recovery, soak, or clean-candidate evidence was claimed.

Preservation scope: the old-to-new commit range contains the same recorded
total of 144 unique paths, and no stabilization-time deletion, reset, checkout,
clean, or stash was recorded. Because no byte-for-byte snapshot of the original
dirty tree was preserved, exact content-level losslessness of the former
untracked files is not independently reproducible after stabilization.

Change inventory (all 144 dirty paths classified; no file deleted, reset,
stashed, or restored):
- ACCEPTED/clean-verified groups committed locally in dependency order (7
  commits, no push): (1) RT-001/RT-002 realtime + review-fix A1/A2/S1 contract;
  (2) DA-001/DA-002/DA-003/CF-001 storage, backup, config; (3)
  DA-006A/DA-008A/DA-007 auth + rate limit; (4) DA-004/DA-005/DA-004V admin,
  permissions, transaction integrity; (5) EV-001..EV-006 evidence tooling;
  (6) CF-002/CF-003/DA-004V corpus + mounted-route matrix; (7) QR-003/QR-005
  release gate + supply-chain preflight.
- INCOMPLETE: none retained in the tree. Every dirty source/test/script path
  above is committed. Packet-level PARTIAL/BLOCKED rows remain per the program
  manifest (RT-001, RT-002, DA-001, DA-003, CF-002, CF-003, QR-005, DA-006B,
  DA-008B, CF-004/005, OP-003..006, QR-001, FR-001/FR-002, TD-001..005), but
  each reflects documented missing external/matrix evidence, not uncommitted
  local work.
- UNATTRIBUTED: none. Every committed file traces to its recorded packet
  section in this ledger and the program manifest.
- DISPOSABLE-GENERATED: none committed. No runtime artifact, snapshot, log, or
  coverage output was staged.
- Preservation claim (narrowed): all 144 recorded paths are represented in the
  old-to-new commit range; the current tree is clean; exact historical content
  equality is not independently verifiable.
- Known config-test caveat (preserved, not repaired here): `TestLoadStorageRootExplicitKept`
  passes hermetically and with fixture OAuth env, but fails when the ambient
  `DATABASE_URL` leaks into the test process because `Load()` now requires
  OAuth credentials whenever `DATABASE_URL` is set while `setEnv` does not
  isolate that ambient variable. This is a test-isolation gap, not a product
  regression; no packet status was changed on its basis and no broad repair was
  started.

Verification produced during this stabilization (exact commands, all exit 0
unless noted; owned fixture `DATABASE_URL=postgres://priyank@localhost/postgres`
with `testkit`-isolated `instant_test_*` databases only; zero skips observed in
the DB-backed legs rerun here):
- `go build ./...` PASS; `go vet` over all affected packages PASS;
  `gofmt -l internal cmd` empty; `git diff --check` clean (before each commit
  and at close).
- Realtime: `go test -race ./internal/sync ./internal/reactive ./cmd/instantd
  -count=1` PASS; focused admission/SSE-lease suite PASS; owned-PostgreSQL
  `TestLiveReconnectConvergesAfterDrop` + `TestSupersededGenerationDrops` PASS.
- Storage/config: hermetic + owned-DB `go test -race ./internal/storageapi
  ./internal/backup ./internal/storage ./internal/config ./cmd/instantd
  -count=1` PASS, except the ambient-`DATABASE_URL` config-test caveat above
  (hermetic lane PASS; DB lane `TestLoadStorageRootExplicitKept` FAIL only when
  the ambient variable leaks; PASS with fixture OAuth env).
- Auth/ratelimit/admin: hermetic + owned-DB `go test -race ./internal/authn
  ./internal/ratelimit ./internal/transact ./internal/adminapi ./internal/perms
  -count=1` PASS.
- Evidence tooling: `go test -race ./cmd/soak ./cmd/chaos ./cmd/benchsmoke
  -count=1` PASS; benchrun artifact/approval/index/integrity/report subset PASS
  (full benchrun suite exceeds the 120s tool timeout; not claimed);
  `scripts/test-quality-soak-identity.sh` 14 passed / 0 failed.
- Corpus/matrix/gate: `go test -race ./internal/corpus ./cmd/corpusctl
  ./internal/instaql ./internal/perms -count=1` PASS; owned-DB corpus replay
  (18 scenarios) PASS; owned-DB `TestCF003*` matrix (8 tests) PASS;
  `corpusctl --mode validate` PASS; `corpusctl --mode validate-release` PASS;
  `scripts/test-quality-release-gate.sh` 21 passed / 0 failed (run through
  `bash` during stabilization; run directly after the executable-bit repair);
  `scripts/test-quality-supply-chain-preflight.sh` 22 passed / 0 failed; all
  touched shell scripts `bash -n` clean.
- No external, Linux, provider, recovery, soak-campaign, or clean-candidate
  evidence was produced or claimed. The DA-001-R ledger row above is marked
  SUPERSEDED only as a stabilization bookkeeping change; it grants no product
  acceptance.

Packet-status changes by this stabilization: none, except the DA-001-R ledger
bookkeeping row above. The program manifest remains canonical; RT-001/RT-002/
DA-001/DA-003/CF-002/CF-003/QR-005 stay PARTIAL, COMPLETE rows stay COMPLETE,
and BLOCKED/NOT_SELECTED/DEFERRED rows are unchanged.

New candidate: code commits close at `c4d9ce6`. The ledger-reconciliation commit
on top of it (this stabilization section plus the truth-ledger corpus update)
is the stabilized candidate; its SHA is the HEAD shown in `git log` at close
and quoted in the stabilization final report. Working tree after that commit
is clean.

## H-00 baseline (2026-09-07T15:51:40Z, go1.27.0 darwin/arm64, HEAD 26a1caf main)
- Porcelain: 13 modified tracked + untracked execution-ledger.md + session-recovery-patch-contract.md (preserved, no reset/stash).
- Diff fingerprint (pre-repair): cb9674d2f612ab57e5ccadc98b276729d3b3739fdb4e65676d5acf8a1a1deaf6.
- Graph: codebase-memory-mcp instant-v2 ready but excludes cmd/instantd + docs by design; used direct source reads for H-01 paths.
- PostgreSQL: Homebrew 17.11, wal_level=logical, superuser priyank, owned isolated DBs via testkit (DATABASE_URL postgres://priyank@localhost/postgres, CREATEDB). No default DB claimed disposable; fixtures create/drop instant_test_* only.
- Corpus: manifest 18 scenarios; integration via INSTANT_TEST_INTEGRATION=1 + DATABASE_URL (per corpus/README + Makefile test-contract, no --suite flag invented).

## H-01 authorization publication (design REJECT→repair→re-review; residual WS wire window pending, not accepted)
- Design: revocation linearization = observed Gen epoch at next refresh/attach boundary (not DB commit, not completed barrier). Wire bytes irrecoverable. Partial-generation allowance (WS at most one superseded envelope, uncertified, healed via retry at same txID; SSE zero via dequeue drop) REMAINS PENDING owner ratification.
- Repairs: ClearServedState atomic + clear-then-bump; RefreshGate/attach I/O outside groupsMu (cap recheck); version-bound fast path; WS/SSE/admin initial-answer fail-closed on nil snapshot + post-render Gen recheck (S1 fix); SSE sseEvent queue with Store.Get dequeue drop + SendRawGen/SendGen (fixed lost SendRaw across SSE init); admin SSE via snapshotOrRefresh flight; WS 10s write timeout (wsWriteTimeout); splice apply outside sub.mu with Gen rechecks + commit guard.
- Matrix: H-01a Emit check (TestEmitRejectsSupersededGeneration) GREEN; H-01b mid-fan-out (TestSwapMidFanOutStopsSpreadAndRefusesCommit 1-frame pinned as pending + TestMidFanOutSwapHealsThroughNotifierRetry healing) GREEN; H-01c Publish refusal (same test commit leg) GREEN; H-01d flight (TestFlightRejectsSupersededRefresh) + fast-path miss (TestH01dFastPathRacingClearIsNotReused) + post-publish fail-closed decision (TestH01dPostPublishSwapFailsClosed) GREEN; H-01e splice bail (TestH01eSpliceOverlappingRevokeBails) GREEN; H-01f SSE queue (TestH01fQueuedSSEEnvelopesDroppedAfterRevoke, 5 stale, Store wiring pinned) GREEN; H-01g blocked-writer independence + detach (TestH01gBlockedWriterDoesNotStallOtherGroups) + 10s bound GREEN (attach rule-load stall fixed); H-01h fail-closed (TestSyncFailsClosedOnRulesLoadError + TestRealtimeRuleOutageDropsAndRecovers + TestSupersededGenerationDrops, live) GREEN.
- Reviews: independent architecture/security REJECT with 7 must-fix (initial queue/mat/slow/rule-only/partial-smuggle); post-repair re-review caught S1 fallback (fixed to fail-closed); final re-review notes only residual WS single-envelope + rule-only indefinite as pending (no approval invented).

## H-02 recovery/watermarks/client (partial, WS live pinned; SSE live gap remains)
- Bounded delivery repair accepted (2026-09-08): members without a raw
  transport are explicitly detached/closed; admin SSE queue overflow ends the
  stream and removes its subscription. Focused PostgreSQL-backed race tests
  passed 10x for overflow/teardown and 2x for the dispatch, watermark,
  generation, retry, SSE-write, and reconnect matrix; the adjacent
  sync/reactive/instantd race suite and vet passed. Sol boundary review
  accepted the repair. This does not close the three client-evidence gaps below.
- TestMidFanOutSwapHealsThroughNotifierRetry (real notifier+retry, fake transport) GREEN -race x2.
- TestLiveReconnectConvergesAfterDrop (live PG loopback, two members, drop, rejoin to equal result+watermark, liveness both directions) GREEN -race.
- TestSupersededGenerationDrops (barrier-parked refresh, deny persist, superseded drop) GREEN live.
- Gaps: no live SSE reconnect leg; delta-member live reconnect not separately pinned (splice bail unit + steady-deny live cover oracle, not live delta reconnect); same-tx corrective-frame client application requires pinned SDK version (slice-collecting bytes is not client proof) — recorded, not claimed.

## H-03 storage (no code change needed; barrier coverage added)
- 5 required tests GREEN -race x2 + full storageapi package GREEN -race.
- New TestDiskBackendSecondUploadWaitsForRootConfirmation (barrier entry signal, 500ms non-ack bound, release completes both) GREEN -race x2 — pins cross-goroutine fsync ordering the 32-writer success test does not.
- Rollback trace (calls==2) holds; external/precreated-dir trust + single-process createMu documented in code (backend.go confirmAppDir EEXIST path); multi-process shared-root out of scope, not claimed as crash proof.

## H-04 verification (owned fixtures, no push/deploy)
- Focused storage (6 tests incl. new barrier) -race x2 GREEN; focused sync Swap/MidFanOut -race x2 GREEN; H01 matrix -race x2 GREEN; H01e -race x2 GREEN.
- Six packages -race GREEN: sync, reactive, storageapi, cmd/instantd, backup, config. go vet (same six) clean. git diff --check clean.
- Live: INSTANT_TEST_INTEGRATION=1 DATABASE_URL=postgres://priyank@localhost/postgres TestLiveReconnectConvergesAfterDrop + TestSupersededGenerationDrops GREEN; RT-001a/b/e + steady-deny GREEN live.
- Corpus: validate GREEN; TestCorpusReplayIntegration 05-permission-deny PASS; 11/17 pagination FAIL preserved pre-existing (instaql/CF territory, untouched by this patch; ledger records stash-proven pre-existing, not re-proven here to preserve dirty tree).

## H-05 docs (no new approval invented)
- Preserved prior dirty decision edits (02-realtime RT-001a pending-ratification bound; envelope SSE-correction pending + approval-scoping sentence) without broadening approval.
- This ledger + truth-ledger appendix record actual dirty fingerprint, pending WS single-envelope + rule-only-indefinite decisions, and live vs hermetic scope. No self-approval of missing security evidence; no inherited green from different tree.

## H-06 review/commit (independent, local only)
- Reviewer inputs: requirement table, exact diff, raw outcomes above, skipped (SSE live reconnect, delta live reconnect, SDK corrective-frame proof), draft handoff. Findings repaired: S1 fallback, SSE Store wiring, attach missing Unlock (deadlock), splice/mat ordering, WS timeout.
- Commit: stage reviewed realtime + storage + tests together (realtime shared interfaces + daemon wiring + tests land together); storage barrier test lands in same train (accepted with realtime; separately reviewable). No git add . indiscriminate; inspect diff --cached --check + staged diff. No push/deploy.

## RT-001 close-out (2026-09-17 IST, HEAD 6b1288601d3be9d0cb6b0af17bdee5b35fd06b07, clean tree, go1.27.1 darwin/arm64)

Owner ratification: `DEC-001-rt001-bounded-rebinding-20260917` (successor to
`DEC-001-single-node-alpha-20260905`, recorded in
`docs/reference/release-envelope.md`; original approval preserved unchanged).
Owner Priyank approved the exact seven-clause bounded-rebinding contract via
explicit structured approval at `2026-09-16T19:05:43Z`. No approval inferred
from implementation or recommendation. Unrelated envelope selections unchanged.

Live evidence (owned fixture `DATABASE_URL=postgres://priyank@localhost/postgres`,
PostgreSQL 17.11 Homebrew, `wal_level=logical`, `testkit`-isolated
`instant_test_*` databases only; one sandboxed attempt failed to connect and
was rerun with escalation approval):
- `INSTANT_TEST_INTEGRATION=1 go test -race ./internal/sync -run
  '^(TestRealtimeRebindDeniesExistingSubscription|TestRealtimeRegrantRestoresExistingSubscription|TestRealtimeRuleOutageDropsAndRecovers|TestSteadyDenySecondCommitDoesNotLeak|TestSupersededGenerationDrops|TestSyncFailsClosedOnRulesLoadError)$'
  -count=1 -v` → 6 PASS, 0 SKIP, exit 0.
- `INSTANT_TEST_INTEGRATION=1 go test -race ./internal/corpus -run
  '^TestCorpusReplayIntegration$' -count=1 -v` → 18/18 scenarios PASS
  including `05-permission-deny`, 0 skips, exit 0.

Hermetic evidence: focused RT-001 suite (21 sync + 3 reactive tests incl. all
packet-named regressions and SSE-lease legs) PASS twice under `-race`; `go
test -race ./internal/sync ./internal/reactive ./cmd/instantd -count=1`
PASS; `go vet` on those three packages exit 0; `go build ./...` exit 0;
`gofmt -l` empty; `git diff --check` clean.

Security review: independent read-only review of the exact candidate returned
ACCEPT with no unresolved blocker (11 falsification targets — stale gate
reuse, >1 superseded WS envelope, stale snapshot/watermark commit, stale SSE
delivery, fail-open on lookup error, group sharing across generations,
late-allow-over-deny, teardown resurrection, ABA/cancellation, lock
inversion, I-O under global lock — all falsified with file:line citations;
full evidence:
`docs/plans/finish-up/rt001-security-review-20260917.md`;
one non-blocking note: rule-doc cache has no TTL, only `Invalidate`
refresh; consistent with the ratified observed-swap boundary). Two earlier
reviewer spawns failed at the provider level with no verdict and were
superseded by this completed review; zero repair cycles consumed.

Status: RT-001 `COMPLETE / ACCEPTED_BOUNDED_REBINDING`. RT-002 remains
PARTIAL; Phase 02 gate `REALTIME_TRUSTWORTHY` remains open. No other packet
changed. No push, publication, deployment, or tag.

## RT-002 close-out (2026-09-17, HEAD bbf5561f8d8e784b8ea4c1a2395c5581b760bac2 clean + work below, go1.27.1 darwin/arm64)

Baseline: HEAD `bbf5561f8d8e784b8ea4c1a2395c5581b760bac2` on `main`, clean tree.
RT-002 `PARTIAL / ACCEPTED_BOUNDED_REPAIR_CLIENT_EVIDENCE_PENDING` per
`program-manifest.md`. Toolchain go1.27.1, PostgreSQL 17.11 Homebrew,
Node v25.1.0, pnpm 10.28.0. DEC-001 wording recorded: RT-002 `REQUIRED`
`Refresh outcome semantics (explicit disconnect + full replay)`
(`docs/reference/release-envelope.md:140`); boundary `explicit disconnect
plus full replay may be sufficient` (`02-realtime-correctness.md:69`). No new
owner decision (policy unchanged; RT-002c allows retry OR disconnect).

Product changes (2 files):
- `internal/sync/frame.go`: `Encode` validates every value with `json.Valid`
  (matches stdlib Marshal rejection of bad RawMessage); invalid payload fails
  before any wire bytes.
- `internal/sync/groups.go`: `dispatchGroup` collects direct-write failures;
  nothing-served + current generation → withhold (error, members kept) for the
  notifier's bounded same-tx retry; partial → detach failed + certify via
  siblings; post-regate total → superseded; queued (`SendRawGen`/SSE) path
  unchanged (immediate detach, preserves overflow teardown). No new test seam.

Regression tests (6 new files, all green with `-race`):
- B encode: `dispatch_encode_failure_test.go`
  (`TestDispatchEncodeFailureServesNothing` both classes + both transports,
  zero bytes, no empty, snapshot nil, Tx 0;
  `TestDispatchEncodeFailureSchedulesSameTxRetry` real notifier, ≥2 attempts
  same tx 7, sent 0, no commit) + `sse_encode_failure_test.go`
  (`TestWriteSSEEventEncodeFailureWritesNothing` + `UnguardedControl`, zero
  bytes before first Write/Flush).
- C chain: `notifier_chain_failure_test.go`
  (`TestNotifierChainAllDeliveryFailsWithholdsAndHeals`: no commit while
  failing, same-tx retry, exactly 1 delivery + 1 commit after heal;
  `TestNotifierChainPartialDeliveryCertifiesAndReconnects`: failed detached +
  closed, healthy certifies, fresh rejoin exact state + watermark, tx2 reaches
  both).
- D SSE: `sse_failure_reconnect_test.go`
  (`TestSSEFailureReconnectFullReplay`: 11 steps — real SSE session/query,
  baseline, overflow via production dispatch, ConnCount 0 + Store 0, missed tx
  via sibling WS transact chain, fresh session/query, exact tree + watermark,
  no empty/malformed, continued delivery).
- E delta: `delta_reconnect_test.go`
  (`TestDeltaReconnectFullReplayCoversMissedTx`: two live 0.23.0 members,
  delta-eligible single-op patch certified on both, drop, missed delta-eligible
  tx to survivor as delta, fresh 0.23.0 full replay (no delta key) exact titles
  + watermark == missed tx, later tx reaches both).
- F SDK: `examples/vite-vanilla/rt002-sdk-same-tx-correction.test.mjs`
  (pinned `@instantdb/core@1.0.65`, lock sha256
  `3aed90cae84d70bf6c4e3e0d822ea437694ded046244c23cb5cffa5611f1cbb2`,
  real `Reactor._handleReceive` → `querySubs` → `dataForQuery` → `notifyOne`;
  stale tx 7 then corrective same-tx 7 applied, `processedTxId` 7, state
  corrected, tx 8 live; `node --test` pass 1 fail 0 exit 0; node_modules
  ignored, unstaged).

Verification (all exit 0, zero skips unless noted):
- Focused 17 twice with `-race` (9 prior + 8 new incl. encode/chain/SSE/delta):
  `TestWatermarkMatchesDeliveredGeneration`, `TestSSESnapshotErrorEndsStream`,
  `TestReconnectConvergesAfterSendFailure`, `TestDispatchRenderFailureServesNothing`,
  `TestDispatchSendFailureDetachesOnlyFailedMember`,
  `TestDispatchMissingTransportDetachesExplicitly`,
  `TestDispatchNeverEmitsEmptyFrames`, `TestSSEOverflowClosesStream`,
  `TestAdminSSEOverflowClosesStream`, `TestDispatchEncodeFailureServesNothing`,
  `TestDispatchEncodeFailureSchedulesSameTxRetry`,
  `TestWriteSSEEventEncodeFailureWritesNothing`,
  `TestWriteSSEEventEncodeFailureUnguardedControl`,
  `TestNotifierChainAllDeliveryFailsWithholdsAndHeals`,
  `TestNotifierChainPartialDeliveryCertifiesAndReconnects`,
  `TestSSEFailureReconnectFullReplay`,
  `TestDeltaReconnectFullReplayCoversMissedTx` → PASS twice.
- `go test -race ./internal/sync ./internal/reactive ./cmd/instantd -count=1` PASS.
- RT-001 live 6/6 PASS (`TestRealtimeRebind*`, `TestRealtimeRuleOutage*`,
  `TestSteadyDeny*`, `TestSupersededGenerationDrops`, `TestSyncFailsClosed*`).
- Mounted CF-003 SSE/delta (`TestCF003AssembledSSE*`, `Delta`, `Room`,
  `Transaction`) PASS; `TestDeltaRefreshNegotiation`,
  `TestSSEInitQueryTreeShape`, `TestSubscriptionCap`,
  `TestLiveReconnectConvergesAfterDrop` PASS; corpus replay 18/18 PASS.
- Pinned SDK `node --test rt002-sdk-same-tx-correction.test.mjs` pass 1 fail 0 exit 0.
- `go vet` (sync/reactive/instantd) exit 0; `go build ./...` exit 0;
  `gofmt -l` empty; `git diff --check` clean.
- Integration legs run with `INSTANT_TEST_INTEGRATION=1
  DATABASE_URL=postgres://priyank@localhost/postgres` (owned `instant_test_*` only).

Reliability review: independent read-only review of the exact candidate
attempted all 12 falsification targets (watermark without delivery,
snapshot/watermark skew, skipped tx, failed still registered, closed still
live, SSE leak, delta/full divergence, duplicate amplification, empty frame,
unbounded retry, deadlock, mock-for-SDK) — all FALSIFIED with file:line
citations; verdict ACCEPT with 6 non-blocking notes. Artifact:
`docs/plans/finish-up/rt002-reliability-review-20260917.md`.

Status: RT-002 `COMPLETE / ACCEPTED_DISCONNECT_REPLAY_DELIVERY`. Phase 02
`REALTIME_TRUSTWORTHY / COMPLETE` (RT-001 + RT-002 + RT-003 green). RT-001 and
RT-003 remain complete. No later packet changed. No push, publication,
deployment, or tag.
