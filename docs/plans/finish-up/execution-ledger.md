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

## DA-001 close-out (2026-09-18, HEAD 3119c31da0719998de923acd9586956d32d8943f + work below, go1.27.1 darwin/arm64, APFS, PostgreSQL 17.11 wal_level=logical)

Baseline: HEAD `3119c31da0719998de923acd9586956d32d8943f` on `main`, clean tree.
DA-001 `PARTIAL / LOCAL_REOPEN_ACCEPTED_ENV_PENDING` per program-manifest.
Envelope selects durable configured local root REQUIRED, temp forbidden,
object backup excluded with explicit 503 (`docs/reference/release-envelope.md:69,142`).

Product changes (within DA-001 lease only):

- `internal/storageapi/backend.go`: symlink defense via Lstat-first
  `rejectSymlinkDir` in `confirmAppDir` (fast + EEXIST paths), `Open`
  (dir + file), and `Delete` (per-key under createMu after security
  re-review). No temp fallback, no behavior weakening.
- `internal/config/config_test.go`: `setEnv` isolates ambient DATABASE_URL/
  OAuth so hermetic config tests pass under integration env (test-only).
- `internal/storageapi/symlink_escape_test.go`: app-dir symlink refused on
  Put/Open/Delete with victim survival; file symlink not dereferenced;
  deterministic file-root and uncreatable-parent refusals (uid-independent).
- `cmd/instantd/daemon_restart_test.go`: real A/B/C daemon processes, same
  binary, same PG fixture, same root+secret, new ports, bounded health/stop.
- `cmd/instantd/daemon_startup_failure_test.go`: missing/relative/
  uncreatable/file/missing-secret (+chmod unwritable when enforced) all exit
  nonzero, diagnosable, no listener, no probe residue, no secret in output.
- `cmd/instantd/daemon_object_disabled_test.go`: all three object routes
  stable 503, no files delta, no S3 construction, storage + local backup
  still work.
- `docs/guides/07-selfhost.md`: persistence across restart, ephemeral
  warning, stable 503 disablement, OP-003/OP-004 separation; no container or
  recovery claim.

Evidence (all exit 0, zero skips on selected integration tests):

- Focused twice under -race: config storage-root/fingerprint (4 hermetic),
  storageapi durability/atomicity/symlink (18), daemon restart/disabled/
  startup + mount/assembly (all PASS).
- `INSTANT_TEST_INTEGRATION=1 DATABASE_URL=postgres://priyank@localhost/postgres
  go test -race ./internal/storageapi ./internal/backup ./internal/config
  ./cmd/instantd -count=1` PASS (all four packages ok).
- `go vet` on affected packages exit 0; `go build ./...` exit 0;
  `gofmt -l` empty; `git diff --check` clean.

Reviews (explicit ACCEPT with file:line citations, artifacts persisted):

- Data-integrity: ACCEPT, 9 targets falsified.
  `docs/plans/finish-up/da001-data-integrity-review-20260918.md`.
- Security: initial REJECT on Delete-via-symlinked-dir, repaired, focused
  re-review ACCEPT. Original finding preserved.
  `docs/plans/finish-up/da001-security-review-20260918.md`.

Status: DA-001 `COMPLETE / ACCEPTED_DURABLE_LOCAL_STORAGE`. Phase 03 remains
open (DA-003 and external/deferred rows separately classified). No later
packet changed. No external provider contact, no push, publication,
deployment, or tag.

## DA-003 close-out (2026-09-19, HEAD 8cabdb4a7a6f06cb16d552dd41d2f798f9030a75 clean + work below, go1.27.1 darwin/arm64, APFS, PostgreSQL 17.11)

Baseline: HEAD `8cabdb4a7a6f06cb16d552dd41d2f798f9030a75` on `main`, clean tree.
DA-003 `PARTIAL / LOCAL_DB_ACCEPTED_EXTERNAL_PENDING` per program-manifest.
Alpha profile: local NDJSON export/restore accepted; runtime object-backup
routes deliberately unwired with stable 503. No S3 credentials, external
provider contact, object-store wiring, or Phase-06 recovery campaign.

Evidence mapping (all exit 0, zero skips on selected tests):

- DA-003a (missing auth config, no panic):
  `TestHandlerRejectsMissingAuthConfig` (`http_test.go:110-123`, GET 500, no
  panic) + `TestHandlerMissingAuthConfigObjectRoutes`
  (`da003_failclosed_test.go:121-155`, PUT/GET object + POST restore-object
  each 500, no panic, zero puts/gets). Central nil-check
  (`http.go:64-69`) precedes routing/DB/store work.
- DA-003b (unauthorized emits nothing, mutates nothing):
  `TestUnauthorizedRestoreWritesNothing` (`http_test.go:128-153`, 401,
  triples 10/attrs 4 unchanged) +
  `TestUnauthorizedObjectRoutesMutateNothing`
  (`da003_failclosed_test.go:457-502`, 3 methods x 3 auth cases each 401,
  GET emits no `kind` bytes, zero puts/gets) + export 401 legs in
  `TestHandlerRoutesAndAuth` (`http_test.go:21-46`). Auth precedes the
  action switch (`http.go:84-95`).
- DA-003c (terminal checksum + record-count contract):
  `TestPutObjectReturnsCounts` (`da003_failclosed_test.go:161-227`, stored
  artifact ends with checksum trailer, records==counts sum, sha non-empty,
  one staging Put/promotion/cleanup) + `TestChecksumCorruption`
  (`import_test.go:63-104`, sha mismatch rejected, target unchanged) +
  `TestTruncatedDumpRejected` (`:109-135`, missing trailer rejected, fresh
  DB 0 apps/0 triples) + `TestImportReadErrorAfterChecksumRollsBack`
  (`import_boundary_test.go:20-38`, read failure after checksum rolls back,
  no app row) + `TestImportRejectsExtraAfterChecksum`
  (`da003_extra_contract_test.go:23-50`, valid extra line rejected, 0/0) +
  `TestImportRejectsRecordsMismatch` (`:52-102`, valid sha + lied records
  rejected, 0/0). Export trailer (`export.go:98-104`), sha+records+extra
  checks (`import.go:92-103`), missing-trailer refusal (`:146`).
- DA-003d (incomplete/staged artifacts unselectable; publish only after
  complete export/upload): `TestStagingObjectKeysArePrivate`
  (`da003_failclosed_test.go:251-281`, staging prefix 400 on GET/PUT/restore,
  zero store calls) + `TestPutObjectPrefixFailurePreservesFinalAndCleansStage`
  (`:323-358`, 502, final preserved, one staging cleanup, no promotion) +
  `TestPutObjectShortSuccessDoesNotPromote` (`:360-388`, 502, final
  preserved, no promotion) +
  `TestPutObjectPromotionFailureReportsUnknownStatus` (`:421-452`, 502 +
  "status unknown", final preserved, staging cleaned) + traversal/escape
  battery `TestObjectKeysRejectTraversalAndEscape`
  (`da003_extra_contract_test.go:104-176`, `..`/leading-`/` 400 on all three
  routes with zero store calls; foreign-app key stays under caller prefix).
  Staging/private/publish/cleanup order (`http_objects.go:76-139`),
  scoping (`:25-36`), staging key shape (`:141-147`).
- DA-003e (failed restore preserves source + exact prior target under
  single-tx atomicity): `TestRestoreObjectCorruptPreservesSourceAndTarget`
  (`da003_failclosed_test.go:508-551`, 400, source bytes identical, target
  10/4) + `TestRestoreObjectTruncatedPreservesSourceAndTarget` (`:557-597`,
  400, source identical, target 10/4) +
  `TestFailedImportPreservesExactDump`
  (`da003_extra_contract_test.go:178-202`, corrupt re-import fails, next
  export byte-identical). Single tx (`import.go:35-39`), commit only after
  checksum+extra validation (`:82-110`), restore-object Get+Import only
  (`http_objects.go:149-176`).

Focused evidence: the 15-test DA-003 set passes twice under `-race` with
the owned database (zero skips). Full `internal/backup` package passes
under `-race` (35 PASS, 0 FAIL/SKIP). Assembled daemon contract passes:
`TestDA001ObjectBackupDisabled` (real daemon 503s + local export + storage),
`TestCF003HTTPMatrixOwnedPostgres` (export trailer + restore round-trip +
object 503), `TestCF003HTTPMatrixHermetic` (backup 401, no files).

Reviews (explicit ACCEPT with file:line citations, artifacts persisted):

- Data-integrity: ACCEPT, 7 targets falsified (partial publication,
  truncation/checksum, early commit, target mutation, source deletion,
  ambiguous promotion, cancellation cleanup).
  `docs/plans/finish-up/da003-data-integrity-review-20260919.md`.
- Security: ACCEPT, 7 targets falsified (nil-auth, unauthorized
  disclosure/mutation, cross-app, staging access, traversal/escape,
  credential disclosure, auth ordering).
  `docs/plans/finish-up/da003-security-review-20260919.md`.

Added evidence (test-only, no production change):
`internal/backup/da003_extra_contract_test.go` pins extra-after-checksum,
records-mismatch, traversal/escape namespace, and byte-exact rollback; all
four green on first run. Production behavior already fail-closed; the gap
was missing pins, not a defect. No red-before-production-fix cycle needed;
re-review confirms each new test fails if its guard is removed.

Status: DA-003 `COMPLETE / ACCEPTED_LOCAL_FAIL_CLOSED_BACKUP`. Phase 03
remains open (DA-006B/DA-008B BLOCKED on external authority; OP/CF/FR rows
unchanged). No later packet changed. Local NDJSON accepted; runtime object
backup excluded (stable 503); in-memory/fake-S3 tests prove internal failure
semantics only; no real S3/provider, recovery campaign, Linux/container
qualification, production or release acceptance claimed. OP-006 remains
separate. No push, publication, deployment, or tag.

## CF-002 close-out (2026-09-19, HEAD c7f9a42d29a7587a99225236b54bb217f940833b clean + work below, go1.27.1 darwin/arm64, PostgreSQL 17.11)

Baseline: HEAD `c7f9a42d29a7587a99225236b54bb217f940833b` on `main`, clean tree.
CF-002 `PARTIAL / ACCEPTED_BOUNDED_CAPTURE` per program-manifest. CF-003 must
not advance here and does not: no matrix, manifest, or coverage file changes.
No external service is contacted; no push, deployment, publication, or tag.

Six-row evidence mapping (all exit 0):

1. Candidate revision, binary, process, configuration, endpoint identity —
   PROVEN locally. `internal/corpus/candidate.go` derives Git SHA/dirty
   (`GitIdentity`, `:54`), binary SHA-256 (`HashFile`, `:71`), loopback
   binding (`RequireLoopbackEndpoint`, `:110`), process liveness
   (`VerifyProcessAlive`, `:142`), binary continuity (`VerifyBinaryDigest`,
   `:163`), revision continuity (`VerifyGitIdentity`, `:176`), and
   secrets-excluded config digest (`ConfigDigest`, `:90`); `Validate` +
   `VerifyLive` (`:189,:237`) gate every capture. The managed lifecycle
   test records `c7f9a42… dirty=true go1.27.1 darwin/arm64`, binary
   `dbfb93be…`, PID/endpoint/fixture per run, and fails closed on drift
   (`cmd/corpusctl/managed_lifecycle_test.go:306-393,428,472,564-568`).
   Hermetic tamper pins: `candidate_test.go` (wrong SHA/binary/endpoint/
   fixture/app all rejected).
2. Equivalent isolated bootstrap/reset before every mutating scenario —
   PROVEN locally. `cf002ResetFixture` deletes the app row (cascading to
   attrs/triples/idents) and recreates app+token (`:251-270`), runs before
   every mutating scenario (`:417-419`), with exact precondition
   (`apps=1 … attrs=0 triples=0 idents=0 rows=[]`, `:424-427`) and exact
   final state (`attrs=2 triples=2 idents=2` plus literal rows, `:487-490`)
   per scenario and cross-scenario equality of both (`:525-530`).
   Observed pre `apps=1 title="cf002-managed" attrs=0 triples=0 idents=0
   rows=[]`, post `… attrs=2 triples=2 idents=2 rows=[todos/id="<entity>"
   todos/title="cf002-managed-todo"]`, identical across the reset in both
   runs. Each run uses a separate owned fixture (`instant_test_4099…`,
   `instant_test_2232…`).
3. Raw and canonical capture per selected transport — PROVEN.
   HTTP+SSE: `TestRecordHTTPSuccessAndRetention`,
   `TestRecordSSESuccessAndRetention` (raw retained, canonical derived,
   headers redacted); managed captures assert raw framing plus canonical
   session masking and double-canonical determinism
   (`managed_lifecycle_test.go:438-470`). WS recording explicitly
   unsupported + fail-closed (`cmd/corpusctl/main.go:122-125`) with no
   output/artifact proven (`TestCF002WSRecordCreatesNoArtifact`). SDK not
   selected (no frozen-SDK claim in DEC-001); external/v1 owned by
   CF-004/CF-005 (BLOCKED, unchanged).
4. Path-scoped masking, payload significant — PROVEN.
   `TestPayloadApplicationFieldsRemainSignificant` (id/token/timestamp/
   cursor/email/title at payload paths significant in both modes;
   frame-root protocol masking retained) plus existing differential-scope
   and timestamp tests. Live over-broad-`title` probe went red in both
   modes; reverted source green.
5. Bounded completion/quiescence, late/error observable — PROVEN.
   `TestCaptureSSEIncompleteStreamFails`,
   `TestCaptureSSEQuiescenceDetectsExtraRecord`,
   `TestRecordFailurePropagation` (no evidence on failure),
   `TestReplaySSEDoesNotAcceptEOFAfterParentCancellation`,
   quiescence-boundary tests; managed SSE capture enforces recordLimit=1
   with hello-shape assertions.
6. Fresh private redacted atomic write-once candidate-bound output —
   PROVEN. Fresh 0700 reservation, 0600 files, symlink/TOCTOU rejection,
   no-replace atomic publication, no-deletion failure semantics
   (`fs_unix.go`, `http_unix_test.go`, `TestRecordOutputFreshnessAndWriteOnce`);
   managed run asserts 0700/0600, publishes 4 evidence files + checksummed
   manifest, verifies (`VerifyCaptureManifest`), and rejects republish
   (`managed_lifecycle_test.go:405-412,516-562`).

Focused verification:

- `go test -race ./internal/corpus ./cmd/corpusctl -count=1` twice: both
  `ok` (exit 0). With integration env: 87 PASS, 0 SKIP/FAIL.
- Managed lifecycle twice under `-race` with integration env, separate
  owned fixtures: run 1 pid 76643 `:64823` `instant_test_4099…` PASS;
  run 2 pid 76956 `:64909` `instant_test_2232…` PASS; no skips; HTTP+SSE
  captures, reset equivalence, manifest/checksum verification each run.
- WS exclusion: direct `record --transport ws` fails with the stable
  documented error and creates no output (PASS).
- Mutation probes red/green: reset-omitted (`CF002_SKIP_RESET=1`) FAILs on
  s2 precondition showing s1 rows, reverted (env unset); over-broad title
  mask FAILs both modes, reverted; SHA/binary/endpoint/manifest-tamper and
  output-reuse rejections green (fail on tamper, pass clean). Zero residue.

Reviews (explicit ACCEPT with file:line citations, artifacts persisted):

- Provenance: ACCEPT, 7 targets falsified.
  `docs/plans/finish-up/cf002-provenance-review-20260919.md`.
- Security: ACCEPT, 7 targets falsified.
  `docs/plans/finish-up/cf002-security-review-20260919.md`.

Added implementation/tests (inside CF-002 write lease only):

- `internal/corpus/candidate.go`: proven-identity + manifest helpers.
- `internal/corpus/candidate_test.go`: hermetic guards incl. payload
  significance and manifest-tamper rejection.
- `cmd/corpusctl/managed_lifecycle_test.go`: managed daemon lifecycle +
  WS no-artifact proof.
- `corpus/README.md`: managed-lifecycle paragraph (user-facing claim
  change only); plain `--mode record` limitation statements preserved.
- No product-code change (no WS recorder added: recording-level WS support
  is not selected; WS replay retains raw/canonical via existing evidence
  paths, and the exclusion is enforced in code, docs, and tests). No
  production endpoint added for SHA exposure; identity comes from the
  built binary, process handle, config, and lifecycle.

Status: CF-002 `COMPLETE / ACCEPTED_CANDIDATE_BOUND_LOCAL_CAPTURE`. CF-003
remains PARTIAL and unchanged; CF-004/CF-005 remain BLOCKED on external
evidence; all other packets unchanged. Accepted transports for recording:
HTTP + SSE. WS recording: explicitly excluded + enforced. SDK/v1 capture:
unclaimed. No remote identity or external fixture equivalence claimed. No
push, deployment, publication, or tag.

## CF-002 runnable-recorder follow-up (2026-09-19, intermediate 92e3a64 clean + work below, go1.27.1 darwin/arm64, PostgreSQL 17.11)

The prior close-out proved a test-only lifecycle. This follow-up extracts it
into the supported entry point `corpusctl --mode managed-record`
(`cmd/corpusctl/managed.go`, flags `--repo/--database-url/--instantd-binary/--output-dir/--timeout`,
visible in `--help`) without changing instantd product behavior. The
previous `TestCF002ManagedLocalLifecycle` became `TestCF002ManagedRecordEndToEnd`,
an end-to-end of the same production code path; orchestration was deleted
from the test file. HTTP+SSE stay selected; WS recording stays excluded.

Direct CLI evidence from clean `92e3a64`
(`go run ./cmd/corpusctl --mode managed-record --output-dir <fresh dir>`,
run 1 → `/private/tmp/cf002run1`, run 2 → `/private/tmp/cf002run2`):

- Run 1: pid 98966 `:51496` fixture `instant_test_4d64ba…`, PASS.
- Run 2: pid 99253 `:51584` fixture `instant_test_acb587…`, PASS.
- Both manifests: `gitSha 92e3a64`, `dirty false`, 64-hex binary SHA-256
  and config digest, PID, loopback endpoint, go1.27.1 darwin/arm64,
  `instant_test_*` fixture + app; identical pre (`attrs=0 triples=0`) and
  literal post (`attrs=2 triples=2` with exact rows) across the reset;
  4 artifacts + manifest, every SHA-256/size recomputed matching,
  0700/0600 modes, no secret or database URL in manifests; evidence
  readable after exit; daemons stopped (connection refused); owned
  databases dropped; reuse exits 1 (`already exists`) with the first
  manifest byte-identical (`d4f54400…`).

Failure probes (red preserved, zero residue): dirty worktree rejected
before any work with no output; `/bin/echo` as binary → `never became
healthy`, no output; missing DATABASE_URL → immediate usage failure;
omitted reset through the production path → precondition red, no manifest;
symlinked output → rejected with empty target; tampered copy (zeroed SHA,
lied checksum) detected on both fields; over-broad masking and
incomplete-SSE/quiescence legs green via the unit battery. Scratch
validation repos and tamper copies removed.

Re-reviews (corrective addenda appended 2026-09-19, each acknowledging the
earlier ACCEPT was test-only):

- Provenance re-review: ACCEPT —
  `docs/plans/finish-up/cf002-provenance-review-20260919.md`.
- Security re-review: ACCEPT —
  `docs/plans/finish-up/cf002-security-review-20260919.md`.

Docs: `cmd/corpusctl/README.md` (managed-record contract + example),
`corpus/README.md` (runnable recorder replaces the test-only claim; plain
record stays caller-asserted). CF-003 not started; CF-004/CF-005 unchanged.
No push, deployment, publication, or tag.

## CF-002 binding/cleanup corrective reconciliation (2026-09-19, implementation 5b7d30b8 + documentation follow-up)

The runnable-recorder follow-up above is preserved as history, including the
now-removed `--instantd-binary` flag and its unsafe claim that failed runs remove
the freshly reserved output directory. Two defects were repaired in
implementation commit `5b7d30b8be1a2e3bf018b0057276ecbb6510cfcf`:

- Candidate binding: the binary override flag and field are gone. Managed mode
  always builds `./cmd/instantd` from the clean `--repo`. A healthy-but-different
  compatible shim is proven healthy, yet the removed flag is rejected before
  build/capture/output reservation; the default path is proven to build and
  proceed to the database prerequisite.
- Safe cleanup: post-reservation `os.RemoveAll(absOut)` is gone. Policy A keeps
  all pre-reservation capture in memory, so early failure creates no output.
  Policy B closes only the pinned reservation after reservation and leaves any
  private incomplete residue untrusted/ineligible. The deterministic pathname-
  replacement regression proves a replacement victim and sentinel survive
  byte-identical while no eligible manifest appears.

The CLI help was re-inspected: managed mode exposes `--repo`, `--database-url`,
`--output-dir`, and `--timeout`, and does not expose `--instantd-binary`.
Corrective provenance and security addenda each return fresh **ACCEPT** verdicts.
Status remains CF-002 `COMPLETE / ACCEPTED_CANDIDATE_BOUND_LOCAL_CAPTURE`.
CF-003 remains `PARTIAL / HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING`;
CF-004 and CF-005 remain `BLOCKED / EXTERNAL_EVIDENCE`. No CF-003/004/005
implementation, manifest, coverage, or status changed. No push, deployment,
publication, or tag.

## CF-003 assembled-legs reconciliation (2026-09-19, HEAD 58dab88e5554f2abfd5d02f77ed4550ffcecaac7 clean, docs only)

Audit finding recorded: all 13 corpus manifest coverage gaps have accepted
assembled-route legs with exact oracles. The accepted legs on `main`, in
commit order, are:

- Query conjunction and ordering (`31d5539`) with exact whole-result oracles
  (`b293a5c`).
- Auth refresh-batch and alias-signout (`a686613`).
- Admin-presence exclusion (`951175c`).
- Runtime-transact (`dcc8373`) with exact whole-result oracle (`255e28b`).
- Transaction WS/SSE-transport (`43ed443`) with WS cardinality-boundary
  (`ab0d75b`) and SSE cardinality-boundary (`e2d97ae`) legs.
- SSE query-concurrency (`778d348`) with exact watermark compare (`19ec8e9`).
- Delta cross-transport (`6bd719e`).
- SSE lookup-lifecycle (`63b82d0`) with strict oracles (`2340195`,
  `f904b42`).
- Magic-code denied leg for the auth-http transport matrix (`356d68d`).
- SSE fanout leg for the rooms transport matrix (`58dab88`, current HEAD).

Gap-classification outcome: no class-(a) remainder — no remaining gap row is
flippable by in-scope work. Rows stay gap for the stated structural reasons
only: WS-recording flips are excluded by enforcement (CF-002 `record
--transport ws` fails closed with no artifact); multi-client stream capture
flips have no capture contract; single-flow HTTP/SSE flips are report-only
scope changes; captured-v1 flips await CF-004/005 external evidence.

Docs-only reconciliation: no code, tests, `corpus/manifest.json`, or CF-002
materials touched. `program-manifest.md` is untouched — no row status
flipped. CF-003 stays `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING`;
CF-004/CF-005 stay `BLOCKED / EXTERNAL_EVIDENCE`. No push, deployment,
publication, or tag.

## CF-003 honest closure — per-gap assembled-route mapping (2026-09-19, HEAD 2c15ac97c2771ea9e1d727a8154db679e84e96eb clean, docs only)

Scope: docs + manifest-notes mapping only. No code, no tests, no
`corpus/manifest.json` edit, no NDJSON fabrication. No gap→covered flip.
`program-manifest.md` has no per-gap notes/oracle column, so the mapping
lives here and in the truth-ledger appendix; status codes are untouched.
CF-003 stays `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING`;
CF-004/CF-005 stay `BLOCKED / EXTERNAL_EVIDENCE`. All legs below were
ACCEPTed on `main` and are present at this HEAD; the cited short SHA is the
acceptance commit for that leg's exact oracle.

Oracle standard cited as "exact": whole-result `reflect.DeepEqual`
(lengths before elements where stated), exact float `processed-tx-id`
compare, exact status/body strings, exact session/presence snapshots. A
subset/ID-only check would fail the cited test.

1. `auth-http-refresh-lifecycle` (http, refresh rotation/replay) —
   `cmd/instantd/runtime_cf003_auth_batch_test.go::TestCF003AssembledAuthRefreshBatchAndSignout`
   @ `a686613`. Proves plural `POST /runtime/auth/refresh_tokens` batch +
   `POST /runtime/signout` alias with exact whole-user DeepEqual
   (`id/type/refresh_token`), exact batch application, unknown-token 401
   exact. Missing flip: checked-in raw HTTP capture + corpus replay
   (`00-smoke.ndjson`-class evidence). Why gap remains: single-flow HTTP
   flip would be a report-only scope change without checked-in raw capture;
   no NDJSON fabricated. Any captured-v1 flip awaits CF-004/005 (BLOCKED).

2. `auth-http-denied-error` (http, client-safe denial) —
   `cmd/instantd/runtime_cf003_auth_batch_test.go::TestCF003AssembledAuthMagicCodeDenied`
   @ `356d68d`. Proves `POST /runtime/auth/verify_magic_code` 400 exact
   `email, code and app-id are required`, 401 exact `authn: invalid magic
   code` twice (replay identical), post-denial guest sign-in exact whole
   user + singular verify exact, no auth-store residue. Missing flip:
   checked-in HTTP denied-error capture. Why gap remains: report-only scope
   change; awaits CF-004/005 for any v1-parity claim.

3. `query-conjunction-gap` (http, conjunction + ordering over HTTP query
   path) —
   `cmd/instantd/runtime_cf003_query_test.go::TestCF003AssembledQueryConjunctionAndOrder`
   @ `31d5539` + `b293a5c`. Proves multi-predicate conjunctions and
   asc/desc/paged ordering over mounted `POST /runtime/framework/query`
   with exact whole-result DeepEqual (not ID-only; `b293a5c` replaced
   slug-only checks). Missing flip: checked-in HTTP query exchange using
   the adapter. Why gap remains: report-only scope change.

4. `query-concurrency-gap` (sse, concurrent subscribers observe one
   ordered snapshot) —
   `cmd/instantd/runtime_cf003_sse_concurrency_test.go::TestCF003AssembledSSEQueryConcurrencyOrderedSnapshot`
   @ `778d348` + `19ec8e9`. Proves two SSE subscribers through a concurrent
   barrier observe one identical ordered whole-result snapshot (lengths
   before elements); `19ec8e9` pins exact float watermark compare
   (`gotTx != float64(txID)` fails). Missing flip: owned multi-client SSE
   capture + raw stream evidence. Why gap remains: no multi-client capture
   contract (CF-002 `managed-record` is single-flow).

5. `refresh-delta-boundary` (ws, delta/full converge) —
   `cmd/instantd/runtime_cf003_delta_transport_test.go::TestCF003AssembledDeltaCrossTransportConvergence`
   @ `6bd719e`. Proves WS 0.23.0 structural `refresh-ok-delta` patch + SSE
   full `refresh-ok` at the exact same tx; equal baseline watermarks/result
   metadata; trigger advances baseline; parsed wire delta applied to WS
   baseline equals parsed SSE full result and exact 4-entity map; exact
   float compares. Missing flip: corpus NDJSON delta leg via replay. Why
   gap remains: WS excluded — CF-002 `record --transport ws` fails closed
   with no artifact (`TestCF002WSRecordCreatesNoArtifact`); package/route
   tests are not corpus acceptance.

6. `refresh-sse-lifecycle` (sse, handshake/updates/reconnect ordered) —
   `cmd/instantd/runtime_cf003_sse_test.go::TestCF003AssembledSSERefreshAndReconnect`
   @ `ec78ad7`. Proves mounted GET/POST SSE exact handshake, init attrs,
   POST responses, add-query ack, initial full tree, tx-driven refresh with
   exact attr IDs + positive watermark, old-session teardown, distinct
   reconnect creds, converged reconnect state (3x race + Sol ACCEPT per
   ledger). Missing flip: checked-in SSE scenario + corpus replay. Why gap
   remains: single-flow SSE flip would be a report-only scope change; no
   NDJSON fabricated.

7. `refresh-convergence-concurrency` (sse, all selected subscribers
   converge after one change) — same leg as (4):
   `cmd/instantd/runtime_cf003_sse_concurrency_test.go::TestCF003AssembledSSEQueryConcurrencyOrderedSnapshot`
   @ `778d348` + `19ec8e9`. Proves one committed admin change converges
   both subscribers to identical refresh frames at the exact tx with exact
   final titles. Missing flip: owned multi-client fixture + raw stream
   evidence. Why gap remains: no multi-client capture contract.

8. `rooms-fanout-positive` (sse, same-node peers ordered presence +
   broadcast) —
   `cmd/instantd/runtime_cf003_room_sse_test.go::TestCF003AssembledRoomFanoutSSE`
   @ `58dab88`. Proves two SSE subscribers sharing one room via
   `Manager.Handle`: join fans ordered presence to both, resync converges
   joiner, set-presence fans update to both, client-broadcast reaches only
   the peer with exact sender session, leave converges survivor to exact
   one-member snapshot; lengths-before-elements DeepEqual.
   Cross-reference: WS variant
   `cmd/instantd/runtime_cf003_room_test.go::TestCF003AssembledRoomLifecycle`
   @ `ec78ad7` (join/presence-update/broadcast/leave, 10x race + Sol
   ACCEPT) proves the WS path but cannot flip corpus. Missing flip:
   multi-client stream capture. Why gap remains: no multi-client capture
   contract; WS flip additionally excluded by enforcement.

9. `rooms-presence-lifecycle` (http, admin presence join/update/leave) —
   `cmd/instantd/runtime_cf003_presence_exclusion_test.go::TestCF003AssembledAdminPresenceExclusion`
   @ `951175c`. Proves enforced exclusion: `GET /admin/rooms/presence`
   returns stable 501 unsupported to an authorized caller, still 501 with
   live WS room presence behind it, 401 missing/foreign without disclosing
   state. DIVERGENCE (explicit, not rewritten): manifest `expectedState`
   still reads "admin presence view reflects join, update and leave
   lifecycle" (positive surface) while product enforces 501 exclusion.
   Owner decision required: rewrite `expectedState` to the 501 exclusion
   vs select and implement a positive admin-presence surface + raw capture.
   `expectedState` is NOT rewritten here. Row stays `gap`. Flipping to
   covered on the current exclusion text would be a report-only scope
   change; flipping to the positive text has no positive capture.

10. `transactions-rollback-error` (http, no partial write) —
    `cmd/instantd/runtime_cf003_transaction_test.go::TestCF003AssembledTransactionMatrix`
    @ `ec78ad7` (rollback after second-step unique-ID failure, exact whole
    query state, 10x race + Sol ACCEPT) + cross-transport denial legs
    `cmd/instantd/runtime_cf003_transaction_transport_test.go::TestCF003AssembledTransactionTransportMatrix`
    @ `43ed443` (per-transport exact validation denial mutates nothing,
    exact whole HTTP query convergence). Missing flip: authored HTTP
    transaction exchange (NDJSON). Why gap remains: report-only scope
    change.

11. `transactions-cardinality-boundary` (ws, cardinality/merge/cascade/
    required) —
    `cmd/instantd/runtime_cf003_transaction_transport_test.go::TestCF003AssembledTransactionTransportMatrix`
    @ `43ed443` + `ab0d75b` (WS same-batch cardinality-one final-wins
    exactly once) + `e2d97ae` (SSE same boundary) +
    `cmd/instantd/runtime_cf003_runtime_transact_test.go::TestCF003AssembledRuntimeTransactMatrix`
    @ `dcc8373` + `255e28b` (exact single-row whole-result, `len==1` +
    DeepEqual; malformed/admin-only/bad-input denials mutate nothing).
    Only `ws.transact.required` retraction is corpus-covered. Missing flip:
    corpus NDJSON cardinality/merge/cascade leg. Why gap remains: WS
    variant excluded by enforcement; SSE/HTTP variants would be
    report-only scope changes without checked-in capture.

12. `transactions-lookup-lifecycle` (sse, lookup persists + visible over
    stream) —
    `cmd/instantd/runtime_cf003_sse_lookup_test.go::TestCF003AssembledSSELookupLifecycle`
    @ `63b82d0` + `2340195` + `f904b42`. Proves lookup-eid update retargets
    seeded entity + second lookup mints fresh entity; each surfaces as
    `refresh-ok` at the exact triggering tx and converges the mounted HTTP
    query path to the exact whole result; lengths-before-elements, exact
    float watermark, strict child-nodes oracle (`f904b42`). Missing flip:
    authored SSE transaction evidence (NDJSON). Why gap remains:
    report-only scope change.

13. `transactions-concurrency-gap` (http, concurrent ordering/final
    state) —
    `cmd/instantd/runtime_cf003_transaction_test.go::TestCF003AssembledTransactionMatrix`
    @ `ec78ad7` (same-lookup concurrent convergence via start barrier to
    one exact visible entity; only success or decoded unique-constraint
    400 accepted; exact baseline + one exact winner; winner saved; baseline
    alone after delete) +
    `cmd/instantd/runtime_cf003_transaction_transport_test.go::TestCF003AssembledTransactionTransportMatrix`
    @ `43ed443` (WS+SSE positive convergence past watermark). Missing
    flip: HTTP concurrency capture proving ordering/final state via
    replay. Why gap remains: report-only scope change (no HTTP concurrency
    capture selected/checked in); the start barrier encourages but does
    not force DB-level overlap, so the assembled leg is boundary evidence
    only, not a fault-scheduled campaign.

Captured-v1 note (applies to all 13): every checked-in scenario oracle is
`regression` from the authored v2 contract (`corpus/manifest.json`
`oracle.kind`), not a captured v1 oracle. Any flip claiming v1 parity
awaits CF-004 pinned-v1 environment + CF-005 differential, both `BLOCKED /
EXTERNAL_EVIDENCE`. No such flip is made here.

`corpus/manifest.json` untouched; all 13 coverage rows stay `gap`;
`program-manifest.md` untouched. No push, deployment, publication, or tag.

## Follow-up packets pointer (2026-09-19, HEAD 302b13816f07bfb1f76d6340572bbc1ddf308436 clean, docs only)

Bounded definitions for the structurally-blocked remainder live in
`docs/plans/finish-up/followup-packets.md` (FU-01 multi-client capture
contract, FU-02 recorder scope decision with WS exclusion as constraint,
FU-03 admin-presence surface decision, FU-04 CF-004/005 external
evidence out of local scope). All four are proposed, not started; no
status flipped here.

## FU-01 preparatory slice (2026-09-19, HEAD a08e60612fb1041e2f8d6464afce8fd1f78580f8 clean, no push/tag)

Locally-doable candidate-side contract/capture slice; core recorder work
and all manifest flips remain gated (see below). One focused commit.

- Contract doc: `docs/plans/finish-up/fu01-multiclient-capture-contract.md`
  defines the SSE-only multi-client capture contract per FU-01
  acceptance 1–3 as definition (subscriber counts, shared-room /
  shared-query topologies, barrier/ordering rules, per-subscriber
  `(subscriber, seq)` retention, canonical derivation by DeepEqual, exact
  replay oracle with exact float watermarks and exact tx IDs;
  late-join/resync/leave/quiescence with a bounded 250 ms quiet window;
  CF-002 compatibility). Authorizes no recorder change, no capture run,
  no NDJSON, no manifest flip. Records one discovered product fact: a
  fresh subscriber's first answer uses the init-query object-tree
  envelope at the current tx, while transactional refreshes use the
  node-list envelope — both exact, pinned separately.
- Shape proof: `cmd/instantd/runtime_fu01_capture_contract_test.go`
  (`TestFU01CaptureContractQueryConcurrency`,
  `TestFU01CaptureContractRoomFanout`) over production-mounted SSE
  routes against owned fixtures (`INSTANT_TEST_INTEGRATION=1`):
  per-subscriber raw retention, canonical derivation, replay of the
  exact oracle from retention alone, bounded quiet proof, late joiners
  (query C converges to identical final titles at the exact trigger tx;
  room B late-joins + resyncs). Existing assembled tests untouched.
- Evidence: focused `-race` PASS (`-count=1` and `-count=2`);
  `TestCF002WSRecordCreatesNoArtifact` PASS (WS exclusion intact);
  `gofmt`, `go vet ./cmd/instantd/`, `git diff --check` clean.
- Remains gated: `corpusctl` multi-client record path (needs FU-02
  scope decision for any manifest effect), checked-in NDJSON legs +
  corpus replay for `rooms-fanout-positive`,
  `query-concurrency-gap`, `refresh-convergence-concurrency`, all
  manifest flips (`corpus/manifest.json` untouched, all rows stay
  `gap`), v1/external (FU-04), `expectedState` (FU-03), WS capture
  (excluded by enforcement). `followup-packets.md`,
  `program-manifest.md` untouched; all terminal rows preserved.

## CF-003 manifest-standard pointer (2026-09-19, HEAD 6090c9ef82186cc4f9658af5dbc2aba7600aba13 clean, docs only)

Standard recorded in
`docs/plans/finish-up/cf003-manifest-standard-decision.md`: the 13
gap rows stay `gap`; no flips without checked-in raw capture
evidence. Assembled-route legs remain the durable local proof, cited
per-row at execution-ledger.md:904-1047. Future flips route via
FU-01 (multi-client rows), FU-02 scope decision (single-flow rows),
FU-04 external (captured-v1 rows); FU-03 Option 1 already decided.
CF-003 stays `PARTIAL`. No manifest, code, or test touched.

## Coordinator run — DEC-001 single-node-alpha finish-up (2026-09-19)

Baseline recaptured: HEAD `65d1df05eb51139a735cd755b09db4f42c745e2a` on
`main`, clean tree (`git status --short` empty, no diff), go1.27.1
darwin/arm64, UTC `2026-09-19T06:21:21Z`. Matches the invocation hint
SHA; the hint's packet counts (29 alpha-required / 22 complete /
CF-003+QR-005 partial / FR-001 pending / OP-003+OP-005+QR-001+FR-002
blocked) are a starting hint only — each packet is reconciled against
current source and its latest handoff before work begins.

STAGE 0 control reconciliation (coordinator, no packet state changed):
- `docs/plans/finish-up/README.md` stale
  `READY_FOR_OWNER_SCOPE_DECISION` corrected to
  `IN_PROGRESS_DEC001_SINGLE_NODE_ALPHA` with the recorded owner
  decisions. Planning baseline retained as historical.
- Supplied FU-02 decision recorded durably in
  `docs/plans/finish-up/fu02-recorder-scope-decision.md` (Option A:
  report-only raw capture + exact replay for the 8 already-accepted
  single-flow HTTP/SSE legs; no general recorder expansion unless
  evidence proves necessity; WS exclusion re-affirmed).
- FU-01 contract (`fu01-multiclient-capture-contract.md`) and FU-03
  Option 1 (`fu03-admin-presence-decision.md`) already on `main`;
  preserved unchanged. `followup-packets.md`, `program-manifest.md`,
  `corpus/manifest.json` untouched. No product or evidence packet
  marked complete by this reconciliation.

Live ledger for this run is maintained in this section and per-packet
handoffs below. Next: STAGE 1 QR-003 corrective release-gate packet.

## QR-003 corrective close-out (2026-09-19, HEAD 65d1df0 main + work below, go1.27.1 darwin/arm64)

# Packet QR-003-corrective handoff

Status: COMPLETE
Candidate: HEAD `65d1df05eb51139a735cd755b09db4f42c745e2a` + uncommitted work below (accepted commit SHA recorded at close)
Decision/profile: DEC-001-single-node-alpha-20260905 (QR-003 REQUIRED)
Implementation agent: OpenCode / Muse Spark 1.3 free / xhigh
Review agent: OpenAI / GPT-5.6 Sol / medium
Repair cycle: 2

## Ledger
| Row | Result | Evidence | Remaining |
| R1 lane selections explicit | GREEN | Per-lane case + lane comments in gate; per-lane zero/skip/allowed/required battery | None |
| R2 zero required fails closed | GREEN | RELEASE_TEST_ZERO_TARGET per all 4 lanes rejects | None |
| R3 skipped required fails closed | GREEN | Per-lane required-skip rejection incl. package-scoped corpus identities | None |
| R4 outside-lane skips allowed | GREEN | Real reactive -short log (66 selected, 2 skipped, 0 disallowed) accepted by production gate; old any-skip would reject | None |
| R5 child failures propagate | GREEN | RELEASE_TEST_FAIL_TARGET per vet + all 4 lanes exit 7 | None |
| R6 real-suite pin | GREEN | Real log through actual copied gate; injected bare skip rejects | None |
| R7 pre-existing checks intact | GREEN | Full battery 44/0: dirty/identity/hash/freshness/symlink/binary/evidence/lane/type/Linux/handoff/seam | None |

## Changes
- scripts/quality-release-gate.sh: lane_disallowed_skips (3 allowlisted outside-lane substrings), package-scoped corpus parent/scenario/skip helpers, per-lane run_test_target (hermetic allowlist / owned-DB strict / contract required-identities / unknown-lane die)
- scripts/test-quality-release-gate.sh: per-lane fixtures + real-log-through-gate block; 44 checks

## Verification
| Command/run | Selected | Exit | Meaning |
| bash scripts/test-quality-release-gate.sh | 44 checks | 0 | Full contract battery incl. real-suite R4/R6, decoy-package, short-only, allowlisted-skip rejections |
| bash -n both scripts; git diff --check | n/a | 0 | Syntax/whitespace clean |
| Real reactive hermetic log via production gate | 66 sel / 2 skip / 0 disallowed | accept; injected rejects | R4/R6 real-behavior proof |

## Independent review
- Verdict: ACCEPT (third Sol review; prior two REPAIR_REQUIRED findings repaired: parent/subtest conflation + mirrored helper; package-unscoped identity + decoy case)
- Positively verified: R1-R7 per rows above; prior-cycle repairs; go.mod corpus identity; no broader claims inferred
- Findings repaired: 3 across 2 cycles (see above)
- Remaining findings: none
- Evidence limitations: owned-DB lanes (test-integration/test-contract real runs) and end-to-end gate run not executed here; owned by phase-08 FR-002 on qualified Linux with owned DB

## Not run
- make test-unit full ./... real lane (long suite); make bench-acceptance real lane; make test-integration/test-contract real lanes (AUTH-RUNTIME-001 NOT_GRANTED, no DB contact); production gate end-to-end (belongs to FR-002)

## Scope audit
- Pre-existing changes preserved: STAGE-0 docs untouched by workers
- Leased paths: only the two gate scripts
- Coordinator-owned integration: STAGE-0 README/fu02/ledger-docs committed alongside in the same focused local commit (no push)
- Unexpected changes: none

## Artifact/provenance
- Candidate SHA: accepted commit SHA recorded below at close
- Binary/config digest: n/a (hermetic gate packet; binary match check preserved, not run end-to-end)
- Campaign: n/a
- Evidence paths/hashes: scripts/test-quality-release-gate.sh battery output (44 passed, 0 failed)
- Environment identity: go1.27.1 darwin/arm64; no external fixture

## Next prerequisite
- STAGE 2A first bounded single-flow capture packet (FU-02 report-only), starting with corpus layout discovery

QR-003-corrective accepted commit: `8d1c0fa0d359353507f0855556ac51e384d8700e` (local only, no push). Tree clean. QR-003 stays `COMPLETE / ACCEPTED_CONTRACT_GATE` (corrective closed).

## CF-003-2A1 repair-1 paused (2026-09-19, HEAD c9483a95, tree has uncommitted packet files)

Implementation handoff (unreviewed COMPLETE): corpus/auth-http-denied-error.json + replay test + single-row manifest flip (auth-http-denied-error gap→covered). Independent Sol review returned REPAIR_REQUIRED with 4 accepted in-scope findings: (1) borrowed scenario 00-smoke false attribution — needs honest transport-evidence self-binding (id/fixture) + minimal validator extension; (2) declared fixture `smoke` mismatches capture/replay app id — needs fixture/id asserts or dedicated fixture + mutation proof; (3) no expired-credential branch despite row wording — needs deterministically seeded expired-code exchange; (4) false no-residue claim (noteFailure persists auth_throttle) — needs claim removal + explicit throttle-state assert. Repair cycle 1 dispatch (Muse Spark 1.3 free xhigh, same lease) failed TWICE at spawn with provider `service_overloaded` (sessions ses_f476360f2ffeyrzi7cWjccCCW6, ses_f4754993dffefnEdudg0xGr2aQ); no substitution made. Packet PAUSED uncommitted; no packet state flipped; QR-003 corrective stays accepted at c9483a95. Next: retry repair-1 when backend recovers.

CF-003-2A1 addendum (2026-09-19, same HEAD): coordinator post-review inspection found the worker's handoff understated its diff — internal/corpus/manifest.go + manifest_test.go ARE modified (transport-evidence self-binding: id/fixture equality + scenario==id for .json evidence, with new contract tests), i.e. F1's validator half is already implemented, but the manifest row + capture envelope still declare borrowed scenario `00-smoke`, so `validate` now FAILS (`transport evidence must self-bind: scenario "00-smoke" does not match coverage "auth-http-denied-error"`, exit 1). The earlier coordinator `validate` green is therefore stale and withdrawn; re-verification belongs to repair-1. Repair-1 scope confirmed: finish the row/envelope flip to self-binding (`auth-http-denied-error`), fix fixture binding (F2), add expired branch (F3), correct no-residue text + throttle assert (F4), then full green. Tree left uncommitted; nothing pushed.

## CF-003-2A1 close-out (2026-09-19, HEAD c9483a95 + work below, go1.27.1 darwin/arm64)

# Packet CF-003-2A1 handoff

Status: COMPLETE
Candidate: HEAD `c9483a95` + uncommitted packet files below (accepted commit SHA at close)
Decision/profile: DEC-001-single-node-alpha (FU-02 Option A report-only)
Implementation agent: OpenCode / Muse Spark 1.3 free / xhigh
Review agent: OpenAI / GPT-5.6 Sol / medium
Repair cycle: 2

## Ledger
| Row | Result | Evidence | Remaining |
| R1 raw capture | GREEN | corpus/auth-http-denied-error.json: 4 deterministic exchanges (400 malformed, 401 invalid x2 identical, 401 expired seeded), redacted, guest leg excluded with reason | None |
| R2 exact replay | GREEN | TestCF003DeniedErrorCaptureReplay: exact bytes + Content-Type, id/fixture/app binding, throttle {2+1}, guest shape; -race twice, zero skips | None |
| R3 single-row flip | GREEN | Only auth-http-denied-error gap→covered (9/13/4 → 10/12/4); validate + validate-release green; 18/18 replay green | None |
| R4 exclusions | GREEN | WS record fails closed; .json evidence http/sse-only (registry + alias guards); no FU-01/03/04, no v1 | None |

## Changes
- corpus/auth-http-denied-error.json (NEW): self-bound envelope + 4 exchanges + notes
- corpus/fixtures/auth-http-denied-error.json (NEW): dedicated fixture (app 0000-4000-8000-000000000003)
- corpus/manifest.json: fixtures[] entry + auth-http-denied-error row only
- cmd/instantd/runtime_cf003_denied_replay_test.go (NEW): exact replay test
- internal/corpus/manifest.go + manifest_test.go: transport-evidence self-binding (id/fixture/scenario==id, http/sse-only, WS alias guard) + 5 contract subtests

## Verification
| Command/run | Selected | Exit | Meaning |
| replay test -race twice (owned fixture) | 1 test | 0,0 | R2 exact replay |
| TestCorpusReplayIntegration | 18/18 | 0 | no corpus regression |
| validate / validate-release | full | 0,0 | R3 gates |
| self-binding contract tests | 5/5 | 0 | F1 + WS-alias guard |
| WS-record exclusion | 1 | 0 | R4 enforcement |
| internal/corpus + cmd/corpusctl (incl. -race) | full | 0 | affected packages |
| gofmt/vet/diff-check | n/a | 0 | static clean |

## Independent review
- Verdict: ACCEPT (terminal; R1-R4 positively verified with file:line-level checks)
- Findings repaired: 5 across 2 cycles (borrowed scenario, fixture mismatch, missing expired branch, false no-residue, WS self-bind bypass)
- Remaining findings: none
- Evidence limitations: DB lanes run on owned testkit fixtures only; no external/v1/release qualification claimed

## Not run
- Full ./cmd/instantd suite per run (affected focused lanes run instead); production gate end-to-end (FR-002 owns it); differential/v1 (FU-04 blocked)

## Scope audit
- Pre-existing changes preserved: QR-003 commit c9483a95 intact; STAGE-0 docs preserved
- Leased paths (+cycle-0 validator extension, disclosed): listed under Changes; nothing else
- Coordinator-owned integration: this handoff + close-out commit (local only, no push)
- Process note: repair-2 handoff prose claimed "no new edit required" while the diff shows its transport-guard edits; coordinator verified substance directly (5/5 subtests, validate green) and accepts the code, not the prose. Worker handoff accuracy itself is a follow-up observation, not a packet defect.
- Unexpected changes: none

## Artifact/provenance
- Candidate SHA: accepted commit SHA at close (below)
- Evidence: checked-in capture + replay test + battery outputs above
- Environment: go1.27.1 darwin/arm64; owned instant_test_* fixtures only

## Next prerequisite
- CF-003-2A2 (auth-http-refresh-lifecycle) reusing the self-bound transport-evidence pattern

CF-003-2A1 accepted commit: `47a95683ece1a825dc8ce23b59af63c4adab882e` (local only, no push). Tree clean. CF-003 stays PARTIAL (single-row flip only: 10 covered / 12 gap / 4 unsupported).

## CF-003-2A2 close-out (2026-09-19, HEAD 4eb6739 + work below, go1.27.1 darwin/arm64)

# Packet CF-003-2A2 handoff

Status: COMPLETE
Candidate: HEAD `4eb6739a33f562b5ce619a8b322a8980161afe40` + uncommitted packet files below (accepted commit SHA at close)
Decision/profile: DEC-001-single-node-alpha (FU-02 Option A report-only)
Implementation agent: OpenCode / Muse Spark 1.3 free / xhigh
Review agent: OpenAI / GPT-5.6 Sol / medium
Repair cycle: 1

## Ledger
| Row | Result | Evidence | Remaining |
| R1 raw capture | GREEN | 7 deterministic exchanges (400/401-batch/401-verify/200-signout-idempotent/400); minted rotation excluded with reason | None |
| R2 exact replay | GREEN | Exact bytes+CT, 4-field fixture binding, live rotation DeepEqual/invalidation/survivor, throttle-empty; -race twice; 4 mutation proofs | None |
| R3 single-row flip | GREEN | Only auth-http-refresh-lifecycle gap→covered (10/12/4 → 11/11/4); validate + validate-release green; 18/18 green | None |
| R4 exclusions | GREEN | WS fails closed; no other files/rows; no v1 text | None |

## Changes
- corpus/auth-http-refresh-lifecycle.json (NEW): self-bound envelope + 7 exchanges + excluded note
- corpus/fixtures/auth-http-refresh-lifecycle.json (NEW): dedicated fixture == actual seed
- corpus/manifest.json: fixtures[] entry + single-row flip (expectedState unchanged)
- cmd/instantd/runtime_cf003_refresh_replay_test.go (NEW): exact replay + live rotation + 4-field binding

## Verification
| Command/run | Selected | Exit | Meaning |
| replay -race twice (owned fixture) | 1 | 0,0 | R2 |
| 4 mutations (creator/admin/txSteps/appId) | 1 each | FAIL pre-replay | binding load-bearing; restored identical |
| TestCorpusReplayIntegration | 18/18 | 0 | no regression |
| validate / validate-release | full | 0,0 | R3 gates |
| WS-record exclusion | 1 | 0 | R4 |
| cmd/instantd package | full | 0 | affected package |
| gofmt/vet/diff-check | n/a | 0 | static clean |

## Independent review
- Verdict: ACCEPT (second review; first: REPAIR_REQUIRED on 4-field binding only; hybrid capture/live rotation design explicitly accepted as non-over-claiming)
- Findings repaired: 1 (fixture binding appId → all four fields)
- Remaining findings: none
- Evidence limitations: owned testkit fixtures only; no external/v1/release qualification

## Not run
- Full-repo sweep (affected lanes run instead); production gate end-to-end (FR-002); differential/v1 (FU-04 blocked)

## Scope audit
- Leased paths only (manifest single-row hunk + fixtures[] entry + 3 new files); no validator/recorder/docs/Makefile changes
- Unexpected changes: none

## Artifact/provenance
- Candidate SHA: accepted commit SHA at close (below)
- Environment: go1.27.1 darwin/arm64; owned instant_test_* fixtures only

## Next prerequisite
- CF-003-2A3 (query-conjunction-gap) via TestCF003AssembledQueryConjunctionAndOrder

CF-003-2A2 accepted commit: `f1c5a407da1fd15ad02ee2f8b4a0cc5e280b7be5` (local only, no push). Tree clean. CF-003 stays PARTIAL (11 covered / 11 gap / 4 unsupported).
