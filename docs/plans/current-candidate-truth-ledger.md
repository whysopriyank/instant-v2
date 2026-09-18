# Current-candidate truth ledger (F-001)

Packet: `F-001` in `docs/plans/finish-up/01-scope-and-decisions.md`.
Candidate: `26a1caf9856110b711315aaed4c5cbeaec3bbc36` on `main`, captured
`2026-09-06T17:29:15Z` (clean tree at capture: G1 `20433cd`, G2 `e8565ce`,
G3 `b1550d1`, G4 `26a1caf` landed above the prior baseline, unpushed).
Toolchain: `go1.27.0 darwin/arm64`.
History (preserved, not current): `14e2988e79851b34e340b41ebaf7ea109132b70e`
captured `2026-09-05T05:19:05Z` (clean) and re-fingerprinted
`2026-09-05T08:11:16Z` (dirty); planning baseline `5ccea25` is an ancestor
of this candidate and its dirty tree no longer exists as uncommitted work.
All dated sections below that cite the old fingerprint remain scoped to it.

Reconciliation record (2026-09-05T08:11:16Z): the tree is now dirty with
in-progress packet work. Modified: `cmd/instantd/refresh.go`,
`cmd/instantd/routes.go`, `cmd/instantd/runtime.go`,
`internal/reactive/reactive.go`, `internal/sync/groups.go`,
`internal/sync/session_query.go` (126 insertions, 15 deletions — RT-001
permission rebinding, PARTIAL, uncommitted). Untracked: this ledger,
`internal/sync/rebind.go`, `internal/sync/rt001_rebind_test.go`. The
clean-tree declaration above is a historical baseline, not the current
state. Language corrected per review: pass-1 rows are historically
verified with current-candidate acceptance pending; unselected surfaces
are deferred/unselected (DEC-001 has no approved exclusions).

## Contract ledger

| Row | Invariant | Evidence | Status |
|---|---|---|---|
| F-001a | Revision, branch, dirty-tree fingerprint and timestamp identify the audited candidate. | `14e2988…` / `main` / clean `status`+`diff` / `2026-09-05T05:19:05Z` / `go1.27.0`, captured before any edit this session | GREEN |
| F-001b | Every old red item is reconciled against current source and its latest executed evidence. | `docs/plans/implementation-pass-1.md` rows Q1a–G1b (historically verified; current-candidate acceptance pending) plus source-presence checks below | GREEN with two caveats (Q2 retrospective baseline, preserved as stated; acceptance language corrected per review) |
| F-001c | Implementation completion is distinct from integration, compatibility, production, and release acceptance. | Status matrix below; no test-exists or old-report claim is treated as acceptance; nothing marked excluded | GREEN |
| F-001d | Historical results remain preserved and are clearly scoped to their candidate/date. | Link audit below; historical bodies untouched | GREEN |
| F-001e | Every newly discovered gap has one stable packet ID and owning phase. | Finish-up cross-reference below; all manifest packets registered | GREEN |

## F-001b — prior red items vs current source

`docs/plans/implementation-pass-1.md` records a COMPLETE bounded functional pass
(started 2026-08-31 from `3873335`). All five checkpoint commits exist in this
candidate's history (`git cat-file -t` = `commit` for each):

| Commit | Component | Pass-1 rows |
|---|---|---|
| `b401aa7` | Explicit-empty metrics config, read-pool comment | G1a, G1b |
| `9516a35` | Atomic OAuth consumption, expiry, configured providers | A1a, A1b, A1c, A2 |
| `53cd367` | Gauge registration ownership, runtime cleanup | M1 |
| `c07707a` | Exact numeric SQL/cache lookup identity | Q4a |
| `cc0efea` | Query ordering, cursors, conjunction, relations, corpus promotion | Q1a, Q1b, Q2a, Q2b |

Post-baseline fixes also present: `c30ca36` (bounded runtime correctness),
`bdefa74` (chaos/corpus evidence paths), `b04b2bb` (benchmark provenance).
The four formerly untracked tests now exist as tracked files:
`internal/adminapi/mutation_integrity_test.go`,
`internal/adminapi/users_store_internal_test.go`,
`internal/authn/attrs_cache_test.go`,
`internal/transact/dispatch_internal_test.go`.

Preserved caveat (not hidden): Q2's pre-fix observations were reconstructed
retrospectively via an isolated baseline copy with a Go overlay, not by
test-first execution on the live tree (`implementation-pass-1.md` lines 133–143).
That limits the Q2 red claim to a baseline/treatment comparison; it does not
reopen the rows, whose green regressions run on the current tree.

## Implementation closures confirmed at this candidate

Each item the phase requires the ledger to mark was read at the cited source
(coordinator reads; two read-only scout contexts corroborated). "Implemented"
below means present in source at the cited lines with acceptance pending — it
is a presence claim, not an acceptance claim:

- Pagination/cursor ordering: `internal/instaql/query.go:197-210` applies the
  requested order before page selection; `internal/instaql/pagination.go:100`
  pages after `applyOrder`. Implemented.
- Exact-number lookup: `c07707a` lineage; high-level same-batch equivalent
  numeric lookups share identity while adjacent large integers stay distinct
  (pass-1 Q4a evidence). Implemented.
- OAuth redemption atomicity: `internal/authn/oauth.go:134`
  (`SELECT … FOR UPDATE` serializes consumers on the state/code key) and
  `:178` (redirect consume + code persist share one commit). Implemented.
- Google/GitHub resolution: `internal/authn/oauth_provider.go:23-33`
  (builtin endpoints); Apple explicitly unsupported
  (`oauth_provider_test.go:105-107`). Implemented with stated exclusion.
- Reactive backoff: bounded retry lifecycle with reset/cap contract tests
  (`internal/reactive/retry_contract_test.go:111,296`). Implemented.
- COPY semantics: `internal/storage/copy.go:29-34` (`CopyTriples` preserves JSON
  null, deterministic cardinality-one winner); no non-test production caller
  found — implementation without integration acceptance (see F-001c).
- Metrics: gauge registration ownership + runtime release of all handles
  (pass-1 M1 evidence). Implemented.
- Config: `envOrAllowEmpty` explicit-empty metrics address; corrected read-pool
  comment (pass-1 G1a/G1b). Implemented.
- Signup-rule assembly: `cmd/instantd/routes.go:33-36` resolves persisted
  per-app rules on the shared catalog cache for signup authorization.
  Implemented.
- Explicit unsupported responses: `internal/sync/session.go:290` emits
  `501 "sync operation is unsupported"` (`unsupported_test.go:41-42`).
  Implemented as an exclusion path, not a feature.
- Missing-mailer failure: `internal/authn/authn.go:487-488` returns
  `ErrMagicCodeDeliveryUnavailable` before generating/persisting a code — safe
  disabled behavior, not a delivery system. Implemented as fail-closed.
- HTTP/SSE corpus adapters: transport mechanics exist, but
  `cmd/corpusctl/main.go:87` still reports `record unavailable`; no complete
  SDK/external fixture lifecycle. Assembly gap (CF-002 owns it).

Defects confirmed present at this candidate (owned by later packets, not
repaired here): daemon assembly hardcodes
`os.TempDir()/instantv2-files` (`cmd/instantd/routes.go:162`) with no backup
object-store injection (DA-001); unwired backup object paths 503
(`internal/backup/http_objects.go:50-52`) (DA-001/DA-003).

## F-001c — status matrix (implementation ≠ acceptance)

| Behavior | Implementation | Integration | Compatibility | Production | Release |
|---|---|---|---|---|---|
| Query ordering/cursors/conjunction/relations | Historically verified (pass-1 Q1–Q2); current-candidate acceptance pending | WS corpus 18-scenario replay cited by pass-1; not re-executed here | Matrix rows pending (CF-003) | Not claimed | Not claimed |
| Exact numeric lookup | Historically verified (pass-1 Q4a); current-candidate acceptance pending | Owned-DB regressions cited; not re-executed here | Comparator/matrix rows pending | Not claimed | Not claimed |
| OAuth local lifecycle | Historically verified (pass-1 A1–A2); current-candidate acceptance pending | Local-provider tests cited; not re-executed here | No v1/provider claim | Not claimed | Not claimed |
| Metrics/config | Historically verified (pass-1 M1/G1); current-candidate acceptance pending | Package tests cited; not re-executed here | N/A | Not claimed | Not claimed |
| Signup-rule assembly | Present in source; acceptance pending | Acceptance pending on this candidate | Matrix rows pending | Not claimed | Not claimed |
| COPY semantics | Present, no production caller | Owned-PostgreSQL COPY acceptance passed | Local COPY/Insert parity accepted; no v1 claim | Not claimed | Not claimed |
| Sync/stream operations | 501 exclusion path present | Covered by unit test only | Deferred/unselected (no approved exclusion) | Not claimed | Not claimed |
| Magic-code delivery | Fail-closed only | No provider acceptance | No claim | Not claimed | Not claimed |
| Corpus recording | Unavailable (`record unavailable`) | N/A | CF-002 owns | Not claimed | Not claimed |
| Storage durability/backup assembly | Defective (temp default, unwired S3) | Missing | Missing | Not claimed | Not claimed |
| RT-001 permission rebinding | In progress, PARTIAL, uncommitted (see reconciliation record) | Missing (no runnable DB here) | Missing | Not claimed | Not claimed |
| RT-002 delivery semantics | Implemented, unproven (runtime proof owed) | Missing (no runnable DB/loopback here) | Missing | Not claimed | Not claimed |
| RT-003 depth-one recovery | Complete (hermetic red-to-green proof) | Hermetic gate/drain tests green | N/A (no compat surface) | Not claimed | Not claimed |
| DA-001 storage assembly | File side complete hermetically (validated root, fsync, refusal, traversal pin); backup smoke owed | Config suite + 10 storage tests green; DB/socket tests env-blocked | Disabled-503 proven; object-backup selected run owed | Not claimed | Not claimed |

Rule applied throughout: `TESTS_EXIST` and old-report prose are observations,
never completion states. "Historically verified" cites the pass-1 bounded
contract only; it does not establish current-candidate acceptance. DEC-001
approval froze the single-node-alpha policy and retained the packet ledger's
REQUIRED/DEFERRED selections. Approval did not by itself convert any DEFERRED
or proposed-exclusion row into an enforced EXCLUDED status. Those restrictions
become eligible for EXCLUDED only after their owning packets establish
consistent product, documentation, corpus, and release-gate enforcement. Until
then they remain deferred/unselected and unavailable as release claims.

## Corpus counts (manifest is authority, verified by execution)

Historical note: the 2026-09-05 capture recorded 18 scenarios; 31 surfaces
(22 covered, 7 gaps, 2 unsupported); 25-row matrix (9 covered, 15 gaps,
1 unsupported), matching `corpus/README.md:58-60` at that time. The committed
DA-004V exclusion work changed the manifest: it is now 18 scenarios;
31 surfaces (22 covered, 6 gaps, 3 unsupported); 26-row matrix (9 covered,
13 gaps, 4 unsupported) per `corpus/README.md` and
`go run ./cmd/corpusctl --mode validate` (this stabilization, exit 0).
Zero v1 captures.

## F-001d — historical scoping

Preserved as historical, not rewritten: `docs/plans/implementation-pass-1.md`
(bounded pass, hermetic + owned-DB evidence, explicit Q2 caveat),
`docs/plans/gaps-backlog-precision-build-contract.md` (evidence ledger and
CP/P/C/T/B/O/PERF/D/REL packet record), prior `tasks/phase-*`, roadmap, and
quality-scorecard prose. Live PostgreSQL evidence cited by pass-1 used the
owned port-55490 cluster with testkit-isolated databases; no default developer
database was claimed. Nothing in this ledger attributes release acceptance to
those runs.

## F-001e — finish-up packet registration

All `program-manifest.md` packets are registered exactly once with an owning
phase: F-001/F-002 (01); RT-001–003 (02); DA-001–003, DA-004V, DA-004–007,
DA-008A/B (03); EV-001–006 (04); CF-001–005 (05); OP-001–006 (06); QR-001–005
(07); FR-001–004 (08); TD-001–005 (09, deferred). Legacy mapping CP/P/C/T/B/O/
PERF/D/REL → finish-up owners is taken from the manifest unchanged. DEC-001
(`docs/reference/release-envelope.md`, `DEC-001-single-node-alpha-20260905`)
was `APPROVED` by Priyank, project owner, at `2026-09-05T08:35:12Z`
(superseding `DEC-001-dev-checkpoint-20260904`); its REQUIRED/DEFERRED packet
selections remain the governing policy. No `AUTH-*` authority has been granted.

## Validation performed

- `git rev-parse HEAD` / `branch --show-current` / `status --short` /
  `diff --name-only` + `--stat --compact-summary` / `go version` captured
  before any work; tree clean.
- Five pass-1 commits plus three follow-up fix commits verified present via
  `git cat-file -t`; `5ccea25` verified as ancestor of HEAD.
- Corpus figures produced by executing Python against `corpus/manifest.json`,
  then cross-checked with `corpus/README.md:58-60` and the gaps-contract
  `C-004` row (line 1133).
- Ledger compared against the gaps-contract master packet table (lines 306–341)
  and the manifest registry; every packet appears once.
- Link/file audit: all cited paths above were opened or executed this session
  except scout-corroborated OAuth/pending-detail lines, which are quoted from
  read-only scout evidence and flagged for the reviewer to re-verify.

## For the reviewer

Fresh read-only review must try to: find an untracked gap, contradict any
`GREEN` above with current-source evidence, or identify a claim lacking
current-candidate support — especially the scout-corroborated OAuth atomicity
lines and any pass-1 integration claim repeated here without re-execution.

## Reconciliation refresh (2026-09-06T13:45:43Z)

HEAD is unchanged (`14e2988e79851b34e340b41ebaf7ea109132b70e`, `main`,
`go1.27.0 darwin/arm64`); `corpus/manifest.json` still holds 18 scenarios.
The tree remains dirty and has grown: 32 tracked files modified (898
insertions, 273 deletions per `git diff --stat`) plus 11 untracked paths,
now including this ledger, `internal/sync/rebind.go`,
`internal/sync/reconnect_live_test.go`, and the `*_test.go` packet files
listed by `git status --short` this date. Nothing below rewrites the
2026-09-05 baseline; it supersedes only the named row descriptions.

- RT-001 permission rebinding: Gen-epoch publication coordination
  implemented (`reactive.Subscription.Gen`/`Frame.Gen`, `json:"-"`;
  bumped on gate swap under `groupsMu`; stamped at attempt start;
  checked in `Emit` and the initial-answer flight). Rows a–e,
  superseded-generation drop, attempt-stamping, and
  incremental-success-stamped tests green with `-race`; corpus
  `05-permission-deny` green. Status: implemented + package/corpus
  evidence executed; independent security review still owed.
- RT-002 delivery semantics: row (a) watermark test, row (d) white-box
  reconnect test, and SSE error-propagation test green with `-race`;
  NEW live-socket leg `TestLiveReconnectConvergesAfterDrop`
  (`internal/sync/reconnect_live_test.go`) green with `-race`
  (0.30s): two live members certified at one generation, one drops, a
  fresh socket rejoins to canonically equal result + equal watermark
  with zero recompute, then proves liveness both directions. The old
  "no runnable DB/loopback here" caveat is withdrawn: all evidence
  above ran against live PostgreSQL over loopback with the normal
  (unsandboxed) binary. Reliability review still owed.
- RT-003 depth-one: unchanged, green.
- DA-003 backup fail-closed: all 5 rows test-pinned, backup package
  green 2x with `-race`; independent data review still owed.
- Corpus pagination (11-query-pagination, 17-after-cursor) failures
  remain proven pre-existing on clean HEAD via stash comparison;
  untouched as instaql/CF territory.
- DEC-001 (`DEC-001-single-node-alpha-20260905`, APPROVED) still scopes
  the dirty tree with no immutable candidate selected; FR-002 must run
  on a clean tree and needs a commit grouping the owner has yet to
  approve.
- Review-closure note: RT-002 reliability review is accepted contingent on one unsandboxed live green — satisfied in-session (updated `TestLiveReconnectConvergesAfterDrop` green with `-race`; `wantTx+1` mutation probe red with exit 1, reverted, zero residue; verbatim outputs pasted in the working session). Security (RT-001) and data (DA-003) reviews remain owed.
- Independent-acceptance note: all four owner-tasked items accepted by independent pass — splice fix (severity confirmed, lock audit holds, G1/G3 must land in same push), fail-closed default, fingerprint fields, DA-003 light touch. RT-001 security and DA-003 data reviews now done (adversarial self-review + independent pass); DA-001 accepted. G1 staged (18 files); G2–G4 commands approved. No commits without owner word. Full stats refresh deferred to commit time.
- Commit record: G1 `20433cd` (RT-001, 18 files), G2 `e8565ce` (RT-002/RT-003, 8 files), G3 `b1550d1` (DA-001/DA-003, 15 files) landed on `main` above `14e2988`, unpushed, in the approved order with G1+G3 in the same push train. This ledger plus the release envelope and selfhost guide commit as G4; the tree is otherwise clean. Corpus 11/17 pagination failures remain pre-existing (proven on `14e2988` HEAD via stash); all other cited evidence re-verified green pre-commit with `-race`.

## Session-recovery patch reconciliation (2026-09-07T16:25:56Z, HEAD 26a1caf main, go1.27.0 darwin/arm64)
Dirty fingerprint (post-repair): `93745bf01e222d5a00d37bc7ff2fd4dae76717c95eddc07d1fcaebebf1eed567` (`git diff | shasum -a 256`); 17 modified tracked + untracked `execution-ledger.md`, `session-recovery-patch-contract.md`, `internal/sync/h01_regression_test.go`, `internal/reactive/h01e_regression_test.go`. Prior `26a1caf` clean sections above retained as history; nothing below rewrites them.
- H-01: observed-epoch linearization; WS single-envelope wire remainder + rule-only-without-Notify indefinite delay recorded as PENDING (no approval invented). SSE queued envelopes drop at dequeue (zero wire leak); initial answers fail closed on post-publish swap; mat cleared atomically clear-then-bump; splice/rule loads outside locks; WS 10s write bound. Matrix H-01a..h pinned per execution-ledger (live H-01h + S1 decision tests green with -race).
- H-02: WS live reconnect (equal result+watermark, liveness) green; SSE live reconnect + delta live reconnect + SDK corrective-frame proof remain gaps (not claimed).
- H-03: no repair needed; 5 required tests green -race x2; new barrier `TestDiskBackendSecondUploadWaitsForRootConfirmation` pins ordering; external-dir trust + single-process scope documented.
- H-04: six packages -race green, vet + diff-check clean; corpus 05 PASS, 11/17 FAIL preserved pre-existing (instaql/CF, untouched).
- DEC-001 `DEC-001-single-node-alpha-20260905` APPROVED scope unchanged; no new decision; every AUTH-* remains NOT_GRANTED; FR-002 still requires clean tree + owner-approved grouping.

## Batch-2 packet refresh (2026-09-08, HEAD 85298d3, main)

The approved DEC-001 batch `DA-002 + DA-004V + DA-007` was implemented with
disjoint leases and independently reviewed where noted. This refresh does not
close Phase 03 or select an immutable release candidate.

- RT-002 bounded repair: missing raw transports now detach/close explicitly;
  admin SSE backpressure now terminates the stream and deferred teardown removes
  the subscription. New regressions passed with PostgreSQL and `-race` (overflow
  teardown 10x; focused delivery/reconnect matrix 2x), followed by the full
  sync/reactive/instantd race suite and vet. Sol accepted the implementation and
  test repair. Status remains `PARTIAL`: live SSE reconnect, live delta
  reconnect, and pinned-SDK corrective-frame application are still unproven.

- DA-002 upload integrity: `internal/storageapi` now binds the presigned
  filename, uses non-overwriting `PutIfAbsent`, serializes same-key retries
  and authorized deletes, cleans only objects created by the current request,
  and joins cleanup failures for reconciliation. Focused, package, and
  live-PostgreSQL `go test -race` passed, including `TestDuplicateFilename409`
  with exact orphan removal and `TestFilesTriples`. Repaired Sol
  data-integrity re-review returned ACCEPT. Status: COMPLETE for the bounded
  single-node packet; no immutable release candidate is selected.
- DA-004V dynamic view exclusion: `internal/perms/viewgate_test.go` pins the
  exact open/closed/dynamic classification and fallback chain; sync gate tests
  passed with `-race`. The DB-backed `internal/instaql` dynamic query test
  passed with the local PostgreSQL environment. Corpus/release-gate
  composition was not executed. Status: PARTIAL; package/runtime evidence is
  green, phase acceptance remains open.
- DA-007 rate-limit saturation: new identities fail closed at a saturated
  local bucket cap; overflow retry is tied to earliest idle-bucket eviction,
  bounded to one minute, and existing-key fairness plus cancellation,
  middleware, memory, and concurrent-admission tests pass with `-race`.
  Sol-advisor review found no lock/counter defect and required the retry
  repair now present. Final Sol packet gate returned ACCEPT after focused
  `-race -count=20` evidence. Status: COMPLETE for the bounded single-node
  packet; no multi-node claim is made.
- Verification executed: `go test -race ./internal/storageapi
  ./internal/ratelimit ./internal/perms -count=1`, focused DA-002/DA-007
  reruns at `-count=2`, sync gate tests, and `go vet` for changed packages;
  all completed green, including the live DB rows above.
- Earlier preflight reported no PostgreSQL response; a later owner-local
  preflight reported `localhost:5432 - accepting connections`, and the
  database-backed rows above passed. No database service was started or
  changed by this run.

## Batch-3 evidence tooling refresh (2026-09-08, HEAD 85298d3, dirty)

The approved evidence batch `EV-001 + EV-004 + EV-005` was implemented with
disjoint leases and then subjected to focused review, repair, and final Sol
acceptance. This refresh closes only the bounded tooling packets; it does not
claim a live soak, chaos campaign, benchmark campaign, compatibility matrix,
qualification, recovery, or release acceptance.

- EV-001 soak transaction ledger: the harness now records real client-event
  IDs, transaction IDs, processed transaction-ID watermarks, reconnect-safe
  event identity, and terminal protocol errors. Watermarks are monotonic,
  idempotent for equal values, coalescing for skipped intermediate values, and
  scoped to the submitting session. `go test -race ./cmd/soak -count=1` passed;
  focused watermark, timestamp, reconnect, identity, and concurrent-error
  tests also passed. Final Sol gate: ACCEPT. Status: COMPLETE for the bounded
  harness contract; short real soak evidence remains outstanding.
- EV-004 chaos provenance: process identities now bind kernel start tokens and
  executable hashes, restart identities must differ, report publication is
  non-overwriting, and `instantdInstance.Stop` refuses to kill a PID whose
  kernel start token no longer matches. The PID-reuse regression and the full
  `go test -race ./cmd/chaos -count=1` suite passed. Final Sol gate: ACCEPT.
  Status: COMPLETE for the bounded harness contract; no real chaos campaign is
  claimed.
- EV-005 benchmark evidence: required database measurements fail closed,
  unsupported fields are rejected, bundle finalization and approval are
  write-once with atomic index publication, interrupted approval recovery now
  requires a valid detached signature, and invalid-signature recovery is
  covered by regression tests. The full
  `go test -race ./internal/benchrun -count=1` suite passed (202.457s), as did
  focused integrity tests and `go vet`. Final Sol gate: ACCEPT. Status:
  COMPLETE for the bounded bundle contract; no live benchmark acceptance is
  claimed.
- Cross-batch verification: `go test -race ./cmd/soak ./cmd/chaos
  ./internal/benchrun -count=1`, `go vet ./cmd/soak ./cmd/chaos
  ./internal/benchrun`, `git diff --check`, and the affected product
  regression suite (`internal/storageapi`, `internal/ratelimit`,
  `internal/perms`, `internal/sync`, `internal/reactive`, `cmd/instantd`) all
  passed. `gofmt -l` reported no files.

## Phase-4 evidence tooling refresh (2026-09-08, HEAD 85298d3, dirty)

The remaining Phase 04 implementation lanes `EV-002 + EV-003 + EV-006` were
completed with disjoint ownership, focused worker tests, coordinator checks,
and final Sol review. This closes the bounded tooling contracts only; it does
not claim a live soak, chaos campaign, benchmark campaign, compatibility
matrix, qualification, recovery, or release acceptance.

- EV-002 soak evidence output: `cmd/soak` now publishes file and directory
  evidence without replacement, rejects symlinked or pre-existing
  destinations, writes completion metadata only after the evidence and
  manifest are durable, and retains incomplete evidence for diagnosis when a
  later publication step fails. Pprof is required rather than silently
  disabled. The focused `go test -race ./cmd/soak -count=1` suite passed,
  including injected publication failures, manifest collisions, racing target
  creation, destination validation, and failed-run non-publication. Final Sol
  review returned ACCEPT after the publication repairs.
- EV-003 soak process identity: the native shell harness now fails closed on
  unsupported platforms, stale or replaced PIDs, executable/hash mismatch,
  endpoint mismatch, and configuration-digest mismatch. The 14-case native
  identity suite passed, including a real `instantd` bind and same-port/
  wrong-address rejection; shell syntax checks passed.
- EV-006 benchsmoke wiring: the historical `cmd/benchsmoke` entrypoint is
  excluded from supported build and acceptance targets, has an explicit
  historical build target, and the supported smoke mapping points to
  `cmd/soaksetup`. Mapping tests, `go vet`, and dry-run Makefile checks passed.

Verification for this refresh: `gofmt -l cmd/soak cmd/benchsmoke`,
`git diff --check`, `go vet ./cmd/soak ./cmd/benchsmoke`,
`go test -race ./cmd/soak ./cmd/benchsmoke -count=1`, shell syntax checks, and
the complete EV-003 identity suite all passed. No commit, push, deployment,
or live qualification campaign was performed.

Phase 04 implementation packets are now bounded-green, but its final evidence
gate remains open wherever the plan requires real-environment evidence (in
particular the short real EV-001 soak). Phase 05 compatibility/recorder work,
Phase 06 Linux and recovery qualification, Phase 07 qualified soak/performance
and release-gate work, and Phase 08 clean-SHA independent acceptance remain
pending or dependency-blocked.

## Phase-5 recorder refresh (2026-09-08, HEAD 85298d3, dirty)

CF-002 now has a bounded HTTP/SSE recording implementation and a final Sol
acceptance for its local capture contract. The program status remains PARTIAL:
the CLI does not provision or reset external fixtures, and endpoint/source/
fixture identities are caller assertions rather than proof of the remote
candidate or database state.

- `corpusctl --mode record --transport http|sse` requires a target, fresh
  private output directory, endpoint/source/fixture identity metadata, and
  bounded capture settings. WebSocket recording remains explicitly unsupported.
- HTTP and SSE evidence retains raw transport material alongside canonical
  forms, applies path-scoped header redaction, preserves bounded quiescence and
  size/time limits, and fails closed on transport, incomplete-stream, or extra-
  record errors. Fixture reset is recorded as caller-owned; it is not verified.
- Output publication is descriptor-relative on Darwin/Linux: fresh 0700
  reservation with no-follow traversal, 0600 same-directory temporary files,
  fsync/close, kernel no-replace publication, and directory fsync. Non-Unix
  platforms fail closed. Failed publication never performs ambiguous named-file
  deletion; private temporary/final residue may remain untrusted, and only a
  successful return makes evidence eligible.
- Verification: native `go test -race ./internal/corpus ./cmd/corpusctl
  -count=1`, `go vet` for both packages, `git diff --check`, Linux and Windows
  cross-compiled test binaries, focused reservation TOCTOU/write-once/failure
  tests, and stale-claim scans all passed. Final Sol gate: ACCEPT.

CF-002 does not close CF-003 matrix coverage, CF-004 pinned-v1 environment
qualification, or CF-005 differential acceptance. SDK/WS capture, external
endpoint lifecycle proof, and real v1/v2 evidence remain separate gates.

## Fast Batch A refresh (2026-09-08, HEAD 85298d3, dirty)

Three bounded lanes were reconciled against the current dirty candidate:

- RT-003 is accepted hermetically. Depth-one fill/shed/drain/resume tests, the
  reactive race package, vet, and diff checks passed.
- DA-004 is accepted for the selected local admin permission-check contract.
  DB-backed race tests cover rollback-only transaction evaluation, runtime
  entity/attribute ordering, original-image create/update classification,
  explicit versus persisted rule provenance, fail-closed null/missing rules,
  and root/nested denied-data non-leakage. Full affected race packages and vet
  passed; final Sol boundary review returned ACCEPT.
- DA-004V is only partial. Runtime/corpus/document exclusion is enforced and an
  explicit-path `validate-release` component passes 18 scenarios/26 coverage
  rows, but QR-003 has not yet composed the complete clean-candidate release
  gate. No compatibility support claim is made for dynamic view rules.

The working tree remains dirty. No immutable candidate, commit, push,
deployment, external provider action, or release acceptance is claimed.

## CF-003 pagination repair refresh (2026-09-09, HEAD 85298d3, dirty)

The previously reproduced corpus failures in `11-query-pagination` and
`17-after-cursor` are repaired. A paginated namespace without an ID triple now
falls back to ID ordering and emits/accepts the legacy three-part ID cursor;
the fallback is applied through a copied private query option and cannot alter
explicit order behavior. The owned-PostgreSQL full corpus replay passed all 18
scenarios, focused pagination/cursor/order tests passed twice with race
detection, the full `internal/instaql` race package and vet passed, and an
independent Sol compatibility gate returned ACCEPT.

Status: `PARTIAL / PAGINATION_REPAIR_ACCEPTED_MATRIX_PENDING`. The repair does
not supply the 15 still-open CF-003 transport, lifecycle, and concurrency matrix
families and does not establish pinned-v1 parity.

The combined Batch A DB-backed race run passed `internal/reactive`,
`internal/transact`, `internal/adminapi`, and `internal/perms`. It also reproduced
the previously recorded CF-003 pagination blocker: InstaQL integration tests and
corpus scenarios 11/17 reject entities without an ID triple when constructing
the default `serverCreatedAt` cursor. That compatibility failure is not caused
by, fixed by, or waived for Batch A; CF-003 remains responsible for it.

## Batch-B admin atomicity refresh (2026-09-08, HEAD 85298d3, dirty)

DA-005 is COMPLETE / ACCEPTED_DB_ATOMICITY_REVIEWED. High-level missing
attributes are emitted as `add-attr` steps in the same transaction as their
requested data writes; a later mutation failure rolls back attrs, idents,
triples, journal state, cache visibility, and callbacks. A bounded single retry
handles concurrent attr/ident naming conflicts by reloading the winning catalog
and re-lowering the original request.

Evidence: the failure-atomicity regression passed under race three times; the
two-request same-attribute convergence test passed under race ten times; the
full DB-backed admin/sync/reactive/transact/daemon race batch, vet, and diff
check passed. Sol returned ACCEPT. No commit or external mutation was made.

## Phase-01 status reconciliation (2026-09-08T12:40:03Z)

F-001: COMPLETE / ACCEPTED_SOURCE_BACKED_LEDGER.
F-002: COMPLETE / OWNER_APPROVED_POLICY_DECISION, evidenced by
`DEC-001-single-node-alpha-20260905`, approved by Priyank at
`2026-09-05T08:35:12Z`.

Working candidate: `85298d365d744e3a5c4f7abea2f3fabb014e8177` on `main`, dirty at
capture. The tracked diff fingerprint captured before this reconciliation was
`5a98014b0910bd1ac4655723f49d2613712851598656d9b3c7c096f26f3a39ed` using
`git diff --no-ext-diff --unified=0 | shasum -a 256`. The contemporaneous
untracked-path inventory was:
`cmd/benchsmoke/mapping_test.go`, `cmd/chaos/postmaster_pid_other.go`,
`cmd/chaos/postmaster_pid_unix.go`, `cmd/chaos/process_identity_darwin.go`,
`cmd/chaos/process_identity_linux.go`, `cmd/chaos/process_identity_other.go`,
`cmd/soak/evidence.go`, `cmd/soak/evidence_test.go`, `cmd/soak/ledger.go`,
`cmd/soak/ledger_test.go`, `docs/plans/finish-up/review-fix-85298d3.md`,
`internal/benchrun/ev005_integrity_test.go`,
`internal/corpus/atomic_rename_darwin.go`,
`internal/corpus/atomic_rename_linux.go`, `internal/corpus/atomic_rename_other.go`,
`internal/corpus/fs_other.go`, `internal/corpus/fs_unix.go`,
`internal/corpus/http_other_test.go`, `internal/corpus/http_unix_test.go`,
`internal/perms/viewgate_test.go`, `internal/storageapi/atomicity_test.go`,
`internal/sync/admission_regression_test.go`, `internal/sync/sse_delivery_test.go`,
`scripts/soak-process-identity.sh`, `scripts/test-quality-soak-identity.sh`.

No immutable release candidate is selected. FR-002 remains blocked on a clean,
owner-approved candidate and independent acceptance. Packet exclusions and
deferred-row enforcement remain owned by their existing packets; this
reconciliation changes no product packet status.

## Batch-4 auth/backup repair refresh (2026-09-08, dirty working tree)

This refresh records bounded implementation progress only; it does not grant
AUTH-RUNTIME-001, provider authority, object-store authority, or release
acceptance.

- DA-006A OAuth local contract: Google OIDC and GitHub OAuth authorization-code
  flows now enforce non-empty PKCE, one-time state/code expiry and consumption,
  committed state burn before provider I/O, one shared provider deadline,
  Google nonce binding, mandatory RS256/JWK/kid/issuer/audience/expiry checks,
  and one forced JWKS refresh for an unknown kid. Provider credentials are
  captured by config and all four selected-provider values are required for a
  database-backed daemon. The direct ID-token route returns explicit 501 before
  parsing. Focused owned-PostgreSQL auth/OAuth tests passed with `-race` three
  times; config race tests passed three times; daemon assembly passed twice;
  vet and diff checks passed. Sol rejected five concrete defects, all were
  repaired, and the second review returned ACCEPT. Status: COMPLETE for the
  local contract. DA-006B real-provider acceptance remains BLOCKED and no real
  provider claim is made.

- DA-003 backup fail-closed: object PUT now stages under a private namespace,
  promotes only after export/upload success, rejects staging keys on public
  GET/PUT/restore routes, and reports successful publication as success even if
  post-promotion staging cleanup fails. Focused backup race tests, `go vet`,
  and `git diff --check` passed with integration disabled. A later owned-
  PostgreSQL race run accepted DB-backed export/restore rollback, checksum and
  truncation rejection, and staged publication. Real object-store behavior and
  the recovery campaign remain open. Status: PARTIAL /
  LOCAL_DB_ACCEPTED_EXTERNAL_PENDING.
- DA-008A magic-code local contract: catalog/schema validation precedes
  delivery; codes remain unpublished until mailer success; resend ownership is
  conditional; failed verification resets preserve `last_sent`; throttle
  lookup errors fail closed to a stable 503; and future email keys are
  normalized while legacy mixed-case codes/users remain usable. Exact TTL
  equality is expired and concurrent consumption requires exactly one winner.
  The focused DB-backed magic-code race matrix passed three times, its highest-
  risk compatibility/expiry/atomic-burn slice passed five times, and the full
  authn race package, vet, and diff check passed. Sol returned ACCEPT. Status:
  COMPLETE / ACCEPTED_DB_SECURITY_REVIEWED. DA-008B real delivery remains
  blocked and the no-mailer runtime route remains a stable non-enumerating 503.
- DA-004 admin permission fidelity: Sol review supplied a coordinator-owned
  rollback-only evaluator contract; no product code was accepted for this
  packet in this refresh. The current admin-side approximation remains
  unaccepted and must not be used as a runtime authorization oracle.

Verification for the refresh: `go vet ./internal/authn`, `go vet
./internal/backup`, `go test -race -count=1 ./internal/authn
./internal/backup ./internal/adminapi` with `INSTANT_TEST_INTEGRATION=0` and
empty `DATABASE_URL`, plus `git diff --check`. No commit, push, deployment,
provider contact, or live object-store/DB acceptance campaign was performed.

## DA-001 assembly repair refresh (2026-09-08, dirty working tree)

`cmd/instantd.mountRoutes` now validates and constructs the configured durable
disk backend before starting auth maintenance, constructing realtime handlers,
or registering routes. The invalid-root test asserts that no handlers are
returned, notifier side effects are not rebound, and the supplied mux has no
assembly routes. Focused race tests for `cmd/instantd`, `internal/config`,
`internal/storageapi`, and `internal/backup`, plus `go vet ./cmd/instantd`
and `git diff --check`, passed with integration disabled.

Status: DA-001 PARTIAL / LOCAL_REOPEN_ACCEPTED_ENV_PENDING. Local disk reopen,
namespace persistence, concurrent first upload, and root-confirmation failure
behavior passed under race detection against the owned PostgreSQL fixture.
Full daemon crash/restart durability, container mount qualification, and
selected external object-store assembly remain unproven; the current release
envelope still keeps object backup unwired and explicitly 503.

## QR-003 composed release-gate refresh (2026-09-09, HEAD 85298d3, dirty)

QR-003 is complete for its bounded implementation contract. `make test-release`
now validates an exact DEC-001 manifest and candidate identity, required packet
handoffs, native-Linux/recovery/soak records, nested artifact size and hashes,
campaign freshness, private snapshot integrity, final source stability, selected
target order, non-skipped test execution, and built-binary identity. It fails
closed on production test seams, unsafe paths or symlinks, dirty candidates,
missing evidence, wrong types, zero tests, final skips, and child failures.

The 21-case hermetic contract suite passed. The actual Make discovery boundary
also passed under inherited `GOFLAGS=-json`; release-corpus validation reported
18 scenarios and 26 rows; focused corpus/InstaQL race tests, shell syntax, and
`git diff --check` passed. Sol and Muse independently returned ACCEPT after two
Sol-found blockers were repaired. Status: `COMPLETE /
ACCEPTED_CONTRACT_GATE`.

This does not claim a release: no clean immutable candidate, completed external
evidence bundle, or FR-002 run exists. FR-002 remains blocked. The accepted gate
does invoke the enforced dynamic-view exclusion validator, so DA-004V's former
gate-pending condition is now closed as `COMPLETE /
EXCLUSION_ENFORCED_GATE_ACCEPTED` without changing the exclusion's scope.

## CF-003 assembled HTTP and QR-005 supply-chain refresh (2026-09-09)

CF-003 now has accepted local assembled-route evidence for a bounded subset of
the selected HTTP matrix. Nine hermetic denial/boundary cases and an owned-
PostgreSQL positive/denial lifecycle passed under race detection. The DB leg
proves exact admin-to-runtime state, guest refresh-token revocation and replay
denial, destructive backup restore and exact recovered query state, object-store
disabled behavior, and disk storage upload/download plus denied-delete
preservation. Sol's first review rejected two false positives; the repair made
restore destructive and corrected `SignOut` to delete the complete token entity
atomically. The new auth regression proves zero remaining token triples, sibling
token preservation, replay denial, and unknown-token idempotence. The second Sol
review returned ACCEPT. CF-003 remains `PARTIAL /
HTTP_ASSEMBLY_ACCEPTED_MATRIX_PENDING`; this is not corpus-wide closure, SSE or
multi-client evidence, real object-store acceptance, or v1 parity.

QR-005 now has an accepted offline inventory/preflight milestone. The read-only
script hashes the selected local build inputs, emits deterministic canonical
JSON, recognizes multiline Docker commands and runner arrays, and fails closed
on mutable actions/images/runners/tools, unhashed local actions, malformed or
missing inputs, and unclassified acquisitions. Its 22-case hermetic contract,
module verification, generated-file check, shell syntax, and diff checks passed;
Sol returned ACCEPT after four parser bypasses were repaired. The current tree
correctly fails preflight because authoritative action commits, image digests,
runner identity, and installed-tool content bindings are absent. Status remains
`PARTIAL / INVENTORY_PREFLIGHT_ACCEPTED_PINS_PENDING`; no digest was invented and
no workflow, image, dependency, signing, publication, or QR-003 evidence schema
was changed.

## CF-003 assembled SSE refresh and OP-005 producer audit (2026-09-09)

CF-003 now includes accepted owned-PostgreSQL evidence through the actual
production-mounted GET/POST SSE routes and notifier. The test pins complete
handshake/init/query frames, exact initial state, a transaction-driven refresh
with exact attribute identities and positive watermark, exact POST responses,
old-session rejection, distinct reconnect credentials, converged reconnect
state, and joined notifier teardown. Three consecutive race runs, vet,
formatting, diff checks, and the repaired Sol review passed. Status advances to
`PARTIAL / HTTP_SSE_ASSEMBLY_ACCEPTED_MATRIX_PENDING`; permission revocation,
multi-client fanout, delta, external-v1 parity, and the rest of CF-003 are not
claimed.

The OP-005 audit found no producer for QR-003's exact seven-outcome recovery
record. Existing chaos, sync, and soak evidence covers only fragments and uses
different report schemas. Native Linux qualification, explicit fault authority,
owned disposable fixtures, frozen recovery/drain budgets, and record-producer
ownership remain unresolved. QR-003 contract acceptance therefore remains a
validator implementation result, not recovery-campaign evidence; OP-005 and
FR-002 stay blocked.

## CF-003 SSE permission and multi-client refresh (2026-09-09)

CF-003 now also has accepted assembled-route evidence for persisted permission
allow/deny/restore and two simultaneous SSE clients sharing a query. Exact
refresh envelopes are bound to the admin transaction that triggered each
refresh. The denied frame contains an exact empty result; closing one client
invalidates only that session while the sibling receives the next exact state.
Database operations and notifier teardown are bounded.

The focused combined race test passed three consecutive runs; vet, formatting,
and diff checks passed. Sol's initial rejection was repaired and its re-review
returned ACCEPT. Status is `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_ACCEPTED_MATRIX_PENDING`. The test harness
explicitly invalidates the rule cache after direct persisted rule changes; it
does not claim an external rule-management route. Delta convergence, assembled
room/admin presence, the transaction matrix, external-v1 parity, and remaining
selected matrix rows remain open.

## CF-003 assembled transaction matrix (2026-09-09)

The mounted admin/runtime HTTP surface now has accepted owned-PostgreSQL
evidence for rollback, cardinality-one replacement, deep merge, lookup
contention convergence, and delete-by-lookup. The concurrency oracle accepts
only success or a decoded unique-constraint failure, then requires exact whole
query state so losing ID/title orphan entities cannot be hidden by a slug-only
filter. Exact winner removal is proven after deletion.

The focused race test passed ten consecutive runs. Sol rejected the first
oracle, the repair closed all three findings, and re-review returned ACCEPT.
Status is `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ACCEPTED_MATRIX_PENDING`. This is
not a deterministic database-overlap campaign and does not close delta,
assembled room/admin presence, external-v1 parity, or remaining matrix rows.

## CF-003 assembled room lifecycle (2026-09-09)

The production-mounted runtime WebSocket route now has accepted owned-
PostgreSQL evidence for two-client join, exact unsolicited presence fanout,
explicit sibling convergence, presence update, peer-only broadcast, and leave.
The sender-broadcast absence check is intentionally bounded to 100 ms after the
exact ACK and peer delivery; later absence is not claimed.

The focused race test passed ten consecutive runs. Two Sol-found lossy-frame
oracles were repaired and final review returned ACCEPT. Status is `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_ACCEPTED_MATRIX_PENDING`.
No separate admin-presence route, delta convergence, external-v1 parity, or
remaining selected rows are claimed.

## CF-003 assembled delta convergence (2026-09-09)

The mounted runtime WebSocket path now has accepted owned-PostgreSQL evidence
for old-client full refresh versus negotiated new-client structural delta at
the exact same triggering transaction. Baseline watermarks/result metadata are
equal, the trigger advances the baseline, malformed or wrong refresh frames are
observable, and the parsed wire delta applied to the captured baseline equals
the parsed full final state and exact expected entity map.

The focused race test passed ten consecutive runs. Muse rejected four oracle
gaps, all were repaired, and bounded re-review returned ACCEPT. Status is
`PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING`.
Duplicate-frame absence is bounded to 100 ms; external-v1 parity and any
remaining selected rows stay open.

## CF-001 PostgreSQL COPY acceptance (2026-09-09)

CF-001 is `COMPLETE / ACCEPTED_POSTGRES_COPY`. Owned-PostgreSQL tests prove JSON
null versus string `"null"`, deterministic last-input cardinality-one behavior,
mixed cardinality-many retention, and exact local parity with the non-COPY
insert path across encoded value, MD5, flags, and checked datatype. A unique
cardinality-many failure in COPY's second insert statement rolls back the
successful first statement and leaves only the exact preexisting triple.

The focused COPY race suite passed ten consecutive runs. Sol rejected the
first rollback fixture because it proved only statement atomicity; the repaired
cross-statement fixture received ACCEPT. No production caller or v1 parity is
claimed.

## Stabilization reconciliation (2026-09-16, old candidate 85298d3 → new candidate on commit)

The dirty candidate described across the refresh sections above (HEAD
`85298d365d744e3a5c4f7abea2f3fabb014e8177`, 98 modified + 46 untracked paths,
recorded tracked-diff fingerprint
`a74a2d54acc21e314c762737e933c574036c5e3e1f08b9c6884fc515b0482ec5` preserved as
a recorded observation that cannot be regenerated from the clean Git history)
was stabilized into focused local commits in dependency order (realtime;
storage/backup/config; auth/rate-limit; admin/permissions/transaction; evidence
tooling; corpus/matrix; release-gate/preflight; ledger reconciliation), with no
recorded reset, checkout, clean, stash, push, deployment, or discarded change.
The old-to-new commit range contains the same recorded total of 144 unique
paths, and no stabilization-time deletion, reset, checkout, clean, or stash was
recorded. Because no byte-for-byte snapshot of the original dirty tree was
preserved, exact content-level losslessness of the former untracked files is
not independently reproducible after stabilization. Packet
states are unchanged from the program manifest (canonical): COMPLETE rows stay
COMPLETE, PARTIAL/BLOCKED/NOT_SELECTED/DEFERRED rows stay as recorded; the only
ledger bookkeeping change is the DA-001-R SUPERSEDED note in
`execution-ledger.md` plus the corpus-counts update above. No external, Linux,
provider, recovery, soak-campaign, or clean-candidate evidence is claimed by
this reconciliation; the config-test ambient-`DATABASE_URL` isolation caveat is
recorded in `execution-ledger.md`. New candidate SHA and final tree state are
recorded in the execution ledger's stabilization section.

## RT-001 close-out (2026-09-17 IST, main at 6b1288601d3be9d0cb6b0af17bdee5b35fd06b07, clean tree)

RT-001 permission rebinding is `COMPLETE / ACCEPTED_BOUNDED_REBINDING`.
Ratification `DEC-001-rt001-bounded-rebinding-20260917` (successor to
`DEC-001-single-node-alpha-20260905`) records the owner-approved seven-clause
bounded exception; original approval preserved unchanged; unrelated envelope
selections unchanged. Live 6/6 RT-001 tests green with zero skips on owned
PostgreSQL 17.11 (`testkit`-isolated `instant_test_*` only); corpus replay
18/18 green including `05-permission-deny`; focused hermetic suite twice under
race, full sync/reactive/instantd race packages, vet, build, gofmt, and
diff-check all green; independent security review ACCEPT with no unresolved
blocker. RT-001a–e each green per `02-realtime-correctness.md`. RT-002 stays
PARTIAL; Phase 02 gate `REALTIME_TRUSTWORTHY` stays open. No other packet or
selection changes. Evidence detail lives in the execution ledger's RT-001
close-out section. New candidate SHA is recorded at commit time in this
session's final report.

## RT-002 close-out (2026-09-17, main at bbf5561f8d8e784b8ea4c1a2395c5581b760bac2 + work below)

RT-002 delivery/watermarks is `COMPLETE / ACCEPTED_DISCONNECT_REPLAY_DELIVERY`
under the already-approved DEC-001 explicit-disconnect/full-replay policy (no
new owner decision; RT-002c allows retry OR disconnect). RT-002a–e each green
per `02-realtime-correctness.md` (watermark semantics, render+encode no-commit,
retry-or-disconnect, exact reconnect incl. SSE/delta, no empty frame).
Evidence: focused 17 green twice with -race, sync/reactive/instantd race
packages, RT-001 live 6/6, CF-003 SSE/delta + corpus 18/18, pinned SDK 1.0.65
same-tx correction pass, reliability review ACCEPT
(`docs/plans/finish-up/rt002-reliability-review-20260917.md`). Phase 02
`REALTIME_TRUSTWORTHY / COMPLETE` (RT-001 + RT-002 + RT-003). RT-001/RT-003
remain complete; no later packet changed. Detail in the execution ledger's
RT-002 close-out section. New candidate SHA recorded at commit time in this
session's final report.

## DA-001 reconciliation (2026-09-19, baseline d097820183cce7db589037449c0c350b345c8cab + repair below)

Baseline `d097820` (main, clean) already records DA-001
`COMPLETE / ACCEPTED_DURABLE_LOCAL_STORAGE` in `program-manifest.md` with the
A/B/C restart, startup-refusal, and 503 evidence plus data-integrity and
security ACCEPT reviews. This section preserves all historical dated sections
above and reconciles the remaining verification gap only: persisted `$files`
metadata is now proven exactly unchanged across the same distinct-process
restarts (the earlier HTTP download Content-Type comparison sniffed bytes and
did not read the stored triple).

- Status: DA-001 `COMPLETE / ACCEPTED_DURABLE_LOCAL_STORAGE` (unchanged).
- Distinct processes: built `instantd` binary; daemon A, B, C share one owned
  `instant_test_*` PostgreSQL fixture and one test-owned durable root with the
  same storage secret; new loopback ports per daemon; PIDs distinct and ports
  confirmed down between restarts
  (`cmd/instantd/daemon_restart_test.go:339-590`).
- Exact blobs: object one via A, object one via B, object two via B, both via
  C — `bytes.Equal` on every leg plus on-disk size under the configured root.
- Exact six-field metadata: `path`, `id`, `size`, `content-type`,
  `location-id`, `key-version` read directly from the owned fixture via
  `platform.LoadAttrCatalog` + `storage.FetchTriples` for the file entity
  (`:354-425`); object one validated via A (`:507-511`), A→B equality
  (`:547-548`), object two validated via B (`:558-562`), A→C and B→C equality
  (`:585-588`). Label-keyed extraction (no map printing/row order) fails if
  any field is omitted; struct equality fails if any field is changed or bound
  to the wrong object.
- Startup refusal: missing/relative/uncreatable/file/missing-secret (+chmod
  unwritable when enforced) exit nonzero with diagnosable errors, no listener,
  no probe residue, no secret in output.
- Stable object-backup 503: `PUT/GET /backup/{app}/object` and
  `POST /restore-object` return `503 {"message":"no object store wired"}`,
  create no files, assemble no backend, need no S3 credentials; ordinary
  storage and local backup export still work.
- Reviews: data-integrity ACCEPT including the exact-metadata addendum
  (`docs/plans/finish-up/da001-data-integrity-review-20260918.md`); security
  ACCEPT after the Delete symlink repair and re-review
  (`docs/plans/finish-up/da001-security-review-20260918.md`).
- Selected alpha profile: durable configured local disk root REQUIRED;
  temporary production storage forbidden; object backup excluded (explicit
  503); no cloud provider, S3 deployment, or migration assembled.
- No claim is made for Linux qualification (OP-003 separate), container
  qualification or execution (OP-004 separate), container/production recovery
  (OP-006 separate), real providers, recovery campaigns, production use, or
  release acceptance. DA-003 and all later packets are unchanged. No push,
  deployment, publication, or tag.

## DA-003 reconciliation (2026-09-19, baseline 8cabdb4a7a6f06cb16d552dd41d2f798f9030a75 + repair below)

Baseline `8cabdb4` (main, clean) records DA-001
`COMPLETE / ACCEPTED_DURABLE_LOCAL_STORAGE` with DA-003
`PARTIAL / LOCAL_DB_ACCEPTED_EXTERNAL_PENDING`. This section preserves all
historical dated sections above and reconciles DA-003 only.

- Status: DA-003 `COMPLETE / ACCEPTED_LOCAL_FAIL_CLOSED_BACKUP` (local
  NDJSON export/restore accepted; checksum, truncation, authorization,
  staging, and rollback evidence passed).
- Rows: DA-003a missing-auth 500/no-panic
  (`http_test.go:110-123`, `da003_failclosed_test.go:121-155`,
  `http.go:64-69`); DA-003b unauthorized 401/no-data/no-mutation
  (`http_test.go:128-153`, `da003_failclosed_test.go:457-502`,
  `http.go:84-95`); DA-003c checksum+records contract
  (`da003_failclosed_test.go:161-227`, `import_test.go:63-135`,
  `import_boundary_test.go:20-38`,
  `da003_extra_contract_test.go:23-102`, `export.go:98-104`,
  `import.go:92-103,146`); DA-003d staging-private/publish-only-after-complete
  (`da003_failclosed_test.go:251-281,323-388,421-452`,
  `da003_extra_contract_test.go:104-176`, `http_objects.go:25-36,76-139`);
  DA-003e failed-restore source+exact-target preservation under single-tx
  atomicity (`da003_failclosed_test.go:508-597`,
  `da003_extra_contract_test.go:178-202`, `import.go:35-39,82-110`).
- Reviews: data-integrity ACCEPT
  (`docs/plans/finish-up/da003-data-integrity-review-20260919.md`);
  security ACCEPT
  (`docs/plans/finish-up/da003-security-review-20260919.md`).
- Boundary: local NDJSON export/restore is accepted; runtime object-backup
  routes remain excluded and explicitly 503
  (`PUT/GET /backup/{app}/object`, `POST /restore-object` →
  `503 {"message":"no object store wired"}` after auth passes;
  `http_objects.go:63-65,150-153`, `routes.go:224-228`,
  `daemon_object_disabled_test.go:70-94`,
  `runtime_cf003_test.go:240-244`); in-memory object-store tests prove
  internal failure semantics only; no real S3/provider behavior is claimed.
- No claim: no recovery campaign, Linux/container qualification, production
  acceptance, or release acceptance; OP-006 remains separate; DA-001 and all
  other packets unchanged; Phase 03 remains open per canonical dependencies
  (DA-006B/DA-008B BLOCKED). No push, deployment, publication, or tag.

## CF-002 reconciliation (2026-09-19, baseline c7f9a42d29a7587a99225236b54bb217f940833b + work below)

Baseline `c7f9a42` (main, clean) records CF-002
`PARTIAL / ACCEPTED_BOUNDED_CAPTURE` with caller-asserted record identities.
This section preserves all historical dated sections above and reconciles
CF-002 only.

- Status: CF-002 `COMPLETE / ACCEPTED_CANDIDATE_BOUND_LOCAL_CAPTURE`
  (local candidate/process/endpoint identity proven; fixture reset locally
  owned and proven; checksum, truncation/quiescence, authorization-adjacent
  redaction, staging-free publication evidence passed).
- Rows: (1) identity via `candidate.go:54-237` + lifecycle `:306-393`
  (git `c7f9a42…`, binary `dbfb93be…`, PIDs 76643/76956, loopback
  `:64823`/`:64909`, secrets-excluded config digest, go1.27.1
  darwin/arm64); (2) reset via `:251-270` with exact pre
  `attrs=0 triples=0` and post `attrs=2 triples=2` equality across runs on
  `instant_test_4099…`/`instant_test_2232…`; (3) HTTP+SSE raw/canonical
  retained, WS record excluded+enforced (`main.go:122-125`,
  `TestCF002WSRecordCreatesNoArtifact`); (4) masking path-scoped, payload
  id/token/timestamp/cursor/email/title significant; (5) incomplete/extra/
  error frames fail with no evidence; (6) fresh 0700/0600 write-once
  no-replace publication with checksummed manifest verification.
- Reviews: provenance ACCEPT
  (`docs/plans/finish-up/cf002-provenance-review-20260919.md`); security
  ACCEPT (`docs/plans/finish-up/cf002-security-review-20260919.md`).
- Boundary: HTTP + SSE recording accepted; WS recording explicitly
  excluded and enforced (stable `record mode is unsupported for ws`, no
  artifact); SDK and v1 capture remain unclaimed; local
  candidate/process/endpoint identity proven; fixture reset locally owned
  and proven; no remote deployment identity or external fixture equivalence
  claimed; CF-003 remains PARTIAL and unchanged; CF-004/CF-005 remain
  blocked on external evidence. No push, deployment, publication, or tag.

## CF-002 runnable-recorder note (2026-09-19, intermediate 92e3a64)

The test-only lifecycle is superseded by the supported runnable recorder
`corpusctl --mode managed-record` (`cmd/corpusctl/managed.go`). Two direct
CLI runs from clean `92e3a64` into `/private/tmp/cf002run1` and
`/private/tmp/cf002run2` record `gitSha 92e3a64`, `dirty false`, verified
binary hashes, PIDs 98966/99253 on loopback `:51496`/`:51584`, owned
fixtures `instant_test_4d64ba…`/`instant_test_acb587…`, identical reset
pre/post states, and fully validating manifests; daemons stopped and owned
databases dropped. Provenance and security re-reviews both ACCEPT with
corrective addenda acknowledging the earlier test-only scope. CF-002 stays
`COMPLETE / ACCEPTED_CANDIDATE_BOUND_LOCAL_CAPTURE`; CF-003/004/005
unchanged. No push, deployment, publication, or tag.
