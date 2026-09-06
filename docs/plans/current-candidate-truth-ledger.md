# Current-candidate truth ledger (F-001)

Packet: `F-001` in `docs/plans/finish-up/01-scope-and-decisions.md`.
Candidate: `14e2988e79851b34e340b41ebaf7ea109132b70e` on `main`, captured
`2026-09-05T05:19:05Z` (clean tree at capture) and re-fingerprinted
`2026-09-05T08:11:16Z` (dirty tree, see below).
Toolchain: `go1.27.0 darwin/arm64`.
Planning baseline `5ccea252c70e47fda970caccf6c7feb5945d3968` is an ancestor of
this candidate; its dirty tree no longer exists as uncommitted work.

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
| COPY semantics | Present, no production caller | No PG-backed acceptance | Deferred/unselected (no approved exclusion; CF-001 owns) | Not claimed | Not claimed |
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
contract only; it does not establish current-candidate acceptance. Rows marked
"deferred/unselected" above became enforceable exclusions at the DEC-001
approval (`2026-09-05T08:35:12Z`); enforcement in code, docs, corpus, and gate
is owned by their respective packets (RT-003, DA-001/004V/008A, CF-001/004/005,
OP-001/002/004, QR-002/004).

## Corpus counts (manifest is authority, verified by execution)

- `corpus/manifest.json`: 18 scenarios; 31 surfaces (22 covered, 7 gaps,
  2 unsupported); 25-row coverage matrix (9 covered, 15 gaps, 1 unsupported).
- `corpus/README.md:58-60` agrees (25 entries: 9/15/1). Zero v1 captures.
- FR-001's cited `18/22/7/2` and `9/15/1` figures match the manifest; no stale
  prose was copied.

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
(superseding `DEC-001-dev-checkpoint-20260904`); proposed exclusions in this
ledger became enforceable exclusions at that timestamp. No `AUTH-*` authority
has been granted.

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
