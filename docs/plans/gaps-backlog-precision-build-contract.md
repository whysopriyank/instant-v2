# Gaps and backlog precision build contract

Date: 2026-09-04
Repository: `instant-v2`
Planning baseline: `5ccea252c70e47fda970caccf6c7feb5945d3968` on
`codex/quality-implementation-checkpoint`, with the pre-existing dirty tree
recorded below.

## 1. Purpose and authority

This is the executable forward contract for closing the repository's verified
gaps and then, where the selected release scope requires it, its production
backlog. It converts the historical phase lists and the production-completion
roadmap into bounded packets that can be assigned one at a time or executed by a
goal-driven coordinator.

This document does not reopen work already completed by
[`implementation-pass-1.md`](implementation-pass-1.md). In particular, Q1a/Q1b,
Q2a/Q2b, A1a/A1b/A1c, the Google/GitHub provider-resolution portion of A2, Q4a,
M1, and G1 remain completed for their recorded bounded contracts. They still need
coverage through the selected end-to-end compatibility and release gates; they
are not new implementation defects.

When status prose conflicts, use this authority order:

1. Fresh source and executed evidence for the exact candidate.
2. This contract's per-packet ledger and handoff records.
3. [`implementation-pass-1.md`](implementation-pass-1.md) for the completed
   bounded pass.
4. [`corpus/README.md`](../../corpus/README.md) and `corpus/manifest.json` for
   current corpus counts and limitations.
5. [`production-completion-roadmap.md`](production-completion-roadmap.md) for
   historical rationale and deferred detail.
6. `tasks/phase-*.md`, which remain historical decomposition and must not be
   treated as current checklists without source confirmation.

The task packets below authorize no implementation by themselves. A user or
coordinator selects a packet. Commit, push, tag, publish, deployment, external
provider calls, live fault injection, and destructive cleanup require the
authority stated in the invocation; a packet never grants those permissions
implicitly.

## 2. Current baseline and evidence classes

Baseline captured at `2026-09-04 01:20:05 +0530` on
`5ccea252c70e47fda970caccf6c7feb5945d3968`. At authoring time the branch
contained these user-owned, uncommitted product changes:

```text
M  cmd/instantd/routes.go
M  internal/adminapi/adminapi.go
M  internal/adminapi/auth.go
M  internal/adminapi/users_store.go
M  internal/authn/authn.go
M  internal/platform/attrs.go
M  internal/storage/storage.go
M  internal/transact/apply_dispatch.go
M  internal/transact/apply_merge.go
M  internal/transact/dispatch_test.go
M  internal/transact/permissions.go
M  internal/transact/permissions_projection_test.go
M  internal/transact/transact_test.go
?? internal/adminapi/mutation_integrity_test.go
?? internal/adminapi/users_store_internal_test.go
?? internal/authn/attrs_cache_test.go
?? internal/transact/dispatch_internal_test.go
```

The product diff is 1,394 additions and 287 deletions across the 13 tracked
files, plus four untracked tests. This contract also adds the new plan and a
two-line user-requested index link in `docs/README.md`; those documentation
changes are contract-authoring work, not part of the product candidate. Every
executing agent must capture its own fresh revision, branch,
`git status --short`, changed-file list, compact diff summary, and capture time
before editing. Existing changes are user-owned. Do not reset, stash, overwrite,
reformat broadly, or attribute them to the new packet.

Use these evidence classes:

| Class | Meaning |
|---|---|
| `CONFIRMED_DEFECT` | Current source establishes the wrong path, and the packet must add trustworthy red evidence before repair. |
| `DIRTY_CANDIDATE` | A fix is already present but unaccepted; do not manufacture red by deleting it. Validate intent, reconstruct baseline only in an isolated copy/overlay when useful, and prove the treatment. |
| `MISSING_EVIDENCE` | Implementation may exist, but the required real path or environment has not been exercised. |
| `POLICY_DECISION` | Correct behavior depends on a release/support decision. No implementation agent may invent it. |
| `CONDITIONAL_DEFECT` | Defective code exists outside the selected production path. Repair it before enabling that path, or explicitly exclude the path. |
| `CONFIRMED_ASSEMBLY_GAP` | An implementation exists in a package but the daemon/workflow does not compose it into the advertised path. |
| `HISTORICAL_ONLY` | Prior evidence is informative but does not accept the current candidate. |

The source index was available but stale or deliberately excluded for several
changed files, `cmd/instantd`, and `docs`. Agents must prefer graph discovery,
check index coverage for every cited/operated path, and read current source when
coverage reports `metadata_changed`, `not_tracked`, `partial`, or `excluded`.

## 3. How to run this contract

### 3.1 One packet at a time

The safest default is one active packet and one exact goal:

```text
Execute only packet <ID> from
docs/plans/gaps-backlog-precision-build-contract.md using the repository's
precision-contract workflow. Use the packet's exact Goal objective. Preserve
the baseline dirty tree, do not start successor packets, and return the required
evidence handoff. Do not commit, push, deploy, contact external systems, or run
destructive/live-fault operations unless this invocation explicitly authorizes
them.
```

If a goal tool is available and the invocation asks for goal tracking:

1. Inspect the current goal first; do not replace an unfinished unrelated goal.
2. Create one goal whose objective is the packet's exact **Goal objective**.
   Do not set a token budget unless the user supplied one.
3. Work only that packet. A goal does not widen file, environment, or mutation
   authority.
4. Mark it complete only when every required row is `GREEN`, all required
   verification was executed and inspected, and no required work remains.
5. Use blocked status only under the goal tool's own blocked threshold. The
   packet handoff may report `BLOCKED` immediately when a required decision or
   external prerequisite is absent.
6. Stop after the packet handoff. The next packet gets a new goal only after the
   prior goal is actually complete.

### 3.2 Sequential goal loop

A coordinator may run the queue automatically, subject to authority:

```text
Execute the applicable packets from
docs/plans/gaps-backlog-precision-build-contract.md in dependency order. Create
one goal per packet using its exact Goal objective. Never keep two active goals,
never mark a partial packet complete, and never silently skip a required packet.
After each packet, perform its skeptical review and produce its handoff before
selecting the next runnable packet. Stop for a required owner decision, missing
external authority/prerequisite, destructive action, live fault, provider call,
performance campaign, publish, or deployment. Conditional packets may be closed
as ACCEPTED_EXCEPTION only when DEC-001 contains an explicit exclusion and the
public docs/gates enforce it.
```

The loop does not run every historical idea. It runs only packets selected by
DEC-001 plus unconditional correctness and evidence-integrity packets.

### 3.3 Per-packet execution loop

Every implementation packet follows this loop:

1. **Baseline:** capture revision, branch, status, diff names/stat, toolchain,
   and relevant prerequisite availability.
2. **Discover:** trace the real entry point through production code; check graph
   coverage and fall back to current source where required.
3. **Ledger:** copy the packet rows into a live ledger with status `PENDING`.
   Preserve every qualifier and split newly discovered independent invariants.
4. **Red:** add or identify the smallest authoritative desired-behavior test and
   run it before implementation. It must fail at the intended assertion. For a
   `DIRTY_CANDIDATE`, record why new red execution is inappropriate and use
   historical/isolated baseline evidence only when safe.
5. **Repair:** make the smallest owning change. Do not broaden support or build a
   generalized framework.
6. **Focused green:** run the exact test; for concurrency, authorization,
   persistence, recovery, and destructive-path safety, reset the fixture and run
   the focused check once more after the first green.
7. **Verification rings:** run package, formatting, lint/static, build, and
   proportional integration checks. Record exact commands, selected/executed
   counts where available, exit status, and skipped prerequisites.
8. **Skeptical review:** try to falsify every row from the diff and raw evidence.
   High-risk packets require a fresh read-only reviewer; security-sensitive
   packets require a security reviewer. Repair at most two review cycles.
9. **Handoff:** return `COMPLETE`, `PARTIAL`, or `BLOCKED` using §8. Do not call a
   later stage ready merely because a default suite passed.

After two focused implementation failures for the same cause, stop that row and
escalate Luna/implementation-worker to Terra/debugger. Escalate unresolved
architecture, authorization, persistence-format, migration, distributed
concurrency, public compatibility, data-integrity, or irreversible decisions to
Sol/architect or security review. Routine implementation returns to the owning
worker after the decision.

### 3.4 Verification command rules

Commands in a packet are templates, not permission to use an arbitrary database
or external target. Agents must record their resolved command and exit status.

```sh
# Hermetic/race lane. Live suites must remain disabled and both DSNs cleared.
INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= \
  go test ./PATH... -race -count=1 -short

# Repository hermetic regression after package checks.
make test-unit

# Static/build rings when the relevant source changed.
gofmt -w <only-agent-owned-changed-go-files>
golangci-lint run ./PATH...
go vet ./PATH...
go build ./PATH...
git diff --check

# Live package lane only after validating an owned isolated PostgreSQL DSN.
make require-integration
INSTANT_TEST_INTEGRATION=1 DATABASE_URL="$OWNED_DATABASE_URL" \
  go test ./PATH... -race -count=1

# Integrated database/corpus lanes use the same verified owned cluster.
DATABASE_URL="$OWNED_DATABASE_URL" make test-integration
DATABASE_URL="$OWNED_DATABASE_URL" make test-contract
```

Never export or echo a DSN containing credentials into evidence. Before a live
lane, prove the server/role is the packet's disposable target, can create isolated
test databases where required, uses logical WAL where required, and is not the
default developer database. Testkit-created databases/slots must be inventoried
afterward and exact owned resources cleaned by their documented lifecycle. A
failed prerequisite is `BLOCKED`/not-run, never a skipped pass.

For a filtered Go test, enumerate it first with `go test <pkg> -list '<regex>'`,
then run the filter and record nonzero selected execution. For a proposed red
test, capture the intended assertion text and nonzero exit before repair. Broad
package success cannot replace the packet's focused assertion.

## 4. Release profiles and necessity rules

DEC-001 must select one profile or write an equally explicit replacement.

| Profile | Required envelope | Packets added beyond unconditional work |
|---|---|---|
| Development checkpoint | Hermetic build/tests and selected package integrations; no production or parity claim | CP-001..003 and only explicitly requested functional packets |
| Single-node alpha | One `instantd`, primary PostgreSQL only, explicitly named SDKs/routes/providers/storage, no comparative performance claim | Required supported P-* packets; C-001..005; O-003; O-005 core; D-001; REL-001..003 |
| Single-node production | Single-node alpha plus durable storage, backup/restore drill, declared capacity/recovery budgets, immutable artifacts | All applicable single-node packets plus B-001, PERF-001 if performance is claimed, and release packets |
| Multi-node production | Multiple daemons sharing PostgreSQL, with explicit room and invalidation semantics | Single-node production plus O-001 and any cross-node room packet selected by P-008 |
| Read-replica production | Queries/refreshes may use a lagging replica | Selected production profile plus O-002 |
| Full frozen-v1 surface | No silent feature exclusion across the frozen protocol and selected SDKs | Implement rather than exclude every P-005/P-007/P-008/provider/capture matrix row; C-005 must account for all accepted differences |

Unconditional before a production claim: CP-001..003, DEC-001, P-001, P-004,
C-001, C-002, D-001, REL-001, and REL-003. T-001/T-002 and B-001 are required
before their respective tools can participate in acceptance. They may instead be
enforceably excluded by DEC-001/REL-001; an unsafe/untrustworthy tool may never be
invoked or counted merely because it remains in the tree.

Conditional work may be excluded only when all of these are true:

- DEC-001 names the exact unsupported route/op/provider/topology/configuration.
- The implementation fails clearly or leaves the path inaccessible; it does not
  issue a false success acknowledgement.
- Corpus/support docs and release gates agree with the exclusion.
- The exclusion does not contradict the chosen compatibility claim.

File-size cleanup, a second root-directory redesign, generic helper frameworks,
pprof-led tuning without a failed budget, restoration of duplicate encoders, and
scenario-count inflation without matrix coverage are not release tasks.

## 5. Dependency and ownership plan

```text
CP-001 transaction candidate ─┐
CP-002 auth/cache candidate ──┼─> CP-003 admin/runtime candidate ─> DEC-001
                              │
                              ├─> P-001..P-011 bounded product packets
                              ├─> C-001/C-002, T-001/T-002, B-001
                              │
product + comparator integrity ─> C-003/C-004 ─> C-005
stable product + owned external orchestration ─> O-005 core recovery
T-001 + T-002 + O-005 core ─────> O-005 destructive-chaos lane
DEC-001 + stable product ───────> O-001/O-002/O-003/O-004 as selected
B-001 + C-005 + O-003/O-004/O-005 ─> PERF-001 when selected
all selected GREEN/excluded ────> D-001 ─> REL-001/REL-002 ─> REL-003
REL-003 + explicit deployment authority ─> REL-004
```

Never run concurrent writers over the same package. Relevant leases:

| Lease | Exclusive paths |
|---|---|
| transaction candidate | `internal/transact/**`, `internal/storage/storage.go` |
| auth/cache candidate | `internal/authn/**`, `internal/platform/attrs.go` |
| admin/runtime candidate | `internal/adminapi/**`, `cmd/instantd/routes.go` |
| reactive | `internal/reactive/**` |
| query | `internal/instaql/**`; `internal/datalog/**` only by explicit transfer |
| COPY | `internal/storage/copy.go` and its storage tests, after CP-001 releases storage |
| sync/presence | `internal/sync/**`; admin projection requires a coordinator-owned interface handoff before `internal/adminapi/**` changes |
| corpus | `internal/corpus/**`, `cmd/corpusctl/**`, `corpus/**` |
| chaos | `cmd/chaos/**` |
| benchmark | `internal/benchharness/**`, `internal/benchrun/**`, benchmark commands and `benchmarks/**` |
| operations | `internal/bus/**`, `cmd/instantd/**`, `internal/config/**`, `Dockerfile`, `scripts/**`; split further before concurrent work |
| provider acceptance | auth/OIDC preflight plus gitignored raw artifacts under `tmp/provider-acceptance/**`; no concurrent auth writer |
| coordinator | Makefile, `.github/**`, protocol/schema/generated artifacts, docs, migrations, shared interfaces |

Recommended dispatch uses the smallest fitting role:

| Packets | Primary role | Mandatory independent role |
|---|---|---|
| CP-001, CP-002, CP-003, P-004, P-005, P-006, C-001, C-002, T-001, T-002, B-001 | `trial_luna_worker` or `trial_luna_deep_worker` for a bounded implementation | `trial_luna_reviewer`; use `trial_sol_security` for CP-001/003, P-001/002/003/009/010 and T-001; use `trial_sol_gate` for P-004 concurrency semantics |
| P-001, P-002, P-003, P-009 | `trial_luna_worker` after DEC-001 and coordinator lease assignment | mandatory `trial_sol_security` before acceptance |
| DEC-001, O-001, O-002, migration/rollback decisions | `trial_sol_architect` for the contract/decision; routine implementation returns to Luna | `trial_sol_gate` when public compatibility, distributed consistency, or rollback is accepted |
| C-003, C-004, C-005, O-003, O-004, O-005, PERF-001 | `trial_luna_test_worker` for bounded evidence; `trial_terra_debugger` when failures have several owners | `trial_sol_gate` for claim-bearing final evidence |
| P-007, P-008, P-010 and cross-component integration | Luna for disjoint pieces; `trial_terra_integrator` for interface reconciliation | security or Sol gate according to the affected boundary |
| P-011 | `trial_luna_test_worker` owns provider preflight and redacted evidence only after explicit external authority | mandatory `trial_sol_security`; `trial_sol_gate` accepts claim-bearing provider evidence |
| D-001, REL-001, REL-002, REL-003, REL-004 | coordinator owns shared files/external decisions; delegate bounded inspections only | fresh release/security gate for REL-003 |

Do not spawn reviewers at the beginning. Review the concrete ledger, diff, tests,
and raw outcomes after the implementation evidence exists. Subagents may not
commit, push, publish, deploy, contact providers, or perform destructive actions.

## 6. Master packet ledger

| ID | Problem / invariant | Class now | Depends on | Necessity |
|---|---|---|---|---|
| CP-001 | Validate dirty transaction ordering and permission-state projection | `DIRTY_CANDIDATE` | — | First |
| CP-002 | Validate dirty auth attribute-cache generation changes | `DIRTY_CANDIDATE` | — | First |
| CP-003 | Validate dirty admin mutation atomicity, journaling, and invalidation wiring | `DIRTY_CANDIDATE` | CP-001, CP-002 | First |
| DEC-001 | Freeze supported release envelope, exclusions, and budgets | `POLICY_DECISION` | CP status known | Before scope-dependent work |
| P-001 | Runtime auth bypasses persisted `$users.create` rules | `CONFIRMED_DEFECT` | CP-002, CP-003 | Production auth |
| P-002 | OAuth handlers are not mounted at `/runtime/oauth/*` | `CONFIRMED_DEFECT` | CP-002, CP-003, DEC-001 | If OAuth supported |
| P-003 | Magic-code endpoint has no runtime delivery adapter | `POLICY_DECISION` + missing assembly | CP-002, DEC-001 | If email auth supported |
| P-004 | Reactive refresh failure immediately requeues and has no bounded retry lifecycle | `CONFIRMED_DEFECT` | CP stabilization | Production realtime |
| P-005 | `serverCreatedAt` order is accepted but silently orders by entity ID | `CONDITIONAL_DEFECT` | DEC-001 | Implement or reject explicitly |
| P-006 | COPY changes nil semantics and has no deterministic cardinality-one winner | `CONDITIONAL_DEFECT` | CP-001, DEC-001 | Before COPY exposure |
| P-007 | Admin presence returns a false-success empty object | `CONDITIONAL_DEFECT` | DEC-001 | If admin presence supported |
| P-008 | Sync/stream operations return success acknowledgements without semantics | `CONDITIONAL_DEFECT` | DEC-001 | Implement or explicit unsupported errors |
| P-009 | Rate-limit bucket-cap behavior deliberately fails open | `POLICY_DECISION` | DEC-001 | Production policy |
| P-010 | Runtime file storage is temporary and object backup store is not assembled | `CONFIRMED_ASSEMBLY_GAP` | CP-003, DEC-001 | If storage/objects supported |
| P-011 | Supported OAuth/OIDC providers lack real deployment/nonce lifecycle acceptance | `MISSING_EVIDENCE` + policy | P-002, DEC-001 | If provider auth supported |
| C-001 | Scenario validation loses the canonicalizer's exact numeric domain | `CONFIRMED_DEFECT` | CP stabilization | Comparator integrity |
| C-002 | Recursive volatile-key masking and no final quiescence window can hide divergence | `CONFIRMED_DEFECT` | CP stabilization | Comparator integrity |
| C-003 | No owned HTTP/SSE capture/replay adapters | `MISSING_EVIDENCE` | DEC-001, supported product paths | Compatibility claim |
| C-004 | Coverage matrix lacks auth/perms/query/refresh/rooms/tx and transport breadth | `MISSING_EVIDENCE` | P/C fixes, DEC-001 | Compatibility claim |
| C-005 | No equivalent-fixture pinned-v1 run or accepted-difference ledger | `MISSING_EVIDENCE` | C-001..004 | Frozen-v1 claim |
| T-001 | Chaos cleanup accepts arbitrary `--pg-data` for recursive deletion | `CONFIRMED_DEFECT` | CP stabilization | Before any chaos run |
| T-002 | Chaos can test HEAD fallback and swallow corpus replay failure | `CONFIRMED_DEFECT` | T-001 | Before any chaos result |
| B-001 | Triad evidence omits pair validation; report serialization errors are ignored | `CONFIRMED_DEFECT` | CP stabilization | Before benchmark acceptance |
| O-001 | Listener reconnects but publisher connection is not recovered | `CONDITIONAL_DEFECT` | DEC-001, stable product | Multi-node only |
| O-002 | Replica reads have no proven commit-visibility barrier before watermark advancement | `MISSING_EVIDENCE` | DEC-001, stable product | Read-replica only |
| O-003 | Native Linux/container DB/TLS/signal behavior is not accepted for current candidate | `MISSING_EVIDENCE` | stable product | Shipped targets |
| O-004 | Current candidate lacks approved-duration/capacity resource evidence | `MISSING_EVIDENCE` | DEC-001, O-003 | Production |
| O-005 | Crash, PostgreSQL bounce, drain, and backup recovery are not accepted end to end | `MISSING_EVIDENCE` | O-003; T-001/T-002 only for destructive-chaos lane | Alpha core; full production lane |
| PERF-001 | No policy-frozen, qualified comparative performance bundle exists | `MISSING_EVIDENCE` | B-001, C-005, O-003..005 | Only for performance gate/claim |
| D-001 | Status, self-host, upgrade, and support documents contradict source/evidence | `CONFIRMED_DEFECT` | Selected packets resolved | Before release gate |
| REL-001 | No candidate gate composes the exact selected acceptance contract | `MISSING_EVIDENCE` | D-001, selected packets | Production/release |
| REL-002 | GoReleaser config exists, but tag-triggered publish/sign/SBOM workflow is absent | `CONFIRMED_ASSEMBLY_GAP` | DEC-001, stable candidate | If publishing |
| REL-003 | No clean immutable-candidate independent acceptance has run | `MISSING_EVIDENCE` | REL-001, selected packets | Release |
| REL-004 | Canary, observation, and rollback drill have not run | `MISSING_EVIDENCE` | REL-003 | Deployment only |

## 7. Detailed task packets

### CP-001 — Transaction and permission candidate stabilization

**Goal objective:** `Validate and complete CP-001 transaction ordering and permission-state projection without losing or broadening the pre-existing dirty changes.`

**Problem and real path:** The dirty candidate changes transaction execution from
operation-grouped order to contiguous payload order in
`internal/transact/apply_dispatch.go`, and replaces repeated storage projection
with an in-memory sequential permission model in
`internal/transact/permissions.go`. It also changes JSON-number projection,
cardinality-many membership/retraction, deterministic deep-merge base selection,
and adds `storage.ValueMD5sTx`. These are authorization and data-integrity changes.

**Contract rows:**

- `CP-001a`: storage mutations occur in observable request order; a later step
  sees earlier successful steps, and a later failure rolls the entire transaction
  back.
- `CP-001b`: each permission decision receives exact pre-step `data` and post-step
  `newData`, including create/update/delete classification.
- `CP-001c`: cardinality-one and cardinality-many add, duplicate add, exact retract,
  retract miss, deep merge, numeric spellings, and delete-entity keep in-memory
  projection identical to committed storage semantics.
- `CP-001d`: fetch/fingerprint/CEL failures fail closed and cause no journal,
  triples, notification, or partial mutation.

**Planned evidence:** Treat the existing tests in
`dispatch_test.go`, `dispatch_internal_test.go`,
`permissions_projection_test.go`, and the `TestPermissionSecurity*` and atomicity
tests in `transact_test.go` as candidate evidence, not automatic proof. Enumerate
them before filtered execution. Add only missing tests that exercise the actual
`Apply` transaction and storage path. Use deterministic values whose request
order differs from operation grouping, and compare exact stored triples plus
permission bindings after each forced interleaving.

**Scope:** `internal/transact/**` and the already-dirty
`internal/storage/storage.go`; no `internal/storage/copy.go`, query changes,
permission-language redesign, new numeric domain, or performance optimization.

**Verification:** focused named tests twice against a fresh owned PostgreSQL
fixture; `go test ./internal/transact ./internal/storage -race -count=1` in both
hermetic and explicitly enabled integration modes; `go vet` and repository lint
for these packages; affected corpus transaction slices. A fresh security/data-
integrity reviewer must challenge reversed assertions, retract misses,
cardinality-many ordering, float conversion, and rollback.

**Complete when:** all four rows are `GREEN`, the final diff is separated from
unrelated baseline files, live exact state is inspected, and the reviewer finds
no blocking authorization/data-integrity defect. Do not commit unless explicitly
authorized.

### CP-002 — Auth attribute-cache candidate stabilization

**Goal objective:** `Validate and complete CP-002 auth attribute-cache invalidation without overwriting the pre-existing auth and platform changes.`

**Problem and real path:** The dirty candidate adds generation-aware caching to
`authn.Service.attrs`, cache invalidation, and reverse-ident lookup support in
`internal/platform/attrs.go`. It must prevent an old in-flight load from
publishing stale system attribute IDs after invalidation.

**Contract rows:**

- `CP-002a`: invalidation advances the per-app generation and an older load cannot
  publish or return stale attributes as current.
- `CP-002b`: apps are isolated, cache hits avoid reloads, and concurrent loads do
  not expose partially populated attribute sets.
- `CP-002c`: missing/malformed auth attributes fail with the existing error
  contract and never poison the cache.
- `CP-002d`: admin/runtime consumers share the service without mutating its
  dependencies or cache state per request.

**Evidence and scope:** Inspect `attrs_cache_test.go` for deterministic barriers;
do not accept sleeps or scheduler luck. Add a controlled query seam only if the
production `platform.Queryer` boundary cannot force the ordering. Own only
`internal/authn/**` and the dirty `internal/platform/attrs.go`; OAuth lifecycle,
mailer implementation, provider policy, and unrelated catalog refactors are
non-goals.

**Verification:** exact cache tests twice under race, complete authn/platform
hermetic packages, explicitly enabled owned-DB authn/platform tests, vet/lint,
then an independent concurrency review. Confirm final status against the captured
dirty baseline.

### CP-003 — Admin mutation and runtime reuse stabilization

**Goal objective:** `Validate and complete CP-003 admin user mutation atomicity, journaling, invalidation, and shared auth-service reuse.`

**Problem and real path:** The dirty admin candidate introduces shared
`authn.Service` reuse, transaction-scoped user/token mutation, narrow unique-
violation handling, transaction journaling, and post-commit invalidation. It also
changes `cmd/instantd/routes.go` to inject the shared auth service.

**Contract rows:**

- `CP-003a`: create-or-find under the same email is atomic; concurrent requests
  produce one user identity and valid token outcomes without swallowing unrelated
  unique violations.
- `CP-003b`: mutation failure rolls back user, custom attributes, token, journal,
  and notification together.
- `CP-003c`: successful user/custom-field/delete mutations record one correct
  transaction and notify exact affected attributes only after commit.
- `CP-003d`: existing-user token minting does not claim attribute changes, and
  request handlers reuse rather than reconstruct the configured auth service.
- `CP-003e`: deterministic failure injection is test-only and does not add an
  exported production callback or runtime behavior that an ordinary caller can
  trigger.

**Evidence:** Drive real admin HTTP handlers with an owned DB and notifier spy;
compare exact rows, tx ID, attr IDs, changes, response identity, and absence of
callbacks on rollback. Use a barrier for same-email concurrency. Existing
`mutation_integrity_test.go` and `users_store_internal_test.go` are proposed
candidate evidence and must be reviewed for exact real-path coverage.

**Scope:** dirty `internal/adminapi/**` plus `cmd/instantd/routes.go`; required
interfaces from CP-001/CP-002 may be consumed but not redesigned. No OAuth route,
mailer, presence, storage, or broad route work.

`Handler.FaultBeforeTokenCommit` is currently an exported production field. The
skeptical review must require an existing dependency seam, a package-private test
seam with no production-binary behavior, or a written justification accepted by
the security reviewer; convenience alone is insufficient.

**Verification:** focused tests twice under race, full adminapi/cmd/instantd and
adjacent auth/platform/transact/storage packages, owned-DB integration, vet/lint,
and fresh authorization/data-integrity review.

### DEC-001 — Supported-release envelope and policy freeze

**Goal objective:** `Produce DEC-001, an explicit owner-approved support and acceptance envelope that determines which conditional packets must be implemented or excluded.`

This is a decision packet, not a coding task. Inventory the actual route/op/config
surface and propose a smallest useful envelope. Record:

- target OS/architectures and container/native distribution;
- single-node versus multi-node, primary-only versus read replica;
- exact frozen SDK names/versions and protocol operations;
- HTTP, WS, SSE, admin, auth, storage, backup, presence, room, sync, and stream
  support;
- Google/GitHub/Apple/custom OIDC and magic-code delivery support;
- COPY exposure, local disk/S3/object backup, and migration/rollback mode;
- security policies: rate-limit bucket-cap behavior, origins, secrets, failure
  envelopes, and unsupported-path behavior;
- corpus/v1 compatibility claim, accepted differences, and whether comparative
  performance is a release gate;
- capacity, latency, error, memory, drain, recovery, RPO/RTO, and target hardware
  budgets.

The durable output is `docs/reference/release-envelope.md`; no later packet may
consume a chat-only decision. Create it from this inventory with:

- a unique decision ID, status (`PROPOSED`, `APPROVED`, or `SUPERSEDED`), UTC
  timestamp, candidate/ref scope, named approving owner, and approval evidence
  reference;
- one row for every packet with `REQUIRED`, `EXCLUDED`, or `DEFERRED`, the exact
  supported/excluded surface, rationale, dependency, and enforcement/evidence
  reference;
- the selected profile and every support, security, compatibility, capacity,
  recovery, RPO/RTO, and performance field above using explicit values—never
  `TBD` in an `APPROVED` artifact;
- an accepted-difference registry with stable IDs and an external-authority
  registry for provider, live-fault, performance, publish, and deploy work;
- change control: a new decision ID and owner approval whenever a required or
  excluded surface, budget, or authority changes.

The owner must approve consequential choices. An agent may recommend defaults but
must not self-approve them. `PROPOSED` or missing fields leave dependent packets
`BLOCKED`; only `APPROVED` drives the sequential loop. An exclusion must be
enforceable in source and docs, not merely prose. No implementation, measurement,
or release action belongs here.

### P-001 — Runtime signup-rule enforcement

**Goal objective:** `Fix P-001 so runtime magic-code and guest user creation enforce the persisted $users.create rule through the assembled daemon.`

**Confirmed path:** `cmd/instantd.mountRoutes` builds `authn.Service` without
`RulesForFn`; `authn.checkCreatePerm` treats nil as allow. WS/runtime transaction
paths already use `CatalogCache.RuleDocFor`, so the smallest likely repair is
assembly wiring, not a new permission engine.

**Red evidence:** Add a daemon-route integration test with persisted `$users`
create deny rules. Drive both guest sign-in and magic-code verification through
the actual mounted HTTP routes. For a denied new signup, both routes must return
HTTP 403 with the client-safe JSON envelope
`{"message":"signup is not allowed"}`; guest sign-in's current generic 500 is a
failure. A rules-load/evaluation failure must return HTTP 503 with
`{"message":"authentication is temporarily unavailable"}`. Assert no user or
token triples are created, a pre-existing magic code is not consumed, no journal/
notification occurs, and no rule/provider/internal error details escape. Add existing-user, allow, and
no-rules controls. If frozen v1 evidence requires different public wording,
DEC-001 must record that exact accepted difference before implementation; status
and no-side-effect semantics remain mandatory.

**Scope:** `cmd/instantd` assembly/tests and only the smallest authn test change if
needed. Do not redesign CEL, change default-open when no rules exist, or modify
admin bypass. Requires fresh security review and two clean focused live reruns.

### P-002 — OAuth route assembly and supported providers

**Goal objective:** `Fix P-002 so every OAuth route selected by DEC-001 is reachable through instantd and preserves the validated OAuth lifecycle.`

**Confirmed path:** `authn.Handler.ServeHTTP` handles
`/runtime/oauth/{start,callback,token,id_token}`, while `mountRoutes` registers it
only under `POST /runtime/auth/`. Freeze exact methods and paths against the
protocol/client before editing.

**Red evidence:** Through the real mux, test every supported route and method,
including start redirect/cookie, callback, code exchange, id-token path, invalid
state, expiry, replay, bad binding, provider error, and wrong method. Assert zero
provider calls for pre-exchange failures and exact persisted state/token effects.
Use local deterministic provider/JWKS servers; real Google/GitHub sandbox checks
are separate runtime evidence. Apple must remain clearly unsupported unless
DEC-001 explicitly selects its reviewed ID-token/exchange contract.

**Scope:** route assembly and authn HTTP tests; provider internals only for a
reproduced provider defect. No new provider catalog or auth framework. Security
review is mandatory.

### P-003 — Magic-code delivery contract

**Goal objective:** `Implement or explicitly exclude P-003 magic-code delivery according to DEC-001, with failure-safe persistence and non-enumerating HTTP behavior.`

The current daemon leaves `Service.Mailer` nil; the service stores a code, logs a
no-mailer notice, and reports success without delivery. DEC-001 must choose a
supported delivery adapter/configuration or make the feature unavailable.

For implementation, test configured delivery, unavailable/misconfigured startup,
provider timeout/error, resend throttling, expiry, retry policy, log redaction,
and whether a delivery failure burns or preserves the stored code. The owner must
decide that last persistence semantic before code. Drive the real route with an
injected deterministic mail server; do not call a real provider in tests or expose
codes in logs. Scope stays in authn, configuration, and daemon assembly. A general
notification platform is a non-goal.

### P-004 — Reactive retry, fairness, and cancellation

**Goal objective:** `Fix P-004 so failed reactive refreshes retry with bounded policy, preserve latest invalidation state, permit healthy progress, and terminate promptly on cancellation.`

**Confirmed path:** `Notifier.Run` drains while work exists; `refreshOne` logs and
immediately re-enqueues the same transaction after any refresh error. A persistent
error can hot-loop and starve useful work; cancellation behavior inside the drain
loop is not an accepted contract.

**Required policy:** one pending retry per subscription; exponential delays of
100 ms, 200 ms, 400 ms, 800 ms, 1.6 s, 3.2 s, then a 5 s cap; success resets the
sequence; a newer transaction replaces the pending watermark without adding a
second timer; unknown change knowledge remains sticky. Production jitter is a
stable per-subscription ±20 percent derived from non-secret subscription identity;
the injected test policy uses zero jitter. Each scheduler pass may execute at
most one due refresh per subscription before yielding, so a broken subscription
cannot monopolize the pass. Retries continue at the capped interval until
success, unsubscribe, or parent cancellation—there is no silent attempt drop.

**Red evidence:** Introduce the smallest package-private clock/timer interface and
barriers needed to advance time without sleeps. Prove attempt counts immediately
before and at every deadline; reset/cap semantics; newest tx wins; sticky unknown
knowledge; healthy-subscription progress; unsubscribe removes the pending timer;
and cancellation while refresh is running or while a timer is pending joins
`Run` within 500 ms of wall time without another attempt. Recovery emits the
exact latest result once and advances its watermark only after successful
materialization.

Own `internal/reactive/**`; do not build a generic scheduler. Verify focused tests
twice under race, full reactive/incremental oracle tests, affected corpus refresh
flows, and mandatory fresh Sol-level concurrency review.

### P-005 — `serverCreatedAt` ordering

**Goal objective:** `Resolve P-005 by implementing exact serverCreatedAt ordering or rejecting it explicitly everywhere the selected compatibility contract requires.`

Current coercion documents `serverCreatedAt` as triple `created_at` order, but
`applyOrder` accepts it and sorts by entity ID. This is a false-success path.

First use v1/client evidence to freeze meaning, ties, direction, null/missing
behavior, cursor tuple, and stability after updates/deletes. If supported, add
three or more entities whose IDs oppose creation order, paginate across a tie and
removed cursor, and compare concatenated pages to the full ordered result through
the actual query executor. If excluded, coercion and every public transport must
return the approved explicit unsupported error and docs/corpus must say so. Do not
silently retain entity-ID behavior, add a new public cursor format, or broaden
arbitrary-precision sorting.

### P-006 — COPY parity or exclusion

**Goal objective:** `Resolve P-006 by making CopyTriples semantically identical to regular storage for its declared domain or enforcing that COPY is not a production path.`

`CopyTriples` converts nil into the string `"null"` before JSON encoding and its
cardinality-one `DISTINCT ON` has no input ordinal tie-break. Only test callers
were identified at planning time.

Red integration must compare regular versus COPY in fresh fixtures for JSON null
versus string `"null"`, duplicate cardinality-one values in both input orders,
cardinality-many duplicates, exact numbers, refs, unique/index errors, batch
boundaries, journal behavior, and rollback. Assert exact stored value/MD5/flags
and deterministic last-winner semantics. Introduce an explicit ordinal or
equivalent deterministic winner; do not rely on staging row order. No production
adoption, bulk-loader redesign, or wider numeric domain belongs here. If excluded,
add an enforceable caller/build/config guard plus truthful docs.

### P-007 — Admin presence projection

**Goal objective:** `Resolve P-007 so the admin presence endpoint returns authorized real room state or an explicit unsupported response, never a successful empty placeholder.`

Current `handlePresence` always returns `{}`. DEC-001 must choose support. For
implementation, define a narrow read-only presence interface in the coordinator,
then wire the existing `RoomHub` without creating a second state store or importing
sync internals directly into admin policy. Test two rooms/apps, multiple sessions,
join/update/leave/disconnect, app isolation, token denial, concurrent snapshot,
and exact response shape through the HTTP route. For exclusion, return the agreed
status/envelope and update corpus/docs. Cross-node presence is separate unless the
release envelope includes it.

### P-008 — Sync/stream placeholder operations

**Goal objective:** `Resolve P-008 so each selected sync/stream operation has real semantics and lifecycle evidence, while every excluded operation fails explicitly instead of returning a false success acknowledgement.`

`Manager.Handle` currently returns `<op>-ok` for server-broadcast, sync-table, and
stream operations without performing the operation. Build an operation matrix
from the frozen protocol and selected SDKs. DEC-001 must classify these fixed
children independently:

| Child | Goal objective | Exclusive state owner |
|---|---|---|
| `P-008-BROADCAST` | `Resolve P-008-BROADCAST with real authorized server-broadcast lifecycle semantics or an enforced unsupported response.` | broadcast/session owner in `internal/sync/**` |
| `P-008-SYNC-TABLE` | `Resolve P-008-SYNC-TABLE with real sync-table lifecycle semantics or an enforced unsupported response.` | sync-table owner in `internal/sync/**` |
| `P-008-STREAM` | `Resolve P-008-STREAM with real stream start/write/subscribe/cleanup semantics or an enforced unsupported response.` | stream owner in `internal/sync/**` |

Each selected child gets its own goal, ledger, tests, review, and handoff using
the parent packet rules. Run the children serially because they share
`internal/sync/**`; an explicit coordinator interface transfer is required for
any other package. The parent is `GREEN` only when every child is `GREEN` or is
an `ACCEPTED_EXCEPTION` backed by the approved DEC-001 row and enforced protocol/
docs behavior. One green child never rolls up the parent.

Each supported operation needs init/auth, create/start, update/append, subscribe,
unsubscribe/remove, reconnect/resync, ordering, authorization, limits, error,
cleanup, and old-SDK behavior through a real session transport. Each excluded op
must fail with the approved compatibility behavior and no state mutation. A mere
ACK test is not evidence.

### P-009 — Rate-limit bucket-cap policy

**Goal objective:** `Resolve P-009 by obtaining an owner-approved bucket-cap security policy and proving the selected behavior under saturation and recovery.`

At `MaxBuckets`, new keys are deliberately admitted without tracking after idle
eviction fails. A security reviewer and owner must choose fail-open with explicit
deployment restrictions or a bounded fail-closed/overflow policy.

Test existing-key throttling, never-admitted siblings at capacity, idle recovery,
exact retry, concurrent shards, memory bound, metrics, and rejected-request side
effects. Do not invent deletion/eviction unavailable to production. Keep the
limiter local unless DEC-001 selects distributed enforcement. This packet is not
authorization to change ordinary class rates or public error envelopes.

### P-010 — Durable file and object-backup assembly

**Goal objective:** `Resolve P-010 by wiring the storage and object-backup backend selected by DEC-001 with durable configuration, tenant isolation, and recovery evidence.`

The daemon hardcodes files under `os.TempDir()/instantv2-files`; backup object
routes have no object store and return unavailable. Choose persistent local disk
or existing S3-compatible components explicitly. Add startup validation for
root/bucket/credentials, then use one shared configured ownership model where
appropriate without conflating file triples and backup objects.

Test restart persistence, atomic upload, partial/write failure, size limit,
presigned expiry/signature, traversal/symlink defense, app isolation, delete,
orphan/metadata behavior, object backup/restore, unavailable backend, and exact
reopen state. Use temp-owned local fixtures or fake S3; a real MinIO/runtime drill
is later evidence. Do not claim S3 merely because an interface implementation
exists. Security and persistence review are mandatory.

### P-011 — Deployed OAuth/OIDC provider and nonce acceptance

**Goal objective:** `For every provider selected by DEC-001, complete P-011 by proving the deployed authorization and identity lifecycle against its real sandbox or explicitly removing the provider from the supported envelope.`

P-002 proves local route assembly and controlled provider behavior. It does not
prove real issuer metadata, provider-specific token/user-info shapes, redirect
registration, key rotation, or operator configuration. Before external calls,
record explicit authority, sandbox accounts/endpoints, secret source, redaction,
cost/rate limits, and cleanup.

For Google/GitHub or another selected provider, exercise start, redirect/cookie,
PKCE where applicable, callback, token/user-info identity normalization, replay,
expiry, provider denial/error, revoked credentials, and redirect mismatch through
the deployed daemon. If OIDC/id-token login is selected, first freeze and test
nonce generation, persisted binding, one-time use, issuer/audience/expiry/subject,
JWKS rotation/cache, algorithm downgrade, and absent/mismatched nonce. Apple
requires its own reviewed ID-token/client-secret contract; the presence of an
Apple signer is not exchange acceptance.

Keep secrets and returned personal data out of source, command arguments, shared
artifacts, and logs. Retain only redacted provenance and result metadata. Without
external authority/credentials, finish local preflight and return `BLOCKED`, not
`GREEN`. The worker owns no product-code edits: provider defects discovered here
become a separate bounded auth packet. Store raw local output only under
`tmp/provider-acceptance/<candidate>/<provider>/` after proving that path remains
gitignored/untracked; store the durable redacted result at
`docs/evidence/provider-acceptance/<candidate>-<provider>.md`, containing DEC-001
decision ID, candidate SHA/deployment digest, UTC window, provider/sandbox kind,
tested lifecycle rows, redacted request correlation IDs, pass/fail/not-run,
cleanup result, and hashes/locations of private raw evidence. External calls
require a matching explicit authority entry in the approved DEC-001 artifact.
A fresh security reviewer must inspect preflight/redaction and a Sol release gate
must accept any claim-bearing result. Do not add providers or redesign token
formats in this packet.

### C-001 — Exact-number scenario loading

**Goal objective:** `Fix C-001 so corpus scenario validation and loading preserve the canonicalizer's declared exact JSON-number domain.`

Scenario validation currently unmarshals frames through default `any`/`float64`,
while canonicalization uses `UseNumber` and symbolic exponent normalization.
Create cases for adjacent integers over 2^53, equivalent spellings, fractions,
signed zero, huge positive/negative exponents, nesting, malformed/trailing JSON,
and invalid frame structure. The loader must avoid float loss and unbounded
expanded allocation. Assert raw bytes remain retained and canonical equality is
unchanged. Scope is `internal/corpus`; do not widen transaction/query numeric
domains.

### C-002 — Comparator field scope and final quiescence

**Goal objective:** `Fix C-002 so differential normalization masks only protocol metadata and replay observes unexpected late frames under an explicit bounded policy.`

Current key-based masking is recursive, so application payload keys named
`tx-id`, `session-id`, or other volatile fields may be hidden. The replayer proves
declared frame count but not absence of a late unsolicited frame after the final
barrier.

Red tests must place each same-named key in metadata and user payload locations;
metadata may normalize while payload remains significant. Define path-aware rules
for session/timestamp/attrs differences. Add a bounded observation/quiescence seam
with a controlled transport clock/channel for on-time completion, late extra
frame, error frame, silent peer, disconnect, and failures on either side. Do not
claim a finite window proves no frame can ever arrive later. Preserve raw and
normalized evidence.

### C-003 — HTTP and SSE capture/replay adapters

**Goal objective:** `Implement C-003 capture and replay only for the HTTP and SSE surfaces selected by DEC-001, with owned fixtures and private provenance evidence.`

Define per-transport request, response, headers, streaming frames, timing/order,
redaction, fixture/reset, and completion contracts. Reuse production handlers and
the corpus canonical policy; do not build copied behavior models. Cover auth,
admin, runtime query/transact, storage/backup controls, SSE init/event/update/
disconnect, malformed bodies, denial, and transport failure as selected.

Evidence must be write-once/private, identify candidate/config/fixture, keep raw
and normalized forms, and fail on zero selected cases. Recording machinery is not
itself required if an equally trustworthy controlled capture path exists. Never
reuse a mutating external fixture without reset.

### C-004 — Supported-surface coverage matrix

**Goal objective:** `Complete C-004 by converting the selected release envelope into an evidence-accounted scenario matrix and filling every unsupported or missing row honestly.`

For each supported family record positive, denial/error, boundary, lifecycle,
concurrency where relevant, transport, fixture owner, expected final state, oracle
origin, raw evidence, and accepted difference:

- auth lifecycle and token refresh/replay;
- dynamic permission bindings/fallback and denied-state non-leakage;
- query cursor/order/conjunction/forward relation and selected numeric domain;
- refresh delta/full equivalence, reconnect/reset, and final convergence;
- same-node rooms/presence/fanout, plus cross-node only if selected;
- transaction cardinality/merge/cascade/required/lookup/missing lookup/rollback;
- selected HTTP, SSE, storage, backup, and SDK versions.

Scenario count is a planning signal, not completion. Preserve the existing 18
authored regressions and distinguish authored-v2 expectations, actual v1 captures,
and accepted differences. Product fixes stay with their owning packages; the
corpus agent only adds fixtures/coverage after those fixes are green.

### C-005 — Pinned-v1 fixture and differential acceptance

**Goal objective:** `Execute C-005 by provisioning the pinned v1 and exact v2 candidate with equivalent resettable fixtures, then account for every selected differential result.`

Required v1 ref is
`a4d2ef33b60f281a437191006e4541d4780f9e4a` unless DEC-001 changes it explicitly.
Preflight Java/Clojure, PG17/pg_hint_plan/logical WAL, MinIO/bucket, bootstrap,
configuration, ports, and exact served revision. Prove v2 revision/tree/config as
served. Fixture setup must be isolated and reset per mutating scenario.

Run `make differential` with explicit full `V1_REF`, `V1_PATH`, `V1_URL`,
`V2_URL`, suite, and fresh private output. Transport/decode failures, both-side
failures, zero selection, fixture mismatch, or unidentified endpoint revision
fail acceptance. Resolve discrepancies in owning packets or record an owner-
approved compatibility difference. This packet requires explicit authority for
local services/external endpoints and credentials; otherwise return `BLOCKED`
with completed preflight evidence.

### T-001 — Chaos directory ownership safety

**Goal objective:** `Fix T-001 so cmd/chaos can delete only a marker-owned disposable PostgreSQL directory and never an arbitrary, ancestor, redirected, or mismatched target.`

The command passes `--pg-data` to `os.RemoveAll` before initialization and again
at cleanup. Red tests use temp-owned fixtures and an injected deletion boundary;
never reproduce against a real cluster or valuable path. Reject empty/root/home,
workspace/ancestor, existing unowned directory, marker mismatch, symlink at any
component, path replacement between validation and delete, and broad parent.
Accept one exact marker-owned disposable directory, preserve sibling sentinels,
and clean partial setup without broadening ownership.

Prefer descriptor/identity-bound deletion where the platform requires it. Scope
is `cmd/chaos/**`; do not run chaos. Fresh destructive-path security review and
two harmless focused reruns are mandatory.

### T-002 — Chaos provenance and failure propagation

**Goal objective:** `Fix T-002 so every successful chaos result proves the exact tested candidate and propagates every build, process, replay, transport, decode, and zero-selection failure.`

The runner retries the dirty working tree then silently falls back to exported
HEAD, and `runCorpusReplay` formats subprocess failure as text rather than an
error. Freeze policy: exact dirty candidate or explicit immutable historical
mode, never fallback. Record revision, tree state/diff fingerprint, binary hash,
configuration, fixture identity, and artifact agreement.

Tests inject working-tree build failure, HEAD availability, replay nonzero,
transport/decode failure, zero scenarios, report-write failure, and success. Each
must drive overall exit/artifact state correctly. Historical mode must be named in
output and cannot certify the current tree. Scope remains `cmd/chaos/**`; still do
not run live faults until this packet and its independent review are green.

### B-001 — Benchmark evidence and serialization integrity

**Goal objective:** `Fix B-001 so pair and triad reports enforce equivalent applicable provenance/resource evidence and serialization failures cannot produce successful artifacts.`

Pair loading calls process/live-resource validation; triad loading does not.
`mustJSON(any)` ignores marshal and redaction errors. Freeze which evidence is
common and which mode-specific before editing.

Create a valid pair and triad, then independently omit/corrupt/stale/mismatch each
required manifest, target, process, runtime, DB, network, fixture, schedule, and
run record. Both relevant modes must reject. Test unsupported/cyclic/non-finite or
redaction-failing values under the chosen typed contract and propagate errors
through artifact finalization and CLI exit. Preserve checksum/content-root and
write-failure tests. Scope is benchmark packages/commands; do not change budgets,
run performance, or turn missing evidence into `unsupported` success.

### O-001 — Multi-node invalidation publisher recovery

**Goal objective:** `If multi-node mode is selected, fix O-001 so both invalidation listening and publishing recover after PostgreSQL connection loss without loss loops or duplicate amplification.`

The listener has a supervisor, but the publisher retains one dedicated connection
and only logs publish failure. Build deterministic connection factories to force
loss during publish/listen, reacquisition failure/backoff, duplicate delivery, and
cancellation. Verify exact app/tx/change identity, local notification once,
eventual peer convergence, no republish echo, no concurrent use of one connection,
and clean release. Decide whether a committed write may acknowledge while peer
publish is unavailable and what repair obligation follows. This distributed-
consistency decision requires architecture review.

### O-002 — Read-replica visibility and watermarks

**Goal objective:** `If read replicas are selected, prove or fix O-002 so refresh watermarks never certify a commit whose state was not visible in the query result.`

The write commits on the primary, refresh reads from `INSTANT_V2_READ_URL`, and a
successful materialization advances `TxID`. First reproduce controlled replica lag
or a deterministic read-source seam: commit value N, return N-1 from the replica,
then catch up. Assert no stale result is marked processed, retry/convergence is
strictly advancing, reconnect is correct, and primary-only behavior is unchanged.
Freeze a visibility-barrier/fallback/error policy; do not merely sleep for replica
catch-up. This affects distributed correctness and requires architecture review.

### O-003 — Native Linux and container runtime acceptance

**Goal objective:** `Accept O-003 on the exact candidate for every shipped OS/container target, including DB-connected startup, outbound TLS, signals, and resource cleanup.`

Run declared Go 1.25 builds/tests on native Linux with race support and inspect
executed/skipped platform cases. Run `make container-verify`, then separately use
owned dependencies to test migration/DB startup, health/readiness, configured
non-root identity, writable persistent paths, real CA trust via bounded TLS fixture,
SIGTERM/WS drain, port/connection cleanup, and image digest/config capture. File
presence is not a TLS handshake test; cross-compilation is not native runtime
evidence. Source repair becomes a new bounded owning packet rather than ad hoc
changes by the runtime worker.

### O-004 — Soak, capacity, and resource stability

**Goal objective:** `Execute O-004 against an immutable qualified candidate using DEC-001's predeclared workload, active duration, correctness, latency, error, and resource budgets.`

First validate short `bench-smoke`/`soak-gate` tooling against a marked owned
fixture. Distinguish total duration from ramp/settle/active time; reconcile the
100-second CI gate with the separate 60-second performance workflow. For the
approved production run, record host/config/candidate/fixture, time-series
memory/FD/goroutine/queue/connection trends, latency/error distributions,
acknowledged-write ledger, reconnect/final convergence, and teardown. A historical
5k×30m result does not accept a changed candidate; 5k×30m is required only if
DEC-001 retains that promise. No tuning or concurrent noisy work during measure.

### O-005 — Recovery, loaded drain, and backup/restore

**Goal objective:** `Execute O-005 on owned resources and prove exact acknowledged-state recovery across selected crashes, PostgreSQL interruption, loaded drain, and backup/restore.`

Execute O-005 as two explicit lanes:

- **Core lane (alpha and above):** requires O-003, a stable candidate, owned
  fixtures, frozen RPO/RTO/drain budgets, and explicit process/DB interruption
  authority. Use a non-destructive external fixture orchestrator—not
  `cmd/chaos`—for clean server restart, PostgreSQL stop/restart, reconnect, and
  loaded SIGTERM. Exercise database backup/restore only when DEC-001 advertises
  that capability; otherwise record its enforced exclusion. This lane may finish
  without T-001/T-002.
- **Destructive-chaos/full production lane:** additionally requires T-001 and
  T-002 green and explicit destructive live-fault authority. It adds abrupt
  process crash points, candidate-provenance enforcement, WAL/checkpoint cases,
  partial backup/import, restore failure preservation, and rollback/upgrade.
  Durable production profiles must include the selected database backup/restore
  drill; object restore is required only when P-010 exposes object backup.

For both lanes, compare exact acknowledged values, tombstones, tx IDs/watermarks,
indexes, subscriptions, and final client state—not only liveness/counts. No
phantom state. Preserve original artifacts on failed restore. Do not use the
default developer DB or infer serving recovery from an isolated tailer test. The
packet handoff must state `CORE_GREEN`, `FULL_GREEN`, or the exact unrun lane;
single-node alpha requires `CORE_GREEN`, while production requires the DEC-001
selected full durability lane.

### PERF-001 — Qualified comparative performance

**Goal objective:** `If selected by DEC-001, execute PERF-001 as one policy-frozen qualified comparison and produce a verified immutable evidence bundle without tuning or selective reruns.`

Prerequisites: B-001, semantic corpus prerequisites, target/runtime qualification,
quiet named hardware, exact revisions/binaries/config/fixtures, approved cells,
attempt counts/order, warmup/active periods, budgets, and interruption policy.
Synthetic results are tool acceptance only. Follow the accepted Wave 6 schedule if
retained; failed qualification stops the run, and partial bundles are never joined
or selectively replaced. Verify offline hashes/content root/provenance before any
claim. Re-measure the canonical encoder allocation concern only as one diagnostic;
optimize later only if an approved end-to-end budget fails. Performance work cannot
repair correctness during measurement.

### D-001 — Truthful status, support, self-host, and upgrade documentation

**Goal objective:** `Reconcile D-001 documentation with the final selected source and evidence, removing stale completion claims without erasing historical records.`

At minimum reconcile:

- current corpus 18/22/7/2/0 counts versus historical 16/20/9 prose;
- completed pass-1 fixes versus still-open compatibility evidence;
- actual Go version, required storage-secret/config, post-commit invalidation
  versus WAL claims, persistent storage/object-store assembly, and supported routes;
- historical chaos/soak/release-tool observations versus current-candidate
  acceptance;
- real tag/release workflow availability;
- same-database migration and rollback claims against executed drill evidence;
- unsupported Apple, streams/sync, presence, cross-node rooms, replica, COPY, and
  other DEC-001 exclusions.

Update current-status sections and links; retain dated historical evidence as
historical. Validate links, commands, versions, counts, and source references.
Documentation may not close a product/evidence packet by prose.

### REL-001 — Composed release gate

**Goal objective:** `Implement REL-001 as one fail-closed gate that composes exactly the checks and external prerequisites selected by DEC-001.`

Inventory existing Make/CI targets and avoid duplicate runners. The gate must
separate hermetic, owned-DB integration, corpus, external v1, container, soak,
recovery, benchmark, and artifact lanes; validate every required variable,
candidate fingerprint, selected test count, and output path. It must not provision
or mutate an unspecified DB/endpoint, accept skipped/zero tests, hide advisory
failures, run unsafe chaos, or imply that `test-release` creates benchmark bundles
or services when it does not. Add contract tests for missing prerequisites and
failure propagation. Coordinator owns Make/CI changes.

### REL-002 — Publish, signing, and SBOM workflow

**Goal objective:** `If publishing is authorized, complete REL-002 so a protected tag builds, verifies, signs, inventories, and drafts immutable artifacts through an auditable least-privilege workflow.`

`.goreleaser.yaml` exists, but current CI does not contain the tag-triggered
GoReleaser job claimed by `UPGRADE.md`. Define tag/ref protection, permissions,
OIDC/cosign behavior, GHCR targets, multi-arch image identity, checksums, SBOM,
provenance, draft/final release, failure cleanup, and verification commands.
Workflow tests/static validation and an explicitly authorized dry run precede any
publish. Never place credentials in source/logs/prompts. Actual tag, push, registry
write, signing, and publication remain separate explicit actions.

### REL-003 — Immutable candidate acceptance

**Goal objective:** `Run REL-003 as a fresh independent acceptance of one clean immutable candidate and issue the final requirement-by-requirement release verdict.`

Require exact clean SHA/tree, toolchain/dependency/config/fixture identities, all
selected packet handoffs, raw artifacts, accepted exceptions, and non-author
review. Run the composed gate on the same candidate; inspect skips and selected
counts. Verify no untracked/private artifact changes build input. A fresh reviewer
challenges authorization, state integrity, compatibility, recovery, evidence
provenance, and claim wording. Status is `COMPLETE` only when every selected row is
green and artifacts agree; otherwise `PARTIAL` or `BLOCKED`. Do not tag/publish/
deploy in this packet unless explicitly and separately authorized.

### REL-004 — Canary and rollback

**Goal objective:** `With explicit deployment authority, execute REL-004 as a bounded canary with predeclared health, correctness, resource, and rollback triggers.`

Name target, image digest, config/secret source, data/traffic scope, observation
window, metrics, acknowledged-state probes, stop thresholds, and responsible
operator. Prove backup/restore and schema-compatible rollback before relying on
binary rollback. Deploy only the accepted immutable artifact, observe, then widen
or roll back exactly under the runbook. Preserve audit evidence and report every
external mutation. Without target/credentials/authority, return `BLOCKED`; never
substitute a local simulation and call it a canary.

## 8. Required packet handoff

Every agent returns this structure:

```markdown
## Status
COMPLETE | PARTIAL | BLOCKED

## Baseline
- Branch/revision:
- Initial status and diff stat:
- Pre-existing files preserved:
- Environment/prerequisites:

## Contract result
| ID | Status | Test/evidence | Observed result | Remaining gap |
|---|---|---|---|---|

## Changes
- Product behavior:
- Test-only seams:
- Compatibility/format/operational effects:
- Unexpected or unrelated files:
- Final versus initial status:

## Verification
### Focused tests
### Package/integration tests
### Static checks
### Build/typecheck
### Runtime/manual evidence
### Not run or incomplete

## Skeptical review
- Strongest falsification attempted and result:
- Weak proxies/fakes/timing assumptions/untested variants:
- Same-context or fresh independent review; reviewer verdict:
- Remaining uncertainty:

## Handoff
- Exact blockers in priority order:
- Safest next action:
- Commit/push/deploy/external mutation performed: yes/no, with authorization and targets
```

Rows are `PROVEN_RED` only after the intended desired-behavior assertion was run
and failed for the intended reason. Rows are `GREEN` only after the repair and
required evidence were run and inspected. `IMPLEMENTED`, skipped, not run, still
running, zero selected tests, source inspection, and historical results are not
`GREEN`.

## 9. Coordinator completion rule

The gaps/backlog program is complete only when:

1. CP-001..003 have reviewable handoffs and the original dirty tree is fully
   accounted for.
2. DEC-001 is owner-approved and every conditional packet is either selected and
   green or enforceably excluded without contradicting the compatibility claim.
3. Every unconditional and selected master-ledger row has a current-candidate
   handoff with no hidden skipped prerequisites.
4. D-001 and REL-001 agree with the final source and support matrix.
5. REL-003 accepts one clean immutable candidate; if deployment was requested,
   REL-004 also completes.

The coordinator must not equate a development checkpoint with production
acceptance, package tests with v1 compatibility, historical measurements with
current-candidate evidence, component existence with daemon assembly, or artifact
configuration with actual publication.

## 10. Speed-run implementation checkpoint — 2026-09-04

This checkpoint records implementation progress, not release acceptance. The
worktree remains a dirty development candidate, DEC-001 remains unapproved, and
no live chaos, external provider, pinned-v1, soak, recovery, performance,
publishing, or deployment campaign was authorized or run.

| Packet | Checkpoint state | Evidence and remaining limit |
|---|---|---|
| CP-001 | `IMPLEMENTED_ACCEPTED` | Transaction fingerprint failure rollback is covered; focused storage/transact checks and independent security review accepted it. Database-backed coverage still depends on an owned integration database. |
| P-003 | `IMPLEMENTED` | Missing delivery now fails with stable HTTP 503 before throttling, code generation, or persistence; focused auth test passed. No real delivery provider is assembled. |
| P-005 | `FOCUSED_GREEN` | `serverCreatedAt` tuple/order regressions and aligned legacy ID-order tests passed. |
| P-006 | `IMPLEMENTED_NOT_INTEGRATION_ACCEPTED` | COPY nil and deterministic winner assertions exist, but the PostgreSQL-backed acceptance test was not executed. |
| P-007 | `FOCUSED_GREEN_EXPLICIT_UNSUPPORTED` | Admin presence now returns stable HTTP 501 instead of false success. |
| P-008 | `FOCUSED_GREEN_EXPLICIT_UNSUPPORTED` | Nine placeholder sync/stream operations now return explicit unsupported error frames without mutation. |
| C-003 | `IMPLEMENTATION_ACCEPTED` | Bounded HTTP/SSE capture/replay, raw plus canonical evidence, status/body/header comparison, redaction, byte limits, quiescence handling, and contained write-once output have focused passing tests and independent review. This is adapter acceptance, not v1 compatibility evidence. |
| C-004 | `IMPLEMENTATION_ACCEPTED` | The manifest has a 25-row transport/fixture/oracle/evidence matrix: 9 covered, 15 gaps, 1 unsupported. Evidence metadata is bound to scenario, transport, and status; independent review accepted the matrix mechanics, but this does not fill the 15 gaps. |
| T-001 | `ACCEPTED` | Descriptor-relative cleanup enforces the original PGDATA identity and an external sidecar ownership proof; PGDATA stays empty for initdb; replacements, symlinks, ancestors, unowned targets, and unsupported platforms fail closed. Independent security review accepted the focused contract. No live chaos run occurred. |
| T-002 | `PARTIAL` | HEAD fallback and swallowed replay failures are removed; cleanup precedes PASS and failures propagate. Same-UID atomic source/snapshot provenance and descriptor-relative postmaster PID validation remain outside T-001 acceptance. |
| B-001 | `IMPLEMENTATION_ACCEPTED` | Pair/triad evidence validation and serialization failure behavior passed focused race tests and affected-command builds. No qualified live benchmark campaign ran. |

One combined compile-only command passed across all packages:
`INSTANT_TEST_INTEGRATION=0 go test ./... -run '^$' -count=1`. This proves the
parallel edits compose at build time; it is not a substitute for the packet
integration, runtime, compatibility, or production gates.

### Remaining executable queue

1. Obtain an owner-approved DEC-001 profile. Until then P-009, P-010, O-001,
   O-002, and support/exclusion decisions cannot be completed honestly.
2. Finish T-002 provenance isolation and descriptor-relative PID handling if
   chaos results are intended to become acceptance evidence.
3. Run P-006 against an owned PostgreSQL fixture and fill the selected C-004
   coverage gaps with real captures.
4. With explicit external authority, run P-011 real-provider acceptance and
   C-005 equivalent-fixture pinned-v1 differential capture. There are still no
   v1 captures.
5. After the supported product surface is frozen, execute O-003 through O-005,
   PERF-001, D-001, and REL-001 through REL-004 as selected. These are the
   deliberately deferred production-hardening and release lanes.
