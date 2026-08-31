# Production completion: implementation plan and release roadmap

Date: 2026-08-31. Baseline: the uncommitted quality-refactor tree based on
`5f78ca0877c1a8c04b173e5c502e8c948ecfb965`.

Status: **IMPLEMENTATION FIRST; PRODUCTION/BENCHMARK HARDENING DEFERRED**.
The user's latest 2026-08-31 instruction supersedes earlier all-Sol routing and
release-first sequencing. The detailed waves below remain a backlog, not an
instruction to run them now. Current work closes the development incident and
commits the existing implementation; it does not begin all remaining repairs.

## Current authority — implementation first

- Include relevant pre-existing implementation, regression tests and project
  history; exclude generated/private artifacts. Commit coherent batches as they
  pass their minimum meaningful checks rather than accumulate uncommitted work.
- No pre-incident backup exists and the program is unreleased. Retain the current
  development schema and documented uncertainty; no backup program, baseline hunt
  or database rollback is needed to close the incident.
- For each next requested implementation slice, make the smallest complete
  functional change and run focused regression/package checks. Preserve existing
  assertions. Do not add a new harness or exhaustive acceptance campaign. Stop at
  this commit checkpoint instead of automatically launching the backlog.
- **Production hardening and benchmark-harness hardening need new approval.**
  B1, C1/C2, broad corpus/v1 capture expansion, soak/recovery, performance campaigns,
  container/release certification, signing and deployment remain deferred. Do not
  execute the unsafe chaos tool while its documented risks remain open.
- Use Luna-heavy routing for bounded work, Terra for moderate integration or
  ambiguity, and GPT-5.6 Sol for genuinely complex/security-sensitive/public-
  compatibility/data-integrity decisions. Earlier all-Sol instructions are historical.
- Local commits are authorized; push, PR, tags, publication and deployment are not.
  No public-release claim follows from a development checkpoint.

Existing strict targets and risk records stay available for the later approved
phase. They are not newly required for every development commit. Deeper checks,
when genuinely required by a changed behavior, remain scoped to that behavior.

Checkpoint: existing implementation committed as `eb88232` on
`codex/quality-implementation-checkpoint`; relevant pre-existing edits were
included after Luna review, with generated/private artifacts excluded. The
[incident record](../reference/quality-incident-followup.md#committed-development-checkpoint)
contains the minimum checks. The next action is a bounded user-selected
implementation slice, not automatic execution of the hardening roadmap.

This completes implementation quality and its evidence, not a replacement service
architecture. Read the [incident follow-up](../reference/quality-incident-followup.md)
first, then the [scorecard](../reference/quality-scorecard.md),
[previous verification](../reference/quality-verification.md), and
[corpus matrix](../../corpus/README.md). The previous execution ledger remains
historical evidence; this document is the forward work queue.

## 1. Acceptance levels and non-negotiable boundaries

Track each work item separately as `PENDING`, `PROVEN_RED`, `IMPLEMENTED`,
`FOCUSED_GREEN`, `INTEGRATED_GREEN`, `BLOCKED`, or `ACCEPTED_EXCEPTION`. A
reproduction is not a fix. An accepted exception is not a passing test.

Release acceptance additionally requires an immutable candidate, compatible
supported surfaces, native/runtime evidence, an independent review, and an
explicit owner decision. No default-suite pass or file-size improvement replaces
these requirements.

- Preserve user changes and existing test assertions. Do not manufacture clean
  historical commits or rewrite already shipped migrations.
- Reproduce inspected defects before repair. For destructive-path findings,
  reproduce rejection with harmless owned fixtures, never valuable directories.
- All live tests use a freshly owned dedicated cluster and unique testkit
  databases/slots. Never migrate, seed, reset or clean the default developer DB.
- Exact protocol/state comparisons take precedence over counts, logs or liveness.
- Keep migration, auth, transaction, permission and public-wire changes explicit.
  A semantic ambiguity stops the affected packet, not unrelated independent work.
- Do not widen numeric support, COPY exposure, unsupported features, or benchmark
  policy under the label of cleanup.
- No chaos execution until C1 and C2 are reviewed and green. No measured
  performance during concurrent tests/builds on the measurement host.
- Coordinator owns status, shared interfaces, integration and final claims.

## 2. Wave 0 — finish delivery hygiene and incident containment first

Current detailed results and private evidence inventory are in the
[incident record](../reference/quality-incident-followup.md).

| Gate | Current outcome | Remaining action / exit criterion |
|---|---|---|
| D1 working-tree preservation | Private tracked patch and 253-file source checkpoint saved; user authorizes relevant pre-existing edits | Review relevance, exclude generated/private files and commit coherent batches |
| D2 affected-DB inspection | Read-only snapshot: Goose versions 1–6 applied in incident window; all 15 application tables currently empty | Preserve findings; current emptiness does not prove historical emptiness |
| D3 historical incident closure | ACCEPTED_EXCEPTION: user confirms no baseline and no public release | Keep current schema and uncertainty; no recovery/backup project needed |
| D4 retained resources | Original cluster stopped; schema/log evidence preserved privately | Retain until explicit owner decision; any future cleanup names exact validated targets |
| D5 recurrence regression | Fresh dedicated cluster: testkit unit/race and two live isolation executions passed; owned DBs cleaned; server stopped | Repeat fixture preflight on each new runtime environment |
| D6 source sanitation | Root generated `corpusctl` ignored without masking command source | Keep binaries, credentials and private captures out of commits |
| D7 source/external delivery | Local implementation commits authorized | Record checkpoint hashes/checks; external delivery remains out of scope |

**Gate distinction:** D3 is closed for development by owner disposition, not by
proof of historical equality. Local commits and bounded implementation do not
wait for production certification. D7 does not authorize deployment.

### Reviewable commit strategy

After explicit authorization, create a `codex/...` integration branch carrying
the current working tree. Creating a new worktree from HEAD alone would omit the
candidate. Use explicit path/hunk staging, inspect every cached diff, and exclude
private evidence/generated binaries. Do not use broad stash/reset as a shortcut.

Suggested atomic groups, adjusted to actual dependency inspection:

1. Testkit/shared helpers and their tests, without prematurely switching consumers.
2. Cohesive package refactors/fixes with all their consumers and tests; retain
   pre-existing user hunks only with the approved inclusion decision.
3. Command relocations with Make/scripts/workflow/generator consumers that depend
   on them. Couple generated schema metadata and hashes with their source changes.
4. Documentation relocation and link consumers together, including these reports.
5. Corpus infrastructure, fixtures, known-gap inventory and contract tests.
6. Subsequent production fixes: one reviewed work packet or tightly coupled group
   per commit, then final evidence/documentation for the exact candidate.

These are dependency groups, not permission to fabricate a pure-rename history.
If existing intertwined changes cannot produce independently buildable commits
without rewriting them, use a larger honest integration commit with a component
review map. Never claim perfect historical attribution from the final diff alone.
Verify relevant tests at each meaningful commit boundary and the complete final
candidate. PR/push/tag/publish/deploy each require their own named destination and
approval; do not treat the presence of an origin remote as approval.

## 3. Decisions that must be frozen before implementation or measurement

| Decision | Proposed direction / required owner choice | Blocks |
|---|---|---|
| Supported release envelope | Enumerate OS/architecture, topology, SDK versions, providers, HTTP/WS/SSE routes and persistence modes | Compatibility/release claims |
| OAuth lifecycle | Atomic single-use code/state consumption; expiry before issuance/exchange; explicitly choose exchange-failure retry policy and expiry boundary | A1 |
| Provider matrix | Fix normal configured built-ins; decide Apple ID-token support versus truthful unsupported configuration | A2; Apple implementation is not implied |
| Query ordering | Conjunction of operators; define total order, ties, nulls and cursor inclusion consistent with required compatibility | Q1/Q2 final assertions |
| Metrics/config | Define duplicate registration ownership and empty-versus-unset address semantics; retain distinct read/write pools unless explicitly changed | M1/G1 |
| Reactive retries | Bound retry frequency; define cap/reset/coalescing/fairness and cancellation behavior | R1 |
| Rate-limit capacity policy | Coordinator plus security reviewer must explicitly accept or change the documented fail-open behavior for new buckets at capacity; acceptance must state deployment restrictions, while a change needs a bounded lease and regression contract | Supported release envelope; no automatic unrelated hardening |
| Numeric domains | Exact lookup values; corpus canonicalizer domain; keep benchmark's declared domain separate | Q4a/Q4b |
| COPY exposure | Repair parity or explicitly exclude production adoption; do not wire it into serving paths | S1/release envelope |
| Chaos source policy | Test current candidate or explicit immutable historical mode; no silent HEAD fallback | C2 |
| Performance policy | Reconcile historical “within 30% of v1” wording with current descriptive V1/V2 policy; choose required comparison cells and absolute budgets | P1, release decision |
| Operational budgets | Predeclare active sessions/writes, run duration excluding warmup, latency/error/resource/recovery limits and target hardware | O1/O2/P1 |

Default recommendation is to fix exposed auth/query correctness before general
release. Unsupported stream operations/cross-node rooms may be excluded only
through an explicit support restriction; adding those features is a separately
bounded project. No exception may silently redefine “full parity.”

## 4. Parallel-agent ownership and scheduling

Use **Luna-heavy routing**, reserving GPT-5.6 Sol for genuinely complex/high-risk
work rather than every packet. Request normal/Standard mode where supported. Global
`service_tier = "default"` was observed during this follow-up; the collaboration
tool cannot set or prove an individual request's billing/service tier. Do not
claim a per-agent tier change from model selection alone.

Up to ten subagents, in addition to the coordinator. This is a capacity ceiling,
not a target utilization metric. Spawn only runnable independent packets.

| Agent | Exclusive write lease | Packet / handoff |
|---|---|---|
| S1 query | `internal/instaql/**`; `internal/datalog/**` only if explicitly transferred | Q1/Q2; returns desired corpus outputs to S5, never edits S5 files concurrently |
| S2 auth | `internal/authn/**` | A1 then A2; requires non-author security review |
| S3 reactive | `internal/reactive/**` | R1; final oracle checks after Q1/Q2 integration |
| S4 values/storage | `internal/transact/**`, `internal/storage/**` | Q4a/S1; no new production COPY caller |
| S5 corpus | `internal/corpus/**`, `cmd/corpusctl/**`, `corpus/**` | Q4b/Q3; owns all scenario/manifest updates, including Q1 promotions |
| S6 fault safety | `cmd/chaos/**` | C1/C2 first; later recovery changes require an explicit additional lease |
| S7 lifecycle | `internal/metrics/**`, `internal/config/**`, `cmd/instantd/**` | M1/G1; owns shared runtime caller changes |
| S8 evidence | `internal/benchharness/**`, `internal/benchrun/**`, `cmd/benchrun/**`, `cmd/benchreport/**`, `cmd/benchsmoke/**`, `cmd/soak/**`, `cmd/soaksetup/**`, `benchmarks/**` | B1/P1; measurement requires a quiet qualified host |
| S9 environment | Initially read-only v1/Linux/container prerequisites | Later bounded fixture/runtime lease and explicit targets; no overlapping source writes |
| S10 reviewer | Read-only, non-author | Reviews security/data-integrity/safety contracts and final evidence |

Coordinator retains `.github/**`, Makefile, Dockerfile, `scripts/**`,
`internal/testkit/**`, migrations, protocol/schema/generated files, all docs, and
unassigned packages. `internal/sync`, bus, waltail and HTTP packages stay unleased
until a coverage/recovery finding needs a bounded repair. Transfer whole exact
files/scopes only after the previous writer stops; never grant overlapping leases.

### Dispatch contract for every worker

Each brief must include: requirement IDs; exact invariant; real entry point;
owned paths; dependencies/decisions; explicit non-goals; current evidence class;
proposed failing observation; focused and adjacent verification; stop conditions;
and required evidence return. Proposed test names are not existing tests.

Workers preserve others' edits and may not commit, push, deploy, contact external
systems or delete unowned resources. Return changed paths, tests actually selected
and executed, assertion/output evidence, before/after behavior, unresolved gaps,
and consumer changes needed from another owner. Do not return a generic “done.”

Each selected defect: reproduce → minimal repair → minimum meaningful check →
commit. Add repeated/race or independent review where the changed behavior needs
it, not as an unrelated campaign. After two failed focused attempts, Luna escalates
to Terra; unresolved complexity/security/compatibility/data-integrity questions go
to Sol. Revise the contract or ask the user rather than run an open-ended rewrite.

## 5. Wave 1 — bounded product and tool repairs

The test names below beginning `TestQuality` are **proposed**, not implemented.
Every packet must record actual names/results once it runs. Package tests use
`-race -count=1`; high-risk focused checks repeat after the first green run.

### A1/A2 — authentication (S2)

**Real path:** OAuth HTTP start/callback/token → provider resolution → persisted
state/code consumption → session/token issuance. Scope stays in authn except
coordinator-approved persistence changes. Source findings, not proven exploits.

1. Freeze expiry, single-consumption, bindings and provider-failure retry rules.
2. `TestQualityOAuthConsumption`: controlled-clock before/at/after expiry;
   barrier-coordinated simultaneous redemption with exactly one success; replay;
   invalid app/redirect/binding; zero exchange calls on invalid/expired callback;
   partial failure/rollback and the chosen retry policy. Assert exact persisted
   state and issued identities, not only success counts. Existing malformed-record
   tests must remain valid and green.
3. Implement conditional/transactional consumption in the real persistence path;
   deletion count or equivalent winner proof must be meaningful. Do not add a
   process-local mutex as a substitute for cross-process persistence correctness.
4. `TestQualityBuiltinOAuthProviders`: normal configured built-in resolution,
   endpoint/capability fields, credentials/redirect/scopes/encoding, identity and
   malformed/error responses through local provider fixtures.
5. Apple, if approved as supported, needs issuer/audience/signature/key/expiry/
   nonce/subject tests and a reviewed trust contract. Otherwise reject it clearly
   as unsupported; no pretend successful generic user-info path.

Verify focused and full `./internal/authn` live/race checks against owned DBs,
then affected HTTP/corpus flows. Non-author security review before integration.
Real-provider sandbox checks remain separately authorized runtime evidence.
Non-goals: new provider catalog, token-format redesign, auth framework rewrite.

### Q1/Q2 — queries (S1, corpus promotion by S5)

**Real path:** query coercion → predicates/entity fetch → relation resolution →
ordering/pagination → projection/cursor. Q1 has existing known-red evidence; Q2
still requires runtime reproduction.

Enumerate `TestCorpusKnownGapIntegration`, then separately run unchanged
`after-cursor` and `forward-relation` subtests with
`INSTANT_CORPUS_KNOWN_GAPS=1 INSTANT_TEST_INTEGRATION=1` on an owned fixture server.
Fix advancement and aliased cardinality-one projection without weakening expected
output. Cover missing optional children, app scoping and permission boundaries.

Add `TestQualityQueryConjunctionAndPagination`: data below/inside/above a two-bound
range; operator order independent; entity IDs deliberately disagree with requested
field order; ascending/descending/ties/nulls under the frozen contract; concatenate
pages and compare to the full ordered result; no duplicates/omissions; filtering
before limiting; existing offset/cursor modes and cursor metadata agree.

Only after correctness is green should parse/plan/fetch/order/project boundaries
be split further. S5 promotes the original fixed cases into default corpus and
updates the manifest/gap count. Verify `./internal/instaql`, affected datalog,
real corpus replay, then sync/reactive oracle checks. No query-engine replacement,
new sort API, index tuning or performance claim in this packet.

### R1 — reactive retry lifecycle (S3)

**Real path:** notifier Run/drain → refreshOne failure → rescheduling, cancellation
and eventual publish. Source finding; first demonstrate persistent failure.

`TestQualityReactiveRetryLifecycle` uses a controlled clock/failure barrier:
bounded attempts before/after retry deadlines; capped/reset policy; cancellation;
healthy subscription progress alongside a failing one; pending changes preserved;
recovery publishes latest exact result; unsubscribe/shutdown cancels scheduled work.
Retain sticky-unknown, detached-epoch and incremental/full-query assertions.

Implement the smallest owning retry state, not a general scheduler framework.
Verify `./internal/reactive` race and live oracle checks; repeat failure/recovery
after Q1/Q2 integration. Backoff without eventual correctness is not completion.

### M1/G1 — metrics and configuration (S7)

M1's existing reproduction is
`go test ./internal/metrics -run '^TestRegisterGaugeServesScrapeTimeValue$' -count=2`.
Run it before repair. Define registration ownership/duplicate policy; do not hide
the defect with a test-only global reset.

`TestQualityMetricsRegistrationLifecycle`: repeated create/register/scrape/close;
one sample per logical gauge; no closed-owner callback; independent active owners;
concurrent scrape/register/close under race. Minimal caller changes stay with S7.

`TestQualityMetricsAddressConfiguration`: unset, explicit empty, valid address and
disabled listener match approved semantics. `TestQualityReadPoolConfiguration`:
empty read URL selects write DSN, distinct pool identities and independent budgets
remain if that is the retained contract; explicit read URL is honored. Align docs
via coordinator. Verify metrics/config and `./cmd/instantd`, live only when needed.
Non-goals: dashboard/exporter changes, pool merger, arbitrary budget changes.

### Q4a/S1 — exact lookup and COPY parity (S4)

`TestQualityExactNumericLookup`: two unique values `9007199254740992` and
`9007199254740993`; actual lookup-based transactions affect exactly the intended
entity, with exact stored/read-back values. Cover negative boundaries, supported
integral representations and explicit malformed/out-of-range policy. No float64
oracle may erase the distinction. Repair normalization only in its owning path.

Refresh COPY's caller inventory. `TestQualityCopyTriplesParity`: regular versus
COPY in separate fixtures; JSON null versus string `"null"`; cardinality-one
duplicate inputs in both orders select the same last winner; cardinality-many,
batch boundaries and rollback/error behavior retain parity. Use an explicit
ordinal or equivalent deterministic winner, not database row order.

Verify `./internal/transact ./internal/storage` and affected query/transaction
corpus. No production COPY adoption or arbitrary-precision storage redesign.
If COPY stays excluded, document the restriction rather than claim S1 is fixed.

### C1/C2 — safe, trustworthy chaos command (S6)

First implement/test an ownership contract for both initialization and cleanup.
`TestQualityChaosDirectorySafety`: reject broad/unowned targets, ancestor paths,
symlink redirection and marker mismatch; accept only owned disposable resources;
preserve sibling sentinels; clean partial setup without broadening ownership.
Use temp fixtures/injected deletion boundaries, never a destructive real repro.

`TestQualityChaosEvidenceContract`: exact tested revision/tree and binary hash;
dirty candidate tested or explicitly rejected; no silent HEAD fallback; optional
historical mode explicitly labeled; failed build/replay/transport/decode and zero
selected scenarios cause nonzero overall exit; required artifacts agree with exit.

Verify `./cmd/chaos` tests/build and independent safety review. Only then may an
explicitly approved live fault run use new disposable resources. Do not remove
the original incident evidence as part of this packet.

### B1 — benchmark acceptance and serialization (S8)

Freeze applicable pair-versus-triad process/resource/provenance requirements.
`TestQualityTriadEvidenceParity`: valid bundle accepted; omit/corrupt each required
record independently and reject; stale/mismatched evidence rejected; pair behavior
and legitimate mode-specific exclusions retained.

`TestQualityReportSerializationFailure`: supported values retain exact snapshots;
unsupported/cyclic/non-finite input, as relevant to the chosen type contract,
cannot silently produce successful empty/invalid output; errors reach finalization
and CLI status. Prefer explicit types/errors over blanket panic or ignored errors.

Verify `./internal/benchrun`, affected commands and `make bench-acceptance`;
retain report/artifact/checksum/content-root snapshots. No policy/budget widening
or performance assertion from unit fixtures.

## 6. Wave 2 — trustworthy corpus, supported coverage and actual v1 parity

S5 may start independent engine characterization in Wave 1, but claim-bearing
scenario acceptance waits for the owning product fixes.

### Q4b/Q3a: comparison and recording mechanics

- `TestQualityCorpusExactNumberLoading`: accept the canonicalizer's declared
  large-number domain without float64 validation loss or huge expansion; reject
  malformed/trailing/non-JSON values and invalid scenario structure.
- Scope volatile-field normalization to protocol metadata paths, not arbitrary
  application keys. Prove same-named user payload fields remain significant.
- Define an explicit bounded observation/quiescence policy for unexpected late
  frames. Test extra/error frames and both-side failures; a finite window is not
  proof that no frame can ever arrive later.
- Specify session-key/timestamp normalization and accepted differences narrowly.
  Preserve raw and normalized evidence with provenance and private permissions.
- Implement recording only with an explicit capture contract; current `record`
  deliberately fails. HTTP/SSE/SDK captures need scoped adapters, not a claim that
  the current WebSocket replay already supports them.
- Add an owned per-scenario fixture/reset lifecycle. Never reuse mutating external
  fixtures across captures without reset or rely on a source pin alone.

### Q3b: coverage completion matrix

For every row, record positive, denied/error, boundary, lifecycle and relevant
concurrency cases; actual transport; fixture owner; expected state; oracle origin;
and raw evidence location. Add tests against real entry points, not copied models.

| Coverage family | Required scope |
|---|---|
| HTTP/SDK | Supported auth, admin, runtime, storage and backup surfaces plus chosen SDK versions |
| SSE | Init/auth, event framing, updates, disconnect and drain/reconnect semantics |
| WS auth | Token refresh, expiry/replay/reinitialize and session authorization lifecycle |
| Permissions | Dynamic bindings and supported fallback; denied reads/writes never leak state |
| Query | Fixed cursors, global ordering, compound predicates, forward relations |
| Refresh | Delta/full equivalence, reconnect/reset and final convergence |
| Rooms | Multi-client same-node fanout/presence/leave; no cross-node claim if excluded |
| Transactions | Cardinality, merge, cascade, required/lookup/missing-lookup and rollback matrix |

Preserve the existing 16 scenarios. The earlier >=50-scenario goal remains a
planning target, not a substitute for the surface matrix; any replacement of that
target must be explicit. Reclassify gaps only after exact assertions execute.
Separate authored v2 regressions, real v1 captures and accepted differences.

### Q3c: pinned-v1 environment and differential acceptance (S9 + S5)

Provision v1 at `a4d2ef33b60f281a437191006e4541d4780f9e4a` and the exact v2
candidate in isolated environments. The previous Java/Clojure checks are not
service readiness. Pinned v1 requires its PG17/pg_hint_plan/logical-WAL,
MinIO/bucket, bootstrap/migration/configuration and build prerequisites.

Prove the revision/configuration actually served, prepare equivalent fixtures,
reset per scenario and run both endpoints. Use the existing `make differential`
entry point with explicit `V1_PATH`, full `V1_REF`, `V1_URL`, `V2_URL`, `SUITE`
and fresh private `DIFFERENTIAL_OUTPUT`. It does not provision/reset endpoints.

Exit: all required supported-surface comparisons accounted for; transport failures
fail the run; discrepancies resolved or individually accepted with owner,
rationale and compatibility impact. No zero-capture parity claim.

## 7. Wave 3 — native platform, container and operating behavior

### L1: native Linux and container verification

Run core checks on actual Linux with declared Go 1.25 and required race-toolchain
support; inspect executed/skipped platform tests. Include
`go test ./cmd/benchsmoke ./internal/benchrun -race -count=1 -short -v` with live
mode disabled. `/proc`, descriptor/executable-replacement, namespace/lock cases
must run on a host that supports them. Cross-compilation is not this evidence.

Run existing `make container-verify` with working Docker/curl. Its present scope
is no-DB health, configured non-root identity and CA-file presence. Separately
test DB-connected startup, real outbound TLS behavior where required, process
signals and resource cleanup against owned dependencies. A CA file existing is
not a TLS handshake test. Record image digest and tested configuration.

### O1: soak, load and resource stability

Start with existing `make build-bench`, explicitly owned/marked `make bench-smoke`
fixture provisioning, and `make soak-gate` against an already running seeded
candidate with explicit app/attr/process identity. These targets do not start the
whole environment automatically.

Then run the approved production-like load/duration and collect time-series
memory/queue/resource trends, latency/error distribution, acknowledged-write
ledger, reconnect/final convergence and teardown. If the historical 5k-session,
30-minute envelope is retained, measure it directly; do not infer it from the
short lane. Freeze exact budgets before execution, not after seeing results.

Planning findings requiring focused correction: `cmd/soak -duration` is total
runtime. The short gate uses 100s for 10s ramp + 30s settle + about 60s active;
workflow/guide examples using 60s allow only about 20s active. Align intended
duration and assertions. Historical transport diagnostics explicitly have
`semantic_claim=false`; add real semantic evidence before claiming write safety.

### O2: recovery and graceful drain

Prerequisites: C1/C2 green and independent review; fresh owned targets; explicit
fault approval; a stable candidate. No runnable unsafe chaos recipe is included
in this plan. There is currently no `make test-recovery` target.

Cover PostgreSQL interruption/restart, server crash/restart, acknowledgement
boundaries, checkpoint/WAL replay where actually used, reconnect, loaded SIGTERM
drain and backup/restore drill in isolated resources. Assert exact acknowledged
state survives, no phantom state appears and clients converge within the approved
budget. Exercise actual production invalidation/direct notification/optional bus
assembly: testing a separate tailer is not proof of serving-path recovery.

The earlier no-DB SIGTERM smoke establishes neither recovery nor loaded drain.
Any required source repair gets a specific transferred lease and regression;
do not let a runtime worker modify multiple owners' packages opportunistically.

## 8. Wave 4 — performance acceptance and remaining decomposition

### P1: qualified performance evidence

First close B1 and the release-policy conflict. Predeclare host/resources,
candidate/harness revisions, fixture/config hashes, required cells, run counts,
warmup/active periods, semantic prerequisites and acceptance budgets. Choose full
matrix versus named subset before collecting results. External runs need approval.

`make bench-run` requires either explicit `BENCH_CONFIG` or `SYNTHETIC=1`;
`make bench-verify BUNDLE=...` verifies an existing bundle. Synthetic runs are
tool acceptance only. A comparative claim needs real qualified targets, full
attempt retention and offline checksum/content-root/provenance verification.

Retain the staged Wave 6 contract if selected: diagnostic stages, then the
immutable seven-block/21-attempt run; failed qualification stops execution;
interruption does not authorize replacement/restart or combining partial bundles.
No tuning during measurement, selective attempt removal or parallel noisy work.

Specifically evaluate the canonical encoder's approximately 15,218 versus 9,028
allocations on the historical 300-entity microbenchmark. Re-measure on the exact
candidate and quantify end-to-end cost. If optimization is needed, optimize the
single canonical path and retain all wire goldens; do not restore duplicate
algorithms automatically. Earlier delta allocation improvements are not latency
proof under a different workload.

### K1: remaining code-quality boundaries

Current command/docs relocation is done; no second root directory redesign is
required. Keep `cmd` entry points thin; SQL migrations in their owning package;
generated outputs beside their authoritative schema; corpus artifacts clearly
separate from private live evidence; docs grouped by purpose.

Reassess the nine >500-line files only after contracts stabilize: authn core;
benchharness runner/session; benchrun runner/live config/collectors/adapter;
instaql query; reactive incremental. Split for distinct policy/ownership and
independent tests, not line quotas. Each proposed extraction records callers,
before/after responsibility and preserved tests; deduplicate only proven identical
policy. No generic helper frameworks, fallback-on-error success paths, dead
wrappers, comment-only scaffolding or TODO placeholders masquerading as completion.

Use each relevant lease holder for package-local changes; shared abstraction
proposals require coordinator review. A justified no-change decision is valid.
Update the whole-package scorecard from the final source, not stale size counts.

## 9. Wave 5 — integrated candidate, independent gate and delivery

### Existing commands, to run against the candidate

```sh
make lint vet build
make test-unit
make test-integration INTEGRATION_PARALLEL=4
make test-contract
make check-generated
make bench-acceptance
make container-verify
make bench-verify BUNDLE=/explicit/qualified/bundle
```

Set `DATABASE_URL` only to the verified dedicated environment for live lanes;
testkit owns individual databases. Run focused desired-behavior tests explicitly
and ensure repaired known gaps are in default discovery. Run example builds and
changed language checks when relevant; preserve strict warnings and race checks.

`make test-release` also invokes short soak and differential, but **does not**
create the raw benchmark bundle, provision endpoints, run long soak/recovery, or
produce release assets. Its success is necessary only within its declared scope,
not a complete production certificate. Extend gate wiring only after the missing
contracts are implemented; never change a failing gate into an advisory message.

### Independent acceptance checklist

- Every exposed auth/query/data-integrity risk repaired and reviewed, or release
  use technically excluded by an explicit approved support contract.
- Exact candidate SHA and clean tracked tree; untracked private artifacts cannot
  affect build inputs. Toolchain/dependency/config/fixture identities recorded.
- All required tests actually executed; zero selections/skips are not passes.
- Whole supported corpus matrix, genuine v1 evidence and accepted differences.
- Native Linux/container/DB/TLS evidence for the chosen deployment envelope.
- Soak/recovery/loaded drain and approved performance/resource budgets satisfied.
- No unresolved unsafe harness path can be invoked by the release workflow.
- Fresh non-author review of high-risk diffs, regression assertions and evidence;
  review identifies its context limits rather than claiming pristine independence.
- Incident disposition and source-delivery decisions recorded honestly.
- Self-host docs use the actual Go version and invalidation model; release policy,
  command durations and support matrix agree with code.

### Release artifacts and rollout (only after authorization)

Choose version/registry/target host and signing authority. Produce immutable
binaries/image, checksums, dependency/SBOM inventory and provenance; signatures
or attestations according to the chosen distribution policy. Existing workflows
do not already supply this publishing lane. Keep release scope proportional to
the chosen self-host/package delivery rather than inventing a platform project.

Publish a final evidence index, configuration guide, backup/restore and upgrade/
rollback runbook. Prove rollback against compatible schema/data before relying on
it; never assume binary rollback reverses a migration. Deploy a bounded canary
only to an approved target, observe predeclared health/error/resource/correctness
budgets, then widen or stop/roll back under the approved runbook. Do not request
real customer traffic or credentials for an unapproved preview.

## 10. Scheduling and completion report

Dependency order:

```text
Wave 0 containment + commit-scope decision
  ├─ S1/S2/S3/S4/S6/S7/S8 independent bounded repairs
  ├─ S5 corpus-engine characterization and fixtures
  └─ S9 read-only runtime prerequisite qualification
       ↓ contracts + reviewed fixes integrated
Wave 2 supported corpus + pinned-v1 comparison
       ↓ stable candidate + safe harness + approved runtime targets
Wave 3 native/container + soak/recovery
       ↓ quiet qualified host + frozen budgets
Wave 4 performance + justified final decomposition (reverify if code changes)
       ↓ immutable final candidate, rerun all affected evidence
Wave 5 independent release gate → approved delivery/canary
```

Native/container setup and independent prerequisite checks may overlap earlier
waves; final evidence must be rerun for the integrated candidate. Do not promise a
wall-clock completion time until v1 bootstrap, external targets, policy decisions
and budgets are known. Parallelism reduces independent implementation time, not
required soak duration or review dependencies.

At each wave report requirement IDs, owner, source revision/fingerprint, proposed
versus executed tests, red/green evidence, artifacts, review findings and exact
blockers. Final status remains PARTIAL while any required gate lacks evidence.
The requested end-to-end write-up must distinguish implemented, tested locally,
verified on native/runtime targets, compatible, release-accepted, and deployed.

## Planning review

A fresh-context, read-only Sol reviewer checked the incident record, source
sanitation, roadmap ownership/dependencies, evidence boundaries and local links.
Verdict: PASS for the planning/follow-up handoff, not production acceptance. Its
non-blocking request for an explicit rate-limit capacity policy decision is now
included above. The reviewer did not execute tests, query the database or
independently reproduce product risks; fresh fixture results remain coordinator-
executed evidence. Product work packets are still proposed.
