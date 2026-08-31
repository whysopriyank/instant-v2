# Production-quality execution ledger

Status: IMPLEMENTATION HANDOFF; FULL PROGRAM PARTIAL. User approved execution
and resumed on 2026-08-31.
Coordinator owns this document; workers return evidence rather than editing it.

Forward work is now specified in the
[production completion roadmap](production-completion-roadmap.md). The separate
[incident follow-up](../reference/quality-incident-followup.md) contains the fresh
read-only database inspection and retention/isolation evidence. The historical
results below are not superseded into claims of complete production readiness.

## Final results

GREEN is bounded to the named invariant, not whole-product readiness. See the
[verification report](../reference/quality-verification.md) for actual commands
and the [package scorecard](../reference/quality-scorecard.md) for remaining risks.

| ID | Result | Evidence / qualification |
|---|---|---|
| R1 | GREEN | Driver decomposition, original declarations/tests retained, partial setup cleanup tested; independent S5 race/review PASS. |
| R2 | GREEN | Atomic transaction order and SQL preserved;40 top-level live/race tests; independent S7 PASS. |
| R3 | GREEN | Exact pagination/notification characterization before/after; repeated live race; independent S10 PASS. |
| R4 | GREEN | Six report snapshots, artifact/hash snapshots, checksum write-failure regression; S5 PASS. |
| R5 | GREEN | One generic projection;82 unchanged goldens; filtered-subtest repair; S7 PASS. Higher allocations explicitly accepted as a tradeoff. |
| R6 | GREEN | Delta/oracle tests, sticky-unknown and detached pending epoch regressions; S7 PASS. Retry policy unchanged. |
| R7 | GREEN | Twenty persisted-corruption regressions and strict route-body tests; S10 PASS. Existing OAuth policy gaps remain open. |
| R8 | GREEN | Exact canonicalization, manifest, failed-differential handling and private evidence files; independent S2 PASS. |
| R9 | GREEN | HTTP decomposition, cross-app storage binding, capped-body rejection, backup rollback/nilstore regressions; S10 PASS. |
| R10 | GREEN, bounded | Strict gates, isolated pool/DSN/slot fixtures and portable Make pin check; declared Go1.25 Linux build PASS. Container runtime remains unverified. |
| R11 | GREEN, bounded | Exact int64/uint64 and finite integral exponent decoding/hash/query regressions; extreme overflow equivalence excluded. |
| R12 | GREEN for review/isolation | All35 packages scored; full four-package parallel live/race suite PASS; no shared public resets. Discovered risks are not all repaired. |
| R13 | GREEN | Eight tool and seventeen doc moves complete; package/build/link/command/generator checks passed; old-HEAD chaos layout fallback preserved. |
| R14 | GREEN, bounded | Real OTLP export, cache/bus/WAL/sync barriers and repeated race tests; full-process crash/drain not claimed. |
| R15 | PARTIAL | Sixteen real v2 authored scenarios;20 narrowly covered surfaces,9 gaps,0v1 captures. Two desired query-gap reproductions remain PROVEN_RED. |
| R16 | PARTIAL | Standard/scorecard/report and independent non-author reviews complete; release/container/soak/v1 evidence and risk resolution remain open. |

Additional cache invalidation regression is GREEN: both catalog and rules loads
retry after an intervening per-app invalidation; deterministic RED/GREEN plus
repeated race and independent review passed. The Make revision test uses a
private two-commit Git fixture and silent recipes so shallow history/echo cannot
counterfeit evidence. The metrics repeat-registration reproduction remains
PROVEN_RED and is explicitly recorded, not repaired.

All write leases were released. Final unit/race, full parallel PostgreSQL/race,
lint/vet/build, declared-toolchain Linux build, generated-output checks, and real
corpus replay passed. No commit, push, or deployment was performed. Full-program
status stays PARTIAL; default-suite success does not waive R15/R16.

Final fixture audit found only the dedicated bootstrap `postgres` database and
no replication slots. The dedicated test server was stopped after verification;
its temporary data/log directory is retained, not recursively deleted. The
server-running statement in the historical checkpoint below is no longer current.

## Historical service-tier checkpoint (2026-08-31)

User requested every subagent move from Fast to Standard while retaining Sol.
The global Codex config now reads `service_tier = "default"`; agent definition
files have no service-tier override. No exposed collaboration control can change
or verify an already-running agent's request tier. All ten agents were interrupted
to preserve work without knowingly continuing at an unverified tier. This is not
completion. User subsequently confirmed “resume”; all ten agents resumed from
their existing leases. The live request tier remains unobservable through tools.

Leases and next actions at that checkpoint (subsequently completed above):

- S1: benchharness complete with unit/race/vet/lint and bounded integer-precision
  evidence; now splitting tools/benchsmoke and tools/chaos. Preserve flags and
  destructive safeguards; do not execute chaos. CLI lease not yet handed off.
- S2: transact extraction verified; now reproducing/fixing catalog cache
  invalidation versus in-flight loads in platform/catalog_cache.go and tests.
- S3: adminapi verified; now decomposing cmd/instantd assembly. Not complete.
- S4: benchrun extraction, checksum write-error repair and lint complete; final
  package/race checks and independent review still need consolidation.
- S5: sync canonical generic encoder, 82 goldens, session split and deterministic
  tests complete; final live/race results need collection. Benchmark allocation
  tradeoff is documented in internal/sync/testdata/README.md.
- S6: bus/waltail isolation and configured-slot fix verified; now fixing reactive
  unknown-to-known knowledge loss and drain snapshot ownership with regressions.
- S7: non-author review of transact/reactive/tracing/testkit/httpjson; uncontested
  scope passed independent checks, awaiting S2/S6 final fixes.
- S8: corpus engine/manifest, 11 initial authored scenarios plus expansions;
  cursor/relation desired-behavior cases exposed existing query gaps. Do not
  change expected output to conceal a defect; root decision still required.
- S9: HTTP/backup decomposition and tests; reproduced/fixed cross-app storage
  authorization binding and capped-body fake EOF. Final full rerun pending.
- S10: non-author HTTP/auth/admin/backup review; confirmed S9 fixes in source,
  independent targeted post-fix verification pending.

Root remaining at the checkpoint: complete command moves only after leases release; repair all
documentation and command consumers; finish scorecards/verification report;
run integrated lint/vet/build/unit/live-race/contracts and independent final gate.
Documents 01–17 were moved to guides/reference/plans/archive, but global reference
updates are NOT complete. tools/ entries have NOT yet been relocated. The new
docs/README.md and quality guide include planned report links not yet written.

Dedicated PostgreSQL remains at 127.0.0.1:55487, data directory
`/tmp/instant-quality-pg.9QIyWF/data`, for resumption. Only use the dedicated
bootstrap DSN `postgres://priyank@127.0.0.1:55487/postgres?sslmode=disable` through
testkit with explicit integration mode. Root owns shutdown after checks finish.
Linter: `/tmp/instant-quality-tools.aTcn07/golangci-lint` v2.13.1.

Important disclosed side effect: early S3 tests called platform.Migrate twice
through a transient defective fixture DSN against the default local `postgres`
database before the dedicated-cluster instruction arrived. Actual delta from
prior migration state was not measured. No shared public schema was dropped and
no destructive recovery attempted. S10 corrected the fixture DSN; live tests now
assert both Pool and DSN connect to the unique owned database and cleanup works.

External gates remain unverified: Docker unavailable, no genuine pinned-v1
differential run, no final release soak/artifact bundle. Do not label these passed.

## Baseline

- Branch `main`, HEAD `5f78ca0877c1a8c04b173e5c502e8c948ecfb965`.
- Existing dirty files: README.md; reactive/changes.go, gate_test.go,
  reactive.go, reactive_test.go; storage/storage.go; sync/nodelist.go,
  nodelist_test.go. Existing untracked: docs/16-product-performance-headroom.md,
  docs/17-production-code-quality-plan.md, instaql/plan_bench_test.go.
- Tracked baseline diff: 939 additions / 50 deletions in eight files. Preserve
  this work except the explicitly approved node-list restructuring.
- Local Go: go1.27.0 darwin/arm64. Module declares Go 1.25.
- PostgreSQL CLI/server binaries available. Docker and golangci-lint are not
  currently on PATH. Pinned v1 checkout exists at ../instant, revision
  a4d2ef33b60f281a437191006e4541d4780f9e4a; runtime readiness is not established.
- Graph indexed but stale; forced refresh failed. Use graph for orientation,
  direct current source for changed/excluded paths; no completeness claim.
- Baseline `go test ./... -short -count=1`: launched before source changes;
  result recorded during integration. Live DB prerequisites not yet provided.

## Frozen contracts

Exported Go signatures, JSON/wire fields and ordering, benchmark policy/artifact
formats, persisted schema, auth/permission decisions, transaction commit and
rollback boundaries remain stable during extraction. No cross-package signature
change without coordinator review. Pure refactoring has no expected red test;
behavior fixes require a focused desired-invariant failing test before treatment.
Malformed JSON maps to existing client-error conventions, never a successful
partial parse; persisted OAuth corruption fails at decoding. Number semantics
must preserve >2^53 precision without changing unrelated benchmark equality.

## Approved contract plan

This retains the planned observations. Actual results and qualifications are in
the final-results table above; the following table is not a current task queue.

| ID | Invariant / real path | Scope / owner | Planned evidence and verification | Red expectation | Status |
|---|---|---|---|---|---|
| R1 | Target driver decomposed; session/evidence/wire contracts unchanged | benchharness / S1 | Existing target/artifact tests, size inventory, package race tests | None for extraction | See final results |
| R2 | Atomic phased Transact and clear lowering | transact / S2 | Mutation/required/perms/merge tests, live PG rollback, package tests | None for extraction | See final results |
| R3 | Admin route groups and single notification flow; user page semantics retained | adminapi / S3 | Route/highlevel tests, pagination fixture, package tests | Characterize current output | See final results |
| R4 | Validated-input report pipeline and focused artifact modules | benchrun / S4 | Report/triad/artifact acceptance tests, deterministic output, race tests | None for extraction | See final results |
| R5 | One canonical node-list projection, exact wire values | sync / S5 | Existing parity/differential fixtures adapted without losing assertions, package/race checks | Characterize approved input domain | See final results |
| R6 | One-decode delta; pending/routing invariants preserved | reactive / S6 | Delta roundtrip, incremental/full comparisons, gate/retry tests | None unless defect reproduced | See final results |
| R7 | Typed OAuth state and explicit HTTP input failures | authn / S7 | Malformed/state regressions RED then GREEN; existing OAuth/token tests | Bad shape/partial decode accepted | See final results |
| R8 | Canonicalization preserves declared equivalence; complete honest corpus inventory | corpus + corpusctl + scenarios / S8 | Canonical idempotence, loader/manifest validation, replay/differential readiness | Missing manifest/unsafe normalization tests as applicable | See final results |
| R9 | Runtime/storage/backup boundaries coherent and compatible | runtimeapi/storageapi/backup / S9 | HTTP/backup/objectstore tests, malformed input regressions | Ignored decode failures | See final results |
| R10 | Strict gates and isolated test fixture contract | root CI/Make/Docker/testkit / S10 | make target tests, missing-DB failure, fixture isolation, toolchain check | Advisory lint / shared fixture | See final results |
| R11 | Benchmark number decoding preserves exact integer distinctions | benchharness / S1 | Public decode regression around 2^53, nested values; RED then GREEN | Adjacent integers alias | See final results |
| R12 | Remaining packages reviewed and fixtures adopted without shared public reset | remaining package leases | Per-package scorecards, parallel live PG suite and race tests | Shared reset where present | See final results |
| R13 | CLI and docs filesystem moved with all consumers updated | coordinator after handoffs | go list/build, command/reference/link checks, CLI tests | Old layout inventory | See final results |
| R14 | Direct tracing and deterministic concurrency evidence | renewed package owners | In-memory/exporter tests, barrier-based focused race checks | Missing specific assertions | See final results |
| R15 | Implemented-surface corpus coverage and real pinned-v1 comparison | corpus coordinator | Manifest route/surface matrix, v2 replay, v1/v2 raw+normalized output | Corpus breadth missing | See final results |
| R16 | Final quality standard and independent falsification | coordinator + Sol reviewers | Size/diff inventory, lint/vet/build/race/integration/contracts, fresh review | Audit findings outstanding | See final results |

## Executed cross-root migration map

Every move below is complete. Package splits preserve imports; embedded
migrations and protocol generated files stay in place. Old source paths in this
table and the approved plan are intentional historical evidence.

| Source | Destination | Owner / dependency | Consumers / verification |
|---|---|---|---|
| tools/benchreport | cmd/benchreport | coordinator after S4 | Make, docs, scripts; go list/build |
| tools/benchrun | cmd/benchrun | coordinator after S4 | Make, workflows, docs; CLI tests |
| tools/benchsmoke | cmd/benchsmoke | coordinator after tool split | scripts/workflows/docs, source-reading tests |
| tools/chaos | cmd/chaos | coordinator after tool split | docs/CI, CLI build |
| tools/corpusctl | cmd/corpusctl | coordinator after S8 | Make/docs, corpus command tests |
| tools/schemagen | cmd/schemagen | coordinator after protocol checks | Make/go:generate/docs, regeneration check |
| tools/soak | cmd/soak | coordinator after CLI handoff | Make/CI/scripts/docs, soak invocation |
| tools/soaksetup | cmd/soaksetup | coordinator after CLI handoff | Make/CI/scripts/docs, setup tests |
| docs/01-state.md | docs/archive/01-state.md | coordinator | all tracked links/comment references |
| docs/02-architecture.md | docs/reference/02-architecture.md | coordinator | all tracked links/comment references |
| docs/03-protocol.md | docs/reference/03-protocol.md | coordinator | all tracked links/comment references |
| docs/04-roadmap.md | docs/plans/04-roadmap.md | coordinator | all tracked links/comment references |
| docs/05-conformance.md | docs/guides/05-conformance.md | coordinator | all tracked links/comment references |
| docs/06-agent-orchestration.md | docs/guides/06-agent-orchestration.md | coordinator | AGENTS/tasks links |
| docs/07-selfhost.md | docs/guides/07-selfhost.md | coordinator | README/run instructions |
| docs/08-tier1-hotpath.md | docs/archive/08-tier1-hotpath.md | coordinator | evidence references |
| docs/09-tier2-architecture.md | docs/reference/09-tier2-architecture.md | coordinator | implementation comment links |
| docs/10-wave0-retract-integrity.md | docs/archive/10-wave0-retract-integrity.md | coordinator | evidence references |
| docs/11-security-audit.md | docs/archive/11-security-audit.md | coordinator | risk/evidence references |
| docs/12-audit-remediation-plan.md | docs/archive/12-audit-remediation-plan.md | coordinator | historical status references |
| docs/13-benchmark-contract.md | docs/reference/13-benchmark-contract.md | coordinator | harness/source-reading tests/docs |
| docs/14-benchmark-running.md | docs/guides/14-benchmark-running.md | coordinator | runbook links |
| docs/15-wave6-execution.md | docs/plans/15-wave6-execution.md | coordinator | benchmark instructions |
| docs/16-product-performance-headroom.md | docs/plans/16-product-performance-headroom.md | coordinator | preserve current user content |
| docs/17-production-code-quality-plan.md | docs/plans/17-production-code-quality-plan.md | coordinator | this ledger and README |

The original corpus paths were preserved. Historical evidence was moved, not
deleted. Ignored generated binaries/results were left untouched. Cross-root moves
occurred only after writers released ownership; consumer updates were mechanical.

## Evidence and handoff

Baseline full short suite passed. R14 tracing public Init test reproduced a real
failure: resource.Merge rejects schema URLs 1.40.0 (dependency default) and 1.34.0
(old semconv import). Minimal repair aligns the import to installed semconv 1.40.0;
export/identity/shutdown tests passed. No provider policy change.

Export/identity/shutdown tests passed, as did final repository checks. Full
acceptance still requires R15/R16, not merely a passing default command. Missing
external/container/v1 evidence remains explicit in the final report.
