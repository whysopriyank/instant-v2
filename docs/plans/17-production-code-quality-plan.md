# Production Code-Quality and Repository Restructuring Plan

Status: **APPROVED — IMPLEMENTATION HANDED OFF; FULL PROGRAM PARTIAL**

Current-policy note (2026-08-31): this retains the historical refactor plan and its
original all-Sol routing. The later
[implementation-first policy](production-completion-roadmap.md#current-authority--implementation-first)
takes precedence: Luna-heavy bounded work, incremental local commits, minimum
relevant tests and no production/benchmark hardening until new approval.

The user approved execution. The original design below is retained as the plan,
not rewritten into a success claim. Current results and remaining release gates
are recorded in the [execution ledger](quality-execution.md) and
[verification report](../reference/quality-verification.md).

Prepared: 2026-08-31

Scope: whole repository, including product code, repository layout, tooling,
tests, corpus, CI, and the final quality write-up.

This document consolidates the code-quality audit, the proposed remediation
program, and the subsequently requested filesystem restructuring. It is a plan,
not evidence of completed remediation. Execution starts only after the user
clearly says **yes** to this plan. Creating this document does not start Phase 0.

The earlier `12-audit-remediation-plan.md` describes a different, historical
program. Its completion labels do not apply here. For this program, the user's
Sol model requirement replaces the earlier program's Luna-heavy routing.

## 1. Objective and limits

Produce a maintainable, production-quality implementation and an evidence-backed
write-up of how the entire repository is organized and verified. Prefer deleting
complexity to redistributing it. Preserve system architecture and supported
behavior while restructuring code ownership and representation.

Included:

- Repository directory taxonomy, file naming, package-local decomposition,
  executable layout, generated files, fixtures, corpus, documentation, artifacts.
- God-file/function decomposition; duplicated behavior/helper removal; typed
  JSON/OAuth/numeric boundaries; deterministic orchestration and tests.
- Whole-repository coverage assessment, isolated integration fixtures, strict
  quality commands, corpus replay, and a pinned-v1 differential verification lane.
- Production code-quality standards, before/after scorecards, and residual risks.

Not automatically included:

- New product features, missing parity implementations, or system architecture
  redesign; production database schema/isolation changes; platform migration.
- Changes to benchmark definitions, workloads, eligibility, scoring, evidence
  budgets, artifact contracts, or approval rules.
- Hosted services, new credentials, deployment, commits, pushes, destructive
  cleanup, or deletion of historical evidence.

Fixes to numeric precision and malformed-input handling are explicit boundary
work, not hidden inside extraction patches. Any externally visible difference
requires a documented contract decision before implementation. Unsupported
features such as placeholder presence behavior must not silently become new
feature projects.

## 2. Quality standard and acceptance principles

| Concern | Standard |
|---|---|
| Ownership | Each package and substantial file has a coherent responsibility and named owner. |
| Files | No unexplained hand-written file above 1,000 lines; 300–600 lines is a useful working range, not a forced quota. Generated files are classified separately. |
| Functions | Coordinators expose phases. Functions around 80+ lines or cognitive complexity 25+ require review, not automatic helper proliferation. |
| Abstractions | No wrapper-only types, identity helpers, speculative frameworks, or generic interfaces hiding a known shape. |
| Types | Raw JSON and dynamic maps have named boundaries; controlled internal shapes use concrete types. |
| Errors | Decode/persistence failures cannot silently become empty/default data. Intentional ignored errors have a specific rationale. |
| Deduplication | One canonical implementation where semantics match; prove equivalence before merging similar-looking code. |
| Compatibility | Preserve exported contracts, routes, wire spellings, ordering, authorization, persisted formats, and transaction atomicity. |
| Tests | Unit tests are hermetic; integration mode fails on missing prerequisites; namespaces are isolated. |
| Concurrency | Observable readiness establishes synchronization; timeouts diagnose failures. |
| Optimization | Manual serializers, pooling, and dual algorithms require a measured benefit and explicit ownership. |
| Gates | Local commands and CI mean the same thing; skipped or unavailable evidence is never counted as passed. |

AI-slop review is based on observable symptoms, not claims about authorship:
duplicate implementations, obsolete explanations, weak boundaries, magical
fallbacks, and abstractions that do not earn their cost. No cosmetic cleanup
campaign should displace unresolved structural problems.

Audit correction: the float64 exact-integer boundary is **2^53**, not 2^23.
Historical graph complexity numbers are discovery hints; remeasure current
source before using them as before/after evidence.

## 3. Model, authority, and concurrency policy

- All implementation agents, including code-writing test/tooling workers and
  repair workers, use **gpt-5.6-sol**. Complex work uses the `smart_worker` role or
  an explicitly configured Sol worker. Do not substitute Luna/Terra to fill slots.
- Behavior-sensitive review and consequential decisions also use Sol.
- One main coordinator owns scope, shared contracts, filesystem moves across
  ownership boundaries, integration, documentation, and final evidence.
- Plan for up to **10 subagents**, subject to the actual runtime capacity.
  This planning session exposes four total slots; a configuration claim does not
  override that limit. Use rolling batches if the larger limit is unavailable.
- One write-capable owner per package directory, including its tests. Two agents
  must not split different files in the same package concurrently.
- Shared-workspace edits are coordinated through exclusive ownership. No worker
  resets, reverts, commits, pushes, deploys, or modifies another owner's files.
- Cross-package exported interfaces stay frozen during extraction. Request an
  explicit handoff/contract decision before changing them.
- After two failed focused repair cycles, stop and reassess with a Sol debugger
  or reviewer. Do not keep rewriting without fresh evidence.

## 4. Phase 0 — baseline, filesystem blueprint, and contract freeze

Status: approved; baseline and execution ledger recorded in `plans/quality-execution.md`.

### 4.1 Baseline

1. Record HEAD, status, current diff, untracked source/docs, toolchain, and test
   prerequisites. The current dirty worktree is the baseline; never reset it.
2. Use the codebase graph first, checking freshness and exclusions. If refresh is
   unavailable, use direct source evidence and record the limitation.
3. Inventory every package, executable, generated file, fixture, corpus scenario,
   CI entry point, and relevant document. Recompute size/complexity hotspots.
4. Record focused/full test results and which integration suites actually ran.
5. Create a compatibility register and a duplicate-helper equivalence matrix.

### 4.2 Proposed repository taxonomy

The intended layout is below. This is a destination taxonomy, not authorization
to rename every package or create empty folders. Retain existing Go package paths
where a directory move would add churn without improving ownership.

```text
instant-v2/
  cmd/
    instantd/                 # thin production entry point
    <tool-name>/              # thin first-party executable entry points
  internal/
    <product-package>/        # existing domain boundaries, decomposed internally
    benchharness/             # benchmark session/execution/evidence mechanisms
    benchrun/                 # benchmark configuration/artifact/report pipeline
    corpus/                   # replay and explicit canonicalization policies
    <tool-package>/           # reusable tool logic only when extraction earns it
    testkit/                  # fixtures; never imported by product runtime code
    httpjson/                 # conditional: proven shared HTTP mechanics only
  corpus/
    <family>/                 # scenarios; retain old paths until loader migration
    fixtures/                 # deterministic fixture profiles
    expected/                 # expected outputs where not inline in NDJSON
    manifest.json             # ownership, oracle, and coverage index
  benchmarks/
    config/
    schema/
    scripts/
    results/                  # generated/ignored outputs, existing retention policy
  scripts/                    # only cross-cutting build/test developer commands
  examples/                   # consumer examples with their own checks
  docs/
    guides/                   # current development/operations/quality guidance
    reference/                # current protocol and implementation contracts
    plans/                    # active execution plans and decision ledgers
    archive/                  # historical snapshots, moved without losing evidence
  tasks/                      # reconcile purpose with active plans; no dual status
  .github/workflows/
  go.mod, go.sum, Makefile, Dockerfile, README.md, AGENTS.md
```

### 4.3 Mandatory migration map

Before any move, produce an exact source-to-destination manifest containing:
old path, new path, reason, owner, affected imports/commands/generation directives,
CI/Docker references, documentation links, and verification command.

Planned move classes:

| Current surface | Intended treatment | Move owner / gate |
|---|---|---|
| `tools/<executable>/main.go` | Move thin first-party entry points to `cmd/<executable>`; extract large reusable logic only where cohesive. | Coordinator after tool owners return a stable split; update all invocation paths atomically. |
| Existing `internal/*` packages | Split cohesive files in place first; no blanket `runtime/` nesting or import-path rewrite. | Package owner under frozen exported interfaces. |
| Protocol generated Go/TS/schema | Keep generated outputs distinguishable and colocated where tooling requires; preserve generation/import contracts. | Protocol owner, then coordinator for path changes. |
| Embedded SQL migrations | Preserve embed-relative placement; reconcile root migration commands with actual authoritative migrations. | Platform owner plus coordinator; do not duplicate or delete migration history. |
| Package fixture/golden/fuzz data | Place next to owning tests under `testdata/`; shared fixtures only in a named shared boundary. | Owning package; no simultaneous test-migration worker. |
| Flat corpus scenarios | Introduce family organization only after loader and manifest compatibility are tested. | Corpus owner; migrate scenarios in a dedicated integration step. |
| Flat documentation | Classify current guides/reference, active plans, historical evidence; preserve useful permalinks or update every tracked link. | Coordinator only, after content classification. |
| Binaries/generated outputs | Inventory tracked/ignored state and consumers first; document output locations. | Coordinator; no automatic deletion or reclassification of evidence. |

Move-only patches precede semantic changes. Verify `go list ./...`, builds,
embed/generation paths, Make/CI/Docker command references, and documentation links
after each move batch. No compatibility wrapper is kept indefinitely merely to
avoid updating internal callers; externally consumed paths require a decision.

### 4.4 Contract decisions

- **Node-list:** one readable encoder is the default. Assess retaining the generic
  implementation and useful new regression cases. A specialized implementation
  must eliminate duplicated projection semantics and justify its complexity with
  a pre-agreed product-level performance criterion. Do not arbitrarily regress a
  proven workload or claim a component benchmark proves end-to-end improvement.
- **HTTP:** share decode/write mechanics only after proving escaping, error,
  status, and size-limit equivalence. Request-specific DTOs stay local.
- **Numbers:** preserve integer precision. Define separately where lexical JSON
  equivalence versus numeric semantic equivalence is required; do not accidentally
  make `1`, `1.0`, and `1e0` either equal or different across every consumer.
- **Corpus:** no new normalization exclusion without a scenario-specific reason.
- **Unsupported behavior:** document and test current scope; do not implement
  missing product features under the label of refactoring.

Gate: exact ownership/move manifest, shared helper contracts, baseline evidence,
and behavior invariants are recorded. Escalate material decisions not covered by
the approved plan rather than inferring authority.

## 5. Wave 1 — independent package decomposition

Each owner receives the common brief in section 11. Exported APIs and durable
formats remain unchanged, allowing dependent packages to proceed concurrently.

| ID | Exclusive ownership | Required structure and outcome |
|---|---|---|
| S1 | `internal/benchharness/**` | Split target contracts/config, readiness/qualification, session lifecycle, evidence budgeting/collection, run/warm-up, receipt correlation, refresh/entity decoding. Pure wire decoders do no I/O; evidence observes rather than controlling protocol behavior. |
| S2 | `internal/transact/**` | One atomic coordinator; catalog preparation/aliases, resolution, authorization, operation dispatch, add/retract/merge/delete operations, required-field postconditions. Split high-level parsing/resolution/expansion under the same owner. |
| S3 | `internal/adminapi/**` | Handler/routing/authentication, query/transact/perms probes, auth/token routes, user orchestration/store/projection, schema/soft-delete/presence contracts. One commit-notification fallback flow. |
| S4 | `internal/benchrun/**` | Separate manifest/run/provenance validation, validated artifact loading, observation normalization, aggregation, rendering, hashing. Decompose `artifact.go` by actual ownership too; preserve signatures, artifact layout, and verification order. |
| S5 | `internal/sync/**` | Resolve node-list design; isolate projection/encoding and lifecycle responsibilities; preserve exact accepted wire behavior and deterministic ordering. |
| S6 | `internal/reactive/**` | Decode each result once; separate entity/page/aggregate differences; clarify pending changes and scratch ownership without changing synchronization or refresh semantics. |
| S7 | `internal/authn/**` | Separate OAuth flow/state/provider responsibilities; establish typed persisted-state decoding and validation; retain authorization, redirect, cookie, PKCE, and token semantics. |
| S8 | `internal/corpus/**`, `tools/corpusctl/**`, corpus root metadata | Separate traversal from normalization policy; validate scenarios and oracle metadata; provide precise replay failures. No scenario mass migration while other corpus writers are active. |
| S9 | `internal/runtimeapi/**`, `internal/storageapi/**`, `internal/backup/**` | Typed HTTP boundaries; decompose backup codecs/import/export/HTTP concerns; retain transaction, object-store, error, and wire contracts. |
| S10 | `Makefile`, `Dockerfile`, `.github/workflows/**`, new `internal/testkit/**`, approved shared HTTP helper | Establish command/fixture/helper contracts first; do not migrate tests owned by S1–S9. CI activation follows working commands rather than adding broken mandatory gates. |

S10 may establish small shared contracts before S1–S9 start. Package owners adopt
them within their own trees. Root documentation and cross-root moves remain with
the coordinator. Helper adoption is paused until the helper contract is stable.

Within every package, use this sequence:

1. Add only missing characterization tests needed to protect an extraction.
2. Move cohesive private code unchanged; compile and test.
3. Simplify the model/coordinator using concrete private types.
4. Delete proven duplication in a separate review unit.
5. Apply explicitly approved boundary repairs with regression tests.
6. Split tests by behavior alongside the new responsibilities, not by arbitrary
   line counts. Preserve test discovery and benchmark identities where consumed.

## 6. Wave 2 — deduplication and remaining repository coverage

Do not launch a cross-cutting editor into an actively owned package. Renew package
ownership leases before adoption. The coordinator owns shared-file integration.

### 6.1 Deduplication matrix

| Candidate | Required proof and end state |
|---|---|
| HTTP read/write/string helpers | Compare malformed input, aliases, escaping, trailing input, and response errors; share mechanics without a universal response envelope. |
| OAuth records | Decode required state once into a private typed model; reject corruption at the boundary. Avoid a new persisted version unless migration is explicitly needed and approved. |
| UUID utilities | Reuse canonical parsing/formatting where semantics match; distinguish validation from random ID generation. |
| Number normalization | Preserve precision and consumer-specific lexical/semantic rules; boundary cases include 2^53 and int64 limits. |
| Node-list paths | One canonical projection; no permanent duplicated full implementation retained by default. |
| Transaction notification | One strategy selection and one fallback call site with preserved context/commit semantics. |
| Reactive delta | Reuse decoded entities rather than parsing the same representation twice. |
| Corpus recursion | One traversal with explicit policy, not repeated map-cloning special cases. |
| Report/hash helpers | Reuse authoritative artifact/hash code; remove identity wrappers such as `exp`. |
| DB fixtures | One isolated fixture contract; each package migrates its own tests. |
| Timing helpers | Direct channels/barriers where possible; no generic synchronization framework without actual reuse. |

### 6.2 Whole-repository coverage assignments

Every remaining area receives a scorecard and a targeted plan. A coherent package
may conclude with no code change; no blanket rewrite or forced file splitting.

| Area / future exclusive lease | Required review |
|---|---|
| `internal/storage`, `internal/triple` | Encoding, atomic operations, query helper ownership, fixtures. |
| `internal/platform` and migrations | Cache/catalog boundaries, embedded migration authority, isolated migration tests. |
| `internal/instaql`, `internal/datalog` | Parser/planner/executor separation, recursion, dynamic shapes, canonical helpers. |
| `internal/perms` | Typed bindings, evaluation boundaries, failure semantics. |
| `internal/bus`, `internal/waltail` | Lifecycle/checkpoint ownership, deterministic readiness/shutdown tests. |
| `internal/config`, `internal/metrics`, `internal/tracing`, `internal/ratelimit` | Configuration and observability contracts, direct tracing tests, concurrency ownership. |
| `internal/protocol`, `tools/schemagen` | Generated/manual boundary, reproducible generation, drift checks. |
| Remaining `tools/*` | CLI parsing versus execution/reporting; split `benchsmoke` and `chaos` hotspots before entry-point moves. |
| `cmd/instantd` | Thin assembly/entry point, not a second home for domain policy. Coordinator integrates dependency changes. |
| `examples`, `benchmarks` non-code assets | Build/typecheck/invocation paths, fixture and generated-output ownership; preserve benchmark policy. |

New consequential findings outside this quality scope are recorded and brought
back to the user. Do not turn this coverage pass into a new feature program.

## 7. Wave 3 — corpus and test-system completion

### 7.1 Test taxonomy

- `test-unit`: hermetic package tests; no live database/network prerequisite.
- `test-integration`: isolated PostgreSQL suites; fail clearly when dependencies
  are missing. Logical-WAL requirements are explicit.
- `test-contract`: scenario validation, golden/property tests, bounded seeded
  fuzz checks where useful, and v2 replay against checked-in expected behavior.
- `test-release`: the agreed combination of static, unit, integration, contract,
  container, soak, artifact, and external differential evidence.

Do not assume `-short` alone separates existing suites; audit and migrate their
selection deliberately. Do not invent a second bespoke test runner when Make and
small existing tooling suffice.

Fixture requirements: unique owned database/schema, scoped migrations and cleanup,
no shared `DROP SCHEMA public`, explicit capabilities, actionable prerequisite
errors. Test two packages concurrently before removing CI serialization. Database
creation privileges and logical replication may require different fixture modes;
never silently fall back to shared production-like state.

Replace fixed sleeps used as readiness assumptions with barriers or observable
events. Use bounded repeated race checks on changed concurrency tests. Add direct
tracing tests that restore global state and do not leak exporters.

### 7.2 Corpus framework and coverage

Retain current NDJSON semantics unless a specific gap requires a small extension.
Validate recording/bootstrap capabilities before promising automatic v1 capture.
Each scenario has an ID, owner, fixture profile, inputs/outputs, normalization
policy, oracle source, tags, and expected error/ordering behavior.

Parallel scenario ownership, after the engine/manifest contract stabilizes:

1. Protocol initialization, negotiation, malformed frames, and ordering.
2. Query filters, relations, order, aggregates, and invalid inputs.
3. Pagination/cursor boundaries and deterministic entity ordering.
4. Transactions, lookup refs, cardinality, required attributes, and rollback.
5. Permissions and binding/allow/deny/fallback behavior.
6. Auth, guests, magic codes, refresh/signout, and OAuth state failures.
7. Admin/runtime route, request, response, and error contracts.
8. Storage and backup round trips and failure boundaries.
9. Reactive/delta/SSE/reconnect/rooms/presence implemented behavior.
10. Migration/startup/configuration/container contracts in the appropriate test
    lane; do not force non-protocol checks into a wire scenario format.

Scenario agents own disjoint new scenario/fixture paths only. Package test changes
return to package owners; the corpus owner alone changes the engine and root
manifest. Aggregate manifest entries through one coordinator-owned integration.

### 7.3 V1 oracle gate

PR verification uses deterministic checked-in expectations and v2 replay.
External differential verification compares pinned v1 and v2 with equivalent
fixtures, captured revisions/configuration, raw/normalized outputs, and exact
diffs. Every accepted difference has a narrow rationale and owner.

Confirm the authoritative v1 source/ref, runtime/bootstrap procedure, isolated
databases, endpoint availability, and known-difference policy. Use existing
approved local assets where possible. New external access or credentials requires
direction. Unavailable differential evidence is not a pass; the full program
cannot claim complete v1 parity without actually running the designated lane.

## 8. Wave 4 — filesystem migration and CI integration

Cross-root moves are coordinator-owned and occur only after package writers have
returned ownership. Preserve test/command semantics while applying the approved
move manifest. Revalidate commands, imports, embed paths, generators, examples,
workflow references, and documentation links. Do not simultaneously rewrite a
tool and relocate its entry point in the same review unit.

Target commands:

```text
make lint
make vet
make test-unit
make test-integration
make test-contract
make build
make container-verify
make test-release
```

CI jobs invoke the same commands: static, unit/race, isolated Postgres integration,
contracts, container, and existing soak. Differential verification starts in the
explicitly agreed manual/release or PR lane; changing protected-branch settings
is an external action requiring separate authorization.

Align the Docker builder and module toolchain explicitly. Check image startup,
health, non-root identity, and required trust-store inclusion without adding new
production infrastructure. Preserve existing benchmark correctness gates.

Record file/function size and duplication improvements as review evidence.
Introduce linter ratchets proportionally; do not add noisy gates that merely
replace complexity with suppressions. Avoid a new quality-report application
when existing logs and a small evidence manifest are sufficient.

## 9. Dependency-aware scheduling

```text
User says yes
  -> Phase 0: baseline + exact filesystem/contract/ownership freeze
  -> Shared fixture/HTTP/command contracts, where justified
  -> Wave 1: independent package decompositions (up to capacity)
       transact -> adminapi reconciliation
       benchharness -> benchrun reconciliation
       corpus engine -> scenario authors
  -> Wave 2: deduplication/adoption + remaining-package coverage
  -> Wave 3: corpus breadth + isolated/deterministic test proof
  -> Wave 4: cross-root moves + CI integration
  -> Wave 5: independent review, bounded repairs, final evidence
```

Dependent packages may start extraction concurrently only while exported APIs are
frozen. An upstream contract change pauses affected downstream work. Integrate
pairwise tests after each dependency pair, then repository-wide checks.

Ten agents do not make shared-file changes or final integration parallel. The
token reset window is a checkpoint boundary, not permission to skip gates. Record
finished/active packages, ownership leases, diffs, evidence, and next actions
before a handoff. Do not promise a full corpus and release proof inside four hours.

## 10. Wave 5 — acceptance and final production-quality write-up

Each worker returns changed paths, before/after responsibilities, deleted
complexity, preserved invariants, exact checks, outcomes, and remaining risks.
Independent Sol review asks whether the refactor simplified the model rather
than merely scattering the same implementation across files.

Whole-program acceptance:

- Every package has a reviewed scorecard; each audit finding is fixed,
  superseded with evidence, or explicitly deferred with user agreement.
- Filesystem migration manifest is complete; imports, commands, links, generated
  paths, and output locations agree. No historical evidence was silently lost.
- No unexplained oversized hand-written files, duplicate semantic paths, weak
  controlled-shape boundaries, or wrapper-only abstractions remain in scope.
- Lint/vet/build pass; unit, integration, race, and contract lanes actually run.
- Parallel integration isolation is demonstrated before claiming it supported.
- Corpus coverage reaches the implemented release-relevant surfaces, with precise
  oracle attribution. No unregistered mismatch in the designated differential run.
- Relevant artifact/report/generation outputs retain their contracts.
- Retained optimizations have the required evidence and no unapproved regression.
- No gate was weakened, silently skipped, or falsely counted as passed.

Final documents, owned by the coordinator:

1. Repository filesystem and package-ownership guide.
2. Production implementation/code-quality standard and exception process.
3. Before/after scorecard and finding-resolution ledger.
4. Corpus coverage/oracle/known-difference matrix.
5. Verification report with revisions, tool versions, commands, artifacts,
   unavailable gates, and remaining risks.

Open external prerequisites yield a partial report, not a production-ready claim.
Commit/push/deploy remains outside this approval unless separately requested.

## 11. Dispatch-ready worker brief

```text
Model: gpt-5.6-sol
Objective: one bounded package-level structural outcome.
Exclusive ownership: exact directories, direct tests, permitted new files.
Context: baseline revision/worktree, contract register, filesystem move manifest.
You are not alone: preserve others' changes; never edit outside your lease.
Non-goals: no unapproved public/persisted/protocol/benchmark-policy changes;
          no commits, pushes, deployment, or unrelated cleanup.
Invariants: concrete wire/output/state/error/order/atomicity contracts.
Required design: responsibility map and complexity that must disappear.
Sequence: characterize -> extract -> simplify -> deduplicate -> verify.
Acceptance: structural conditions plus exact observable behavior.
Verification: focused commands, corpus slices, relevant artifacts/benchmarks.
Stop: cross-owner edit, changed contract, contradictory evidence, unsafe action,
      unproven parity, or two focused failed repair cycles.
Return: changed paths; before/after map; removed complexity; checks/results;
        evidence; remaining risks; requested integration action.
```

## 12. Approval record

- Planning document: prepared.
- Filesystem restructuring: explicitly included above.
- Sol-only implementation routing: recorded.
- Implementation approval: **RECEIVED — user explicitly said yes on 2026-08-31**.
- Phase 0 / rewrites / moves / tests / CI changes: **IN PROGRESS**, tracked in the execution ledger.

Execution is authorized; commits, pushes, and deployments remain unauthorized.
