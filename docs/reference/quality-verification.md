# Production code-quality verification

Date: 2026-08-31. Baseline: `5f78ca0877c1a8c04b173e5c502e8c948ecfb965` on
`main`; result is an uncommitted working tree, not a published release.

Later checkpoint: that implementation was committed as `eb88232` on
`codex/quality-implementation-checkpoint`. See the
[incident/delivery closure](quality-incident-followup.md#committed-development-checkpoint)
for the user's pre-release disposition and fresh minimum commit checks. The
results below remain historical evidence, not a new production-hardening run.

Status: **PARTIAL against the full approved program**. The structural and
targeted correctness work is implemented, with non-author Sol reviews. Final
repository checks are recorded below. Full corpus breadth and genuine pinned-v1
comparison, container runtime, release soak, and release artifact evidence are
not established. No claim of complete parity or production readiness is made.

## What changed

The work concentrated on implementation ownership and code quality, not a new
service architecture. Target execution, transactions, reports/artifacts, admin
routes, OAuth records, sync projection, and backup/HTTP operations now have
cohesive package-local files and explicit coordinators. Runnable tools live under
`cmd/`; documentation is grouped into guides, reference, plans, and archive.
Embedded SQL migrations stayed in their authoritative package.

This is more than relocation: the node-list duplicate algorithm was deleted;
transaction dead capture/wrappers were removed; notification fallback was
centralized; user pagination moved before projection; reactive delta processing
reuses decoded arrays/indexes; shared HTTP decoding rejects partial input; corpus
normalization uses one traversal with explicit field policy. The retained dynamic
JSON boundaries are intentional, not hidden behind casts or generic frameworks.

Illustrative coordinator sizes (lines, not a performance metric):

| File | Before | After |
|---|---:|---:|
| benchharness/target.go | 3,008 | 323 |
| transact/apply.go | 1,173 | 110 |
| adminapi/adminapi.go | 1,232 | 112 |
| benchrun/report.go | 1,205 | 125 |
| benchrun/artifact.go | 1,069 | 263 |
| authn/oauth.go | 611 | 234 |
| sync/session.go | 577 | 285 |
| cmd/benchsmoke/main.go | 1,363 | 141 |
| cmd/chaos/main.go | 1,042 | 42 |
| cmd/instantd/main.go | 843 | 71 |

The full [package scorecard](quality-scorecard.md) records retained large files,
remaining risks, and no-change decisions. New tests add code; these numbers must
not be presented as an equivalent reduction in total repository size.

## Reproduced correctness repairs

| Boundary | Failing observation before repair | Verified treatment |
|---|---|---|
| Benchmark decoding/hashing | Adjacent integers above 2^53 rounded/aliased; query selection matched both | Exact signed/unsigned 64-bit integers and finite-range integral exponent equivalents; safe-number snapshots retained |
| OAuth persisted records | Mandatory-field corruption could reach exchange/consume state | Typed validation before use; 20 corruption cases reject without changing records |
| HTTP JSON | Valid prefix plus trailing input/read error was accepted or misclassified | Explicit malformed-body response; aliases and valid escaping preserved |
| Storage authorization | Token for app A could sign/delete app B data | Four control paths bind target UUID to authenticated UUID; six allowed and six denied/alias cases |
| Storage request limit | Limiter-created EOF concealed overflow after valid JSON | Exact cap accepted; whitespace, second-object, and junk overflow rejected |
| Backup import | Reader error after checksum could commit an app | Scanner error checked before commit; rollback asserted |
| Backup object restore | Missing object store panicked | Existing unavailable response returned |
| Catalog cache | Old in-flight catalog/rules load could repopulate after invalidation | Per-app version check retries before publishing/returning stale snapshots |
| Reactive invalidation | Known suffix revived an unknown/capped batch; old drain consumed next epoch changes | Unknown knowledge stays sticky; transactions and change maps detach together |
| Tracing startup | Resource schema 1.34.0 conflicted with dependency default 1.40.0 | Matched semconv version; real OTLP export/resource/shutdown test |
| Artifact checksums | Destination write failure returned success | Error propagates through index writer and finalization |
| Verification tooling | Lint failure was advisory; Make 3.81 lost revision variables | Required lint, explicit shell continuity, full-pin mismatch/forwarding tests |
| Filtered golden tests | Selecting one subtest changed generated input | Inputs generated before subtest selection; all 82 goldens unchanged |

Slot creation also now honors `Tailer.Slot`, matching the streaming path. Its
pre-fix live execution was deliberately not run because it targeted a shared
literal slot; static mismatch evidence plus isolated configured-slot lifecycle
tests were used. This exception is recorded rather than inventing safe RED proof.

## Evidence and environment

Local environment: macOS/arm64, Go 1.27.0, GNU Make 3.81, golangci-lint 2.13.1,
PostgreSQL 17.11. Module and Docker builder declare Go 1.25. Dedicated local
PostgreSQL used port 55487 with logical WAL; tests created separate owned
databases/slots. Default developer PostgreSQL was not used after the incident
described below.

Final cleanup inspection found no remaining test-owned databases or replication
slots. The dedicated cluster was stopped; its temporary data/log directory was
retained. No cleanup was attempted against the default developer database.

| Check actually run | Result |
|---|---|
| Pre-change `go test ./... -short -count=1` | PASS; live suites skipped without prerequisites |
| Current `make test-unit` | PASS: race enabled; 31 packages passed, four had no tests; live mode disabled |
| Current repository golangci-lint | PASS: zero issues |
| `make vet build` | PASS |
| `GOTOOLCHAIN=go1.25.0 GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` | PASS using the declared toolchain |
| `make test-contract` with dedicated DATABASE_URL | PASS: validate, package race, discover test, real 16-scenario v2 replay |
| `make check-generated` | PASS: fresh Go/TypeScript generator output byte-compared without editing tracked files |
| Source protocol operation/hash tests | PASS; actual handwritten constants checked, generated hash matches schema |
| `make test-integration INTEGRATION_PARALLEL=4` | PASS: all 35 package results successful/no-test; race enabled against separate owned PostgreSQL databases |
| Vite example `pnpm build` | PASS: TypeScript + production bundle using existing dependencies |
| Local `bin/instantd` no-database startup and SIGTERM | PASS: loopback health200 `{"ok":true,"db":false}`, clean exit0, ports released; not Docker evidence |
| Python example syntax/AST | PASS; no SDK network execution |
| Scoped Linux benchmark CLI build/test compilation/lint | PASS; Linux-specific runtime tests not executed on macOS |
| Formatting and `git diff --check` | PASS; repeated after source integration |

Individual owners also ran live/race suites and targeted repeated failure tests.
Non-author reviewers reran transaction, cache, reactive, sync, HTTP/backup,
fixture/slot, corpus, CLI, and benchmark checks. Benchmark reports retain six
summary/rendering snapshots plus artifact/checksum/content-root comparisons.
Direct command outputs were inspected; zero selected tests were not accepted as
evidence.

## Corpus and performance limits

The corpus contains **16 authored v2 regressions**, **20 narrow covered
surfaces**, **nine explicit gaps**, **two unsupported surfaces**, and **zero v1
captures**. Its real fixture bootstrap exercises migrations, transactions, rules,
WebSocket dispatch, queries, and notifications. The
[coverage/oracle matrix](../../corpus/README.md) is authoritative.

Cursor advancement and aliased cardinality-one relations have opt-in desired
behavior reproductions that were run and failed for the intended reason. They
are `PROVEN_RED`, not passed or fixed. Their exclusion from the ordinary corpus
is explicit. Broader transaction/permission/auth/room/delta/SSE/HTTP capture remains
incomplete; package tests are not relabeled as a v1 oracle.

The canonical generic node-list path matches the original generic allocation
profile, but uses roughly 15,218 allocations for the measured 300-entity workload
versus roughly 9,028 for the removed typed path. This is an acknowledged
maintainability tradeoff. The 10k-entity delta microbenchmark reduced allocations
from about 80,344 to 60,206 and bytes from 13.74 MB to 8.20 MB. Timing was noisy
under parallel work; no throughput or latency improvement is claimed. Benchmark
integer equality deliberately excludes equivalence between `1e400` and its
expanded 401-digit literal.

## Review and preservation

All implementation workers used the user-selected GPT-5.6 Sol. Ten were used in
the initial parallel batch; disjoint package leases prevented concurrent writers.
At the user's request, work paused for a Standard service-tier transition and
resumed after confirmation. Global config was verified as `default`; tools do
not expose actual per-request billing tiers.

Independent review here means non-author agents reviewed unfamiliar assigned
packages. The runtime's total-thread cap prevented spawning additional pristine
review threads, so existing agents cross-reviewed non-owned work in new bounded
briefs. These were not author self-approvals. Review found and caused repairs to
storage authorization, size-limit handling, golden selection, Make shell
continuity, and historical CLI-layout fallback.

Pre-existing dirty README, reactive, storage, sync, performance-plan, and query
benchmark work was retained. Original sync assertions were captured in exact
wire fixtures before the user-approved canonical-path change. Moves preserve
history/content; original SQL migration comments retain historical document
paths to avoid rewriting migration bytes. Generated protocol descriptions/header
paths and schema hash changed only to reflect documentation/command relocation;
wire operation definitions did not change.

No source commit, push, deployment, external issue creation, or production data
operation was performed. Build/test tools created ignored local binaries and
temporary verification artifacts. Temporary Git commits used by a test are
confined to that test's private directory, not this repository.

## Disclosed verification incident

The subsequent [incident follow-up](quality-incident-followup.md) records a fresh
read-only snapshot, retained private schema/log evidence, and repeated fixture
checks on a new dedicated cluster. It found versions 1–6 recorded in the incident
window and all fifteen application tables currently empty. Historical pre-incident
state is still unknown; no rollback or default-database cleanup was performed.

Before the dedicated-cluster instruction reached one worker, a transient testkit
DSN bug caused two `platform.Migrate` calls to target the default local `postgres`
database while queries used a private fixture. The actual schema delta versus
its prior migration state was not measured. No shared schema was dropped and no
destructive recovery was attempted. The user was notified during execution.

The DSN constructor was corrected. Repeated live isolation tests now open both
the fixture pool and its returned DSN and assert `current_database()` equals the
unique fixture name, preserve exact cross-fixture data, and verify cleanup.

## Remaining release gates

Docker was unavailable; container health/non-root/trust-store checks are coded
but not executed locally. No final release soak or externally approved live
benchmark bundle was produced. The pinned v1 checkout exists, but equivalent v1
fixture provisioning and actual differential endpoints/evidence are missing.
`record` remains explicitly unavailable. None of these gates is waived by passing
unit tests or by the speedrun request.

Use the [scorecard risk list](quality-scorecard.md) to prioritize the next bounded
work. In particular, query correctness, OAuth consumption/expiry, immediate
notifier retries, and unsafe chaos-tool deletion/evidence behavior remain open.
The next release decision must resolve these risks and run the missing external
lanes; this report is not a recommendation to deploy unconditionally.

The [production completion roadmap](../plans/production-completion-roadmap.md)
now defines those follow-up packets, exclusive agent ownership, dependencies and
acceptance gates. It is a forward plan, not evidence that the repairs have run.
