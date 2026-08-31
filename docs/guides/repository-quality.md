# Repository ownership and code-quality standard

This guide governs implementation structure, not a service-architecture rewrite.
Protocol, persisted data, authorization, transaction boundaries, and benchmark
policy remain explicit compatibility contracts.

## Filesystem

```text
cmd/                 runnable Go commands; private command assembly and helpers
internal/            production packages, with tests beside their owners
  protocol/          handwritten protocol plus generated types and schema
  platform/          catalog, rules storage, and embedded migrations
  testkit/           opt-in, isolated integration-test infrastructure
  httpjson/          small shared JSON transport mechanics
corpus/              registered wire scenarios, fixture profiles, oracle manifest
benchmarks/          benchmark policy inputs, scripts, and result conventions
examples/            client usage examples
scripts/             repository verification entrypoints used by Make and CI
docs/guides/         operational and contributor instructions
docs/reference/      contracts, standards, current evidence
docs/plans/          active plans, decisions, and execution ledger
docs/archive/        historical assessments, retained with their original claims
```

Command-specific helpers stay in their command package until there is an actual
second consumer. Domain logic stays in its owning internal package. Do not move
embedded migrations away from their embedding package, edit generated output by
hand, or introduce generic `utils`, `common`, or `manager` packages to hide unclear
ownership. Generated binaries belong in ignored output locations, not source
folders.

## Decomposition and abstraction

Split by a stable responsibility and a reason to change: input decoding, catalog
resolution, transaction phases, transport lifecycle, evidence collection, or
rendering. A coordinator should reveal phase order without hiding state changes
behind a chain of one-line wrappers. A file-size threshold is a review trigger,
not a quota: new handwritten files over 500 lines or functions over 100 lines
require a documented explanation of cohesion and a considered split. Smaller
files do not automatically mean a simpler implementation.

Prefer private concrete types for controlled shapes. Retain dynamic values at
genuinely dynamic JSON/query/userinfo boundaries. Decode and validate once at a
boundary; do not scatter unchecked assertions, partial parses, or coercions that
silently lose integer precision. Name the supported numeric domain and equality
policy rather than assuming all JSON consumers want identical normalization.

Deduplicate only after comparing semantics, callers, errors, ordering, escaping,
and lifecycle. Share mechanical HTTP decoding/writing without inventing one
universal response envelope. Keep only one canonical node-list encoder. Retain
an optimized second path only with measured benefit, explicit compatibility
tests, and an owner; benchmark latency separately from allocation cost.

## Evidence before changing behavior

For extraction, preserve declarations and SQL where practical, run existing
characterization tests, then simplify in a separately reviewable step. For a
behavior repair, first demonstrate the intended invariant failing through a
real entry point. Pure helper tests do not prove an unreachable production bug.
If the pre-fix execution could damage data, record static baseline evidence and
verify only the safe isolated treatment; never run a dangerous reproduction for
the sake of a red test.

Keep wire fields, JSON escaping, permission checks, commit/rollback boundaries,
artifact hashes, and error order in the review contract. A change to any of these
is not a mechanical cleanup. Reviewers must inspect control/data flow and the
test assertions, not merely accept a worker's green-test claim.

## Test lanes and fixture ownership

Use `make test-unit`, `make test-integration`, and `make test-contract` for their
declared lanes. Integration requires `INSTANT_TEST_INTEGRATION=1` and a dedicated
`DATABASE_URL` with database-creation privileges; use `testkit.NewPostgres` rather
than connecting test fixtures directly to a shared database. Each fixture owns
its database and cleanup; replication slots must also be uniquely owned. Never
reset a shared `public` schema. Unit mode must not contact a database merely
because a developer's shell has a DSN.

Concurrency tests must signal admission, drain completion, send completion, or
another actual transition. A fixed sleep is not readiness proof. Bound waits and
release barriers on failure. Test relevant live paths with race detection; race
freedom alone does not prove notification/order semantics.

`make lint` and `make vet` are mandatory. Handle errors that affect correctness;
explicitly discard only understood best-effort cleanup errors. Use narrow,
explained suppressions for frozen compatibility strings or platform-specific
stubs, not global rule disabling. A failing command, skipped integration test,
missing tool, or absent external oracle is not a passed gate.

## Parallel work and exceptions

The coordinator owns requirements, contract decisions, shared files, integration,
and final evidence. One writer owns each package at a time. A handoff includes
changed paths, preserved invariants, deleted complexity, exact commands/results,
and open risks. Non-author Sol review is required for security-sensitive,
transactional, concurrency-sensitive, or public-compatibility changes. Review
followups return to the owner rather than introducing competing writers.

Exceptions record the invariant, rationale, evidence, owner, and followup trigger
in the execution ledger. Existing risks are not silently fixed as part of a file
move, and uncovered release gates are never hidden by a broad “production-ready”
label. Commit, push, deployment, and destructive external cleanup require their
own authorization.
