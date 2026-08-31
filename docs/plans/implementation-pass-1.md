# Functional implementation pass 1

Started 2026-08-31 from `3873335` on
`codex/quality-implementation-checkpoint`, initially clean.

Status: IN PROGRESS. The user approved proceeding with the implementation-first
plan. Scope is concrete functional defects, minimum meaningful tests and
incremental local commits. No production/benchmark/chaos/soak hardening, broad
capture expansion or deployment is part of this pass.

## Contract ledger

Every row has goal FIX. Test names prefixed `TestQuality` in the broader roadmap
are proposed observations, not existing evidence. Replace pending results with
actual executed test names/outcomes before claiming completion.

| ID | Desired invariant / real path | Ownership | Planned observation / expected red | Status |
|---|---|---|---|---|
| Q1a | Emitted after-cursor advances query results | query worker; root owns corpus promotion | Existing known-gap `after-cursor` repeats first page; preserve desired expectation | PENDING |
| Q1b | Aliased cardinality-one relation retains reference and child projection | query worker, explicit consumer handoff if needed | Existing known-gap `forward-relation` yields empty child/lost reference | PENDING |
| Q2a | All supported predicates in an operator map apply together | query worker | Two-bound range returns intersection, independent of map iteration | PENDING |
| Q2b | Requested global ordering precedes page selection | query worker | Seed IDs conflicting with requested field order, compare exact pages | PENDING |
| A1a | Expired OAuth code cannot issue a token | auth worker | Past/boundary expiry rejected through actual redemption | PENDING |
| A1b | Persisted code has one successful concurrent consumer | auth worker | Controlled competing redemption, exact issued/persisted state | PENDING |
| A1c | Invalid/expired callback state does not reach provider exchange | auth worker | Fake provider counts no calls; existing malformed-state invariants retained | PENDING |
| A2 | Existing supported built-ins resolve endpoints used by exchange | auth worker | Normal configured resolution, local provider exchange fixtures | PENDING |
| Q4a | Adjacent exact numeric lookup values above 2^53 identify distinct entities | transaction worker | Actual lookup transaction changes only intended entity, exact readback | PENDING |
| M1 | Repeated gauge registration does not break scraping | metrics worker | Existing scrape-time-value test with `-count=2`; real lifecycle regression | PENDING |
| G1a | Unset metrics address uses default; explicit empty disables it | config worker | `TestLoadMetricsAddressConfiguration`: empty case RED then all cases GREEN; existing serveHTTP guard inspected | GREEN |
| G1b | Empty read URL means same DSN, not shared pool | config worker; root docs | Corrected comment; default marker assertion; unchanged runDatabase creates separate pools after DSN fallback | GREEN (inspection/comment correction) |

## Boundaries and execution

- Sol handles the public-query implementation contract and security-sensitive
  OAuth work. Luna handles bounded metrics/config/numeric work and implementation
  after a sufficiently concrete query contract. Escalate actual ambiguity, not
  routine code size. No overlapping package writers.
- Auth owns `internal/authn`; transaction owns `internal/transact`; metrics owns
  `internal/metrics`; configuration owns `internal/config`. Query ownership is
  assigned after its bounded contract. Root owns docs, corpus promotion,
  integration and commits. No worker may commit or edit outside its lease.
- A new disposable PostgreSQL instance at loopback port 55490 is owned by this
  pass. Tests use `internal/testkit` to create unique databases. Root owns server
  shutdown. Never use default PostgreSQL or retained incident clusters.
- Preserve error envelopes, data formats and existing supported behavior. Apple
  ID-token/new-provider support, COPY adoption and query-engine replacement are
  not included. Do not broaden numeric domains or add sorting modes.
- Each fix gets a meaningful failing observation before treatment, focused green
  and adjacent checks. Concurrency/security checks use race/repeat only where
  needed; no blanket release suite. A doc-only correction needs inspection, not
  an invented failing test.
- Exact failure contracts or compatibility ambiguities require a bounded decision
  before changing policy. Non-author review is scoped to the changed boundaries,
  not a new repository-wide security audit.
- Root commits each completed coherent component, preserving other active work.
  An integrated short suite is the final build/regression smoke, not production
  certification. Keep unresolved roadmap risks explicitly open.

## Evidence and commits

Configuration: `envOrAllowEmpty` distinguishes absent and explicit-empty values
only for the metrics address. Config package ordinary/race tests and vet passed
in the worker; root reran `go test ./internal/config ./cmd/instantd -short -count=1`
with integration disabled. Existing runtime code skips the metrics listener for
empty address and creates distinct read/write pools; no pool redesign or new
runtime listener test was added for the comment correction.

Other rows await their specific handoffs; green configuration does not close them.
