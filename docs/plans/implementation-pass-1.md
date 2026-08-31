# Functional implementation pass 1

Started 2026-08-31 from `3873335` on
`codex/quality-implementation-checkpoint`, initially clean.

Status: COMPLETE for this bounded functional pass. The user approved proceeding with the implementation-first
plan. Scope is concrete functional defects, minimum meaningful tests and
incremental local commits. No production/benchmark/chaos/soak hardening, broad
capture expansion or deployment is part of this pass.

## Contract ledger

Every row has goal FIX. Test names prefixed `TestQuality` in the broader roadmap
are proposed observations, not existing evidence. Replace pending results with
actual executed test names/outcomes before claiming completion.

| ID | Desired invariant / real path | Ownership | Planned observation / expected red | Status |
|---|---|---|---|---|
| Q1a | Emitted after-cursor advances query results | query worker; root owns corpus promotion | Unchanged desired fixture now normal scenario 17; combined cursor regression | GREEN |
| Q1b | Aliased cardinality-one relation retains reference and child projection | query worker; root integration | Unchanged desired fixture now scenario 18; `TestForwardAliasProjectionAndRootIsolation` | GREEN |
| Q2a | All supported predicates in an operator map apply together | query worker | `TestWhereMultiBoundConjunction`, shared coercion normalization | GREEN |
| Q2b | Requested global ordering precedes page selection | query worker; root integration | `TestQueryFieldOrderCursorAndNulls`, cursor/domain/metadata cases | GREEN |
| A1a | Expired OAuth code cannot issue a token | auth worker | `TestQualityOAuthConsumptionExpiry`: before/at/after expiry | GREEN |
| A1b | Persisted code has one successful concurrent consumer | auth worker | `TestQualityOAuthConsumptionConcurrent`: controlled DB contention; one winner and one persisted token | GREEN |
| A1c | Invalid/expired callback state does not reach provider exchange | auth worker | Expiry and bindings/retry tests count provider calls and compare stored state | GREEN |
| A2 | Existing supported built-ins resolve endpoints used by exchange | auth worker | Built-in resolution and local HTTP exchange tests | GREEN (Google/GitHub; Apple explicitly unsupported) |
| Q4a | Exact adjacent numeric lookups identify distinct entities; equivalent spellings share same-batch identity | transaction worker | `TestLookupRefResolutionPreservesLargeIntegers`, `TestHighLevelSameBatchEquivalentNumericLookupsReuseEntity`, normalization unit cases | GREEN |
| M1 | Repeated gauge registration does not break scraping; runtime releases callbacks | metrics worker; root runtime wiring | Repeated scrape and lifecycle tests; `TestRunDatabaseReleasesGaugesIntegration` | GREEN |
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

### Bounded decisions

Query: normalize operator conjunction during Coerce so the existing normal and
incremental consumers receive the same conditions. Preserve current ID-ascending
ties, apply null-first ascending/null-last descending, and implement the existing
inclusive cursor flags. Existing ID-only cursor tuples cannot recover a removed
entity's previous field position; an absent explicit-order cursor returns a
controlled query error rather than silently restarting page one. These choices
do not establish v1 parity or add a new cursor format/server-created-at feature.
Ordering retains the existing float64 numeric domain, or homogeneous strings,
plus null/missing values. Mixed/unsupported/non-finite ordering values fail
explicitly. Exact arbitrary-precision numeric sorting is not claimed by Q4a's
separate exact-lookup fix.

OAuth: preserve existing provider-exchange-failure local-state rollback and
burn-before-token-issuance semantics. An independent Sol contract review requires
transaction-local locking/read/check/delete, no nested pool acquisition under the
lock, expiry before and after bounded provider exchange, and no new Apple feature.
Local rollback cannot restore a provider authorization code consumed upstream.

## Evidence and commits

Configuration: `envOrAllowEmpty` distinguishes absent and explicit-empty values
only for the metrics address. Config package ordinary/race tests and vet passed
in the worker; root reran `go test ./internal/config ./cmd/instantd -short -count=1`
with integration disabled. Existing runtime code skips the metrics listener for
empty address and creates distinct read/write pools; no pool redesign or new
runtime listener test was added for the comment correction.

OAuth: the failing observations included two concurrent winners, accepted expired
codes, expired callbacks contacting the provider, missing built-in endpoints and
accepted HTTP 401 identity responses. Transaction-local locking and consumption
now pass the focused `TestQuality(OAuth|BuiltinOAuth)` group with race detection;
root repeated it against the owned port-55490 instance (2.665s). Worker full authn
live/race, build and vet passed; independent Sol security review found no remaining
blocking defect. Rollback, expiry during exchange and single-connection tests are
additional post-fix guards, not separately claimed pre-fix failures. Local provider
fixtures do not establish real Google/GitHub end-to-end acceptance.

Metrics: repeated registration previously caused HTTP 500 on the second scrape
test run. Registration now replaces a logical series and returns an idempotent,
generation-aware close handle. The runtime releases all eight handles before its
resources close, including startup failure. The startup regression first found
three leaked gauge families, then passed twice with race detection (root 2.389s).
Metrics package race tests passed twice (root 1.737s). Bounded review also caught
one retained callback in removed slice storage; clearing that slot and a focused
storage assertion close the issue without a new lifecycle harness.

Numeric lookup: the original float conversion made the second adjacent-integer
lookup mutate the first entity. Exact decoding fixed that, but bounded review
caught a same-batch regression: `1`, `1.0`, and `1e0` acquired separate cache keys
and failed the unique index. Shared exact decimal canonicalization now preserves
both distinct large integers and equivalent numeric identity, including nested
values; it does not change original emitted application values. The real
high-level lowering/transaction regression and original large-integer regression
passed twice in the worker. Root repeated them with normalization and adjacent
lookup checks on owned PostgreSQL (1.235s). Non-author review found no remaining
blocker; package vet and hermetic checks passed.

Queries: the two existing desired-behavior corpus cases are now normal scenarios
17 and 18, with every client input and expected server output unchanged. Root's
post-integration query race run passed (3.475s), vet passed, and the full normal
18-scenario PostgreSQL/WS corpus replay passed with race detection (6.063s).
Manifest validation reports 18 authored v2 regressions, 22 narrow covered surfaces,
seven gaps, two unsupported surfaces, and zero v1 captures. No capture/harness
expansion was performed. Paging/ordering helpers now have one cohesive
`internal/instaql/pagination.go` owner; no query-engine replacement.

Independent Sol review caught combined-cursor indexing and optional/projected
child attachment issues before acceptance. Root completed those bounded fixes,
added focused regressions and received a final no-blocker review. The query worker
was interrupted for an explicit handoff; its unfinished edits were completed and
verified by the coordinator rather than accepted as green.

Q2 evidence caveat: its new tests were not executed before the worker edited
production code. Root subsequently built an isolated query-package copy from
`3873335`, retaining current dependencies/testkit. The two-bound test returned
two rows instead of one; the three-row global-order test selected `y` instead of
`a` because SQL had already limited candidates. Both assertions passed unchanged
against the current production files via a Go overlay (0.930s). This is a
retrospective baseline/treatment comparison, not claimed test-first execution.
The temporary copy remains at `/tmp/instant-query-baseline.br1gyI`; it is not
tracked and contains no database data. The initial two-row ordering probe was
insufficient because the SQL sentinel included both rows; the three-row probe
exercises selection beyond that sentinel.

## Final checkpoint

| Commit | Component |
|---|---|
| `b401aa7` | Explicit-empty metrics configuration and read-pool comment |
| `9516a35` | Atomic OAuth consumption, expiry and configured providers |
| `53cd367` | Gauge registration ownership and runtime cleanup |
| `c07707a` | Exact numeric SQL/cache lookup identity |
| `cc0efea` | Query ordering, cursors, conjunction, relations and corpus promotion |

Final integrated smoke passed:

```sh
INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./... -short -count=1
```

This is hermetic package/build regression evidence, not a complete live suite.
Live evidence above used `INSTANT_TEST_INTEGRATION=1` and the owned port-55490
`DATABASE_URL`, with testkit creating and dropping unique databases. Final audit
found zero leftover test databases, zero replication slots and zero public tables
in that cluster's administrative `postgres` database. Its exact owned server was
stopped successfully; `/tmp/instant-implementation-pg.qRGXIc` is retained. No
default developer database or earlier incident resources were changed.

Static/build checks: scoped vet, Go compilation through the selected tests,
format checks and `git diff --check` passed. A new repository-wide linter campaign
was not run; the linter is not on PATH. No real provider calls, v1 comparison,
native Linux/container certification, soak, recovery or benchmarking was run.
The broad roadmap remains open and requires separate approval for hardening.

The initial working tree was clean. Only this pass's listed code, tests, promoted
fixtures and status documentation were included. Changes are committed locally;
no push, PR, tag or deployment was performed. Non-author reviews challenged the
OAuth transaction boundary, numeric cache identity and query composition; their
essential findings were repaired and focused checks rerun. The Q2 retrospective
evidence limitation above remains explicit rather than being called test-first.
