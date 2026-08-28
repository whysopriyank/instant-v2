# Phase 3 — Query engine

**Read first**: `docs/02-architecture.md` §4 (datalog/instaql interfaces),
`docs/03-protocol.md` §6 (InstaQL surface), v1 sources `db/datalog.clj` (3,500 LOC),
`db/instaql.clj` (2,326 LOC).

Agents `planner` and `instaql` run in parallel across the IR boundary. The first commit of this
phase (main orchestrator) adds the `datalog.Pattern/Plan` types; after that, neither agent
needs to renegotiate — they type-check.

## Current status (2026-08-28)

Partial. Datalog planning, InstaQL coercion/evaluation, pagination, indexing,
and real-Postgres query tests exist. The broad v1 corpus gate, JS optimistic
evaluation harness, and EXPLAIN coverage across every index family remain to
be completed.

## 3A — `internal/datalog` (owner: `planner`)

- [ ] `pattern.go` — `Specifier` (entity/attr/value) shapes; `IndexKind ∈ {ea,eav,av,ave,vae}`;
      specifier binding-path semantics, symbol-map derivation, nested `or`/`and` group IR.
- [ ] `index.go` — `bestIndex(pattern, attrCatalog) IndexKind` matching v1's attribute-driven heuristic
      (the same function name appears in `db/instaql.clj`; preserve tiebreak rules).
- [ ] `plan.go` — `Plan` type carrying `patterns []Pattern + join order + topic set`;
      `Topic()` extraction (attr/etype keys for the invalidator in Phase 4).
- [ ] `sqlgen.go` — `Plan → pgx` SQL: one CTE per query with join scaffolding.
      Trace back to `datalog.clj:3260-3414` (`query`/`query-nested` + `hsql/format`).
      `query` vs `query-nested` semantics preserved (join-rows must include every matching triple
      for client-side iso re-query — not just projected rows).
- [ ] `loader.go` — per-subscription cached loader sharing (the "dataloader" note in the v1 docstring).
      Single execution per plan shape per tx; results cached, reused across replays within a session.
- [ ] Tests: `TestDatalogPatterns` table-driven from v1's pattern fixtures; `EXPLAIN` cost asserts
      that each generated CTE hits the intended partial index; `TestTopicExtraction`.

Acceptance: `corpus 03-query-*` patterns compiled through `datalog.Plan` produce identical
join-row multisets vs v1 (checked via storage fixtures + SQL snapshot).

## 3B — `internal/instaql` (owner: `instaql`, depends on 3A types only)

- [ ] `coerce.go` — `$`-options map coerce (`where/order/limit/first/last/offset/before/after+Inclusive/aggregate/fields`)
      at the same defaults as `db/instaql.clj:100-133`.
- [ ] `where.go` — `parseWhere` / `makeJoin` / `extendObjects` equivalent: dotted-path traversal across refs,
      `$in` (+ legacy `in`), `$not`/`$ne`, `$isNull`, `$gt`/`$gte`/`$lt`/`$lte`, `$like`/`$ilike`,
      `$entityIdStartsWith` (yes, port the dashboard hack).
- [ ] `paginate.go` — `order`/`orderBy`, `limit`/`first`/`last`, `offset`, `before`/`after` cursors
      (+ inclusive variants), `page-info{startCursor,endCursor,hasNextPage,hasPreviousPage}` construction
      (`db/instaql.clj:924-1021`), cursor opaqueness, stability across tx boundary (savepoint).
- [ ] `aggregate.go` — `aggregate: "count"` only (admin-only gate at `instaql.clj:1190` preserved).
- [ ] `fields.go` — `fields` projection; `$$ruleParams` threading; `$files` URL rewriting hook (Phase 5 storage).
- [ ] `eval_local.go` — `EvaluateLocal(LocalStore, Query) (map[string]any,error)` — the **second impl**
      that matches `client/packages/core/src/instaql.ts` (953 LOC) for optimistic-update scenarios.
      Reuse the same IR so the server envelope and the client's local eval agree.
- [ ] Permissioned reads: `perms`'s `where` compilation from Phase 2 wired as a `where` augmentation
      for `view` rules; field-level filtering after the CTE.
- [ ] Tests: every `where` operator isolated; `TestInstaQLCoerce`; JS harness job that runs
      `client/packages/core/src/instaql.ts` on identical fixtures and diffs envelopes vs `internal/instaql`.

## Phase 3 exit gate (blocks Phase 4)

```
go test ./internal/datalog ./internal/instaql -race -count=1
corpus 03-query-* suites: v2 envelopes diff 0 vs v1 envelopes
JS-harness job: instaql.ts vs internal/instaql on the same fixtures diff 0
paginate stability suite: cursor round-trip (savepoint stability)
EXPLAIN suite green on ea/eav/av/ave/vae flag-index selection
```
