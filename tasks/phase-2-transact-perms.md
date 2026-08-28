# Phase 2 — Transactor + permissions

**Read first**: `docs/03-protocol.md` §4 (tx-steps grammar), §8 (permission doc + fallback chain),
`docs/02-architecture.md` §4 (transact/perms interfaces).

Agents `transact` and `permissions` run in parallel on disjoint packages behind the Phase 0 schema.

## Current status (2026-08-28)

Partial. Tx-step validation, lookup/cascade handling, CEL checks, rate-limit
hooks, admin bypass, and required-attribute persistence/validation are covered
by focused tests. Rules persistence is present as a schema/table but is not
yet wired through every transact plane, and broad v1 corpus verification is
still open.

## 2A — `internal/transact` (owner: `transact`)

- [ ] `txstep.go` — spec for every op: `add-triple`/`deep-merge-triple`/`retract-triple`,
      `delete-entity`, `add-attr`/`update-attr`/`delete-attr`/`restore-attr`, `rule-params`.
      Field order and tuple shape must match 03 §4 exactly (corpus asserts order).
- [ ] `lookup.go` — `[attrId,value] → entityId` resolution ( ↔ `instaml.ts:extractLookup:120` and
      server lookup-expansion SQL); missing-ref semantics; bare-uuid passthrough.
- [ ] `validate.go` — `create`/`update` mode `required?` checks, `unique?` pre-checks,
      `cardinality` violations, `checked_data_type` enforcement (delegating type checks to `triple`).
- [ ] `apply.go` — single `pgx.Tx`: resolve lookups → pre-checks → write triples + journal row
      via `internal/storage`; `delete-entity` cascade generation; `add-attr` missing-attr synthesis
      (mirrors `instaml.ts:createMissingAttrs`); `TxID`/`ISN` allocation.
- [ ] Property tests: random `tx-steps` round-trips through `Validate`→`Apply`→re-read vs v1 SQL replay.

Acceptance: `corpus 04-transact-*` replays through `Validate+Apply` in isolation, matching v1 outcomes
(same triple set, same order, same errors), plus `TestTxStepSpec` covering every grammar branch.

## 2B — `internal/perms` (owner: `permissions`)

- [ ] `ruledoc.go` — JSONB `rules.code` parse into typed `RuleDoc`; `bind` topo-sort with cycle detection;
      reserved namespaces (`$users/$files/$default/$streams/$rateLimits`); fallback-chain resolver
      `etype.allow.action → etype.allow.$default → $default.allow.action → $default.allow.$default → etype.fallback.action`
      (hard-test every branch; this single function is why `db/cel.clj` is 1,907 LOC — don't shortcut its coverage).
- [ ] `celenv.go` — `cel-go` environment per `(app, ruleVersion)` with cache; custom func
      `rateLimit.bucket.limit(key)`, `auth.*`, `data.*` variable bindings; extension `bindings`.
- [ ] `requestctx.go` — `modifiedFields`/`time`/`ip`/`origin` bindings parity with `db/proto.clj` protobuf.
      Timezone and truncation semantics tested (v1 has edge-case bugs here — match them).
- [ ] `check.go` — `Check(ctx, doc, action, subject) (bool,error)` for write-path per-step gates;
      expects `rule-params` threaded through.
- [ ] `where.go` — view-rule → `where` clause compilation for Phase 3 (merge point): parses CEL `view`
      expression, extracts attribute patterns for pre-filtering (the "attr-pattern extraction" in `db/cel.clj`).
- [ ] Coverage gate: branch coverage ≥ 95% on `ruledoc.go` + `check.go` fallback-chain paths.

Acceptance: `corpus 02-perms-*` through `perms.Check` matches v1 (allow/deny, field-level, bind expansion);
`celenv` custom-function surface matches `db/cel_builder.clj` listing.

## 2C — Permissioned write integration (owner: `transact`, depends on 2B cache API)

- [ ] Wire `perms.Check` into `transact.Apply` (the `permissioned_transaction.clj:769` equivalent):
      per-step decision before commit; optimistic `check` vs post-commit `where` not confused.
- [ ] `rule-params` threaded end-to-end (header payload ↔ CEL variable).
- [ ] Rate-limit buckets enforced (local first; Hazelcast/Bucket4j parity deferred to Phase 6).

## Phase 2 exit gate (blocks Phase 3)

```
go test ./internal/transact ./internal/perms -race -count=1 -cover
corpus transact+perms suites pass through the packages in isolation
perms branch coverage ≥ 95% on ruledge; regression fixtures captured for every denied/allowed split
```
