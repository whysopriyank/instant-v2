# Phase 1 — Storage engine

**Read first**: `docs/reference/02-architecture.md` §3 (storage layout), `docs/reference/03-protocol.md` §7 (attr shape),
`docs/guides/05-conformance.md` row "Storage parity".

This phase may run two agents in parallel — `data` and `platform` — behind the Phase 0 schema.

## Current status (2026-08-28)

Complete for the implemented surface. Catalog/attribute flags (including
requiredness), typed values, triple CRUD/COPY, transaction journaling, limits,
and real-Postgres tests are present. Full corpus-scale storage parity remains
dependent on the incomplete Phase 0 corpus.

## Context for every Phase 1 dispatch

```
# Contract (02 §4 — leaf types)
triple.Value (tagged union), Triple, Attr, AttrCatalog,
storage.InsertMany / FetchByPatterns / DeleteByPredicate / CopyBulk,
platform.EnsureApp / EnsureAttr / FlagDerivation (ea/eav/av/ave/vae)
```

## 1A — `internal/triple` (owner: `data`)

- [ ] `value.go` — `Value{kind, raw json.RawMessage, ref uuid.UUID}` + `MarshalJSON`/`UnmarshalJSON`
      preserving v1's jsonb semantics (`number` vs `string` vs `bool` distinguished;
      `ref` is `{id: uuid}` shape in db and plain uuid in tx-step values).
- [ ] `attr.go` — `Attr{ID, App, Etype, Label, ValueType, Cardinality, ForwardIdent, ReverseIdent, Unique, Index, Required, CheckedDataType, OnDelete … }`
      with the exact field names from 03 §7 (Go fields map to kebab/`?` JSON via struct tags).
- [ ] `hash.go` — `valueMD5` derivation (parity with `db/model/triple.clj`); unit test vs v1 fixture table.
- [ ] Fuzz: `go test -run FuzzValue` — json-encode → decode round-trips; ref constraint; 1024-byte cap.

Acceptance: `go test ./internal/triple -run TestCorpusValueCodec` and fuzz corpus against v1 truth table.

## 1B — `internal/platform` (owner: `platform`, disjoint from `data`)

- [ ] `apps.go` — `apps(id, title, admin_token, connection_string NULL)` CRUD; BYOP interface stubbed.
- [ ] `attrs.go` — `attrs` CRUD; derivation of boolean flag columns from `(index?, unique?)`
      exactly as in v1; `idents UNIQUE(app_id,etype,label)` maintenance; `checked_data_type` enforcement.
- [ ] `idents.go` — `(app_id,etype,label) → attr id` resolution; error semantics matching `model/ident`.
- [ ] `rules.go` — thin JSONB doc for `rules(app_id,code)`; versioning key (Phase 2 consumes it).
- [ ] Tests against real PG via testcontainers; topology assertions (flag columns, constraints).

## 1C — `internal/storage` (owner: `data`, after 1A types compile)

- [ ] `pg.go` — `pgxpool.Pool` wiring, statement timeouts (`*query-timeout-seconds*` parity), PG error-code mapping (`pgerrors`).
- [ ] `triples.go` — `InsertMany` (single `txn` → INSERT + journal row), `FetchByPatterns` (pattern → WHERE with flag predicate),
      `DeleteByPredicate`, `FetchAttrsForApp`.
- [ ] `copy.go` — bulk `COPY FROM` path for migration-size loads (parity with `jdbc/copy.clj`).
- [ ] `journal.go` — `transactions`+`wal_logs` writer; per-app tx-id/isn sequence (monotonic).
- [ ] Tests: `TestStorageRoundTrip` replays corpus DB-granularity fixtures; `EXPLAIN` sanity for flag indexes.

## Phase 1 exit gate (blocks Phase 2)

```
go test ./internal/triple ./internal/platform ./internal/storage -race -count=1
corpus DB-granularity fixtures pass against real Postgres
no package reaches into another's tables (grep `FROM triples|attrs|apps` outside `storage` fails)
```

Collect regression fixtures before closing: every v1 triple-encoding edge case captured as `corpus/01-value-*.ndjson`.
