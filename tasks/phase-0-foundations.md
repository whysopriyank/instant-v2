# Phase 0 — Foundations + golden corpus

**Read first**: `docs/04-roadmap.md` Phase 0 + `docs/05-conformance.md` §2.

## Current status (2026-08-28)

Partial. The Go scaffold, protocol schema/generator, embedded migrations, and
WebSocket corpus replayer exist. The checked-in corpus is still limited to
smoke and transact/refresh scenarios. `corpusctl record` is future work, its
v1 flags are advisory, and the planned ≥50-scenario live-v1 recorder/replay
exit gate remains open.

## 0.1  Repo scaffold

- [ ] `go.mod` pinned to Go `^1.24` with real `require` entries (first `go mod tidy` green)
- [ ] `.golangci.yml` (span of `staticcheck`+`govet`+`errcheck`+`ineffassign`; no pedantic noise)
- [ ] `Dockerfile` (multi-stage, distroless final) + `.dockerignore`
- [ ] GH Actions `ci.yml`: `make vet lint test corpus-check` on every push
- [ ] `cmd/instantd/main.go` that boots, reads env, binds `:8080`/`:8081`, and shuts down gracefully
- [ ] `internal/protocol/` empty package present so `go vet ./...` passes

## 0.2  Migrations (read: `server/resources/migrations/01_bootstrap.up.sql` → goose)

Port verbatim, no redesign:

- [ ] `migrations/01_bootstrap.sql` — `apps`, `attrs`, `idents UNIQUE(app_id,etype,label)`,
      `triples(app_id,entity_id,attr_id,value jsonb,value_md5)` + ea/eav/av/ave/vae flags
      and partial unique indexes (only flag-column indexes), 1024-byte cap CHECK,
      `ref-values-must-be-uuid`.
- [ ] `migrations/02_rules.sql` (v1 `04_add_rules.up.sql`) — `rules(app_id PK, code jsonb)`.
- [ ] `migrations/03_journal.sql` (v1 `95/97`) — `transactions` + `wal_logs`.
- [ ] `migrations/04_auth.sql` — `app_users`, `refresh_tokens` (hashed), `magic_codes`,
      oauth tables; PKCE field (migration 15) included.
- [ ] `migrations/05_files.sql` — S3 triage tables referenced by storage routes.
- [ ] `tests`: `TestMigrations` using `testcontainers-go/postgres` asserts
      `SELECT * FROM information_schema` topology matches v1 bootstrap + every CHECK held.

## 0.3  Protocol schema + schemagen

See `docs/03-protocol.md` for the full contract.

- [ ] `internal/protocol/schema/protocol.schema.json` — JSON Schema covering every `op` (client→server
      and server→client), tx-step tuple shapes, attr json shape, InstaQL option/operator enums,
      error envelope `{status,type,message,hint?}`, REST request/response maps for
      `/admin/*`, `/runtime/*`, `/storage/*`.
- [ ] `tools/schemagen/` — reads the JSON Schema; emits:
      - Go types into `internal/protocol/` (wire envelopes, `TxStep` tagged union, `Attr` struct with `?`-suffix field tags mapped to Go names)
      - `internal/protocol/protocol.d.ts` for tooling / corpusctl consumption
      - Self-test: generated types round-trip the corpus frames under `go test ./internal/protocol`.

## 0.4  corpusctl (the oracle)

- [~] `tools/corpusctl/` — `replay`/`differential` WebSocket entrypoints exist;
      `record` requires a future SDK proxy and live-v1 workflow (see 05 §2.2).
- [ ] Fixture seeding: ports of `server/test` fixtures + `examples/*` app seeds.
- [ ] SDK pinning: `@instantdb/core` at ≥ 4 versions including `<0.17.5` and `<0.20.4`.
- [ ] Canonicalization rules per 05 §5 baked into diffing.
- [ ] **≥ 50 scenarios** before the exit gate (distribution in 05 §2.3). Each NDJSON
      carries `meta: {sdkVersion, seedFixture, featureGates}` header.
- [ ] `make corpus` / `make corpus-check` / `make replay TARGET=ws://...` wired and green;
      live-v1 discovery/boot is future work.

## Phase 0 exit gate (blocks everything)

```
go test ./... -race
future live-v1 corpusctl replay against an explicit WebSocket URL   (100% of corpus)
schemagen output compiles (Go + d.ts) and is committed
```

No ported-logic PR may land until this gate is green on `main`.

## Parallelism note

Phase 0 is intentionally single-writer (infra). Splitting it costs more than it saves;
one focused agent carrying it through beats three contending on `go.mod` and `schema/`.
