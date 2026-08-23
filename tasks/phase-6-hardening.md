# Phase 6 — Hardening + release polish

**Read first**: `docs/03-protocol.md` §5 (refresh-ok shape),  `01-state.md` §4 debt list.

Everything in this phase is additive; the Phase 4 invariants stay green with every change.
Each workstream may run independently — none shares a package with another.

## 6A — Delta sync (largest user-visible win)

- [ ] Design additive feature flag `delta-refresh` (negotiated like `patch-presence`/`batch-messages`; old SDKs fall back to full envelopes).
- [ ] Extend `internal/reactive.Novelty` to emit structural patches (per-entity add/update/remove + pagination page-info delta)
      instead of full `instaql-result` when the session negotiates the flag.
- [ ] Client interop: since SDK is frozen, document that a pre-`delta-refresh` client simply never negotiates it; no SDK change needed.
- [ ] Benchmarks: bandwidth and reconciliation time on large result sets (e.g. 10k-entity queries with 1-row mutations).

## 6B — Backup / restore

- [ ] Parity with `backup.clj` (1,118 LOC) + `restore`: streaming export/import of app dump
      (`apps+attrs+triples+rules+transactions`), `PipedInputStream`-style chunked HTTP responses,
      checksum, resume. Document wire format.
- [ ] Upgrade path from v1 self-host dumps (consume v1 export ZIPs).

## 6C — Performance + ops hardening

- [ ] pprof-guided tuning: JSON encode pools, pgx batch, topic-index hot loop, CTE planning cache; connector stats hook.
- [ ] Connection + per-app rate-limit policy (Bucket4j-equivalent, local-first; interface stub from Phase 2 made concrete).
- [ ] Resource limits: per-app subscription cap, WAL backpressure signal when invalidator queue depth grows.
- [ ] Chaos harness: PG bounce mid-stream → sessions reconnect, LSN resumes without phantom reads; `replay --target v2` still diffs 0 post-chaos.
- [ ] Capacity target: steady-state throughput within 30% of v1's Postgres-bound ceiling (the gate is regression protection, not ambition).

## 6D — Docs + examples + release

- [ ] Docs narrative: "self-host first" getting-started, replacing hosted-docs assumptions (`instantdb.com` → local).
- [ ] Every `examples/*` from v1 (chat, cursor presence, etc.) replayed against v2 end-to-end.
- [ ] Signed container image (`goreleaser`), SBOM, `UPGRADE.md` for v1 self-host operators, migration guide for admin tokens.
- [ ] `v1.0.0` tag; post-release corpus stewardship note (who adds scenarios for new ops).

## Phase 6 exit gate — the repo is maintainable

```
delta-refresh suite (when implemented): bandwidth delta measured and documented
chaos run (PG bounce mid-stream) green; soak green
capacity gate: within 30% of v1 Postgres-bound ceiling; no regressions insidePhase 4 suites
examples/* all green against v2; artifacts (image+SBOM+UPGRADE.md) published
```
