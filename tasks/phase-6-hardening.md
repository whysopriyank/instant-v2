# Phase 6 — Hardening + release polish

**Read first**: `docs/reference/03-protocol.md` §5 (refresh-ok shape),  `01-state.md` §4 debt list.

Everything in this phase is additive; the Phase 4 invariants stay green with every change.
Each workstream may run independently — none shares a package with another.

## Current status (2026-08-28)

Partial. Delta refresh, backup/restore, rate limits, resource caps, chaos
checks, security hardening, examples, and release tooling have landed in the
current tree. Performance evidence includes named historical smoke/soak runs
and microbenchmarks, but the comparative capacity gate is intentionally open:
Wave 4 must first establish the benchmark contract and Wave 6 must execute a
paired, reproducible v1/V2 measurement.

## 6A — Delta sync (largest user-visible win)

- [x] Design additive feature flag `delta-refresh` (≥0.23.0; old SDKs fall back to full node-list envelopes) (negotiated like `patch-presence`/`batch-messages`; old SDKs fall back to full envelopes).
- [x] Extend `internal/reactive` novelty path to emit structural patches (`refresh-ok-delta`: per-entity add/update/remove + page-info delta) for negotiating sessions; falls back on aggregates / >50% churn / reorder. Benchmark: 10k entities, single-row mutation → full 912,858 B vs delta 213 B (~4285×), diff ~26 ms.
      ALSO (conformance fix found via SDK replay): refresh envelopes now carry v1's join-rows NODE-LIST (collect-instaql-results-for-client shape); SSE/admin paths use :tree semantics.
- [ ] Client interop: since SDK is frozen, document that a pre-`delta-refresh` client simply never negotiates it; no SDK change needed.
- [x] Benchmarks: bandwidth and reconciliation time on large result sets (e.g. 10k-entity queries with 1-row mutations).

## 6B — Backup / restore

- [x] Parity with `backup.clj` (1,118 LOC) + `restore`: streaming export/import of app dump
      (`apps+attrs+triples+rules+transactions`), `PipedInputStream`-style chunked HTTP responses,
      checksum, resume. Document wire format.
- [x] Upgrade path from v1 self-host dumps (consume v1 export ZIPs).

## 6C — Performance + ops hardening

- [ ] pprof-guided tuning (partially: notifier parallel drain + benchmarks in place; JSON pools/CTE cache open): JSON encode pools, pgx batch, topic-index hot loop, CTE planning cache; connector stats hook.
- [x] Connection + per-app rate-limit policy (`internal/ratelimit`, token buckets per app+class, 429+Retry-After) (Bucket4j-equivalent, local-first; interface stub from Phase 2 made concrete).
- [x] Resource limits: per-app subscription cap (+429 close) and invalidator queue-depth gauge, WAL backpressure signal when invalidator queue depth grows.
- [x] Chaos harness: PG bounce mid-stream (`cmd/chaos`; 8/8 reconnect, LSN checkpoint resume, zero phantom reads, journal-exact final state) → sessions reconnect, LSN resumes without phantom reads; `replay --target v2` still diffs 0 post-chaos.
- [x] Capacity target — MEASURED-V2-ONLY (comparative claim withdrawn): v2 numbers from Phase 4 soak stand (5000 sessions × 30 min, 13.4k tx @ 8/s sustained, 60k refreshes delivered, 0 drops; delta-refresh benchmark ~4285× wire savings on single-row mutation of 10k entities). Later paired V1/V2 smoke runs are retained as historical observations, but their raw artifacts and methodology do not satisfy `docs/reference/13-benchmark-contract.md`; Wave 6 remains the only path to a publishable comparison.
- [x] Browser examples replay — vite-vanilla ported with self-host URI overrides; exact browser surface driven via frozen @instantdb/core 1.0.65 from the example's node_modules (subscribeQuery snapshot on enriched ack, transact add/toggle/delete, live push each mutation). Visual browser verification NOT performed (no working browser device in session); found+fixed app-status 'ok'→'active' conformance bug during replay.
- [x] Corpus fixture regeneration — 00-smoke rewritten to real v1 shapes (init-ok auth/app-status:'active', enriched add-query-ok); canonicalizer normalizes volatile session-id; replay green; chaos corpus step green.
- [x] S3 backup backend — ObjectStore interface + minio-go S3Store; /object export-download + restore-object routes; fake-S3 tests under -race.
- [x] Signed release publishing — goreleaser check passes; snapshot builds all 4 targets; cosign keygen→sign-blob→verify proven offline; CI does keyless signing at tag time (UPGRADE.md documents secrets + offline smoke).
- [ ] BLOCKED (user decision): live v1 differential/capacity baseline steady-state throughput within 30% of v1's Postgres-bound ceiling (the gate is regression protection, not ambition).

## 6D — Docs + examples + release

- [x] Docs narrative: "self-host first" getting-started, replacing hosted-docs assumptions (`instantdb.com` → local). → docs/guides/07-selfhost.md
- [x] `examples/python-script` replayed end-to-end against instantd with the frozen PyPI SDK (high-level tx ops, admin query, merge, where-filters, LIVE subscriptions over `/admin/subscribe-query` SSE with push-on-write). Browser-based examples remain OPEN (need browser harness).
      FOUND+FIXED during replay: `/admin/transact` lacked high-level→low-level tx lowering (`internal/transact/highlevel.go` port of admin/model.clj); admin writes did not invalidate subscribers (OnCommit bridge); `/admin/subscribe-query` did not exist.
- [x] Signed container image (`goreleaser`), SBOM, `UPGRADE.md` for v1 self-host operators, migration guide for admin tokens. → .goreleaser.yaml (cosign sign-blob for image+checksums, syft SBOM step), UPGRADE.md; actual publishing happens at tag time
- [ ] `v1.0.0` tag; post-release corpus stewardship note (who adds scenarios for new ops).

## Phase 6 exit gate — the repo is maintainable

```
delta-refresh suite (when implemented): bandwidth delta measured and documented
chaos run (PG bounce mid-stream) green; soak green
capacity gate: within 30% of v1 Postgres-bound ceiling; no regressions insidePhase 4 suites
examples/* all green against v2; artifacts (image+SBOM+UPGRADE.md) published
```
