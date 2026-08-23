# Phase 6 — Hardening + release polish

**Read first**: `docs/03-protocol.md` §5 (refresh-ok shape),  `01-state.md` §4 debt list.

Everything in this phase is additive; the Phase 4 invariants stay green with every change.
Each workstream may run independently — none shares a package with another.

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
- [x] Chaos harness: PG bounce mid-stream (`tools/chaos`; 8/8 reconnect, LSN checkpoint resume, zero phantom reads, journal-exact final state) → sessions reconnect, LSN resumes without phantom reads; `replay --target v2` still diffs 0 post-chaos.
- [ ] Capacity target (OPEN): steady-state throughput within 30% of v1's Postgres-bound ceiling (the gate is regression protection, not ambition).

## 6D — Docs + examples + release

- [x] Docs narrative: "self-host first" getting-started, replacing hosted-docs assumptions (`instantdb.com` → local). → docs/07-selfhost.md
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
