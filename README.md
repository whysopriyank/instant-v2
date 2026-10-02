# Instant v2

A from-scratch Go rewrite of the [InstantDB](https://github.com/instantdb/instant) sync server,
taken over after the original team's sunset. Compatibility with the frozen v1
TypeScript SDK is a project goal; the current service is not a verified drop-in
replacement.

## Goal

Single-binary, self-host-first realtime backend providing:

- InstaQL queries over an EAV triple store in Postgres (datalog-compiled CTEs)
- Optimistic multiplayer transactions (tx-steps / InstaML wire grammar)
- CEL-based per-app permissions
- WebSocket sync with post-commit invalidation (optional Postgres LISTEN/NOTIFY
  across peer processes; the WAL tailer is a separately verified component, not
  the production serving path), presence, rooms
- Auth (magic code, OAuth/OIDC), Admin API, local-disk file storage with
  HMAC-presigned URLs (object/S3 backup storage is a separate, alpha-excluded path)

## Non-goals (v2)

- Multi-cloud clustering (Hazelcast/gRPC/SNS mesh) — single-node first, optional HA later
- Aurora-specific failover machinery
- Stripe billing / dashboard plane (CLI + API only)
- Breaking the frozen wire protocol (see `docs/reference/03-protocol.md`)

## Documents

For the production-quality refactor, start with the
[repository quality guide](docs/guides/repository-quality.md),
[package scorecard](docs/reference/quality-scorecard.md), and
[verification report](docs/reference/quality-verification.md). The last report
separates completed implementation from remaining release and parity gaps.

| Doc | Contents |
|---|---|
| [`docs/archive/01-state.md`](docs/archive/01-state.md) | V1 assessment: subsystem map, LOC, hot paths, what is kept/dropped |
| [`docs/reference/02-architecture.md`](docs/reference/02-architecture.md) | V2 target design: packages, data flow, key type contracts |
| [`docs/reference/03-protocol.md`](docs/reference/03-protocol.md) | Frozen wire protocol contract (WS ops, tx-steps, REST surface) |
| [`docs/plans/04-roadmap.md`](docs/plans/04-roadmap.md) | Phased roadmap with milestones and acceptance criteria |
| [`docs/guides/05-conformance.md`](docs/guides/05-conformance.md) | Cross-verification strategy against v1 (golden corpus, differential testing) |
| [`docs/reference/13-benchmark-contract.md`](docs/reference/13-benchmark-contract.md) | Accepted Wave 4 benchmark contract for the Wave 5 harness and Wave 6 measurement |
| [`docs/plans/16-product-performance-headroom.md`](docs/plans/16-product-performance-headroom.md) | Product-only performance opportunities, evidence, risk classification, and low-risk implementation order |
| [`docs/guides/06-agent-orchestration.md`](docs/guides/06-agent-orchestration.md) | Sub-agent architecture: ownership map, contracts, escalation gates |
| [`tasks/`](tasks/) | Per-phase task backlogs consumed by agents |

## Stack decision (record)

**Go**, chosen over Rust for this specific port because:

1. `jackc/pglogrepl` covers pgoutput logical replication (v1 reimplements this in Java+Clojure).
2. `cel-go` is the reference CEL implementation with the AST surgery surface that `db/cel.clj`
   depends on (macros, bindings ext, AST walking). `cel-rust` is community-grade by comparison.
3. Clojure→Go is data-shape translation; Clojure→Rust forces ownership re-architecture of every
   recursive transform in the 3.5k-LOC query compiler — exactly where protocol-fidelity bugs hide.
4. Solo-maintainer velocity: seconds-long builds sustain the port→diff→fix loop.

Performance thesis is **not** raw speed (the workload is Postgres-bound); it is memory density,
sub-second cold start, single-binary deploy, and architectural wins (delta sync, no cluster mesh).

## Current status (2026-10-02)

An internal single-node alpha was accepted on September 26. **No public alpha
has been published, and production readiness is not established.** The current
[public-alpha envelope](docs/reference/public-alpha-release-envelope.md) selects
one daemon with primary PostgreSQL 17 on Linux amd64, plus new container,
restore and official-v1 comparison requirements. Historical acceptance does
not qualify the new candidate.

The [actual v1 rehearsal](docs/plans/next-release/v1-differential-rehearsal-20261002.md)
observed 2 agreements and 16 failures across 18 selected scenarios; compatibility
qualification remains blocked. Query tuples omit v1's fourth timestamp element,
so SDK `serverCreatedAt` ordering, infinite queries and cursor fidelity are
unqualified; cursor input and other wire/behavior differences also remain.
See the [precise compatibility limits](docs/plans/next-release/v1-compatibility-observations.md).
These discrepancies are not approved exclusions; the owner's compatibility
decision remains pending.

Email delivery (503), direct ID-token sign-in (501), admin presence (501),
dynamic view rules, sync/stream operations and object-store backup (503)
remain outside the approved alpha surface. HA/read replicas, real-provider
acceptance and comparative performance claims are unselected. Use the
[self-host guide](docs/guides/07-selfhost.md) for required startup settings and
the [distribution reference](deploy/README.md) for the prepared deployment path.

## Historical acceptance (2026-09-26)

**Alpha acceptance:** the DEC-001 single-node-alpha release gate passed on
candidate `f7dc5b10327a3b6a31d540c426e62d710d4078af` with evidence from
qualification campaign `alpha-20260926f` on an owned Linux host: native
Linux lanes (1891 tests), 7/7 crash/restart/drain recovery outcomes, and a
scoped 900 s, 500-session soak (7200/7200 transactions acknowledged,
committed and refreshed, none dropped). This is an **alpha** acceptance for
testing only; it is not a v1-production-ready claim, and nothing has been
tagged, published, or deployed. See
[`docs/plans/finish-up/program-manifest.md`](docs/plans/finish-up/program-manifest.md)
and [`docs/reference/release-envelope.md`](docs/reference/release-envelope.md);
the evidence copies and their hashes are registered in
[`docs/plans/finish-up/evidence-alpha-20260926f.md`](docs/plans/finish-up/evidence-alpha-20260926f.md).

That internal alpha acceptance was scoped by DEC-001: no
production-readiness, provider-verified OAuth/email, v1-parity, container
qualification, or restore-drill claim is made. Implementation is well beyond
the planning stage. The current repository is a
working, testable single-node service with optional Postgres LISTEN/NOTIFY
invalidation for peers. The status below is deliberately conservative: a
phase is only “complete” when its implementation and stated verification are
present; missing corpus breadth or a hardened comparative benchmark keeps it
partial.

| Area | Status | Evidence / remaining boundary |
|---|---|---|
| Foundations, protocol schema, migrations | Partial | `internal/protocol`, embedded migrations, and corpus tooling exist; 18 authored regression scenarios run against isolated v2 fixtures, not the original ≥50-scenario or genuine v1-oracle gate. |
| Storage, catalog, triple CRUD | Complete for the implemented surface | Real-Postgres storage tests, typed value encoding, catalog flags, batching, limits, and migrations are covered. |
| Transactions and permissions | Partial | Tx-step, CEL, cascades, required attributes, admin bypass and persisted rules on selected admin/runtime paths are implemented and tested; dynamic view rules remain excluded. |
| Query engine | Partial | InstaQL, pagination, indexes, local evaluation, and differential fixtures exist; broad v1 corpus and JS-harness coverage remain incomplete. |
| Reactive sync / SSE / WAL | Partial | WS/SSE, query grouping, invalidation, delta fallback, incremental refresh, and live logical-decoding verification exist; the production assembly uses post-commit notification, while the WAL tailer remains an independently verified component. |
| Platform APIs and backups | Partial | Admin/runtime/storage routes and v1/v2 backup import/export are implemented; HTTP corpus coverage and some platform parity scenarios remain open. |
| Hardening | Partial | Rate limits, queue gates, resource caps, security fixes, chaos checks, and race validation are present; comparative benchmark methodology and the Wave 5 harness are accepted, while qualified Wave 6 measurement remains pending and unselected for public alpha. |

Start with [`docs/plans/04-roadmap.md`](docs/plans/04-roadmap.md) for the reconciled
roadmap and [`docs/guides/05-conformance.md`](docs/guides/05-conformance.md) for the actual
corpus and verification limits. Performance numbers in the historical audit
sections are evidence from named runs, not a current v1-versus-v2 claim until
the required qualified Wave 6 paired measurement is complete.
