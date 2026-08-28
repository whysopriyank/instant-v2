# Instant v2

A from-scratch Go rewrite of the [InstantDB](https://github.com/instantdb/instant) sync server,
taken over after the original team's sunset. The v1 TypeScript client SDK is treated as a
**frozen conformance oracle**: it must work against v2 unmodified.

## Goal

Single-binary, self-host-first realtime backend providing:

- InstaQL queries over an EAV triple store in Postgres (datalog-compiled CTEs)
- Optimistic multiplayer transactions (tx-steps / InstaML wire grammar)
- CEL-based per-app permissions
- WebSocket sync with WAL-tail invalidation, presence, rooms, streams
- Auth (magic code, OAuth/OIDC), Admin API, file storage (S3-compatible)

## Non-goals (v2)

- Multi-cloud clustering (Hazelcast/gRPC/SNS mesh) — single-node first, optional HA later
- Aurora-specific failover machinery
- Stripe billing / dashboard plane (CLI + API only)
- Breaking the frozen wire protocol (see `docs/03-protocol.md`)

## Documents

| Doc | Contents |
|---|---|
| [`docs/01-state.md`](docs/01-state.md) | V1 assessment: subsystem map, LOC, hot paths, what is kept/dropped |
| [`docs/02-architecture.md`](docs/02-architecture.md) | V2 target design: packages, data flow, key type contracts |
| [`docs/03-protocol.md`](docs/03-protocol.md) | Frozen wire protocol contract (WS ops, tx-steps, REST surface) |
| [`docs/04-roadmap.md`](docs/04-roadmap.md) | Phased roadmap with milestones and acceptance criteria |
| [`docs/05-conformance.md`](docs/05-conformance.md) | Cross-verification strategy against v1 (golden corpus, differential testing) |
| [`docs/13-benchmark-contract.md`](docs/13-benchmark-contract.md) | Accepted Wave 4 benchmark contract for the Wave 5 harness and Wave 6 measurement |
| [`docs/06-agent-orchestration.md`](docs/06-agent-orchestration.md) | Sub-agent architecture: ownership map, contracts, escalation gates |
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

## Status (2026-08-28)

Implementation is well beyond the planning stage. The current repository is a
working, testable single-node service with optional Postgres LISTEN/NOTIFY
invalidation for peers. The status below is deliberately conservative: a
phase is only “complete” when its implementation and stated verification are
present; missing corpus breadth or a hardened comparative benchmark keeps it
partial.

| Area | Status | Evidence / remaining boundary |
|---|---|---|
| Foundations, protocol schema, migrations | Partial | `internal/protocol`, embedded migrations, and corpus tooling exist; the checked-in corpus is still a small smoke/transact set, not the original ≥50-scenario gate. |
| Storage, catalog, triple CRUD | Complete for the implemented surface | Real-Postgres storage tests, typed value encoding, catalog flags, batching, limits, and migrations are covered. |
| Transactions and permissions | Partial | Tx-step, CEL, cascades, required attributes, and admin bypass are implemented and tested; rules persistence is not wired into every write plane. |
| Query engine | Partial | InstaQL, pagination, indexes, local evaluation, and differential fixtures exist; broad v1 corpus and JS-harness coverage remain incomplete. |
| Reactive sync / SSE / WAL | Partial | WS/SSE, query grouping, invalidation, delta fallback, incremental refresh, and live logical-decoding verification exist; the production assembly uses post-commit notification, while the WAL tailer remains an independently verified component. |
| Platform APIs and backups | Partial | Admin/runtime/storage routes and v1/v2 backup import/export are implemented; HTTP corpus coverage and some platform parity scenarios remain open. |
| Hardening | Partial | Rate limits, queue gates, resource caps, security fixes, chaos checks, and race validation are present; Wave 4 benchmark methodology and Wave 6 hardened v1 comparison are still pending. |

Start with [`docs/04-roadmap.md`](docs/04-roadmap.md) for the reconciled
roadmap and [`docs/05-conformance.md`](docs/05-conformance.md) for the actual
corpus and verification limits. Performance numbers in the historical audit
sections are evidence from named runs, not a current v1-versus-v2 claim until
the Wave 4/5 harness and Wave 6 paired measurement are complete.
