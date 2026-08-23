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

## Status

Planning complete; implementation not started. Start at `docs/04-roadmap.md` Phase 0.
