# Phase 06 — Topology, platform, and recovery

Phase gate: `RUNTIME_RECOVERABLE`
Packets: `OP-001` through `OP-006`
External authority: required for environment and fault runs

Product repair packets run separately from evidence campaigns. A qualification
runner that discovers a defect stops the affected run and creates a new packet;
it must not patch production code mid-measurement and resume the same evidence.

## OP-001 — Multi-node publisher recovery

**Goal objective:** `If multi-node mode is selected, make invalidation publishing recover from PostgreSQL connection loss with an explicit acknowledgment and reconciliation contract.`

The listener is supervised; the publisher retains one dedicated connection and
only logs later failures.

Required rows:

- publisher connection is reacquired with bounded backoff and cancellation;
- one connection is not concurrently misused;
- exact app/transaction/change identity is retained;
- listener/publisher recovery cannot create echo or duplicate amplification;
- commits during publish outage have a declared acknowledgment and
  reconciliation outcome;
- peer subscriptions eventually converge in the accepted recovery budget.

Architecture review must decide whether a local commit may acknowledge while
remote invalidation is unavailable. Write lease: `internal/bus/**`; coordinator
integrates `cmd/instantd/invalidation.go`. Unit tests use controlled factories;
final green requires an owned two-node PostgreSQL-bounce run. If multi-node is
excluded, enforce it, accept its exclusion rows, and mark the packet
`EXCLUDED_APPROVED`.

## OP-002 — Read-replica visibility barrier

**Goal objective:** `If read replicas are selected, ensure a refresh watermark cannot certify a commit whose state was not visible in the returned replica result.`

Architecture chooses one policy before implementation:

- consistency-sensitive refresh reads use the primary;
- wait for replay to the commit LSN with bounded fallback/error;
- or explicitly expose bounded eventual consistency without strict watermark
  claims.

Red evidence deliberately lags the replica: commit N on writer, return N-1 on
refresh, then catch up. Assert the declared watermark, retry, reconnect, and
convergence behavior without sleeps as the correctness mechanism. Coordinator
owns read-pool assembly; reactive/sync changes require their separate leases and
review. If replicas are excluded, configuration/gates/docs must enforce that.

## OP-003 — Native Linux qualification

**Goal objective:** `Run all selected platform-sensitive behavior on the declared native Linux target and bind results to its OS, kernel, architecture, toolchain, binary, and candidate.`

Execute rather than cross-compile descriptor, `/proc`, process identity,
filesystem replacement, advisory lock, signal, race, and cleanup tests. Record
selected and skipped tests; unexplained skips block the target. No product edits
occur during this run.

## OP-004 — Container runtime qualification

**Goal objective:** `Qualify the exact container image through database-connected startup, persistent storage, outbound TLS, health/readiness, non-root operation, signals, and cleanup.`

Static image inspection and no-DB health are insufficient. Required evidence:
image digest/config, mounted durable paths, owned database identity, actual TLS
handshake using system trust, health/readiness responses, a selected transaction
and subscription, SIGTERM behavior, port/connection release, and cleanup. Registry
publication is out of scope.

## OP-005 — Crash, bounce, and loaded drain

**Goal objective:** `Prove exact acknowledged-state recovery through selected daemon crash points, PostgreSQL interruption, restart/reconnect, and loaded SIGTERM drain on owned resources.`

Prerequisites: phases 02–04, OP-003/004, frozen RPO/RTO/drain budgets, and explicit
fault authority.

Test at least:

- crash before acknowledgment;
- crash after acknowledgment but before refresh delivery;
- crash during refresh;
- PostgreSQL stop/restart while clients and writes are active;
- idle, moderate, and saturated SIGTERM drain.

Compare exact values, tombstones, transaction IDs/watermarks, indexes,
subscriptions, accepted/unfinished operations, close codes, process exit time,
and final client state. Liveness/counts alone are insufficient. Multi-node bounce
additionally consumes OP-001.

## OP-006 — Backup and restore drill

**Goal objective:** `Restore a selected database/object backup into a fresh target, reject incomplete or corrupt input, and prove the application reopens with exact logical and object state.`

Prerequisites: DA-001/003 and an owned isolated target. Record backup manifest,
checksums, schema/migration version, object hashes, restored triples/metadata,
queries, subscriptions, and failure-injection outcomes. A failed restore preserves
the original backup and prior target according to the declared atomicity policy.

## Phase completion

The selected distribution and topology lanes must be green on the exact
candidate. Conditional topology lanes may be enforceably excluded. Historical
chaos, tailer, container, or restore results do not accept the current source.
