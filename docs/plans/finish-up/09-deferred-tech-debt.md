# Phase 09 — Deferred technical debt

Phase gate: `SELECTED_DEBT_RETIRED`
Packets: `TD-001` through `TD-005`

These are real debts found during the audit, but they are not automatically
release blockers. DEC-001 or fresh evidence may promote a packet into an earlier
phase. Otherwise, execute one packet per later goal after the release behavior is
stable. Do not mix cleanup with a correctness or measurement run.

## TD-001 — Realtime efficiency and error observability

**Goal objective:** `Remove avoidable realtime group contention and make non-fatal render/send failures observable without changing accepted delivery semantics.`

Candidate debts:

- group keys compact JSON but do not canonicalize object-key ordering, so
  semantically equivalent queries may not share a group;
- snapshot-flight coordination is package-global across independent managers;
- some WS/SSE render, encode, and send errors only debug-log or silently return;
- admin SSE encode failure can skip a frame while the stream continues.

First measure/reproduce each behavior. RT-002 owns correctness; this packet may
only improve grouping scope, contention, and observability after its semantics
are frozen. Split into separate goals if more than one production package needs
behavioral change. Run exact equivalence/concurrency tests and compare metrics or
logs, not merely code shape.

## TD-002 — Request lifecycle and origin hardening

**Goal objective:** `Propagate request cancellation through authentication work and enforce the DEC-001 production origin policy without changing protocol responses unexpectedly.`

Current magic-code verification uses `context.Background()` rather than the HTTP
request context. WS origins default to wildcard for compatibility. These are
separate rows:

- disconnect, deadline, and shutdown cancellation reaches database/provider
  work without corrupting a committed redemption;
- production origin configuration is explicit, validated, and covered for
  allowed, missing, and denied origins.

Write lease: auth HTTP lifecycle first, then config/sync origin work under a new
lease. Require security review. Do not change defaults until DEC-001 decides the
compatibility policy.

## TD-003 — Deferred migration and schema families

**Goal objective:** `Convert every explicitly deferred schema family into an approved exclusion or an independently migrated, rollback-aware packet with owning runtime behavior.`

Migration `001` defers OAuth/profile history, email verification, webhooks,
rule-version history, indexing jobs, and WAL/audit partition families. Advisory
unlock errors are also weakly observed.

Inventory each family against DEC-001. For selected behavior create a separate
migration packet containing forward schema, compatibility/read-write path,
backfill, lock budget, retry, rollback/roll-forward strategy, database tests, and
upgrade drill. For excluded behavior, remove any public implication that the
schema/runtime exists. Never add all deferred tables as speculative scaffolding.

## TD-004 — Modularity hotspots

**Goal objective:** `Reduce one proven maintenance hotspot at a time while preserving exact behavior and avoiding abstraction-only churn.`

The audit found twelve hand-written non-test Go files over 500 lines, including
roughly 900-line auth and corpus HTTP files. Size alone does not authorize a
split. Select a file only when ownership confusion, repeated change collisions,
or testability evidence identifies a stable boundary.

Each goal names one file, one responsibility boundary, unchanged public API and
protocol, movement map, and exact pre/post package and corpus checks. No feature,
optimization, dependency, or cross-package redesign may ride with the move.

## TD-005 — Storage reconciliation and abandoned-object operations

**Goal objective:** `Provide bounded detection and repair for abandoned or metadata-inconsistent objects that predate or escape request-time compensation.`

DA-002 prevents new common orphan cases but cannot prove older deployments contain
none. After the durable backend is selected, define an inventory report,
dry-run-only default, exact tenant/object identity, age/safety window, immutable
audit output, and separately authorized deletion mode. Never delete solely from a
filename mismatch or broad prefix. Destructive cleanup requires explicit target
and authority plus security/data-integrity review.

## Phase completion

The phase has no universal deadline. It is complete for a selected milestone
when every owner-promoted debt packet is green and remaining rows are explicitly
deferred with rationale. `DEFERRED` is honest backlog state, not production
acceptance or proof that the issue is harmless.
