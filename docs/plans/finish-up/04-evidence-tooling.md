# Phase 04 — Evidence and harness integrity

Phase gate: `EVIDENCE_TRUSTWORTHY`
Packets: `EV-001` through `EV-006`

This phase repairs the tools before their output is used to accept the product.
It does not run destructive chaos, long soak, or qualified performance.

## EV-001 — Per-transaction soak ledger

**Goal objective:** `Make cmd/soak account for every committed transaction through acknowledgment, required refresh, terminal error, and final quiescence.`

Replace the single `lastTxAt` model with a transaction-ID ledger. The protocol
owner must define whether one commit may yield multiple refresh frames.

| Row | Invariant |
|---|---|
| EV-001a | Every committed transaction creates exactly one ledger entry. |
| EV-001b | Refresh/ack frames correlate to known entries; unknown, duplicate, missing, or invalid-order outcomes fail. |
| EV-001c | Protocol error frames become classified terminal failures. |
| EV-001d | Completion requires zero unresolved entries after bounded quiescence. |
| EV-001e | Evidence records send/ack/refresh times and terminal reason per transaction. |

Write lease: `cmd/soak/**`; do not change product refresh semantics. Deterministic
tests cover overwritten writes, out-of-order, duplicate, missing, late, unknown,
and error frames. If transaction identity cannot be correlated from the protocol,
stop and escalate to RT-002 instead of inventing timing heuristics.

## EV-002 — Mandatory soak evidence and atomic output

**Goal objective:** `Ensure a successful soak cannot discard mandatory diagnostics, overwrite prior evidence, or publish a partial bundle as complete.`

DEC-001 labels artifacts required or optional. Required pprof startup, event
write, flush, close, rename, and finalization failures must affect exit and
eligibility. Create a fresh output directory, write temporary artifacts, publish
atomically, and add a completeness manifest. Existing/finalized output must fail.

Write lease: `cmd/soak/**`; `scripts/quality-soak.sh` only with a coordinator
lease. Inject writer, pprof, close, disk-full/permission, existing-path, symlink,
and rename failures. Do not add metrics or modify workload budgets.

## EV-003 — Soak process identity

**Goal objective:** `Bind every quality-soak process sample to the expected executable, candidate, configuration, endpoint, PID, and start instance.`

`kill -0` and a numeric PID are insufficient. On supported Linux targets record
process start time, executable identity/hash, command/config digest, endpoint,
and candidate SHA. Detect stale PID reuse, wrong executable, replacement, and
endpoint mismatch. If a platform cannot provide trustworthy identity, mark the
evidence lane unsupported/blocked rather than accepting PID-only sampling.

Write lease: `scripts/quality-soak.sh` and narrow provenance helpers. No live
long soak is part of this packet.

## EV-004 — Chaos provenance and safe success

**Goal objective:** `Finish T-002 so a chaos PASS proves the exact source, binary, process, fixture, and replay outcome, with no candidate fallback or swallowed failure.`

T-001 deletion safety is already implementation-accepted. Remaining rows:

- atomic source/snapshot provenance resists same-UID replacement;
- postmaster/process identity is descriptor- or start-instance-bound;
- build, start, transport, decode, replay, zero-selection, report-write, and
  cleanup failures all propagate;
- no fallback from requested dirty candidate to HEAD;
- report cannot emit PASS before cleanup and complete artifact finalization.

Write lease: `cmd/chaos/**`. Use harmless temporary fixtures and injected process
boundaries; do not run faults. Require destructive-path security review.

## EV-005 — Benchmark collector and bundle integrity

**Goal objective:** `Make required benchmark collector failures and bundle-finalization failures render evidence ineligible and immutable.`

This goal contains two sequential rows in `internal/benchrun/**`:

- `EV-005a`: define when database Before/After snapshots are required; propagate
  collector failure into report and claim eligibility.
- `EV-005b`: make checksums/content root/finalization atomic and write-once;
  approved bundles cannot be truncated or regenerated in place.

Inject before/after collector failure, unsupported collector, serialization,
existing bundle, interruption, permission, symlink, and checksum mismatch.
Preserve checksum algorithm/schema unless separately reviewed. No live benchmark
or budget changes.

## EV-006 — Benchsmoke entrypoint wiring

**Goal objective:** `Make advertised benchmark build and acceptance targets compile and exercise cmd/benchsmoke, or formally remove that command from the supported harness.`

The Makefile currently omits `cmd/benchsmoke` from benchmark package tests and
builds, while `make bench-smoke` invokes another command.

First decide in DEC-001's harness-entrypoint row whether benchsmoke is the
supported entry point or historical code. This is not a worker decision.
If supported, include its package/binary in `BENCH_PACKAGES`, `build-bench`, and
the intended acceptance path without duplicating runners. If historical, remove
the false roadmap/command claim and enforce the replacement mapping.

Coordinator owns Makefile; command tests remain under `cmd/benchsmoke/**`. Verify
package test selection, build, target-to-binary mapping, and a disposable short
smoke run only after prerequisites are validated.

## Phase completion

The phase gate closes when all selected harnesses fail closed under injected
failure and reviewers confirm their success states cannot omit mandatory
evidence. Passing harness unit tests do not constitute product soak, chaos,
recovery, or performance acceptance.
