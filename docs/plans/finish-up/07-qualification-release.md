# Phase 07 — Capacity, performance, and release controls

Phase gate: `RELEASE_CONTROLS_READY`
Packets: `QR-001` through `QR-005`

Qualification runs are serialized on a quiet named target. Correctness repairs
invalidate affected measurements and require a new candidate/run.

## QR-001 — Production-like soak

**Goal objective:** `Run the DEC-001 capacity and duration envelope with trustworthy per-transaction accounting, immutable evidence, frozen budgets, and exact teardown.`

Prerequisites: EV-001..003, selected platform qualification, immutable candidate,
owned fixture, approved host and budgets.

Record ramp, settle, active measurement, and teardown separately. Required
time-series evidence includes sessions, acknowledged-write ledger, refresh lag,
errors, reconnect/convergence, memory/RSS, file descriptors, goroutines, queues,
connections, CPU where selected, and cleanup. The current short 150-session CI
lane is smoke evidence only. A 5k×30m run is required only if DEC-001 retains that
promise, but any replacement must still name capacity/duration budgets before the
run.

## QR-002 — Qualified comparative performance

**Goal objective:** `If performance is claimed, produce one policy-frozen v1/v2 comparison on qualified targets with full provenance, attempt order, collectors, budgets, and immutable raw evidence.`

Prerequisites: EV-005/006, CF-004/005, OP-003/004, QR-001, quiet hardware, and
explicit execution authority.

Freeze candidate binaries, host, workload cells, fixtures, configuration,
warmup/active periods, attempt count/order, interruption policy, and pass budgets
before execution. Failed qualification stops the campaign; partial bundles cannot
be joined or selectively replaced. Synthetic benchmarks remain harness evidence,
not product claims. Optimize only in a later packet after a confirmed budget miss.

If DEC-001 makes no comparison claim, enforce that absence in public docs, accept
the exclusion rows, and mark this packet `EXCLUDED_APPROVED`.

## QR-003 — Composed fail-closed candidate gate

**Goal objective:** `Create one gate manifest and entrypoint that composes exactly the checks and external prerequisites selected by DEC-001, rejecting dirty candidates, missing artifacts, skipped or zero tests, and advisory failures.`

Inventory and reuse current Make/CI targets. Keep separate lanes for hermetic,
owned-DB integration, corpus, external v1, container, soak, recovery, performance,
and artifact checks. The gate must validate variables, endpoint/candidate
fingerprints, selected test/scenario counts, output freshness, and packet handoff
state. It must not provision or mutate unspecified resources or imply that
`test-release` created services or evidence it merely expects.

Coordinator owns Makefile, workflows, and shared gate manifest. Add contract
tests for every missing prerequisite and failure propagation. Running the gate on
a final candidate belongs to phase 08.

## QR-004 — Reproducible publish, signing, and SBOM workflow

**Goal objective:** `If publishing is selected, assemble a least-privilege protected-tag workflow that builds, inventories, signs, attests, verifies, and drafts immutable release artifacts.`

Define protected ref/tag policy, GitHub permissions, OIDC/cosign behavior,
registry targets, multi-arch identity, checksums, SBOM, provenance, draft/final
release, retention, verification, and failure cleanup. Pin third-party actions and
base images by approved immutable references.

Static validation and an authorized dry run precede publication. Actual tag,
push, registry write, signing, and release publication remain separately
authorized mutations. If publishing is excluded, remove false documentation
claims and enforce the exclusion.

## QR-005 — Supply-chain dependency reconciliation

**Goal objective:** `Make release build inputs reproducible and reviewable by reconciling mutable actions, runtime-installed tools, image bases, module locks, generated artifacts, and provenance capture.`

This is a bounded supply-chain packet, not a dependency-upgrade campaign. Record
every build input; pin only those required by the selected release workflow;
validate generated files and module sums; ensure provenance reports the resolved
digests. Require security/release review.

## Phase completion

All selected qualification and release-control packets are green, but no release
is accepted yet. Phase 08 must run the gate on one clean immutable candidate and
reconcile public truth from that evidence.
