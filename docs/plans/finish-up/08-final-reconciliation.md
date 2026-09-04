# Phase 08 — Documentation truth and immutable candidate acceptance

Phase gate: `FIRST_RELEASE_ACCEPTED`
Packets: `FR-001` through `FR-004`

## FR-001 — Documentation reconciliation

**Goal objective:** `Reconcile every current public and operator-facing document with the approved release envelope, current source, and accepted evidence while preserving historical reports as historical.`

Mandatory corrections include:

- current corpus 18/22/7/2 and matrix 9/15/1 counts;
- repaired implementation packets versus remaining environment acceptance;
- actual Go 1.25 requirement;
- required storage secret/development override in self-host startup;
- configured durable storage/object-store reality;
- post-commit plus LISTEN/NOTIFY serving behavior versus WAL wording;
- real tag/publish workflow availability;
- unsupported Apple, presence, sync/stream, cross-node rooms, replicas, COPY, or
  other DEC-001 exclusions;
- frozen-v1, same-database upgrade/rollback, chaos, soak, recovery, and
  performance claims only where current evidence exists;
- stale `tasks/phase-*`, roadmap, quality-scorecard, quality-verification,
  conformance, orchestration, self-host, upgrade, README, and corpus prose.

Write lease: current docs only. Preserve historical artifacts and label their
candidate/date; do not rewrite them as current. Validate local links, commands,
versions, counts, support tables, and evidence references. Documentation cannot
close a missing product or environment packet.

## FR-002 — Clean immutable candidate acceptance

**Goal objective:** `Run the composed release gate in a fresh independent context against one clean immutable candidate and issue a requirement-by-requirement verdict tied to its exact artifacts.`

Require full SHA/tree, clean status, toolchain/dependency/config/fixture identity,
selected packet handoffs, approved exceptions, private raw evidence references,
and non-author review. Run the gate on the same candidate and inspect selections,
skips, exit codes, artifact checksums, and content root. Any source/config/build
input change invalidates affected evidence.

A fresh reviewer tries to falsify authorization, data integrity, compatibility,
recovery, provenance, and claim wording. Status is `COMPLETE` only when every
selected row is green and all artifacts agree. This packet does not tag, publish,
or deploy without separate authority.

## FR-003 — Publish accepted artifacts

**Goal objective:** `With explicit publication authority, publish only the FR-002-accepted immutable artifacts and verify every external digest, signature, SBOM, provenance record, and release reference.`

Preflight exact tag/ref, registry/repository, permissions, dry-run result, artifact
digests, rollback of draft state, and responsible operator. Report every external
mutation. If no publication is requested, this packet remains unselected; local
release acceptance may still complete under the chosen profile.

## FR-004 — Canary and rollback

**Goal objective:** `With explicit deployment authority, execute a bounded canary using the accepted digest, predeclared traffic/data scope, health and correctness budgets, observation window, stop triggers, and proven rollback target.`

Before deployment, name target, artifact digest, configuration/secret source,
schema compatibility, backup/restore readiness, traffic scope, metrics, exact
state probes, rollback command/owner, and abort thresholds. Observe and either
widen or roll back exactly under the runbook. Preserve evidence of deployment,
decisions, probes, metrics, and rollback result.

Without target credentials and authority, return `BLOCKED`; a local simulation is
not a canary.

## Final completion rule

`FIRST_RELEASE_ACCEPTED` means:

- DEC-001 is approved;
- every required packet is green and every exception is enforceable;
- the current docs describe exactly the accepted candidate;
- one clean immutable candidate passed independent acceptance;
- published artifacts, if selected, match that candidate;
- deployed canary, if selected, uses the accepted digest and completed its
  declared observation/rollback contract.

The final handoff must state the achieved profile precisely—for example
`single-node alpha accepted`—and must not translate it into broader production,
v1-parity, multi-node, replica, performance, provider, or deployment claims.
