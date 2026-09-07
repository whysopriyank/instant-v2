# Proposed release envelope

Decision ID: `DEC-001-single-node-alpha-20260905`
Status: `APPROVED` — single-node alpha release profile
Proposed at (UTC): `2026-09-05T08:31:04Z`
Approved at (UTC): `2026-09-05T08:35:12Z`
Candidate scope: `main` at `14e2988e79851b34e340b41ebaf7ea109132b70e`
WITH uncommitted work (RT-001 rebinding PARTIAL, RT-002 delivery repair
implemented-unproven, F-001 ledger) — no immutable release candidate is
selected; FR-002 must run on a clean tree.
Supersedes: `DEC-001-dev-checkpoint-20260904` (retained in git history)
Decision owner: **Priyank, project owner**
Owner approval: **APPROVED by Priyank, project owner, at
`2026-09-05T08:35:12Z`. Evidence: owner approval recorded in the finish-up
working session (goal `goal-b42ffd44-b3b2-455a-8de3-f46f87a7d149`) following
the F-002 single-node-alpha proposal; successor changes require a new
owner-approved decision per the schema below.

This is the durable proposed output for
[DEC-001](../plans/gaps-backlog-precision-build-contract.md#dec-001--supported-release-envelope-and-policy-freeze)
and [F-002](../plans/finish-up/01-scope-and-decisions.md). It selects the
smallest defensible first release: **single-node alpha** on primary
PostgreSQL. It does not authorize production use, accept an exception, or
select a later packet. An agent must not treat any `DEFERRED` row as
`EXCLUDED`, nor treat this approved profile as broader than its rows:
approval covers exactly the selections above, nothing implied.

## Decision schema and change control

An approved replacement must retain this decision ID as its predecessor and use
a new ID in the form `DEC-001-<scope>-<UTC date>`. It must contain explicit
values (not `TBD`) for every field below, a named owner, timestamp, and immutable
approval evidence. Any changed packet selection, supported/excluded surface,
budget, external authority, candidate/ref, or compatibility claim requires a
new owner-approved decision.

`EXCLUDED` is valid only when the approved decision names the exact surface and
the source, support documentation, corpus matrix, and release gate enforce the
failure/inaccessibility. This proposal has no accepted exclusions: rows marked
"proposed exclusion" become `EXCLUDED` only on approval. `DEFERRED`
means unselected and unavailable as a release claim, not silently unsupported.

## Evidence basis

- Current-candidate truth ledger
  [`docs/plans/current-candidate-truth-ledger.md`](../plans/current-candidate-truth-ledger.md)
  (F-001, reconciled 2026-09-05; pass-1 rows historically verified with
  current-candidate acceptance pending).
- RT-001 permission rebinding: repair implemented, PARTIAL, uncommitted;
  runtime red/green owed to a runnable host.
- RT-002 delivery semantics: repaired per the frozen transparent re-gate +
  explicit-disconnect policy (reconnect from full snapshot); runtime proof
  owed to a runnable host.
- Current evidence still lacks a pinned-v1 differential run, a qualified
  performance bundle, release artifacts, deployed provider acceptance, and
  production recovery/soak/container acceptance.

## Proposed profile

| Area | Proposed single-node-alpha value |
|---|---|
| Release level | Single-node alpha: one `instantd`, primary PostgreSQL only |
| Distribution | From-source on an owner-named Linux target qualified via OP-003; container unqualified until OP-004; Go ≥1.25 |
| Protocol | WS + SSE transports for covered query/transact paths (unit/harness suite: fallback, overflow-detach, per-IP cap, heartbeat, init guard, snapshot-error close) + HTTP admin/runtime/storage per CF-003 matrix; presence and sync/stream OPERATIONS explicitly unsupported until their rows pass (501 enforced at the op dispatcher, pinned by `unsupported_test.go`) — SSE-transport selection CORRECTED post-approval from a stale exclusion row; owner ratification pending, no broader change |
| Auth | Magic-code delivery excluded (503 retained); Google/GitHub local contract required, real-provider acceptance excluded (no provider claim); Apple excluded |
| Permissions | Dynamic view rules excluded (rejection enforced); live sessions transparently re-gated at refresh boundaries (reauthorize-or-detach realtime policy) |
| Storage | Durable configured local root required (DA-001 assembles); object backup excluded (503 enforced) |
| Topology | Single node; no multi-node (OP-001) or read-replica (OP-002) claim |
| Security | Bounded fail-closed rate-limit overflow (DA-007 implements); production requires explicit `INSTANT_V2_WS_ALLOWED_ORIGINS` (wildcard dev-only, gate-rejected for alpha) |
| Compatibility | No frozen SDK, transport, or v1-parity claim; corpus matrix closed per CF-003 for selected surfaces |
| Reliability | RPO: no promise; RTO 1h single-node restart; drain 30s SIGTERM |
| Performance | No comparative performance claim or acceptance gate (QR-002 excluded) |
| Harnesses | `cmd/benchsmoke` declared historical (EV-006 removes false claims); soak/chaos/bench entrypoints supported for dev use with mandatory artifacts per EV repairs |
| Delivery | No tag/publish/sign/SBOM/canary; only a named owner may authorize external mutations |

## Proposed alpha qualification envelope

Guardrails for the alpha (not production SLOs):

| Area | Proposed value | Consequence |
|---|---|---|
| Verification | Hermetic build/tests plus only the package integrations explicitly selected by the coordinator | Passing checks establish only their recorded scope |
| Soak | One 500-session × 15 min run on a named owned host, full time-series + exact teardown (QR-001); 150-session CI lane is smoke only; 5k×30m explicitly not required | O-004-style capacity claims cannot be made |
| Live databases/endpoints | No authority is granted by this artifact | Any packet needing an owned database, local service, or credentials stays blocked until separately authorized |
| Chaos/live faults | No chaos invocation or live fault authority | Destructive lanes stay deferred; unsafe chaos output cannot be accepted |
| Publishing/deployment | No tag, publish, signing, SBOM, registry, canary, or rollback authority | REL-002/REL-004 and FR-003/FR-004 remain unselected |

## External-authority registry

| Authority ID | Activity | State | Required approver/evidence before use |
|---|---|---|---|
| `AUTH-PROVIDER-001` | Real OAuth/OIDC provider or magic-code delivery acceptance | `NOT_GRANTED` | Named owner, target/credentials scope, redacted artifact location, security review |
| `AUTH-FAULT-001` | Live process, PostgreSQL, destructive chaos, or fault injection | `NOT_GRANTED` | Named owner, exact owned resources, rollback/cleanup plan, safety review |
| `AUTH-PERF-001` | Soak or comparative performance campaign | `NOT_GRANTED` | Named owner, quiet hardware, frozen workload/budgets, artifact destination |
| `AUTH-PUBLISH-001` | Tag, sign, SBOM, registry, or release publication | `NOT_GRANTED` | Named owner, protected ref, credential/OIDC model, dry-run evidence |
| `AUTH-DEPLOY-001` | Canary, traffic, production data, or rollback operation | `NOT_GRANTED` | Named operator, target, data/traffic scope, runbook, rollback authority |
| `AUTH-RUNTIME-001` | DB-backed/loopback runtime proof runs (red/green/corpus slice) on an owned host | `NOT_GRANTED` | Named owner, exact host/database scope, cleanup plan |

## Accepted-difference registry

No accepted differences are proposed or approved. A future entry must have a
stable ID, exact v1/v2 behavior, impacted surface, evidence artifact, expiry or
review date, enforcement location, compatibility-claim effect, and owner
approval reference. A missing entry is a discrepancy, not an implicit exception.

## Packet selection ledger

All rows are scoped to the candidate above. `REQUIRED` means required to finish
the proposed alpha. `DEFERRED` means no work is selected by this proposal; rows
marked "proposed exclusion" are enforced as exclusions only on approval. No row
is `EXCLUDED` because an unapproved proposal cannot create an enforceable
support restriction.

| Packet | Selection | Exact surface / rationale |
|---|---|---|
| F-001 | `REQUIRED` | Current-candidate truth ledger; reconciled |
| F-002 | `REQUIRED` | This decision; approved by Priyank at `2026-09-05T08:35:12Z` (reaffirmed `2026-09-06` working session) |
| RT-001 | `REQUIRED` | Dynamic permission rebinding (transparent re-gate); runtime proof owed |
| RT-002 | `REQUIRED` | Refresh outcome semantics (explicit disconnect + full replay); runtime proof owed |
| RT-003 | `REQUIRED` | Depth-one recovery or validation exclusion of depth one |
| DA-001 | `REQUIRED` | Durable local storage assembly; object backup proposed exclusion |
| DA-002 | `REQUIRED` | Upload atomicity and signed metadata |
| DA-003 | `REQUIRED` | Backup fail-closed (local paths; object paths stay 503) |
| DA-004V | `REQUIRED` | Dynamic view rules: enforce exclusion consistently |
| DA-004 | `REQUIRED` | Admin permission-check fidelity |
| DA-005 | `REQUIRED` | Admin provisioning/mutation atomicity |
| DA-006A | `REQUIRED` | OAuth local lifecycle contract (no provider claim) |
| DA-006B | `DEFERRED` | Real provider acceptance; proposed exclusion from alpha |
| DA-007 | `REQUIRED` | Rate-limit saturation policy (bounded fail-closed) |
| DA-008A | `REQUIRED` | Magic-code fail-closed retention and enforcement |
| DA-008B | `DEFERRED` | Real delivery acceptance; proposed exclusion from alpha |
| EV-001..006 | `REQUIRED` | Harness repairs incl. benchsmoke historical ruling |
| CF-001 | `DEFERRED` | COPY acceptance; proposed exclusion (no production caller) |
| CF-002 | `REQUIRED` | Recorder and fixture lifecycle |
| CF-003 | `REQUIRED` | Selected coverage matrix closure |
| CF-004/005 | `DEFERRED` | Pinned-v1 env/differential; proposed exclusion (no parity claim) |
| OP-001/002 | `DEFERRED` | Multi-node/replica; proposed exclusion |
| OP-003 | `REQUIRED` | Native Linux qualification of the declared target |
| OP-004 | `DEFERRED` | Container qualification; proposed exclusion until declared |
| OP-005 | `REQUIRED` | Crash/bounce/drain core |
| OP-006 | `DEFERRED` | Backup/restore drill; requires backup selection |
| QR-001 | `REQUIRED` | Alpha-envelope soak (500×15m) |
| QR-002 | `DEFERRED` | Comparative performance; proposed exclusion |
| QR-003 | `REQUIRED` | Composed fail-closed candidate gate |
| QR-004 | `DEFERRED` | Publish workflow; unselected |
| QR-005 | `REQUIRED` | Supply-chain reconciliation (bounded) |
| FR-001/002 | `REQUIRED` | Documentation truth; immutable acceptance on a clean tree |
| FR-003/004 | `DEFERRED` | Publish/canary; unselected |
| TD-001..005 | `DEFERRED` | Debt; promote only on material evidence |

## Approval record

| Field | Recorded value |
|---|---|
| Decision ID / predecessor | `DEC-001-single-node-alpha-20260905` / `DEC-001-dev-checkpoint-20260904` |
| Approved profile and candidate/ref | Single-node alpha; `main` at `14e2988e79851b34e340b41ebaf7ea109132b70e` with uncommitted RT-001/RT-002/F-001 work disclosed above; no immutable candidate selected |
| Approving owner | Priyank, project owner |
| Approval timestamp (UTC) | `2026-09-05T08:35:12Z` |
| Immutable evidence reference | Finish-up working session approval record (goal `goal-b42ffd44-b3b2-455a-8de3-f46f87a7d149`); this file's git history |
| Packet changes | None from the proposal: REQUIRED/DEFERRED ledger above stands as approved |
| Authority grants | None granted; every `AUTH-*` registry row remains `NOT_GRANTED` |
| Support/budget values | Profile and alpha envelope tables above, approved as written |
