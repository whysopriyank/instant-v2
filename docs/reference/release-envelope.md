# Proposed release envelope

Decision ID: `DEC-001-dev-checkpoint-20260904`
Status: `PROPOSED` — not an approval and not a release claim
Proposed at (UTC): `2026-09-04T02:10:07Z`
Candidate scope: `codex/quality-implementation-checkpoint` at
`5ccea252c70e47fda970caccf6c7feb5945d3968`
Supersedes: none
Decision owner: **UNASSIGNED**
Owner approval: **PENDING** — name, UTC timestamp, and immutable approval
evidence reference are required before this document can become `APPROVED`.

This is the durable proposed output for
[DEC-001](../plans/gaps-backlog-precision-build-contract.md#dec-001--supported-release-envelope-and-policy-freeze).
It selects the smallest credible **Development checkpoint** default from the
available evidence. It does not authorize production use, support a public
surface, accept an exception, or select a later packet. An agent must not treat
any `DEFERRED` row as `EXCLUDED`, nor treat this proposal as owner approval.

## Decision schema and change control

An approved replacement must retain this decision ID as its predecessor and use
a new ID in the form `DEC-001-<scope>-<UTC date>`. It must contain explicit
values (not `TBD`) for every field below, a named owner, timestamp, and immutable
approval evidence. Any changed packet selection, supported/excluded surface,
budget, external authority, candidate/ref, or compatibility claim requires a
new owner-approved decision.

`EXCLUDED` is valid only when the approved decision names the exact surface and
the source, support documentation, corpus matrix, and release gate enforce the
failure/inaccessibility. This proposal has no accepted exclusions. `DEFERRED`
means unselected and unavailable as a release claim, not silently unsupported.

## Evidence basis

- The candidate is a development checkpoint with a pre-existing dirty product
  tree; the precise preservation and stabilization work is CP-001 through
  CP-003 in the [precision contract](../plans/gaps-backlog-precision-build-contract.md).
- The repository documents a single-binary, self-host-first direction, but also
  says that current status is partial and does not establish production readiness
  or frozen-v1 compatibility in the [root README](../../README.md) and
  [quality verification](quality-verification.md).
- Current evidence lacks a pinned-v1 differential run, a qualified performance
  bundle, release artifacts, deployed provider acceptance, and production
  recovery/soak/container acceptance. These are tracked by C-003 through C-005,
  O-003 through O-005, PERF-001, and REL-001 through REL-004.

## Proposed profile

| Field | Proposed development-checkpoint value | Approval-required follow-up |
|---|---|---|
| Profile | `Development checkpoint` | Owner may select a different profile only in a new decision. |
| Candidate/ref | The ref named above; no immutable release candidate is selected. | Pin a clean immutable SHA for any release acceptance. |
| OS/architecture | No native or container support target is selected. The authoring host is not a support claim. | Name each OS/arch and native/container distribution. |
| Topology | No deployment topology is selected; no multi-node or read-replica claim. | Select single-node/multi-node and primary/replica semantics. |
| SDK/protocol compatibility | No frozen SDK version, transport, or v1-parity claim. | Name SDK versions, operations, and accepted differences. |
| Runtime/API surface | No public HTTP, WS, SSE, admin, auth, storage, presence, rooms, sync, or stream support claim. | Enumerate supported routes/operations and enforced exclusions. |
| Providers and magic code | No deployed Google, GitHub, Apple, custom OIDC, or magic-code delivery support claim. | Select providers, delivery path, nonce/retry lifecycle, and evidence. |
| Persistence and backup | No durable local-disk/S3/object-backup, migration, rollback, or restore support claim. | Select durable modes and prove the required drills. |
| Security policy | No owner acceptance of rate-limit capacity behavior, origins, secrets, or failure envelopes. | Record exact policy and security-review evidence. |
| Compatibility | No corpus, pinned-v1, or differential compatibility claim. | Freeze corpus/v1 policy and accepted-difference registry. |
| Performance | No comparative performance claim or performance acceptance gate. | Declare hardware, cells, budgets, and measurement authority. |
| Capacity/recovery | No capacity, latency, error, memory, drain, recovery, RPO, or RTO promise. | Declare measurable budgets and qualifying run authority. |

## Proposed development constraints and budgets

These are guardrails for the proposed checkpoint, not production service-level
objectives:

| Area | Proposed value | Consequence |
|---|---|---|
| Verification | Hermetic build/tests plus only the package integrations explicitly selected by the coordinator. | Passing checks establish only their recorded scope. |
| Live databases/endpoints | No authority is granted by this artifact. | A packet requiring an owned database, local service, or credentials remains blocked until separately authorized. |
| Chaos/live faults | No chaos invocation or live fault authority. | T-001/T-002 remain deferred; unsafe chaos output cannot be accepted. |
| Soak/recovery/performance | No campaign duration, load, latency, error, resource, RPO/RTO, or comparative budget. | O-004, O-005, and PERF-001 cannot run as acceptance work. |
| Publishing/deployment | No tag, publish, signing, SBOM, registry, canary, or rollback authority. | REL-002 and REL-004 remain deferred. |

## External-authority registry

| Authority ID | Activity | State | Required approver/evidence before use |
|---|---|---|---|
| `AUTH-PROVIDER-001` | Real OAuth/OIDC provider or magic-code delivery acceptance | `NOT_GRANTED` | Named owner, target/credentials scope, redacted artifact location, security review. |
| `AUTH-FAULT-001` | Live process, PostgreSQL, destructive chaos, or fault injection | `NOT_GRANTED` | Named owner, exact owned resources, rollback/cleanup plan, safety review. |
| `AUTH-PERF-001` | Soak or comparative performance campaign | `NOT_GRANTED` | Named owner, quiet hardware, frozen workload/budgets, artifact destination. |
| `AUTH-PUBLISH-001` | Tag, sign, SBOM, registry, or release publication | `NOT_GRANTED` | Named owner, protected ref, credential/OIDC model, dry-run evidence. |
| `AUTH-DEPLOY-001` | Canary, traffic, production data, or rollback operation | `NOT_GRANTED` | Named operator, target, data/traffic scope, runbook, rollback authority. |

## Accepted-difference registry

No accepted differences are proposed or approved. A future entry must have a
stable ID, exact v1/v2 behavior, impacted surface, evidence artifact, expiry or
review date, enforcement location, compatibility-claim effect, and owner
approval reference. A missing entry is a discrepancy, not an implicit exception.

## Packet selection ledger

All rows are scoped to the candidate above. `REQUIRED` means required to finish
the proposed development checkpoint. `DEFERRED` means no work is selected by this
proposal. No row is `EXCLUDED` because an unapproved proposal cannot create an
enforceable support restriction.

| Packet | Selection | Exact surface / rationale | Dependency and enforcement/evidence reference |
|---|---|---|---|
| CP-001 | `REQUIRED` | Validate the existing transaction ordering and permission-state dirty candidate. | First; handoff under the precision contract. |
| CP-002 | `REQUIRED` | Validate the existing auth attribute-cache dirty candidate. | First; handoff under the precision contract. |
| CP-003 | `REQUIRED` | Validate the existing admin mutation/runtime dirty candidate. | CP-001 and CP-002; handoff under the precision contract. |
| DEC-001 | `REQUIRED` | Persist this proposed support/authority inventory; owner approval remains pending. | CP status known; this document. |
| P-001 | `DEFERRED` | Runtime signup-rule wiring is not selected for this checkpoint. | CP-002/003; no auth support claim. |
| P-002 | `DEFERRED` | OAuth route assembly is not selected. | CP-002/003 and a future envelope; no provider support claim. |
| P-003 | `DEFERRED` | Magic-code delivery adapter/policy is not selected. | CP-002 and future delivery authority. |
| P-004 | `DEFERRED` | Reactive retry lifecycle is not selected. | Stabilized candidate; no realtime support claim. |
| P-005 | `DEFERRED` | `serverCreatedAt` ordering is not selected. | Future query/order surface decision. |
| P-006 | `DEFERRED` | COPY parity is not selected and must not be exposed. | CP-001; no COPY support claim. |
| P-007 | `DEFERRED` | Admin presence semantics are not selected. | Future admin-surface decision. |
| P-008 | `DEFERRED` | Sync/stream semantics are not selected. | Future protocol-surface decision. |
| P-009 | `DEFERRED` | Rate-limit capacity policy is not owner-accepted. | Future security owner and review. |
| P-010 | `DEFERRED` | Durable object/file storage assembly is not selected. | CP-003 and future storage decision. |
| P-011 | `DEFERRED` | Deployed provider/OIDC nonce acceptance is not selected. | P-002 plus `AUTH-PROVIDER-001`. |
| C-001 | `DEFERRED` | Exact-number corpus loading is not selected; no comparator claim. | Stabilized candidate. |
| C-002 | `DEFERRED` | Volatile masking/quiescence work is not selected; no comparator claim. | Stabilized candidate. |
| C-003 | `DEFERRED` | HTTP/SSE capture adapters are not selected. | Future supported paths and capture authority. |
| C-004 | `DEFERRED` | Coverage matrix expansion is not selected. | Product/corpus selection. |
| C-005 | `DEFERRED` | Pinned-v1 differential acceptance is not selected. | C-001 through C-004 and external local-service authority. |
| T-001 | `DEFERRED` | Chaos safety repair is not selected; chaos invocation remains prohibited. | Stabilized candidate; `AUTH-FAULT-001` for any future run. |
| T-002 | `DEFERRED` | Chaos provenance repair is not selected; chaos output is not acceptance evidence. | T-001 and `AUTH-FAULT-001`. |
| B-001 | `DEFERRED` | Benchmark evidence repair is not selected; benchmark output is not acceptance evidence. | Stabilized candidate; no performance claim. |
| O-001 | `DEFERRED` | Multi-node invalidation recovery is not selected. | Future multi-node topology decision. |
| O-002 | `DEFERRED` | Read-replica visibility is not selected. | Future read-replica decision. |
| O-003 | `DEFERRED` | Native Linux/container acceptance is not selected. | Future shipped-target decision. |
| O-004 | `DEFERRED` | Soak/capacity evidence is not selected. | Future budgets and `AUTH-PERF-001`. |
| O-005 | `DEFERRED` | Recovery/drain/backup acceptance is not selected. | O-003, future durability decision, and `AUTH-FAULT-001`. |
| PERF-001 | `DEFERRED` | Qualified comparison is not selected. | B-001/C-005/O-003..005, budgets, `AUTH-PERF-001`. |
| D-001 | `DEFERRED` | Final release-support documentation reconciliation follows selected packets only. | Final selected handoffs. |
| REL-001 | `DEFERRED` | Composed release gate is not selected. | Future approved envelope and final docs. |
| REL-002 | `DEFERRED` | Publishing/signing/SBOM workflow is not selected. | `AUTH-PUBLISH-001`. |
| REL-003 | `DEFERRED` | Immutable release-candidate acceptance is not selected. | REL-001 and an approved release envelope. |
| REL-004 | `DEFERRED` | Canary/rollback is not selected. | REL-003 and `AUTH-DEPLOY-001`. |

## Approval record

Approval is intentionally blank. To approve a successor, record all fields
below and change its status to `APPROVED` only after the named owner has made the
decision outside the implementation agent.

| Field | Required value |
|---|---|
| Decision ID / predecessor | New ID and `DEC-001-dev-checkpoint-20260904` |
| Approved profile and candidate/ref | Explicit profile, clean SHA/tree, configuration scope |
| Approving owner | Name and role |
| Approval timestamp (UTC) | ISO-8601 UTC timestamp |
| Immutable evidence reference | Issue, signed record, or durable approval artifact |
| Packet changes | Every row changed from this ledger, with reason and enforcement location |
| Authority grants | Registry IDs, exact targets, credentials/data scope, expiry, and rollback/cleanup plan |
| Support/budget values | Every profile field and capacity/recovery/performance budget |
