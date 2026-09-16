# Phase 01 — Scope, decisions, and candidate truth

Phase gate: `SCOPE_FROZEN`
Packets: `F-001`, `F-002`
Parallel execution: no

This phase prevents implementation agents from inventing product, security,
topology, compatibility, or release policy. It creates the durable decision that
all later packets consume and records the actual state of the candidate without
rewriting historical evidence.

## F-001 — Candidate truth ledger

**Goal objective:** `Create a source-backed current-candidate truth ledger that distinguishes implemented behavior, unaccepted implementation, confirmed defects, explicit exclusions, and missing evidence without changing product code.`

**Class:** documentation/evidence integrity.
**Risk:** high because every successor goal consumes this status.

### Contract ledger

| Row | Invariant | Evidence | Status |
|---|---|---|---|
| F-001a | Revision, branch, dirty-tree fingerprint and timestamp identify the audited candidate. | Git baseline and diff inventory | `GREEN` |
| F-001b | Every old red item is reconciled against current source and its latest executed evidence. | Current code, tests, prior handoffs | `GREEN` |
| F-001c | Implementation completion is distinct from integration, compatibility, production, and release acceptance. | Status matrix with evidence class | `GREEN` |
| F-001d | Historical results remain preserved and are clearly scoped to their candidate/date. | Link audit | `GREEN` |
| F-001e | Every newly discovered gap has one stable packet ID and owning phase. | Finish-up cross-reference audit | `GREEN` |

### Ownership and method

Write lease: finish-up plan documents and a new current-status ledger under
`docs/plans`; do not edit product code, historical evidence bodies, or public
support claims yet. Use source and executed evidence ahead of prose. Where graph
coverage is stale, partial, or excluded, read the current file directly.

The ledger must explicitly mark the implementation-level closures already found:
pagination/cursor fixes, exact-number lookup, OAuth redemption atomicity,
Google/GitHub resolution mechanics, reactive backoff, COPY semantics, metrics,
config work, signup-rule assembly, explicit unsupported responses, missing-mailer
failure, and HTTP/SSE corpus adapter mechanics.

It must also register the new finish-up packets from phases 02–08. Do not mark an
item green solely because a related test exists or because an old report says it
passed.

### Verification and review

- Validate every local link and cited file.
- Compare corpus counts to `corpus/manifest.json` rather than copying old prose.
- Compare the ledger against the existing master packet table.
- Fresh read-only reviewer tries to find an untracked gap, contradicted status,
  or claim lacking current-candidate evidence.

**Complete when:** a single current ledger accounts for every old and new packet,
with no product edits and no unresolved status contradiction hidden as green.

## F-002 / DEC-001 — Owner-approved release envelope

**Goal objective:** `Produce and obtain owner approval for DEC-001, selecting the smallest useful release profile and explicitly deciding every conditional product, topology, security, compatibility, durability, and evidence obligation.`

**Class:** policy decision. No implementation worker can self-approve it.

### Required decision rows

| Area | Decision that must be durable |
|---|---|
| Release level | Development checkpoint, single-node alpha, single-node production, multi-node production, or read-replica production |
| Distribution | Native OS/architectures, container, public image/package, Go version |
| Protocol | Supported HTTP, WS, SSE, admin, room, presence, sync and stream operations |
| Auth | Magic code, Google, GitHub, Apple, custom OAuth/OIDC, nonce/replay requirements |
| Permissions | Dynamic view rules and live-session rule-change behavior |
| Storage | Durable local root, S3/object storage, backup/restore, COPY exposure |
| Topology | Single/multi-node, primary/read replica, invalidation guarantees |
| Security | Origins, secret requirements, rate-limit saturation/distribution policy |
| Compatibility | Named SDK versions, pinned-v1 differential claim, accepted differences |
| Reliability | Capacity, latency, errors, drain, RPO/RTO, recovery and soak budgets |
| Performance | Whether comparative performance is a release gate or expressly unclaimed |
| Harnesses | Supported chaos/soak/benchmark entrypoints and which artifacts are mandatory |
| Delivery | Tag/publish/sign/SBOM/canary targets and who may authorize external mutations |

### Recommended initial profile

Propose single-node alpha on primary PostgreSQL, durable configured local storage,
selected HTTP/WS/SSE operations, explicit unsupported errors for incomplete
presence/sync/stream operations, and no replica/multi-node/Apple/v1-parity/
comparative-performance claim until their phases pass.

The owner may select a broader profile, but the decision must add its dependent
packets rather than treating existing code as proof.

### Durable output

Update `docs/reference/release-envelope.md` with a decision ID, status, UTC date,
candidate/ref scope, named approving owner, evidence reference, and one row per
finish-up packet: `REQUIRED`, `EXCLUDED`, or `DEFERRED`.

An exclusion is valid only when the path is inaccessible or fails explicitly,
the corpus and docs agree, and the release gate enforces the exclusion.

Current status (2026-09-08): F-001 is complete as a source-backed ledger; F-002
is complete because `DEC-001-single-node-alpha-20260905` is owner-approved. This
freezes policy scope only; the working tree remains dirty and no immutable
release candidate or downstream packet is accepted.

### Stop conditions

Return `BLOCKED` if no owner approves the profile. Do not infer approval from
source code, prior chat, roadmap priority, or availability of credentials.

## Phase completion

`SCOPE_FROZEN` requires F-001 green and F-002 approved. If F-002 is blocked,
unconditional correctness packets may be prepared, but no conditional topology,
provider, storage, compatibility, performance, publish, or deployment claim may
be completed.
