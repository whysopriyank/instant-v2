# Phase 03 — Storage, backup, admin, and authentication integrity

Phase gate: `DATA_AND_AUTH_TRUSTWORTHY`
Packets: `DA-001` through `DA-008B`, including `DA-004V`, `DA-006A/B`, and `DA-008A/B`
Parallelism: only DA-002, DA-004V, and DA-007 may run concurrently; all other leases
overlap with assembly, auth, or shared policy and run as separate goals.

## DA-001 — Durable storage profile and runtime assembly

**Goal objective:** `Assemble the DEC-001 storage profile so production never silently uses temporary storage and every disabled object/backup path fails explicitly.`

Current daemon assembly hardcodes `os.TempDir()/instantv2-files` and does not
inject the existing backup object store. `internal/storageapi` and
`internal/backup` also present competing S3 abstractions.

| Row | Invariant |
|---|---|
| DA-001a | Production storage root/backend is explicit, validated, writable, durable, and included in configuration fingerprinting. |
| DA-001b | Restart/reopen preserves stored objects and metadata. |
| DA-001c | Object backup is either correctly assembled or disabled at startup/by explicit stable response. |
| DA-001d | One object-store ownership model is used; credentials never enter logs or artifacts. |
| DA-001e | Self-host/container mounts and permissions agree with runtime validation. |

Prerequisite: approved storage profile. Write lease is `internal/config/**`,
`internal/storageapi/**`, `internal/backup/**`; coordinator alone integrates
`cmd/instantd`. Do not add cloud providers or migrations not selected by DEC-001.

Red evidence must show the current temporary default and missing object assembly.
Green evidence includes restart against a durable temporary test mount, disabled
mode, malformed/unwritable path, and selected object-store backup/reopen. Require
security and data-integrity review. Stop if the owner has not selected a backend
or supplied authority for an external object store.

## DA-002 — Upload atomicity and signed metadata

**Goal objective:** `Prevent orphaned uploads and bind every mutable upload metadata field to the presigned request integrity check.`

Write lease: `internal/storageapi/**` only.

| Row | Desired invariant | Red evidence |
|---|---|---|
| DA-002a | Metadata-link failure removes only the newly written object. | Force duplicate filename/database failure; inspect exact backend keys. |
| DA-002b | Cleanup failure is surfaced and leaves a reconcilable record, not false success. | Inject delete failure. |
| DA-002c | Filename/path metadata cannot be altered on a valid signed URL. | Change filename query value after signing. |
| DA-002d | Retry/cancellation cannot delete a pre-existing object belonging to another request. | Interleave two controlled uploads. |

Before changing the signature shape, determine whether v1 URL compatibility is
required. If it is public-format sensitive, obtain architecture/security review.
Use deterministic fake backends plus one real disk-backend test. Run storageapi
race tests and exact state comparison. Do not redesign file metadata or add a
garbage collector in this packet.

## DA-003 — Backup fail-closed and restore integrity

**Goal objective:** `Make backup authorization, streaming failure, finalization, and restore behavior fail closed with verifiable completeness.`

Current risks include a nil authorization callback panic and a response that may
send HTTP 200 before export later fails.

Required rows:

- constructor or handler rejects missing authorization configuration without a
  panic;
- unauthorized requests produce no data or object-store mutation;
- clients receive a completion/checksum contract capable of detecting truncated
  streaming export;
- incomplete backup artifacts cannot be selected for restore;
- failed restore preserves the source backup and prior target state according to
  the declared atomicity policy.

Write lease: `internal/backup/**`; runtime assembly remains DA-001. Add injected
auth, writer, close, truncation, checksum, and restore failures. Run focused tests
twice, package tests, and security/data-integrity review. Do not execute a live
recovery campaign here; phase 06 owns it.

## DA-004 — Admin permission-check fidelity

**Goal objective:** `Make admin permission-check endpoints reproduce their declared runtime authorization semantics without returning unrestricted admin data for a denied check.`

DEC-001 must choose whether these endpoints evaluate persisted rules, an explicit
request override, or both with unambiguous response metadata. They must never
default silently to permissive behavior.

Contract rows:

- selected rules source and version are explicit;
- denied query checks do not return `Admin: true` unrestricted results;
- transaction checks receive the same relevant `auth`, `data`, `newData`,
  `request`, and parameter bindings as runtime;
- malformed/missing rules and binding failures fail closed;
- response distinguishes evaluation from optional preview data.

Write lease: `internal/adminapi/**`; use `internal/perms` and runtime behavior as
read-only oracle unless an interface handoff is approved. Red tests compare the
admin check with the same rule/data through the runtime path. Security reviewer
must challenge rule-source substitution and denied-data leakage.

## DA-004V — Dynamic data-dependent view rules

**Goal objective:** `Implement the DEC-001-selected runtime semantics for data-dependent view permissions, or enforceably exclude those rules without leaking data or implying compatibility.`

DEC-001 selects an explicit exclusion, not support. Runtime behavior safely
rejects rules that require per-record SQL filtering before protected rows are
fetched or returned. This remains a compatibility exclusion, not an
authorization bypass, and the release must not claim those rules are supported.

If support is selected in a future decision, architecture/security review must
define rule-to-query pushdown,
relationship traversal, missing attributes, nulls, pagination before or after
filtering, count/aggregate leakage, and fail-closed behavior for rules that
cannot be translated. Tests compare exact runtime results under allow, deny, and
mixed records and prove cursors/counts do not reveal filtered rows.

Write lease: `internal/perms/**`; query planner/runtime integration requires a
coordinator interface handoff and sequential leases. Do not use a post-filter
model that first fetches unauthorized data. Mandatory security review. If
excluded, runtime, docs, corpus, and release gate must all reject it consistently.

## DA-005 — Admin provisioning and mutation atomicity

**Goal objective:** `Define and enforce whether high-level admin attribute provisioning is atomic with its requested data mutation.`

Current code commits missing attributes before starting the requested mutation.
Owner/API review must choose:

- atomic provisioning plus mutation; or
- documented retained schema side effects with exact idempotency semantics.

The recommended behavior is one atomic transaction when storage boundaries make
that possible.

Red evidence forces the data mutation to fail after provisioning and compares
exact schema, triples, journal, invalidation, and response state. Include retry
and concurrency for two requests creating the same attribute. Write lease:
`internal/adminapi/**`; any transact/storage interface change is coordinator
owned. Require database-backed tests and data-integrity review. Do not broaden
this into schema migration redesign.

## DA-006A — OAuth/OIDC local lifecycle contract

**Goal objective:** `For each provider selected by DEC-001, implement a bounded one-time state and nonce lifecycle, strict provider configuration, and a callback transaction boundary that avoids holding database locks across external I/O.`

Contract rows cover startup validation; issuer/JWKS/audience/algorithm rules;
nonce creation, persistence, mismatch, replay and one-time use; state/token
expiry; callback replay; provider timeout; JWKS rotation; and concurrent callback
single-winner behavior. Architecture review must preserve atomic redemption while
removing avoidable pooled-connection and row-lock occupation during provider I/O.

Write lease: `internal/authn/**`; coordinator owns config/routes. Use deterministic
local provider servers and owned-database tests, then auth race tests. Apple is a
separate extension if selected; signing code alone is not exchange acceptance.
Mandatory security review.

## DA-006B — Real provider acceptance

**Goal objective:** `Exercise every DEC-001-selected OAuth/OIDC provider through the deployed daemon and retain redacted candidate-bound evidence for start, redirect, callback, identity, denial, expiry, replay, rotation, and configuration failure.`

This external evidence packet owns no product edits. Cover redirect/cookie/PKCE
where applicable, token/user-info or ID-token processing, normalized identity,
replay, denial/error, revoked credentials, redirect mismatch, and selected JWKS
rotation. A product defect stops the run and returns to DA-006A or a new packet.

Secrets and personal data stay out of source, command arguments, prompts, and
durable evidence. Without credentials and explicit sandbox authority, return
`BLOCKED` after preflight. Require security and release-evidence review.

## DA-007 — Rate-limit saturation and topology policy

**Goal objective:** `Implement or enforce DEC-001's rate-limit saturation policy so bucket exhaustion and selected node topology have an explicit abuse-resistant outcome.`

First characterize the current behavior with a tiny cap and rotating identities:
memory remains bounded but new keys bypass enforcement when no entry is evictable.
DEC-001 selects one of:

- bounded fail-closed overflow;
- shared/distributed limiting for multi-node mode;
- perimeter enforcement plus documented process-local fallback;
- explicitly accepted fail-open alpha behavior with enforced deployment limits.

Write lease: `internal/ratelimit/**` and its narrow middleware/config tests.
Distributed infrastructure is not authorized unless multi-node production is
selected. Verify old-key fairness, new-key abuse, eviction, cancellation, memory
bound, and node-count semantics. Require security review. If policy remains
unapproved, return `BLOCKED` after characterization.

## DA-008A — Magic-code delivery assembly

**Goal objective:** `If magic-code authentication is selected, assemble its delivery abstraction and prove locally that send failure, throttling, code persistence, verification, cancellation, and privacy behavior are fail closed through the real HTTP route.`

The current route returns stable 503 before generating or persisting a code when
no mailer exists. That is safe disabled behavior, not a working delivery system.

Required rows:

- configured provider validates at startup and sends only to the normalized
  requested address;
- provider failure/timeout produces no usable persisted code and has explicit
  throttle/retry behavior;
- success persists one bounded-expiry code without logging it;
- verification is one-time and request cancellation propagates safely;
- provider configuration, secret handling, and redacted diagnostics follow
  DEC-001 privacy policy.

Write lease: `internal/authn/**`; coordinator owns config/routes. Local fake
provider tests exercise the real HTTP handler and deterministic delivery seam.
This packet never contacts a real recipient. If excluded, retain and document
the stable fail-closed response and enforce it in the release gate.

## DA-008B — Real magic-code delivery acceptance

**Goal objective:** `Exercise the DEC-001-selected magic-code delivery provider through the deployed daemon and retain redacted candidate-bound evidence for send, receipt, denial, timeout, bounce where observable, expiry, one-time verification, and cleanup.`

This external evidence packet owns no product edits. Preflight exact provider,
sender/domain, allowed test recipient, candidate/config identity, credential
source, privacy/redaction rules, rate/throttle limits, and cleanup. A product
defect returns to DA-008A or a new bounded auth packet; do not patch and continue
the same evidence run.

Never contact a real provider or recipient without explicit authority naming the
target. Store no code, recipient address, provider secret, or personal content in
source, prompts, shared logs, or durable markdown. Without authority, return
`BLOCKED` after safe preflight. Require security and release-evidence review.

## Phase completion

Every packet selected by DEC-001 must be `COMPLETE`. Object storage, providers,
or distributed limiting may reach packet state `EXCLUDED_APPROVED` only when all
exclusion rows are `ACCEPTED_EXCEPTION` and the disablement is enforced
consistently. Database-backed and real-provider rows cannot be closed with mocks.
