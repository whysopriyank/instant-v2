# Instant v2 finish-up program

Status: `IN_PROGRESS_DEC001_SINGLE_NODE_ALPHA`
Owner decisions recorded: DEC-001 single-node-alpha (approved
`DEC-001-single-node-alpha-20260905` + successor
`DEC-001-rt001-bounded-rebinding-20260917`); FU-01 managed multi-client
recorder path; FU-02 Option A report-only single-flow capture
(`fu02-recorder-scope-decision.md`); FU-03 Option 1 stable 501 exclusion;
FU-01 2B executed 2026-09-23 (recorder + 3 multi-client flips, CF-003
19/3/4); QR-005 pins and FR-001 docs accepted 2026-09-23;
WS recording excluded by enforcement; no v1-parity, provider,
multi-node, replica, performance, publication, deployment, or canary
claim selected.
Planning baseline: `5ccea252c70e47fda970caccf6c7feb5945d3968` (historical)
Last source audit: 2026-09-04

## Purpose

This directory is the executable finish-up plan for the first complete release
cycle of instant-v2. It converts the repository-wide discovery in
[`../gaps-backlog-precision-build-contract.md`](../gaps-backlog-precision-build-contract.md)
into small, ordered goal contracts suitable for OpenCode, DeepSeek, Muse Spark
1.3, Codex, or another harness that can create goals and delegate bounded work.

The earlier contract remains the detailed evidence ledger and historical packet
record. This program is the orchestration layer. It deliberately does not ask a
single model context to implement the entire remaining repository.

## Governing rule

One goal run owns one packet inside one phase document. A phase is a milestone
and may require several sessions; this is intentional because the storage,
compatibility, and release phases are too broad for one small-model context. A
run may complete multiple packets only when the phase explicitly declares a
single atomic batch with disjoint file leases. It must never silently select a
second packet or continue into the next phase.

Every new session starts from this index, reads
[`00-operating-contract.md`](00-operating-contract.md), selects the first runnable
packet, and then loads only its phase document plus the source files it cites.

## Program sequence

| Phase | Goal | Depends on | Expected sessions |
|---|---|---|---:|
| 01 | Freeze the supported release profile and current truth | — | 1 owner decision session |
| 02 | Make realtime authorization and delivery trustworthy | 01 | 2–3 implementation sessions |
| 03 | Close storage, backup, admin, and auth integrity gaps | 01; parts may follow 02 | 3–5 sessions |
| 04 | Make chaos, soak, and benchmark evidence fail closed | 01; EV-001 additionally requires RT-002 | 2–4 sessions |
| 05 | Complete selected corpus coverage and pinned-v1 differential | 02–04 selected product paths | 3+ sessions plus external fixture session |
| 06 | Qualify selected topology, runtime, and recovery behavior | 01–05 as applicable | 2–5 environment sessions |
| 07 | Qualify capacity/performance and assemble release controls | 04–06 | 2–4 sessions |
| 08 | Reconcile public truth and accept one immutable candidate | all selected phases | 1–2 sessions |
| 09 | Retire explicitly deferred maintainability and schema debt | release scope or later owner priority | one session per packet |

Phase documents:

1. [`01-scope-and-decisions.md`](01-scope-and-decisions.md)
2. [`02-realtime-correctness.md`](02-realtime-correctness.md)
3. [`03-data-auth-integrity.md`](03-data-auth-integrity.md)
4. [`04-evidence-tooling.md`](04-evidence-tooling.md)
5. [`05-compatibility.md`](05-compatibility.md)
6. [`06-topology-recovery.md`](06-topology-recovery.md)
7. [`07-qualification-release.md`](07-qualification-release.md)
8. [`08-final-reconciliation.md`](08-final-reconciliation.md)
9. [`09-deferred-tech-debt.md`](09-deferred-tech-debt.md)

Use [`packet-template.md`](packet-template.md) for any defect discovered during a
phase that cannot be repaired inside the active packet's scope.

Harness entrypoints:

- [`program-manifest.md`](program-manifest.md) — canonical packet order,
  dependencies, initial classification, and legacy mapping.
- [`goal-run-prompts.md`](goal-run-prompts.md) — copy-paste prompts for one packet
  or a coordinator-only phase loop.

## Why the work is split this way

The remaining work contains several different kinds of uncertainty:

- owner policy decisions that an implementation agent must not invent;
- security, data-integrity, and distributed-consistency changes needing an
  independent review;
- local source repairs that can be proven with focused deterministic tests;
- external acceptance campaigns requiring owned databases, services,
  credentials, hardware, or deployment authority;
- release claims that are valid only for one clean immutable candidate.

Combining those into one goal encourages guessed policy, overlapping edits,
untraceable evidence, and false completion. The phase boundaries place decisions
before implementation, implementation before compatibility evidence, and
candidate qualification before release claims.

## Global state and classification vocabulary

Packet lifecycle state, contract-row result, and evidence class are separate
fields. A harness must not infer one from another.

Packet states:

| State | Meaning |
|---|---|
| `NOT_SELECTED` | Conditional packet not selected by the current envelope. |
| `READY` | Dependencies satisfied and available for a goal. |
| `PENDING` | Selected but not yet started; dependencies may still be open. |
| `ACTIVE` | One goal currently owns the packet lease. |
| `PARTIAL` | Useful work exists but required rows remain. |
| `BLOCKED` | Named decision, authority, environment, dependency, or review is absent. |
| `COMPLETE` | Every required row is accepted at the required evidence level. |
| `EXCLUDED_APPROVED` | Owner exclusion is enforced in code, docs, corpus, and gates. |
| `DEFERRED` | Deliberately postponed planning backlog; not complete or accepted. |

Contract-row results are only `PENDING`, `PROVEN_RED`, `GREEN`, `BLOCKED`,
`ACCEPTED_EXCEPTION`, or `SUPERSEDED`.

Evidence classes describe why work exists, for example `CONFIRMED_DEFECT`,
`CONTRACT_GAP`, `ASSEMBLY_GAP`, `POLICY_DECISION`, `MISSING_EVIDENCE`,
`CONDITIONAL_DEFECT`, `HARNESS_DEFECT`, `DOCUMENTATION_DEFECT`, `TECH_DEBT`, or
`EXTERNAL_MUTATION`. They are not statuses.

`IMPLEMENTED`, `TESTS_EXIST`, or `LOOKS_FIXED` are observations, not completion
states.

## Phase selection algorithm

The coordinator must:

1. Read the phase-result records at the end of completed phase documents or in
   the current execution ledger.
2. Select the lowest numbered phase whose prerequisites are satisfied and which
   DEC-001 marks required.
3. Create one goal using the packet's exact goal text.
4. Dispatch only the packet workers allowed by the phase ownership map.
5. Wait for implementation, focused verification, and required independent
   review.
6. Write a packet handoff with raw command outcomes and unresolved rows.
7. Complete the goal only if every packet row is `GREEN` or an enforceable
   `ACCEPTED_EXCEPTION`.
8. Stop. A new session selects the next packet. The phase gate closes only when
   all of its required packets are complete.

If a prerequisite is missing, the coordinator records `BLOCKED` with the exact
missing artifact or authority. It must not replace a real-provider, Linux,
PostgreSQL, v1, chaos, performance, signing, or deployment run with a mock and
call the phase complete.

## Default release recommendation

Until the owner approves something broader, the smallest defensible first
release is a **single-node alpha**:

- primary PostgreSQL only;
- no read replica or cross-node invalidation claim;
- explicitly selected HTTP/WS/SSE operations;
- explicit `501` for presence/sync/stream operations that remain unsupported;
- durable configured local storage, with object backup excluded unless assembled;
- Google/GitHub only if real-provider acceptance is authorized and passes;
- no Apple OAuth claim;
- no frozen-v1 or comparative-performance claim until their evidence phases pass.

This is a recommendation, not approval. Phase 01 must record the owner's actual
decision.

## Program completion

The finish-up program is complete only when:

- DEC-001 is approved and names every supported or excluded surface;
- every unconditional and selected packet has a current-candidate handoff;
- every exclusion fails clearly and is reflected in corpus and documentation;
- the composed gate rejects missing prerequisites, skipped selections, and
  mismatched evidence;
- one clean immutable SHA passes independent acceptance;
- public status, self-host, upgrade, support, and release documents describe
  exactly that candidate;
- deployment/canary is complete only if deployment was separately authorized.

## Current known constraint

The audited worktree is a moving development candidate with 57 tracked modified
files and 24 untracked files. All executing agents must recapture the baseline.
No run may attribute release evidence to an unrecorded or changing tree.
