# Precision packet template

Use this only when a phase discovers an independent defect or missing decision
that cannot fit its active file lease. Register the packet in the phase ledger;
do not implement it opportunistically.

## `<ID>` — `<bounded title>`

**Class:** one or more of `CONFIRMED_DEFECT`, `CONDITIONAL_DEFECT`, `EDGE_DEFECT`,
`CONTRACT_GAP`, `ASSEMBLY_GAP`, `MISSING_EVIDENCE`, `EVIDENCE_GAP`,
`VERIFICATION_GAP`, `POLICY_DECISION`, `HARNESS_DEFECT`, `EXTERNAL_EVIDENCE`,
`ENVIRONMENT_EVIDENCE`, `RELEASE_EVIDENCE`, `DOCUMENTATION_DEFECT`,
`TECH_DEBT`, `MAINTAINABILITY`, `OPERABILITY`, or `EXTERNAL_MUTATION`
**Risk:** `low | medium | high | critical`
**Goal objective:** `<one externally observable outcome>`

### Problem narrative

Describe the current production entry point, state transition, trust/persistence
boundary, observed wrong behavior, and user impact. Separate confirmed source
facts from hypotheses.

### Contract ledger

| Row | Desired invariant | Real path | Red expectation | Evidence | Status |
|---|---|---|---|---|---|
| `<ID>a` | | | | | `PENDING` |

### Decisions and prerequisites

- Required decision:
- Required predecessor packet:
- Required environment/authority:
- What may be safely completed without them:

### Ownership

- Exclusive write lease:
- Read-only dependencies:
- Coordinator-owned files/interfaces:
- Explicit non-goals:

### Deterministic evidence design

- Setup and fixture ownership:
- Exact desired-behavior test:
- Failure injection or concurrency barriers:
- Exact state/result comparison:
- Cleanup:

### Implementation boundary

State the smallest acceptable owning-layer change. List designs or support
expansions the worker must not invent.

### Verification

| Ring | Command or run | Required result |
|---|---|---|
| Focused red | | Intended assertion, nonzero exit |
| Focused green | | Exact test passes |
| Adjacent | | Owning packages pass |
| Static/build | | Relevant checks pass |
| Real-path evidence | | Named artifact/result |

### Independent review

Name the required reviewer role and the specific claims it must attempt to
disprove.

### Stop conditions

List missing decisions, authority failures, unsafe targets, repeated failures,
or contradictory requirements that force `BLOCKED`/`PARTIAL`.

### Completion

State the exact conditions for `GREEN` and any permitted
`ACCEPTED_EXCEPTION`. A test file, implementation diff, or historical run alone
must never satisfy completion.
