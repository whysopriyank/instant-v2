# Goal-mode operating contract

## Exact coordinator goal

```text
Execute exactly one selected packet from one phase in docs/plans/finish-up. Preserve all
pre-existing work, create bounded sub-agents only where the phase grants disjoint
ownership, prove each invariant through the real production path, and stop after
the packet handoff. Do not begin a successor packet or phase. Do not commit, push, publish,
deploy, contact external providers, interrupt services, or delete resources
unless the current invocation explicitly authorizes that action.
```

## Session bootstrap

Before creating workers or editing files, the coordinator must capture:

```sh
git rev-parse HEAD
git branch --show-current
git status --short
git diff --name-only
git diff --stat --compact-summary
go version
```

Record the UTC timestamp and the selected phase. Existing changes are user-owned.
Never reset, stash, overwrite, broadly format, or reclassify them as phase work.

Read repository `AGENTS.md`, this file, the selected phase, its prerequisites,
and the cited current source. Historical task checkboxes are not authority.

## Goal lifecycle

When the harness exposes `/goal` or a goal API:

1. Inspect the current goal. Do not replace an unrelated unfinished goal.
2. Create one goal with the selected packet's exact goal objective.
3. Do not set a token budget unless the user supplied one.
4. Keep row status inside the packet ledger; do not create one goal per test.
5. Complete the goal only after the packet completion rule is satisfied.
6. If the harness has a delayed blocked threshold, return a `BLOCKED` phase
   handoff immediately but update goal state only according to harness rules.
7. End the session after the handoff.

## Context-budget rules for small models

- Load one phase document, not the whole finish-up directory.
- Give each worker only its packet, relevant source paths, baseline summary, and
  required evidence format.
- Scouts return symbol paths and exact observations, not broad repository prose.
- Test workers return selected test names, exit codes, and the first actionable
  failure, not full logs.
- Reviewers receive the contract ledger, diff, tests, raw outcomes, and draft
  claim. They are not told the author's confidence or desired verdict.
- If context pressure rises, stop after the current packet handoff. Never compress
  an unresolved architecture or security decision into an assumption.

## Agent dispatch contract

The coordinator owns requirements, cross-package decisions, shared files,
integration, and the final phase verdict.

| Work | Preferred role | May write? |
|---|---|---|
| Entry points, call paths, existing tests | scout | No |
| Bounded product or harness repair | implementation worker | Only leased paths |
| Focused verification and log reduction | test worker | Tests only when assigned |
| Multi-cause failure after two attempts | debugger/integrator | Owning path after root cause |
| Architecture/public compatibility decision | architect | No |
| Authorization, secrets, destructive paths | security reviewer | No |
| Final correctness/evidence challenge | reviewer/release gate | No |

Use no more than three concurrent sub-agents by default. Concurrent writers must
have disjoint package-level leases. `cmd/instantd`, Makefile, `.github/**`, shared
interfaces, migrations, protocol schema, and public docs remain coordinator-owned
unless a phase explicitly grants a narrower lease.

Workers may not commit, push, tag, publish, deploy, contact people/providers,
operate unowned services, or perform destructive actions.

## Per-packet loop

Every implementation packet follows the same bounded loop:

1. **Discover:** trace the production entry point and check graph/index coverage.
2. **Ledger:** copy every requirement row with status `PENDING`.
3. **Red:** run a deterministic desired-behavior test and record the intended
   failing assertion. If the behavior is already repaired, record that fact and
   create a regression guard without manufacturing a failure.
4. **Repair:** make the smallest owning change. Do not redesign adjacent systems.
5. **Focused green:** run the exact test. High-risk tests receive one clean rerun.
6. **Adjacent checks:** package test, formatter/static/build checks proportional
   to the changed surface.
7. **Review:** independently try to falsify every requirement. Security,
   persistence, distributed state, and public compatibility require a fresh
   specialist review.
8. **Repair limit:** after two focused failed attempts or two review/repair
   cycles, stop and escalate rather than continuing blindly.
9. **Handoff:** report status, changes, evidence, not-run work, risks, and next
   prerequisite.

## Evidence rules

A passing command proves only what it executed. Record the command, working
directory, selection count where available, exit status, and artifact path.

- Zero selected tests are failure, not evidence.
- Compile-only is not runtime acceptance.
- Package tests are not v1 compatibility.
- Synthetic benchmarks are not production performance.
- Historical evidence does not accept a changed candidate.
- A component implementation is not daemon assembly.
- A configuration file is not a successful publish.
- A listener test is not serving-path recovery.
- A finite quiescence window does not prove no later event can ever occur.

External evidence must name the candidate SHA/binary digest, configuration
fingerprint, fixture identity, UTC window, host/runtime identity, result, cleanup
outcome, and private raw-artifact location/hash. Secrets and personal data must
not enter source, prompts, shared logs, or durable markdown.

## Authority gates

Stop before any of the following unless the invocation supplies exact authority:

- owner policy approval;
- external provider calls or credentials;
- starting/stopping PostgreSQL, v1, MinIO, or other services;
- live chaos, crash, signal, or deletion operations;
- long soak or performance use of named hardware;
- tag, registry, release, signing, deployment, canary, or rollback mutations.

The absence of authority is a valid `BLOCKED` outcome, not permission to invent a
simulation.

## Required phase handoff

```markdown
# Phase NN handoff

Status: COMPLETE | PARTIAL | BLOCKED
Candidate: <full SHA and dirty-tree fingerprint>
Decision/profile: <DEC-001 ID/status>

## Ledger
| Row | Result | Evidence | Remaining |

## Changes
- <file/symbol and behavior>

## Verification
| Command/run | Selected | Exit | Meaning |

## Independent review
- Reviewer role/context:
- Findings repaired:
- Remaining findings:

## Not run
- <exact lane and reason>

## Scope audit
- Pre-existing changes preserved:
- Phase-owned files:
- Unexpected changes:

## Next prerequisite
- <one exact artifact, decision, authority, or next phase>
```

The phase is `COMPLETE` only when all required rows are `GREEN` or enforceable
`ACCEPTED_EXCEPTION`, all mandatory evidence has been inspected, and no required
work remains.
