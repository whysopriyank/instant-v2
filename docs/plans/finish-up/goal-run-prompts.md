# Goal-mode prompts

These prompts are model-agnostic. Replace angle-bracket values and use the
harness's native goal and sub-agent syntax. Do not paste the entire finish-up
directory into the model context.

## Mandatory prompt routing

| Packet kind | Required prompt | Restriction |
|---|---|---|
| Ordinary source/harness repair | Prompt A | Implementation only inside the packet lease |
| F-002 or another owner decision | Prompt C only | Read-only proposal; no self-approval or completion without owner approval |
| Provider, v1, database service, Linux/container, chaos, recovery, soak, performance, publish, or deploy evidence | Prompt D, then Prompt A only after authority passes | Preflight is safe; external mutation requires exact authority |
| Multi-packet phase coordination | Prompt B | Coordinator is read-only and dispatches fresh packet contexts |

When a packet combines classes, the strictest routing applies. Prompt A must not
be used to make F-002 implementation changes or bypass an authority gate.

## Prompt A — Execute one packet

```text
Create one goal with the exact Goal objective from packet <PACKET_ID> in
docs/plans/finish-up/<PHASE_FILE>.

Read, in order:
1. repository AGENTS.md;
2. docs/plans/finish-up/00-operating-contract.md;
3. the <PACKET_ID> section only from <PHASE_FILE>;
4. the <PACKET_ID> row in program-manifest.md;
5. DEC-001 and only the predecessor handoffs cited by that packet;
6. current production source/tests on its real path.

Capture the Git/toolchain baseline before editing. Preserve all pre-existing
changes. Work only <PACKET_ID>; do not select another packet.

Build a live row ledger. Prove trustworthy desired-behavior red evidence unless
the behavior is already repaired or a precise blocker is recorded. Make the
smallest owning-layer change. Run the focused green test, proportional adjacent
checks, and the required fresh review. Never claim an unexecuted environment,
provider, v1, chaos, soak, performance, publish, or deployment lane.

Sub-agents:
- first use one read-only scout only if the production path is not already clear;
- use one implementation worker per disjoint package lease;
- use a test worker for focused execution/log reduction when valuable;
- spawn the required reviewer after the concrete diff and evidence exist;
- never assign overlapping write leases or more than three writers;
- workers may not commit, push, publish, deploy, contact external systems, or
  perform destructive actions.

Stop after two failed focused repair attempts, any missing owner decision or
authority, any required cross-package/public contract not covered by the packet,
or a rejecting mandatory review. Return the exact handoff from
00-operating-contract.md. Mark the goal complete only when every required row is
GREEN or an enforceable ACCEPTED_EXCEPTION and no required work remains.
```

## Prompt B — Coordinator-only phase loop

Use this only when the harness can create fresh worker contexts and the phase
contains multiple packets. The coordinator does not edit product code.

```text
Create one coordinator goal: complete the selected packets of phase <NN> without
implementing them in the coordinator context.

Read README.md, 00-operating-contract.md, program-manifest.md, the phase file,
DEC-001, and existing packet handoffs. Select the first ready packet. Dispatch it
in a fresh worker context using Prompt A. Wait for its handoff and required
review. Validate scope, evidence, and status before updating the phase ledger.

Start the next packet only in another fresh context. Never allow overlapping
package leases. Stop the coordinator goal on missing policy/authority, rejected
review, changed baseline, unresolved interface decision, or any PARTIAL/BLOCKED
required packet. Do not skip it and declare the phase complete.

The phase goal is complete only when every DEC-001-selected packet in this phase
has an accepted handoff and the phase completion rule is satisfied. Then return a
phase summary and stop before the next phase.
```

## Prompt C — Decision packet

```text
Execute decision packet <ID> as a read-only architecture/security analysis.
Present bounded options, consequences, dependent packets, default recommendation,
and exact proposed decision text. Do not implement the preferred option and do
not mark it approved. Approval must name the owner and be written to the durable
decision document before implementation packets consume it.
```

## Prompt D — External evidence packet

```text
Execute only the preflight for <PACKET_ID> unless this invocation explicitly
names the owned environment, allowed external mutations, credential source,
cleanup authority, and evidence destination. Never echo secrets. Bind evidence to
the exact candidate, binary/image, config, fixture, host, and UTC window. If any
identity or authority is missing, return BLOCKED after safe preflight; do not
substitute mocks or historical results.
```

## Recommended Muse Spark 1.3 context envelope

For each worker provide only:

- operating contract;
- one packet section;
- its manifest row and decision references;
- baseline summary;
- cited source files/functions and relevant tests;
- predecessor handoff summaries, not their complete logs.

Ask workers to return paths, test names, exit codes, exact failed/passed
invariants, and concise evidence references. Keep broad searches and noisy output
inside scouts/test workers. A fresh session should consume the handoff rather
than the entire previous conversation.
