# Finish-up program manifest

This is the canonical selection table for goal-mode coordination. It records
planning state, not live execution state. Each executing session must reconcile
its packet against current source and the latest handoff before changing status.

## Packet registry

| Order | Packet | Phase | Current state / evidence class | Depends on | Evidence level required |
|---:|---|---:|---|---|---|
| 1 | F-001 candidate truth | 01 | `COMPLETE / ACCEPTED_SOURCE_BACKED_LEDGER` | — | source + review |
| 2 | F-002 DEC-001 envelope | 01 | `COMPLETE / OWNER_APPROVED_POLICY_DECISION` | F-001, owner | approved decision |
| 3 | RT-001 permission rebinding | 02 | `COMPLETE / ACCEPTED_BOUNDED_REBINDING` | F-002 rule policy | integrated + security review |
| 4 | RT-002 delivery/watermarks | 02 | `COMPLETE / ACCEPTED_DISCONNECT_REPLAY_DELIVERY` | F-002, RT-001 | integrated + reliability review |
| 5 | RT-003 depth-one recovery | 02 | `COMPLETE / ACCEPTED_HERMETIC_RECOVERY` | F-002 | focused + package |
| 6 | DA-001 durable storage assembly | 03 | `PARTIAL / LOCAL_REOPEN_ACCEPTED_ENV_PENDING` | F-002 storage profile | integrated + restart |
| 7 | DA-002 upload integrity | 03 | `COMPLETE / ACCEPTED` | F-002 compatibility policy | focused + DB-backed data review + Sol gate |
| 8 | DA-003 backup fail-closed | 03 | `PARTIAL / LOCAL_DB_ACCEPTED_EXTERNAL_PENDING` | DA-001 | integrated + data review |
| 8a | DA-004V dynamic view rules | 03 | `COMPLETE / EXCLUSION_ENFORCED_GATE_ACCEPTED` | F-002 permission policy | DB/runtime + corpus/release-gate evidence |
| 9 | DA-004 admin permission fidelity | 03 | `COMPLETE / ACCEPTED_DB_SECURITY_REVIEWED` | F-002 permission policy | DB integration + security review |
| 10 | DA-005 admin mutation atomicity | 03 | `COMPLETE / ACCEPTED_DB_ATOMICITY_REVIEWED` | F-002, stable admin candidate | DB integration + data review |
| 11 | DA-006A OAuth local contract | 03 | `COMPLETE / ACCEPTED_DB_SECURITY_REVIEWED` | F-002 provider policy | focused/integrated + security review |
| 12 | DA-006B provider acceptance | 03 | `BLOCKED / MISSING_EVIDENCE` | DA-006A, credentials/authority | real environment |
| 13 | DA-007 rate-limit policy | 03 | `COMPLETE / ACCEPTED_SINGLE_NODE` | F-002 security/topology policy | focused/integrated + Sol security gate |
| 13a | DA-008A magic-code local contract | 03 | `COMPLETE / ACCEPTED_DB_SECURITY_REVIEWED` | F-002 auth policy | focused/integrated + security review |
| 13b | DA-008B magic-code provider acceptance | 03 | `BLOCKED / EXTERNAL_EVIDENCE` | DA-008A, provider/recipient authority | real environment |
| 14 | EV-001 soak transaction ledger | 04 | `COMPLETE / ACCEPTED_BOUNDED_HARNESS` | RT-002 semantics | focused + short real smoke |
| 15 | EV-002 soak evidence output | 04 | `COMPLETE / ACCEPTED_BOUNDED_HARNESS` | F-002 artifact policy | focused failure injection |
| 16 | EV-003 soak process identity | 04 | `COMPLETE / ACCEPTED_NATIVE_SCRIPT` | F-002 target OS | native target |
| 17 | EV-004 chaos provenance | 04 | `COMPLETE / ACCEPTED_BOUNDED_HARNESS` | accepted T-001 safety | focused + security review |
| 18 | EV-005 benchmark evidence | 04 | `COMPLETE / ACCEPTED_BOUNDED_BUNDLE` | F-002 artifact policy | focused + reviewer |
| 19 | EV-006 benchsmoke wiring | 04 | `COMPLETE / HISTORICAL_ENTRYPOINT_ENFORCED` | F-002 harness-entrypoint decision | build + smoke |
| 20 | CF-001 COPY acceptance | 05 | `COMPLETE / ACCEPTED_POSTGRES_COPY` | F-002, owned PostgreSQL | DB integration |
| 21 | CF-002 recorder/fixtures | 05 | `PARTIAL / ACCEPTED_BOUNDED_CAPTURE` | F-002 surfaces | integrated capture |
| 22 | CF-003 matrix closure | 05 | `PARTIAL / HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING` | selected product packets, CF-002 | real-path matrix |
| 23 | CF-004 pinned-v1 environment | 05 | `BLOCKED / EXTERNAL_EVIDENCE` | F-002, service authority | qualified v1 runtime |
| 24 | CF-005 differential | 05 | `BLOCKED / EXTERNAL_EVIDENCE` | CF-003/004 | compatibility + independent review |
| 25 | OP-001 publisher recovery | 06 | `NOT_SELECTED / CONDITIONAL_DEFECT` | multi-node selected | fault integration + distributed review |
| 26 | OP-002 replica visibility | 06 | `NOT_SELECTED / CONDITIONAL_DEFECT` | replica selected | lag integration + distributed review |
| 27 | OP-003 native Linux | 06 | `BLOCKED / ENVIRONMENT_EVIDENCE` | target selected | native runtime |
| 28 | OP-004 container runtime | 06 | `BLOCKED / ENVIRONMENT_EVIDENCE` | container selected, DA-001 | container runtime |
| 29 | OP-005 crash/bounce/drain | 06 | `BLOCKED / ENVIRONMENT_EVIDENCE` | phases 02–04, OP-003/004 | recovery campaign |
| 30 | OP-006 backup/restore drill | 06 | `BLOCKED / ENVIRONMENT_EVIDENCE` | DA-001/003, owned target | restore campaign |
| 31 | QR-001 production soak | 07 | `BLOCKED / ENVIRONMENT_EVIDENCE` | EV-001..003, OP qualification | qualified soak bundle |
| 32 | QR-002 comparative performance | 07 | `NOT_SELECTED / MISSING_EVIDENCE` | EV-005/006, CF-005, QR-001 | qualified comparison |
| 33 | QR-003 composed gate | 07 | `COMPLETE / ACCEPTED_CONTRACT_GATE` | selected packet interfaces stable | gate contract tests |
| 34 | QR-004 publish/sign/SBOM | 07 | `NOT_SELECTED / ASSEMBLY_GAP` | publish selected, QR-003 | dry-run workflow evidence |
| 35 | QR-005 supply-chain inputs | 07 | `PARTIAL / INVENTORY_PREFLIGHT_ACCEPTED_PINS_PENDING` | release workflow selected | static + release review |
| 36 | FR-001 documentation truth | 08 | `PENDING / DOCUMENTATION_DEFECT` | all selected behavior resolved | link/source/evidence review |
| 37 | FR-002 immutable acceptance | 08 | `BLOCKED / RELEASE_EVIDENCE` | FR-001, QR-003, all selected packets | clean independent gate |
| 38 | FR-003 publication | 08 | `NOT_SELECTED / EXTERNAL_MUTATION` | FR-002, publish authority | external verification |
| 39 | FR-004 canary/rollback | 08 | `NOT_SELECTED / EXTERNAL_MUTATION` | FR-002/003, deploy authority | deployment evidence |
| 40 | TD-001 realtime efficiency/observability | 09 | `DEFERRED / TECH_DEBT` | RT-001/002 stable | focused performance/reliability |
| 41 | TD-002 request lifecycle/origin hardening | 09 | `DEFERRED / TECH_DEBT` | DA-006A policy | focused + security review |
| 42 | TD-003 migration/deferred schema inventory | 09 | `DEFERRED / POLICY_DECISION+TECH_DEBT` | release profile | decision/integration by selected row |
| 43 | TD-004 modularity hotspots | 09 | `DEFERRED / MAINTAINABILITY` | behavior stable | equivalence + package tests |
| 44 | TD-005 storage reconciliation | 09 | `DEFERRED / OPERABILITY` | DA-001/002 | reconciliation integration |

Phase 01 completion closes only the source-backed candidate-truth and
owner-policy decision packets. The current working candidate is dirty and no
immutable release candidate has been selected. F-001/F-002 completion does not
complete FR-001, FR-002, or any product, compatibility, qualification, or
release-evidence packet.

DEC-001 packet selections remain REQUIRED or DEFERRED as recorded in the
approved envelope. A DEFERRED row, including one described as a proposed
exclusion, is not EXCLUDED until its exact surface is inaccessible or fails
explicitly and that restriction is consistently enforced by product behavior,
documentation, corpus coverage, and the composed release gate.

## Legacy packet mapping

| Existing ID | Finish-up owner |
|---|---|
| CP-001..003 | F-001 accounts for accepted candidate work; unresolved admin semantics move to DA-004/005 |
| DEC-001 | F-002 |
| P-001 | F-001 records implemented signup-rule assembly; RT-001/DA-004/DA-004V own remaining permission gaps |
| P-002/P-011 | DA-006A/B |
| P-003 | DA-008A/B; request-context/origin cleanup also appears in TD-002 |
| P-004 | F-001 records backoff closure; RT-002/003 own remaining delivery/queue gaps |
| P-005 | F-001 records implementation closure; CF-003 supplies surface evidence |
| P-006 | CF-001 |
| P-007/P-008 | CF-003 if implemented; otherwise enforceable exclusions |
| P-009 | DA-007 |
| P-010 | DA-001..003 and OP-006 |
| C-001/C-002 | F-001 records implementation acceptance; CF-002/003 verify complete comparator/capture use |
| C-003/C-004 | CF-002/003 |
| C-005 | CF-004/005 |
| T-001 | prerequisite accepted; EV-004 retains remaining T-002 work |
| T-002 | EV-004 |
| B-001 | F-001 records accepted core; EV-005/006 own newly found evidence gaps |
| O-001/O-002 | OP-001/002 |
| O-003 | OP-003/004 |
| O-004 | QR-001 |
| O-005 | OP-005/006 |
| PERF-001 | QR-002 |
| D-001 | F-001 initial ledger, FR-001 final public reconciliation |
| REL-001..004 | QR-003/004 and FR-002..004 |

## Selection rules

1. Start with order 1 and 2.
2. After F-002, apply DEC-001's REQUIRED/DEFERRED selections. Do not convert a
   DEFERRED or proposed-exclusion row to EXCLUDED until the required enforcement
   evidence is complete.
3. Among selected packets, choose the lowest order whose dependencies are green.
4. A packet with environment/authority prerequisites may be skipped temporarily
   in favor of an independent ready packet, but remains blocked—not complete.
5. A product defect discovered in phases 05–07 returns to a new bounded packet
   before evidence resumes on a new candidate.
6. FR-001 waits until supported behavior is stable; FR-002 is always the final
   local acceptance packet.
7. Phase 09 packets are not automatic release blockers. Promote one before
   FR-002 only when DEC-001 selects the affected behavior or evidence shows it
   materially changes release correctness, security, or operability.
