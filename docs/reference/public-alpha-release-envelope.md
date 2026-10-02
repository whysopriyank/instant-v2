# Public alpha release envelope

Decision ID: `DEC-002-single-node-public-alpha-20261002`
Predecessor: `DEC-001-single-node-alpha-20260905`.
Decision owner: Priyank, project owner.
Implementation scope authorized by the owner's instruction: “okay, go ahead and
complete it properly for the public alpha release”, and confirmed scope answer:
“Keep explicit alpha differences; implement restore and release requirements”.
Recorded at UTC `2026-10-02T14:05:51Z`. This records implementation authority;
it does not grant public publication, repository visibility or license changes.

The supported topology is one `instantd` and primary PostgreSQL 17, on an owned
Linux amd64 host. Inherit the supported surfaces, SDK delta-refresh soak scope,
permissions exclusions, local OAuth contract and CF-003 exceptions from the
[predecessor envelope](release-envelope.md). Production use, HA, read replicas,
general v1 drop-in compatibility and a comparative performance claim remain
unselected. No prior candidate acceptance applies to the new binary.

The public candidate additionally requires durable local file restoration from
v1 ZIP, empty-target protection for both restore formats, exact-state restore
qualification, a
DB-backed container check and a working alpha-only distribution pipeline.
Email delivery (503), direct ID-token sign-in (501), admin presence (501),
dynamic view rules and object-store backup routes (503) remain explicit alpha
differences. Real external-provider acceptance is unselected.

## Qualification selection

The source-controlled selection is
[qualification-policy.json](../plans/next-release/qualification-policy.json).
Five live records are mandatory: native Linux, recovery, soak, container and
restore. Every record binds the same immutable candidate,
binary, nonsecret configuration and distribution image, with its own hashed
fixture descriptor. Synthetic records and historical acceptance cannot pass.

CF-004/CF-005 parity and drop-in acceptance are approved exclusions for this
bounded alpha under the owner confirmation below. The actual pinned-v1
comparison remains failed evidence: 2 agreements and 16 failures across 18
scenarios. Excluding a release claim does not turn these results into PASS or
qualify an unmodified SDK. Preserve the
[full rehearsal](../plans/next-release/v1-differential-rehearsal-20261002.md).

## Owner-confirmed compatibility limits — 2026-10-02

Priyank's direct confirmation selected these exact answer strings:

> Document explicit compatibility limits; no v1 parity claim (Recommended)

> Apache-2.0; public source and images

The coordinator received this confirmation around `2026-10-02T16:56Z`
(approximate receipt minute; exact receipt seconds are not asserted). It
authorizes the documented compatibility scope and Apache-2.0/public source and
images selection. It does not establish runtime acceptance, successful
publication or changed remote visibility. The earlier implementation
authorization record is historical; this confirmation supplies its subsequent
compatibility/license/visibility choice.

The following stable limits define what this alpha cannot claim. They retain
the observed differences rather than masking them or rewriting raw captures.
Evidence: [rehearsal inventory](../plans/next-release/v1-differential-rehearsal-20261002.md)
and [query details](../plans/next-release/v1-compatibility-observations.md).

| Limit | Exact boundary and affected surface |
|---|---|
| CF-LIMIT-001 | Query node-list triples omit v1's fourth stored timestamp element. No frozen-SDK `serverCreatedAt`, client ordering, infinite-query or timestamp/cursor fidelity claim. |
| CF-LIMIT-002 | Candidate serialized cursor strings differ from pinned v1's join-row vector input; pagination/cursor interchangeability is not claimed. |
| CF-LIMIT-003 | Error status/type/message/hint/original-event differ for protocol validation, denied writes, unknown attributes, required-field failures and room boundaries. Candidate rejects object-shaped tx-steps that pinned v1 accepts as an empty transaction. |
| CF-LIMIT-004 | Candidate accepts repeated initialization/reset; pinned v1 rejects it and retains prior subscriptions. No reinitialization lifecycle parity. |
| CF-LIMIT-005 | Conflicting repeated attribute/forward-identity declarations are accepted in the candidate rehearsal but rejected by v1 as `record-not-unique`. No schema-identity conflict parity. |
| CF-LIMIT-006 | Title/name/ref-only fixtures lacking ID triples produce nonempty root results in the candidate and empty results in v1. Entity enumeration, aggregate and relation parity for those fixtures is not claimed. |
| CF-LIMIT-007 | Explicit null event IDs and absent presence values differ on observed transaction/room replies. The narrow duplicate/remove-query field repair does not qualify all acknowledgments or presence envelopes. |
| CF-LIMIT-008 | Unique-attribute physical index handling differs for `index? false`; v2 forces indexing while v1 retains false. The separately retained common-index fixture rerun still fails wire comparison. |

These are compatibility claim exclusions, not newly disabled endpoints or
changes to tested v2 behavior. Existing email/direct-ID-token/admin-presence,
dynamic-view, stream and object-backup alpha exclusions remain unchanged.
There is no production or general v1 replacement claim. Any later parity
selection requires its own correct common fixtures, raw evidence and acceptance.

The first alpha is prepared without a comparative performance headline. The
owner was offered the comparative matrix separately; while unanswered, the
minimal implementation retains the predecessor's no-performance-claim policy.
Changing that selection requires a real qualified comparative campaign before
a claim is published. The 500-session, 900-second active-write delta-refresh
soak is still mandatory, with retained semantic acknowledgement/convergence
and teardown evidence. It is not a comparative result.

The restore drill requires at least 1,000,000 triples in each of v1 ZIP and
v2 NDJSON, within 600 seconds per format, exact normalized logical state and
object hashes, and unchanged state/source for corrupt, truncated, oversized,
wrong-app and nonempty-target rejection. Only known rejection is covered by
the unchanged-state promise. Lost COMMIT replies require reconciliation;
objects are retained because SQL might have committed. Crashes before commit
may leave orphan objects. There is no crash-atomic PostgreSQL/filesystem claim.
Restore locks the shared restore tables; use a maintenance window for alpha.

Native v2 NDJSON contains SQL records and file metadata, not object bytes.
Back up and restore the durable file volume alongside it, preserving app/file
keys and ownership. A SQL-only NDJSON export is not a complete file backup.
The drill must identify any companion volume artifact and verify it separately.

Linux arm64 and Darwin arm64 archives can be produced, but compilation alone
does not qualify their runtime. Distribution documentation must identify
the runtime platforms actually checked. Alpha tags and immutable SHA/version
tags are allowed in the prepared workflow; no `latest` tag is created.

## External authority and publication

Use only owned disposable local databases and the previously authorized
bigbeast/cutiepie/cutiewhy qualification hosts. Resource names must start
`iv2q-<campaign>-`, private networks and loopback endpoints only, with verified
cleanup. Fault authority remains the scoped recovery lane; this does not grant
fault injection against existing services. Comparative benchmark authority
and production deployment remain ungranted.

Publication is a final separately reviewed step under `AUTH-PUBLISH-001`.
The owner selected Apache-2.0 and public source/images; this records intended
distribution scope, not proof that visibility or publication has changed.
Dry-run artifacts do not establish successful GitHub OIDC signing or public
pull verification; those must be verified after the authorized workflow run.
