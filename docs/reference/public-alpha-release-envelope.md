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
qualification, a pinned official-v1 comparison for the selected corpus, a
DB-backed container check and a working alpha-only distribution pipeline.
Email delivery (503), direct ID-token sign-in (501), admin presence (501),
dynamic view rules and object-store backup routes (503) remain explicit alpha
differences. Real external-provider acceptance is unselected.

## Qualification selection

The source-controlled selection is
[qualification-policy.json](../plans/next-release/qualification-policy.json).
Six live records are mandatory: native Linux, recovery, soak, container,
restore and v1 differential. Every record binds the same immutable candidate,
binary, nonsecret configuration and distribution image, with its own hashed
fixture descriptor. Synthetic records and historical acceptance cannot pass.

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
Repository visibility and license are still awaiting the owner decision.
Dry-run artifacts do not establish successful GitHub OIDC signing or public
pull verification; those must be verified after the authorized workflow run.
