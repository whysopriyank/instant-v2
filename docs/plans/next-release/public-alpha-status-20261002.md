# Public alpha status — 2026-10-02

**Public alpha is not accepted or published. Production readiness is not
established.** The restore/release implementation is substantially complete,
but the final release gate still needs fresh candidate records. The owner has
approved explicit compatibility limits with no v1 parity claim; historical
comparison failures remain retained. This report supersedes the implementation
backlog portions of the October 1 readiness review, not historical acceptance.

## Implemented in this release branch

- v1 ZIP restores actual durable file bytes and their metadata; both restore
  formats reject nonempty targets. Known failures preserve target state;
  uncertain commit outcomes retain objects and require reconciliation.
- Restore sequence changes roll back, archive limits are checked before blob
  writes, and owned blob cleanup is durable. Successful restore invalidates
  cached app catalogs/rules; UUID aliases share cache keys.
- Daemon version/readiness commands, nonroot image storage and temporary roots,
  CA certificates, reference Compose settings, alpha-only version/SHA tags,
  archives, SBOM/checksum/signing/provenance workflow and real container smoke.
- Public evidence producers and fail-closed candidate/binary/configuration/
  image/fixture checks. Native package build failures cannot disappear behind
  passing named tests. Release builds require the exact candidate's version tag.
- Duplicate/remove-query replies echo the fields observed in pinned v1.
  Public docs describe required configuration and actual compatibility limits.

## Actual test evidence

Rehearsal source was `4a2e0fcc6a0c239c71d1bc72fe4653c96ed52996`.
Later fixes and tagged binary metadata change the identity; its passes cannot
be promoted to final candidate acceptance. Raw rehearsal evidence is retained
on bigbeast under `~/iv2q-public-alpha-20261002/`.

| Check | Observed result | Qualification boundary |
|---|---|---|
| 500-session soak | PASS: 900 active seconds, 7,200 acknowledged/committed/refreshed transactions, 3,600,500 deliveries, zero dropped/unresolved | One whole-table delta-refresh workload at 8 total transactions/s; transport lag is diagnostic, not independent all-recipient content proof |
| Recovery | PASS: all seven crash/restart/drain scenarios, zero missing acknowledgements or partial transactions | Rehearsal binary; fresh successor run required |
| v1 ZIP restore | PASS: 1,000,007 triples in 13.927059 seconds; independent logical/object hashes matched, real downloads and restart checked | Genuine pinned official v1 exporter; rehearsal image |
| v2 NDJSON restore | PASS: same triples in 18.534823 seconds including companion-volume restoration and startup | File bytes require the companion durable volume archive |
| Five rejected restore inputs | PASS: specific corrupt, truncated, oversized, wrong-app and nonempty responses; exact target/source hashes unchanged | Known rejection, not uncertain commit/crash atomicity |
| Container runtime | PASS: 9/9, actual UID65532/readiness/TLS/transaction/subscription/storage/restore/drain/cleanup | Corrected owned producer, rehearsal image; 0.156-second drain |
| Linux native rehearsal | FAIL: stale nonempty-restore expectation and dependency-download failures | Test fixed; fresh full pinned Linux run is still required |
| v1 differential | 18 observed: 2 agree, 16 fail; original physical-fixture blocks and aligned reruns retained | Owner approved CF-004/CF-005 parity/drop-in claim exclusions with stable documented limits; these results remain failures |
| Local integrated checks | Full short/race suite passed; scoped lint zero issues; real assembled warmed-schema restore RED then GREEN | Workstation implementation checks, not Linux release acceptance |
| Tagged artifact rehearsal | Go1.25.14, three matching archive payloads, six checksums, populated SPDX SBOMs; offline local signature accepted original and rejected tampering | Candidate `4d1e206`; final source successor must rebuild. No OIDC/public publication proof |

The v1 differences include omitted tuple timestamps affecting SDK
`serverCreatedAt`/ordering/infinite-query fidelity, cursor representation,
error envelopes, initialization and other query behavior. See the
[full comparison](v1-differential-rehearsal-20261002.md) and
[precise limits](v1-compatibility-observations.md). The small reply-field fix does
not establish broad parity. The owner selected “Document explicit compatibility
limits; no v1 parity claim (Recommended)” and “Apache-2.0; public source and
images”, received around `2026-10-02T16:56Z` (approximate minute). The
[approved envelope](../../reference/public-alpha-release-envelope.md#owner-confirmed-compatibility-limits--2026-10-02)
defines CF-LIMIT-001 through CF-LIMIT-008. The five required runtime records
remain native Linux, recovery, soak, container and restore. No failed
comparison becomes PASS and no public-ready claim follows from this scope change.

No comparative v1/v2 performance result is qualified. The benchmark harness
exists; an efficiency or speed claim needs the separately selected paired
matrix. The successful soak establishes only the workload described above.

## Release work still required

1. Freeze the corrected source and approved compatibility selection, build matching tagged Go1.25.14 binaries/image,
   rerun the five runtime lanes, assemble actual records and complete the gate.
   Final scripts and fixture producers are staged; a new soak needs 17 minutes.
   Private-source transfer to the owned bigbeast qualification directory is now
   owner-authorized; the earlier automatic approval rejection is historical and
   was not bypassed.
2. Apply the owner-selected Apache-2.0/public source and images choice and
   configure the protected alpha tag and `public-alpha` environment. The choice
   is recorded; actual remote visibility/protection is not claimed here.
3. On the separate final publication grant, publish the reviewed version/draft,
   verify real OIDC signatures/SBOM/provenance and public pull, then test the
   actual published image before accepting the release. QR-004 preparation
   and FR-003 actual publication are distinct stages.

## After the bounded alpha

Email delivery, direct ID-token sign-in and admin presence remain explicit
unsupported alpha differences. Dynamic read rules, dedicated sync/streams,
S3/object backup and cross-node operation are also outside the selected scope.
Implement them when the supported product promises them, with actual acceptance
rather than configuration-only claims.

For a production profile, first select the target app/auth/permission and SDK
contract; then qualify upgrade/rollback, representative sustained workload and
operator recovery/monitoring. HA, read replicas and multi-node rooms require
separate consistency and recovery qualification if selected. A public alpha
pass alone establishes none of these. There is no defensible production
completion percentage or ETA from packet counts.
