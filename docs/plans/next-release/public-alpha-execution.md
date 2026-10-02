# Public alpha execution — 2026-10-02

Baseline: `main` at `421d69f5ab4c1ed4ff524b7d8c6a51873e50a610`; the pre-existing untracked `docs/plans/readiness-review-20261001.md` is preserved.
Owner instruction: “okay, go ahead and complete it properly for the public alpha release.”
Owner scope answer: “Keep explicit alpha differences; implement restore and release requirements (Recommended)”.
Publication is the last external step; existing policy requires a separate final grant. No publication, visibility change or deployment is authorized by this ledger.

| ID | Desired invariant / real path | Planned observation | Red / scope | Status |
|---|---|---|---|---|
| DEC-002 | Explicit single-node public-alpha successor scope, existing unsupported email/id-token/admin-presence differences retained | Approved envelope, exact release validator/gate profile | Parent docs; historical DEC-001 acceptance preserved | IMPLEMENTATION SELECTED |
| OP-004D-H | `instantd healthcheck` probes DB readiness without loading server secrets or starting the daemon; optional TLS probe uses system roots | Built executable against HTTP/TLS test servers: healthy, no DB, unhealthy, malformed, untrusted TLS | Parent `cmd/instantd`; current command ignores args | GREEN / reviewed |
| DA-009-S | Created-object cleanup durably removes only owned blobs and reports fsync failures | Injected directory sync failures in real DiskBackend Delete / PutIfAbsent rollback | Parent `internal/storageapi`; no storage redesign | GREEN / reviewed |
| DA-009 | v1 ZIP restores exact blobs linked to metadata, preserves pre-existing object bytes | Owned-DB restore, HTTP download, failure/collision/reopen tests | Restore worker `internal/backup`; daemon wiring parent | GREEN / reviewed; live drill pending |
| DA-010 | Nonempty restore targets rejected; known failed restores preserve logical/object state and source; sequence never moved on rollback | Actual DB export equality, independent nonempty cases, transaction sequence rollback | Restore worker; reviewed table-lock ceiling for alpha restore only | GREEN / reviewed; live drill pending |
| OP-004D / QR-004 | Working multiarch alpha-only distribution and publish/sign/SBOM/provenance pipeline; no latest tag | GoReleaser check/snapshot, image config and exact image runtime, workflow source/CLI checks | Distribution worker Dockerfile/GoReleaser/deploy/publish/container script | PENDING |
| QR-003R | Fail-closed public gate with selected real record producers and candidate/binary/config/image/fixture bindings; DEC-001 still validates | Missing/wrong/stale/synthetic record red/green contract tests; actual new campaign | Qualification worker cmd/qualify + qualify/gate scripts | PENDING |
| CF-004 / CF-005 | Selected supported surfaces compared against official pinned v1 with equivalent fixtures and explicit differences | Owned official-image provenance, captured raw outcomes and real differential | Parent runtime/corpus; no general drop-in claim | PENDING |
| OP-006 | Both v1 ZIP and v2 NDJSON exact-state/object restore drill, selected 1M-triple / 600-second budget, rejected hostile inputs | Real fixture/source manifests, reopened state, hashes and timings | Parent owned campaign, no developer DB | PENDING |
| EV-007 / QR-002 | Comparative claim only if owner selects and qualified paired matrix passes | Existing semantic benchmark bundle/report; no synthetic acceptance | No comparative claim prepared while optional question remains unanswered | NOT_SELECTED |
| OP-003 / OP-005 / QR-001 / FR-002 | Fresh immutable-candidate Linux, DB, recovery, scoped soak and composed public gate | Owned iv2q-prefixed host campaign and retained hashes | Parent; prior acceptance not reused | PENDING |
| FR-003 | Reviewable draft alpha artifacts, publish only after final grant | Snapshot checksums/SBOM and qualified image; post-publish verification after grant | Parent; license/visibility choices pending | PENDING |

## Restore data-integrity decision

Non-author review by qualification worker before implementation accepted: fully validate archive/bounds/blob-metadata mapping before blob writes; write only new objects with PutIfAbsent, durably clean them on known precommit failures with bounded independent cleanup, and never remove objects after an ambiguous COMMIT reply. SQL state is transactional. An unknown commit outcome requires reconciliation rather than automatic retry; crash-before-commit can leave unreferenced objects. No crash-atomic filesystem/PostgreSQL transaction is claimed. Known rejected inputs must preserve target logical/object contents and source bytes. Cleanup failures are explicit, never rewritten into successful rollback. Global restore table locks are a deliberate alpha ceiling, not an HA contract.

## Verification environment

Owned local PostgreSQL 17.11 cluster: `/tmp/instant-v2-public-alpha-pg-20261002/data`, loopback port 55471; tests create their own databases. Existing developer PostgreSQL is not used. Local Go 1.27.1 uses CGO_ENABLED=0 for the workstation's Apple SDK linker mismatch. Owner-named bigbeast has Linux/Docker available; remote runs must use unique iv2q-prefixed resources and preserve unrelated workloads.

## Integrated verification and review

The assembled daemon ZIP restore/download check passed against the owned DB
(`/tmp/instant-v2-public-alpha-route-green.log`). Full hermetic race results:
1,566 passing test/subtest outcomes, 327 integration/short-mode skips, zero
failures (`/tmp/instant-v2-public-alpha-unit-20261002.jsonl`). These are local
implementation checks, not Linux acceptance. Backup owned-DB checks subsequently
passed 77 outcomes, zero failures, one short-mode memory test skipped.

Independent reviewers found and resolved: uncertain PutIfAbsent cleanup now
requires reconciliation/HTTP503; Delete retries still sync after unlink; scratch
images provide nonroot writable /tmp; container smoke exercises actual ZIP
restore/download; release version stamping matches the literal policy tag;
draft archive checks run with publisher access; v1 comparisons require each
scenario's complete frame count. The delete-retry regression reproduced RED
then full storage race checks passed (`/tmp/instant-v2-public-alpha-delete-retry-
red.log`, `/tmp/instant-v2-public-alpha-delete-retry-green.log`). Restore/root
high-risk source review closed with no further findings; qualifier review
approved integration after the truncated-capture rejection.

Official v1 image is pinned by digest
`sha256:3f3488b753c739f8ae3c35fa9c2b5848f16687e8687d9f50fbd01dd789403fac`
for source tag `a4d2ef33b60f281a437191006e4541d4780f9e4a`. Its actual
`backup-app-on-primary!` exporter emitted 1,000,007 triples and a ZIP packed
from real exported config/entity shards plus its owned S3 blob (6,959,837 bytes).
This proves fixture provenance, not a successful candidate restore yet.

Fresh immutable-candidate Linux/container/restore/differential/recovery/soak
evidence remains pending. QR-004 stays PARTIAL until real OIDC/publication
verification; the public gate must refuse that state. No public-ready claim
is established by the prepared workflow or a local signing test.
