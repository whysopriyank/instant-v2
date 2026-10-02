# DEC-002 — next release selection (step 1)

Historical proposal. The 2026-10-02 implementation follows the owner's public
alpha instruction and retained-auth-differences answer under
[DEC-002-single-node-public-alpha-20261002](../../reference/public-alpha-release-envelope.md).
Its canonical qualification policy prepares an alpha without a comparative
performance claim; this proposal's performance selection is not the active gate.

Decision ID (proposed): `DEC-002-single-node-public-alpha-20260926`
Status: `SELECTED_UNDER_OWNER_DELEGATION` — the owner asked the coordinator on
2026-09-26 to study v1 and select the next release claim, including the
container distribution method, performance claim and publication target. This
file is that selection. It becomes the approved envelope when the owner
confirms it. Until then, `DEC-001-single-node-alpha-20260905` and the
`f7dc5b1` acceptance stay authoritative, and nothing here changes the current
gate.
Supersedes (on approval): DEC-001 for *future* candidates only. The DEC-001
alpha acceptance on `f7dc5b1` remains valid for its recorded scope
([evidence register](../finish-up/evidence-alpha-20260926f.md)).
External authority: selecting a target is **not** a grant.
`AUTH-PUBLISH-001` and `AUTH-DEPLOY-001` remain `NOT_GRANTED` until the owner
grants them by name ([envelope](../../reference/release-envelope.md)).

## 1. What v1 actually is (research basis)

Sources:
- the pinned v1 checkout `../instant` at `a4d2ef33b60f281a437191006e4541d4780f9e4a`;
- instantdb.com/docs (self-hosting, backups);
- github.com/instantdb/instant/releases;
- a GHCR registry query made on 2026-09-26.

| Topic | v1 fact | Source |
|---|---|---|
| Server image | `ghcr.io/instantdb/server`, amd64+arm64 manifest, tags `latest` and `<commit sha>` only (no semver). **The tag for our exact pin exists** (registry HTTP 200 for `a4d2ef33…`). | `.github/workflows/publish-self-hosted-images.yml`; registry query |
| Image hygiene | Base `amazoncorretto:26` (full JDK); no `USER` (runs as root); no `HEALTHCHECK`; no cosign signature, SBOM or provenance. | `server/Dockerfile.self-hosted:1,24` |
| Releases | GitHub Releases page is empty. One shared version string, `v1.0.65`, is published to npm/PyPI on merge. | `client/packages/version/src/version.ts`; releases page |
| Dependencies | Custom Postgres 17 + `pg_hint_plan` image with `wal_level=logical`; `wal2json` slot `aggregator`; MinIO/S3 for storage; `JAVA_OPTS` heap tuning expected. | `self-hosting/docker-compose*.yml`, docs |
| Topology | Compose single-node, or Swarm with `server: replicas: 2`. | `self-hosting/swarm.yml:124` |
| Backup/restore | Per-app ZIP (`config.json` + `entities/*.jsonl` + `files/`), nightly on Cloud with 7-day retention, restore via dashboard `/intern/restore`. None of this is in the self-hosting docs, and it needs an undocumented `S3_APP_BACKUPS_BUCKET`. | `server/src/instant/{backup,restore}.clj`; docs/backups |
| Upgrades | Manual PG16→17 runbook: `pg_dump`, delete volume, `pg_restore`, re-point the replication slot. | `self-hosting/UPGRADE_DB.md` |
| Client surfaces | WS: `init`, `add-query`/`remove-query`, `transact`, `refresh-ok`; rooms (`join-room`, `set-presence`, `client-broadcast`, `refresh-presence`/`patch-presence`); opt-in sync/stream ops. HTTP runtime: magic code, refresh token, guest, OAuth token/id_token, signout, storage (upload, files, signed URLs). Admin: query/transact, `*_perms_check`, users, refresh tokens, rooms presence, storage, `subscribe-query`, `/admin/sse` (streams only). | `client/packages/{core,admin}/src` |

What the v2 candidate has today, from source:
- It routes all the core, auth, storage, admin and `subscribe-query` surfaces above.
- Room ops exist, single-node only.
- Sync/stream ops return 501, and `/admin/sse` (v1 streams) does not exist.
- Backup has v2 NDJSON and `restore-v1zip`. **The v1 ZIP importer skips `files/` blobs** (`internal/backup/v1zip.go:19`).
- Restore runs in one transaction, but merges into a non-empty app (`ON CONFLICT DO NOTHING`).

## 2. Selections

### 2.1 Release level

**Single-node public alpha**: the first *published* build. It is still an
alpha, not production-ready, and carries no SLA or data-durability guarantee
beyond the qualified restore drill. Multi-node (v1's Swarm `replicas: 2`)
stays unselected. It is the largest remaining topology gap with v1 and is the
natural subject of DEC-003 (OP-001/OP-002).

### 2.2 Container distribution (OP-004, plus a new packet OP-004D)

The target is parity with v1's distribution: GHCR, multi-arch, tagged by
commit. It then improves on v1 in every row below.

| Property | v1 | DEC-002 selection for v2 |
|---|---|---|
| Registry / name | `ghcr.io/instantdb/server` | `ghcr.io/whysopriyank/instant-v2` (matches the existing `.goreleaser.yaml` scaffold) |
| Architectures | amd64, arm64 | amd64, arm64 (one OCI index) |
| Tags | `latest`, `<sha>` | `vX.Y.Z-alpha.N` (immutable), `<sha>`, moving `alpha`. **No `latest`** until production. Docs pin by digest. |
| Base / size | full JDK (`amazoncorretto:26`) | `scratch` + static Go binary + CA bundle (current Dockerfile) |
| User | root | `65532:65532` (current, asserted by `container-verify`) |
| Healthcheck | none | built-in `instantd healthcheck` subcommand (no shell or curl in `scratch`) plus Dockerfile `HEALTHCHECK` |
| Metadata | none | OCI labels: source, revision, version, created, licenses |
| Signing | none | cosign keyless via GitHub OIDC, verifiable by certificate identity = this repo's publish workflow |
| SBOM / provenance | none | SPDX SBOM (syft, via goreleaser `sboms`) attached and attested; SLSA build provenance (`actions/attest-build-provenance`) |
| Also shipped | npm SDKs only | GitHub Release: static binaries (linux amd64/arm64, darwin arm64) + `checksums.txt` + signatures |
| Reference deployment | compose/Swarm templates | `deploy/compose/docker-compose.yml`: v2 + `postgres:17` pinned by digest with `wal_level=logical`, a named volume for `INSTANT_V2_STORAGE_ROOT` owned by 65532, and a healthcheck-gated start. No MinIO is needed (durable local storage). |

**OP-004 acceptance** is unchanged from `06-topology-recovery.md:61`. The
**published digest** (or a local build proven bit-identical to it) must pass
all of these on an owned host:
- DB-connected startup;
- persistent-volume survival across a container restart;
- an outbound TLS handshake against the system trust store;
- health and readiness;
- one transact and one subscription;
- a SIGTERM drain within 30 s;
- port release and cleanup.

The result is recorded as a new gate record `container` (packet OP-004).

### 2.3 v1 parity claim (CF-004 / CF-005)

The claim is **"selected-surface parity with InstantDB v1 at `a4d2ef33`"**,
not a drop-in replacement. Named SDKs: `@instantdb/core`,
`@instantdb/admin` and `@instantdb/react` **v1.0.65**, the version pinned at
that ref.

| Tier | Surfaces | Parity requirement |
|---|---|---|
| T1 core (required) | WS `init`, `add-query`/`remove-query`, `transact`, `refresh`; admin `query`/`transact`; runtime auth (magic code, refresh token, guest, signout, OAuth token/id_token contract); storage upload/files/signed URLs; literal permission rules | Captured-v1 oracle per corpus row; every divergence is fixed or becomes a named approved difference |
| T2 collaboration (required, single-node) | `join-room`/`leave-room`, `set-presence`, `client-broadcast`, `refresh-presence`/`patch-presence`, admin rooms presence | Same, within one node |
| T3 admin extras (selected if CF-005 triage shows they are implemented) | `*_perms_check`, admin users/refresh tokens, `subscribe-query` | Same, or explicit 501 plus a documented difference |
| Excluded (stable 501/reject, documented) | sync/stream ops and `/admin/sse` (v1 streams); DA-004V dynamic data-dependent view rules; cross-node rooms | Enforced by the gate as in DEC-001 |

**CF-004 v1 endpoint:**
- Run the **official `ghcr.io/instantdb/server:a4d2ef33b60f281a437191006e4541d4780f9e4a`**, recording its manifest digest as identity. This is better provenance than building from source.
- Run it inside `self-hosting/docker-compose.local.yml`: Postgres 17 + `pg_hint_plan` + `wal_level=logical` + `wal2json`, and MinIO, because storage is selected.
- Use v1's documented settings, with no `JAVA_OPTS` starvation and `wal2json` enabled. The 2026-08-24 run showed that a misconfigured v1 invalidates any comparison.
- Run on an owned host (bigbeast), bound to loopback, under the `iv2q-*` naming.
- Before any comparison, prove the served revision, health, and fixture reset.

**CF-005:**
- Run `make differential` over the T1/T2 (and selected T3) matrix.
- Replace every `regression` and `spec` oracle for selected rows with a captured-v1 oracle.
- Grow the corpus to cover each selected surface's allowed and denied paths.
- An independent review closes it.

### 2.4 Restore drill (OP-006, plus product packets DA-009 and DA-010)

- **Target:** a fresh Postgres 17 on an owned host, into a new, empty v2 app.
- **Inputs, both required:**
  1. a v2 NDJSON export;
  2. a **v1 backup ZIP** produced by the official v1 image, as the migration path from v1 (Cloud or self-host) into v2.
- **DA-009 (product):** the v1 ZIP importer restores `files/` blobs into v2 storage and links them to `$files` metadata. They are skipped today. Parity with v1 `restore.clj`.
- **DA-010 (product), atomicity policy:** restore stays one transaction, all or nothing. Restore into a **non-empty** app is refused with an explicit error instead of silently merging, and a failed restore leaves the target and the source archive unchanged. This improves on the current merge semantics. v1's behaviour is recorded in CF-005 triage.
- **Drill proves:**
  - exact logical state (triples, attrs, rules) and object hashes match the source;
  - schema and migration version are recorded;
  - truncated, corrupt, wrong-app and oversized inputs are rejected with the target unchanged.
- **Budgets:**
  - RPO = time since the operator's last export. Scheduled backups are not claimed, and this is stated plainly.
  - RTO (provisional): restore of a 1M-triple fixture within **10 min** on the owned host. It may be recalibrated once, by a recorded decision, after the first dry drill and before the candidate campaign.
- The result is recorded as a new gate record `restore` (packet OP-006).
- Object/S3 backup remains excluded.

### 2.5 Performance claim (QR-002, Wave 6)

**What exists.** Historical head-to-head, 2026-08-25, v2 at `f23bb78` against
real v1 at the pin, on one Apple M4 Pro (`docs/archive/01-state.md:167-191`),
at 1,000 sessions × 8 tx/s × 3.5 min:

| Metric | v2 | v1 |
|---|---|---|
| p99 refresh lag | 71 ms | 1,108 ms |
| CPU | 0.31 cores | 4.33 cores |
| RSS | 100 MB | 5.8 GB |
| CPU per delivered update | ≈0.23 ms-core | ≈10.1 ms-core |

**Why none of it can be claimed yet.** The contract rejects these runs as a
claim (`docs/reference/13-benchmark-contract.md:11-30`):
- the runs were single and short;
- there was no semantic recipient ledger;
- v1 *coalesces* invalidations, so v1's "32.5% delivery" may be coalescing rather than loss;
- no raw bundle survives.

**Selected claim.** A **resource-efficiency and convergence-latency** claim,
not "N× faster". Throughput is Postgres-bound and is not the thesis
(README: memory density, cold start, single binary). The claim covers:
- Metrics, each as a paired ratio with a 95% bootstrap CI:
  - p99 semantic-convergence lag;
  - CPU core-ms per 1,000 recipient-coverages;
  - peak RSS per subscriber and absolute peak RSS.
- **Contract amendment (new packet EV-007):** add *time-to-ready (cold start)* and *idle RSS* as measured metrics under the same 7-pair AB/BA rule. These are structural JVM-versus-Go differences and the most robust headline.
- Scales: 300 subscribers (Wave 6 entry), then 1,000, where the historical gap was widest.
- Families: those in the contract that cover fan-out; the exact list is frozen in the Wave 6 schedule *before* the first paired run.
- Gate: contract §9, i.e. exactly 7 AB/BA pairs, all outcomes shown, CI excludes 1.0, no contradicting scale cell, and no post-hoc selection.
- The published claim states only the numbers that pass, with the CI, e.g. "at 1,000 subscribers, v2 used X× less CPU per delivered update (95% CI [a, b])".

**Fairness preconditions:**
- v1 runs from the official pinned image with its documented Postgres (`pg_hint_plan`, `wal2json`) and a heap sized per v1 docs;
- the same host, quiet and single-tenant (bigbeast);
- separate databases and the same seed.

`AUTH-PERF-001` must be extended by the owner from "soak only" to
comparative runs on owned hosts. Selecting this claim does not grant it.

### 2.6 Publication target (QR-004 → FR-003) and canary (FR-004)

- **Workflow (QR-004):** `.github/workflows/publish.yml`, triggered only by a protected `v*-alpha.*` tag.
  - Permissions are scoped to the one job: `contents: write`, `packages: write`, `id-token: write`, `attestations: write`.
  - All actions are pinned by commit SHA, following the repo's existing convention.
  - Steps: goreleaser (already scaffolded: buildx multi-arch, SBOM, cosign), `actions/attest-build-provenance`, then **draft** GitHub Release.
  - A post-publish job re-pulls the digest and runs `cosign verify`, attestation verify and the OP-004 smoke against the *published* digest.
- **Dry run first:** `goreleaser release --snapshot --clean` plus signing and SBOM generation with no push, with its evidence recorded. Only then request `AUTH-PUBLISH-001`.
- **Targets:** GHCR `ghcr.io/whysopriyank/instant-v2`, and GitHub Releases on `whysopriyank/instant-v2`. The first tag is `v0.2.0-alpha.1`.
- **Owner inputs:**
  - make the repository and package public or keep them private;
  - protect `v*` tags;
  - grant `AUTH-PUBLISH-001`.
- **FR-004 canary: `NOT_SELECTED`.** There is no production traffic, and a local simulation is not a canary (`08-final-reconciliation.md`). The post-publish verification above replaces it for this release.

### 2.7 Unchanged or carried over

| Area | Selection |
|---|---|
| Auth providers (DA-006B/DA-008B) | Not claimed unless the owner supplies a test OAuth app and a mail provider. v1 self-host prints magic codes to stdout when no provider is set; v2 fails closed with "missing mailer". Recorded as a documented difference (safer). |
| Permissions | Literal rules as DEC-001; DA-004V stays excluded |
| Storage | Durable local root + HMAC presigned URLs; S3 excluded |
| Deferred schema TD-003 | Out, unless a selected surface needs it (checked in CF-005 triage) |
| Security, rate limits, soak, recovery | DEC-001 rows carried over unchanged; OP-003/OP-005/QR-001 re-run on the new candidate |
| Go version | 1.25.x pinned by digest in the qualification image, as today |

## 3. Packet deltas and gate impact

| Packet | Status under DEC-002 |
|---|---|
| CF-004, CF-005 | `REQUIRED` |
| OP-004 + OP-004D (distribution hardening: healthcheck subcommand, OCI labels, multi-arch, compose reference) | `REQUIRED` |
| OP-006 + DA-009 + DA-010 | `REQUIRED` |
| QR-002 + EV-007 | `REQUIRED` (claim limited to metrics that pass §9) |
| QR-004, FR-003 | `REQUIRED`, but FR-003 is blocked on `AUTH-PUBLISH-001` |
| FR-004, OP-001, OP-002, DA-006B, DA-008B | `NOT_SELECTED` |
| **QR-003R gate revision** | `REQUIRED`: new decision and profile literals; packet inventory (add CF-004/005, OP-004, OP-006, QR-002, QR-004, DA-009/010, EV-007); lanes `container: run`, `external_v1: run`, `performance: artifact`; new records `container`, `restore`, `v1_differential`, `performance`, each with a schema, a `cmd/qualify` producer, gate validation and contract tests |

Order:
1. QR-003R and the record contracts.
2. OP-004D + OP-004.
3. CF-004 → CF-005, with DA-009/010 in parallel.
4. OP-006.
5. EV-007 → QR-002.
6. QR-004 dry run.
7. A new qualification campaign on the new candidate.
8. FR-003 on grant.

## 4. Owner confirmation needed

1. Approve this selection as `DEC-002-single-node-public-alpha-20260926`, or amend it.
2. Choose public or private for the repo and package.
3. Grant `AUTH-PERF-001` (comparative runs on owned hosts), now or when QR-002 starts.
4. Grant `AUTH-PUBLISH-001`, later, after the QR-004 dry run.
