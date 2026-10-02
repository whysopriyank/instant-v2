# Public alpha distribution contract (OP-004D / QR-004)

Baseline: `421d69f5ab4c1ed4ff524b7d8c6a51873e50a610` on `main`;
only `docs/plans/readiness-review-20261001.md` was untracked before work.
Scope: Dockerfile, `.goreleaser.yaml`, `.github/workflows/publish.yml`,
`deploy/**`, `scripts/quality-container.sh`. The coordinator owns the
`instantd healthcheck` command and release qualification evidence.

| ID | Desired invariant / real path | Test/evidence; red expectation | Verification | Goal / status |
|---|---|---|---|---|
| OP-004D-1 | source and GoReleaser images contain static instantd, CA roots, writable owned storage root, UID/GID 65532, built-in readiness healthcheck, OCI metadata | inspect both actual Dockerfiles and built image; existing source Dockerfile lacks healthcheck and writable root; GoReleaser source context is invalid | `goreleaser check`; `make container-verify` on owned Docker host | FIX / PARTIAL |
| OP-004D-2 | published OCI index contains Linux amd64 and arm64; version and full SHA are immutable; moving alpha only | GoReleaser actual config and registry index; existing config publishes latest | `goreleaser check`; postpublish index inspection | FIX / PARTIAL |
| OP-004D-3 | reference Compose uses pinned PG17, logical WAL, persistent DB/blob volumes, health-gated startup and mandatory secrets | `docker compose config`; start/restart real reference deployment; no compose exists | config on Docker host; OP-004 campaign | FIX / PARTIAL |
| QR-004-1 | only protected alpha tags in authorized protected environment can publish; write/OIDC permissions restricted to publisher job; pinned actions | inspect exact workflow; no publish workflow exists | YAML/action validation; authorized Actions run | FIX / PARTIAL |
| QR-004-2 | checksums signed with identity-verifiable keyless bundle; OCI digest signed; SPDX SBOM and build provenance attached; draft binary release | local static archives/SBOM/checksums, offline local-key signature dry run; Actions OIDC/registry exercise after grant | snapshot; cosign verification; postpublish job | FIX / PARTIAL |
| OP-004 / QR-004-3 | published digest is re-pulled and identity/signatures/provenance verified before DB-connected transact/subscription/persistence/drain smoke | existing smoke has no DB or storage root and fails current daemon startup | `make container-verify`; postpublish job | FIX / PARTIAL |

At packet creation, no commit, tag, push, publication, infrastructure change, or license choice
was authorized by this packet. Tag rules and the `public-alpha` environment
require owner setup. Container runtime and OIDC evidence cannot be replaced
by YAML checks or a snapshot. The owner subsequently selected Apache-2.0 and
public source/images on 2026-10-02. `LICENSE` is included in binary archives and
images, and the OCI license label declares `Apache-2.0`. This selection does not
substitute for the final candidate gate or actual signed publication checks.

## Evidence

- Installed GoReleaser 2.18.1, Syft 1.51.1, cosign 3.1.3.
- Baseline `goreleaser check` passed but reported deprecated Docker config;
  this does not prove its binary-only Docker build context works.
- Baseline snapshot stopped at sandbox-denied Go build cache access;
  rerun uses a task-local cache. Docker is absent on this local host.
- CLI formats confirmed from primary docs:
  [GoReleaser Docker v2](https://goreleaser.com/customization/package/dockers_v2/),
  [SBOM](https://goreleaser.com/customization/sbom/),
  [cosign checksum bundle](https://goreleaser.com/customization/sign/sign/),
  [attestation action v3](https://github.com/actions/attest-build-provenance/tree/v3).

## Implementation and verification handoff (2026-10-02)

Status: **PARTIAL**. Tooling is implemented; runtime/release acceptance is
not claimed. Local host has no Docker. Coordinator must run OP-004 on the
owned Docker host and an independent reviewer must inspect the supply-chain
boundary before this packet can close. OIDC/registry postpublication checks
require `AUTH-PUBLISH-001`, tag protection, environment approval, visibility
and license decisions.

| Observation | Command / inspected outcome |
|---|---|
| GoReleaser schema | `goreleaser check`: exit 0, no deprecated Docker config |
| Cross-platform snapshot | `GOCACHE=/private/tmp/instant-distribution-go-cache goreleaser release --snapshot --clean --skip=docker,sign --timeout=10m`: exit 0 twice; Linux amd64/arm64 and Darwin arm64 archives, populated SPDX 2.3 SBOMs, checksums |
| Artifact inspection | 6 checksum entries recomputed; 3 archive binaries equal compiled artifacts byte-for-byte; both Linux ELF binaries reported statically linked |
| Signature mechanics | cosign 3.1.3 ephemeral local key with empty signing service config signed the actual `dist/checksums.txt`; bundle verification accepted the original and rejected an appended-byte copy |
| Syntax | actual GoReleaser/workflow/Compose files parse with PyYAML; all 7 workflow shell blocks and `scripts/quality-container.sh` pass `bash -n`; `git diff --check` passes |
| Actions provenance | official GitHub API resolved every newly added action ref to its exact 40-character commit; exact action.yml inputs were inspected |
| Toolchain limitation | local snapshot uses **Go 1.27.1**, so it is mechanics evidence, not a Go 1.25.x-qualified candidate. Publisher pins Go 1.25.14; its stripped/version-stamped binary must be qualified as those exact bytes |
| Not run | Docker builds, real Compose config/start, full container smoke, ARM64 execution, full GoReleaser Docker snapshot, keyless OIDC signing, registry index/signature/SBOM/provenance checks, published pull/smoke, owner-managed protections |

Self-audit corrected a dirty-tree risk (registry error output now goes to
runner temp), removed ARM64 emulation from the CA/root build stage, made
helper containers explicitly owned/prefixed, and verifies container/network/
volume cleanup. No constants-only test suite or application test seams were
added. The smoke exercises existing soaksetup/soak plus real presigned
upload/download byte comparison across daemon recreation; it does not claim
long-load performance or provider integration.

The offline signature intentionally omits transparency verification and must
not substitute for release keyless verification. An initial local sign
attempt declined the default transparency-log prompt; no log submission
occurred. Custom offline signing emitted a sandbox TUF-cache warning but
successfully wrote and verified the local-key bundle. The ephemeral private dry-run key was removed; its public key, signed
checksum copy and bundle remain under the private task temp directory; `dist/` is ignored. All concurrent edits outside
the packet's owned files belong to the coordinator/other workers.

No commit, tag, push, publication, deployment, or external-state mutation
occurred. Safest next action: build the exact Go 1.25.14 release binary on
the owned host, copy it under `linux/amd64/instantd` in a staged context,
build `deploy/Dockerfile.release` with the actual OCI metadata labels, and
run `CONTAINER_IMAGE=<owned local tag> bash scripts/quality-container.sh`.
GoReleaser supplies those labels and stages both platform binaries in the
protected release workflow. Do not silently replace the candidate with the
source-build development Dockerfile.


Coordinator-requested retained facts support was added to the same smoke:
`CONTAINER_EVIDENCE_DIR` (fresh destination) and `CONTAINER_IDENTITY_FILE`
(bare identity object) preserve raw runtime outputs and schema-1 `facts.json`.
Image revision, actual process UID, copied daemon binary SHA and pulled
RepoDigest are observed/checked, not taken from copied expected constants.
HTTP statuses, soak completion/lag accounting, object bytes/hashes, elapsed
stop seconds and the failed port probe derive the required observations.
Configuration/endpoint/fixture input identity remains coordinator-owned.
This branch is syntax-checked only locally; a real Docker run and its raw
facts must still be inspected. Local image IDs cannot substitute for a
published/retained OCI manifest digest. The short soak's convergence measure
is the existing refresh-watermark accounting, not an independent content
oracle for every subscriber.


The coordinator selected `v0.1.0-alpha.1` for the first published version.
Current owned-host runtime qualification covers Linux amd64 only; Linux
arm64 and Darwin arm64 are compiled-only until matching-host execution.
A loopback owned registry may supply a genuine prepublication RepoDigest;
its local image verification still cannot replace the final signed GHCR
published-digest verification.


## Current staged acceptance (coordinator review, 2026-10-02)

The implementation handoff above records earlier mechanics evidence and remains
historical PARTIAL. Current QR-004 prepublication acceptance requires the exact
immutable candidate's tagged Go 1.25.14 archives, populated SPDX inventories,
verified checksum/tamper checks, the actual staged multiarch image and an
independent pipeline/supply-chain review. It does not require an already
published release: the authoritative packet explicitly places static validation
and a dry run before publication. Real GitHub OIDC signing, GHCR index/signature/
provenance verification and anonymous public pull are FR-003 postpublication
checks, separately authorized. Owner tag protections, the `public-alpha`
environment, visibility and license are explicit publication prerequisites,
currently unconfigured or undecided. No snapshot establishes these facts.

Rehearsal candidate `4a2e0fc` passed the live Linux-amd64 image smoke after the
Docker PID-column and loopback namespace corrections. Tagged binary metadata
changed its identity, so that rehearsal and the initial untagged snapshot cannot
close final QR-004/OP-004 acceptance. Final tagged artifacts/runtime remain
pending and must match the eventual workflow's compiled bytes.
