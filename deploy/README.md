# Single-node public alpha distribution

The release is an alpha. Email delivery, direct ID-token login, admin rooms
presence, dynamic read rules, cross-node operation, streams, S3 backup and
scheduled backups are outside its supported envelope. Authorization-code
OAuth still needs operator-provided OAuth apps. This reference deployment
starts one daemon with durable local storage and PostgreSQL 17.

## Verify before starting

Use an exact version tag (first intended tag: `v0.1.0-alpha.1`) to discover
its digest; deploy the verified digest, never the moving `alpha` alias.

```sh
TAG=v0.1.0-alpha.1
IMAGE=ghcr.io/whysopriyank/instant-v2
DIGEST=$(docker buildx imagetools inspect "$IMAGE:$TAG" --format '{{json .Manifest}}' | jq -er .digest)
IDENTITY="https://github.com/whysopriyank/instant-v2/.github/workflows/publish.yml@refs/tags/$TAG"
cosign verify --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com "$IMAGE@$DIGEST"
cosign verify-attestation --type spdxjson --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com "$IMAGE@$DIGEST"
gh attestation verify "oci://$IMAGE@$DIGEST" --repo whysopriyank/instant-v2 \
  --signer-workflow whysopriyank/instant-v2/.github/workflows/publish.yml \
  --source-ref "refs/tags/$TAG"
```

Download the draft release artifacts as an authorized operator. Public users
can download them once the owner publishes the verified draft. Check the
signature before checking the downloaded archive hashes:

```sh
cosign verify-blob --bundle checksums.txt.sigstore.json \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com checksums.txt
sha256sum --check --ignore-missing checksums.txt
```

## Start the reference Compose deployment

Copy `compose/.env.example` to a private file outside the checkout, set its
permissions to `0600`, fill every required setting, and set
`INSTANT_V2_IMAGE_DIGEST` to the verified `sha256:...` digest. Generate the
PostgreSQL password and storage signing secret with `openssl rand -hex 32`.
The password must be URI-safe because it is embedded in the PostgreSQL DSN.
Keep the signing secret stable across restarts. Compose interpolation and
`docker inspect` expose environment values to operators with Docker access;
keep the environment file and Docker socket access restricted.

```sh
docker compose --env-file /private/path/instant-alpha.env -f deploy/compose/docker-compose.yml config --quiet
docker compose --env-file /private/path/instant-alpha.env -f deploy/compose/docker-compose.yml up -d --wait
```

The API binds only to host loopback. Exposing it needs the operator's existing
TLS ingress and app-origin settings. The image's `/data` directory is owned
by UID/GID 65532; fresh named volumes inherit that ownership. Existing or
host-mounted roots must already be writable by 65532. Preserve both named
volumes; `down -v` destroys database and file data. See the self-host guide
for app creation and backup commands. Recovery depends on operator exports;
there are no scheduled backups in this alpha.

## Local release dry run

Prerequisites: Go 1.25.14, GoReleaser 2.18.1, Syft 1.51.1, cosign 3.1.3,
Docker/buildx. A snapshot builds separate amd64/arm64 images locally without
registry publication. It cannot create a local multiarch OCI index.

```sh
goreleaser check
goreleaser release --snapshot --clean --skip=sign
```

For a host without Docker, add `--skip=docker,sign` instead. That produces
binary archives, SPDX SBOMs and checksums only, with no container evidence.
To exercise the same checksum-bundle format without OIDC or a public
transparency-log write, generate an ephemeral local key and an empty service
config outside the checkout:

```sh
SIGN_DIR=$(mktemp -d)
export COSIGN_PASSWORD=local-dry-run-only
cosign generate-key-pair --output-key-prefix "$SIGN_DIR/cosign"
cosign signing-config create --out "$SIGN_DIR/offline.json"
cosign sign-blob --key "$SIGN_DIR/cosign.key" --signing-config "$SIGN_DIR/offline.json" \
  --bundle "$SIGN_DIR/checksums.sigstore.json" dist/checksums.txt
cosign verify-blob --key "$SIGN_DIR/cosign.pub" --insecure-ignore-tlog \
  --bundle "$SIGN_DIR/checksums.sigstore.json" dist/checksums.txt
rm -rf "$SIGN_DIR"
unset COSIGN_PASSWORD
```

The local-key check proves signing mechanics; keyless identity and
transparency are exercised only by the protected workflow after publication
authorization. Offline verification's `--insecure-ignore-tlog` must never be
used for downloaded releases.

## Publisher setup (owner action)

Protect `v*-alpha.*` tags with a ruleset, restrict tag creation, and configure
required reviewers for the `public-alpha` environment before granting
`AUTH-PUBLISH-001`. The workflow requires `github.ref_protected`; an
unprotected tag is skipped. The publisher refuses existing version/full-SHA
tags, publishes one amd64/arm64 OCI index with an `alpha` alias, creates a
draft binary release, and verifies signatures/provenance plus container
behavior after pulling the published digest. It never creates `latest`.

Choose repository/package visibility and the project's license separately.
The checkout currently declares no license; image metadata records
`NOASSERTION`. Neither this scaffold nor a local snapshot grants publication
or proves OP-004/QR-004 acceptance. Keep the release draft until the new
candidate's release gate and postpublish verification pass.

## Retained OP-004 facts

The default smoke removes its temporary outputs. For a qualification run,
use a fresh output directory and supply the schema-1 identity object defined
in `docs/plans/next-release/qualification-contract.md`:

```sh
CONTAINER_IMAGE="$IMAGE@$DIGEST" \
CONTAINER_EVIDENCE_DIR=/private/path/evidence/container \
CONTAINER_IDENTITY_FILE=/private/path/container-identity.json \
  bash scripts/quality-container.sh
```

`CONTAINER_EVIDENCE_DIR` must not exist. It is retained on success or failure,
including image inspection, actual process UID, HTTP readiness statuses,
TLS-probe output, setup IDs, the existing soak harness's completion manifest
and raw events, before/after blob bytes, drain timing, server logs and cleanup
status. `facts.json` is written only after all runtime assertions pass and
binds the supplied identity after verifying image revision, pulled image
RepoDigest and the actual `/instantd` bytes. A local Docker configuration/image
ID is not an OCI manifest digest and is refused for retained qualification.
The identity's configuration/endpoint/fixture hashes must come from the
coordinator's retained campaign inputs; the producer does not invent them.
The probe uses `/health` for both liveness and DB readiness, before and after
container recreation. The short soak proves transaction acknowledgement and
refresh watermark convergence, not independent per-subscriber content parity
or load capacity. A failed cleanup has no `cleanup.complete` and exits nonzero.

Platform qualification is Linux amd64 on the currently selected owned host.
Linux arm64 and Darwin arm64 are cross-compiled artifacts until separately
executed on a matching host; a multiarch index alone is not ARM64 runtime
qualification. The initial selected release version is `v0.1.0-alpha.1`.
