# Upgrading from InstantDB v1 (self-host) to instantd (v2)

Audience: operators running the v1 self-host server (`instant` Clojure/Java
stack) who want to move to the Go daemon.

## What stays compatible

- **Client SDKs are untouched.** The frozen v1 JS/React SDK is a conformance
  oracle for v2 — every client keeps working with zero changes. Point
  `apiURI` at the v2 daemon.
- **Physical data layout.** v2 keeps v1's triple-store shape in Postgres
  (`attrs`, `triples`, `apps`, transactions journal, ref-as-uuid-string JSON
  codec, `value_md5`). A v1 database is readable by v2's queries.

## What changes

| Area | v1 | v2 |
|---|---|---|
| Process model | JVM + Hazelcast mesh + SNS | single static binary per node |
| Config | `config.edn`, env mix | `DATABASE_URL`, `INSTANT_V2_HTTP_ADDR`, `INSTANT_V2_STORAGE_SECRET` |
| Storage backend | S3 (+ Tika MIME) | local disk HMAC-presigned URLs (S3 interface stubbed) |
| Presence patches | editscript patch-frames | full-snapshot refresh-presence (correct for all SDK versions; more bytes on large rooms) |
| User JWTs | none issued | none issued — opaque refresh tokens stored sha256-hashed, same as v1 |

## Migration steps

1. **Stop writes on v1**, note the final WAL LSN / transaction id if you want a cutover point.
2. **Dump each app** from v1. Two supported paths:
   - *Same-database upgrade*: v2 reads the existing schema in place. Run v2's
     migrations (`internal/platform.Migrate` runs automatically on boot), then start
     instantd against the v1 `DATABASE_URL`. No data copy needed.
   - *Cross-database*: export per-app dumps from the v1 DB and import them via
     v2's backup plane (`POST /backup/{app_id}/restore`) or the v1 dump importer in
     `internal/backup` (accepts v1 table exports of apps/attrs/triples/rules/
     transactions).
3. **Admin tokens carry over** — same `app_admin_tokens` table. Hosted-style
   `*-admin-token` env vars have no equivalent; tokens live in the DB only.
4. **OAuth apps**: re-set provider secrets via env (`INSTANT_OAUTH_APPLE_KEY_P8`
   etc.). v1's stored OAuth config rows are honored where present.
5. **Start v2 replicas.** Multiple instances are safe: migrations take an advisory
   lock; invalidation flows through logical-replication WAL tailing, not cluster
   gossip.

## Rollback

Because v2 is additive on the same tables, stopping instantd and restarting v1
against the same `DATABASE_URL` is a valid rollback path within a release.

## Cutting a signed release

1. Tag: `git tag vX.Y.Z && git push origin vX.Y.Z` — CI (.github/workflows/ci.yml) runs
   `goreleaser release --clean`. Requires these repo secrets/variables:
   - `GITHUB_TOKEN` (automatic), `COSIGN_YES=true` (keyless OIDC signing)
   - `GITHUB_REPOSITORY` is provided by Actions.
2. Artifacts per release: tar.gz archives (linux/darwin × amd64/arm64),
   checksums.txt (cosign-signed), SBOMs, multi-arch GHCR manifest
   `ghcr.io/<repo>:<tag>` + `:latest`, per-arch image digests signed keyless.
3. Local dry-run without publishing:
   - `goreleaser check`
   - `goreleaser build --snapshot --clean`
4. Offline signing smoke (no Rekor/network):
   `cosign sign-blob --key k.key --signing-config sc.json --new-bundle-format --bundle b.bundle FILE`
   then verify with `--bundle b.bundle --insecure-ignore-tlog`.
   Current cosign CLI refuses `--tlog-upload=false`; use a signing-config.
