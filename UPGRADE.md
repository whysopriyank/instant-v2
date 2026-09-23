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
4. **OAuth apps**: re-set provider secrets via env (`INSTANT_OAUTH_GOOGLE_CLIENT_ID`/
   `_SECRET`, `INSTANT_OAUTH_GITHUB_CLIENT_ID`/`_SECRET`). Apple OAuth is explicitly
   excluded from this alpha (DEC-001): `INSTANT_OAUTH_APPLE_KEY_P8` exists in
   source but is not wired into the builtin provider list. v1's stored OAuth
   config rows are honored where present for Google/GitHub.
5. **Single-node deployment.** This alpha release profile is single-node only
   (DEC-001): run exactly one `instantd` against the database. Migrations take
   an advisory lock so a restart cannot race a concurrent boot, but running more
   than one `instantd` instance against one database is not a qualified or
   supported topology in this release. The production invalidation path is
   post-commit notification, optionally propagated to peer processes via
   Postgres LISTEN/NOTIFY (`INSTANT_V2_INVALIDATION_BUS=postgres`); the
   WAL-based logical-replication tailer (`internal/waltail`) is a separately
   verified component, not the production serving path.

## Rollback

v2's migrations are additive on v1's existing tables on the boot ("up") path,
so stopping instantd and restarting v1 against the same `DATABASE_URL` is
architecturally compatible. This has not been exercised as a formal drill
(the backup/restore drill, OP-006, is not yet run) — treat it as an
unverified fallback, not an accepted rollback procedure.

## Cutting a signed release

No CI workflow currently publishes a release: `.github/workflows/ci.yml` runs
only on push to `main` and on pull requests, and does not invoke `goreleaser`.
Tag/publish/sign/SBOM/canary are explicitly unselected for this alpha
(DEC-001; FR-003/FR-004 and QR-004 remain `NOT_SELECTED`). The committed
`.goreleaser.yaml` describes the intended archive/SBOM/GHCR/cosign shape for a
future release workflow, but only its local dry-run commands are real today:

1. Local dry-run without publishing:
   - `goreleaser check`
   - `goreleaser build --snapshot --clean`
2. Offline signing smoke (no Rekor/network):
   `cosign sign-blob --key k.key --signing-config sc.json --new-bundle-format --bundle b.bundle FILE`
   then verify with `--bundle b.bundle --insecure-ignore-tlog`.
   Current cosign CLI refuses `--tlog-upload=false`; use a signing-config.

Publishing a real tagged release requires a new owner-authorized CI workflow
(FR-003) that does not exist yet.
