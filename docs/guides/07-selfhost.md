# Self-hosting instantd (v2)

This guide covers controlled single-node alpha testing. No public release is
available yet and production readiness is not established. Follow the current
[public-alpha envelope](../reference/public-alpha-release-envelope.md); the
September 26 acceptance applies only to its historical DEC-001 candidate.

## Quickstart

Selected runtime: one daemon and primary PostgreSQL 17 on Linux amd64.
For a source run, use Go 1.25+ and Docker for the database. Serving uses
post-commit notification; the separately tested WAL tailer is not the assembled
serving path. The reference database configuration retains logical WAL.
Google and GitHub OAuth configuration is mandatory when a database is set,
even for testing that does not exercise provider login.

```sh
# Run from the repository root. Export the four OAuth variables with your app settings.
: "${INSTANT_OAUTH_GOOGLE_CLIENT_ID:?set Google client ID}"
: "${INSTANT_OAUTH_GOOGLE_CLIENT_SECRET:?set Google client secret}"
: "${INSTANT_OAUTH_GITHUB_CLIENT_ID:?set GitHub client ID}"
: "${INSTANT_OAUTH_GITHUB_CLIENT_SECRET:?set GitHub client secret}"

# 1. Local PostgreSQL and durable file storage
export POSTGRES_PASSWORD="$(openssl rand -hex 32)"
export INSTANT_V2_STORAGE_SECRET="$(openssl rand -hex 32)"
export INSTANT_V2_STORAGE_ROOT="$PWD/.instant-files"
mkdir -p "$INSTANT_V2_STORAGE_ROOT"
docker run -d --name instant-pg -p 127.0.0.1:5432:5432 \
  -e POSTGRES_USER=instant -e POSTGRES_DB=instant -e POSTGRES_PASSWORD postgres:17 \
  -c wal_level=logical

# 2. Wait for pg_isready, then run the source daemon on loopback
docker exec instant-pg pg_isready -U instant -d instant
export DATABASE_URL="postgres://instant:$POSTGRES_PASSWORD@127.0.0.1:5432/instant?sslmode=disable"
export INSTANT_V2_HTTP_ADDR=127.0.0.1:8080
export INSTANT_V2_WS_ALLOWED_ORIGINS=http://localhost:3000
go run ./cmd/instantd
```

Keep the generated database password, storage secret and file directory for
subsequent starts; do not regenerate them for an existing installation. The
OAuth settings must be exported in the shell running the daemon. For a verified
published image, the [distribution reference](../../deploy/README.md) documents
signature/provenance verification and the real
[Compose deployment](../../deploy/compose/docker-compose.yml), using the
[required settings template](../../deploy/compose/.env.example). Its required
image digest is not evidence that an image has already been published.

The daemon creates its schema on boot (embedded migrations under an advisory
lock, so a concurrent boot cannot race the migration step). This alpha's
release profile is single-node only: running more than one
`instantd` instance against one database is not a qualified or supported
topology in this release.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN |
| `INSTANT_V2_HTTP_ADDR` | `:8080` | Listen address (`INSTANT_V2_HTTP_PORT` also accepted) |
| `INSTANT_V2_STORAGE_SECRET` | — (required) | HMAC key signing storage presigned URLs. Startup fails without it; set `INSTANT_V2_INSECURE_DEV_SECRETS=1` to allow the legacy DSN-derived dev fallback |
| `INSTANT_V2_STORAGE_ROOT` | — (required) | Durable file-storage root. Startup fails without it (or on a malformed/unwritable root) unless `INSTANT_V2_INSECURE_DEV_SECRETS=1`, which uses an explicit `./.instant-dev-files` directory — never temp storage. Mount a writable volume here (see below) |

### File storage volume

The container image runs as UID/GID `65532:65532` and validates its storage root at
startup: the directory must exist (or be creatable), be absolute, and accept
synced writes, otherwise the daemon refuses to start before exposing any
route. Mount a persistent volume owned by `65532` and point
`INSTANT_V2_STORAGE_ROOT` at it:

```sh
# host path /srv/instant-files owned by 65532:65532
docker run -v /srv/instant-files:/data:rw -e INSTANT_V2_STORAGE_ROOT=/data …
```

Persistence across daemon restart (DA-001b): stopping the daemon and starting
a new `instantd` process with the same PostgreSQL database, the same
`INSTANT_V2_STORAGE_ROOT`, and the same `INSTANT_V2_STORAGE_SECRET` returns
the exact same objects and metadata. The storage fingerprint (root plus quota
plus credential posture, never the secret value) is logged at startup and is
identical across such restarts.

Warning: ephemeral filesystem or container layers lose blobs on restart. A
container without a mounted persistent volume at `INSTANT_V2_STORAGE_ROOT`
loses every stored object when the container is replaced, even though the
database rows survive. Object-backup routes stay a stable explicit `503`
(`{"message":"no object store wired"}`) for this alpha: no S3-compatible
store is assembled and no object-backup provider is selected. The new public
candidate requires its own native, container and restore qualification;
historical native acceptance does not establish those results.

Additional configuration:

| Env var | Default | Purpose |
|---|---|---|
| `INSTANT_V2_MAX_SUBS_PER_APP` | 2000 | Live subscription cap per app (0 would mean unlimited) |
| `INSTANT_V2_MAX_WS_CONNS` | 20000 | Concurrent websocket connection cap |
| `INSTANT_V2_MAX_SSE_CONNS` | 10000 | Concurrent SSE stream cap |
| `INSTANT_V2_MAX_UPLOAD_BYTES` | 536870912 (512 MiB) | Per-upload body ceiling; over-cap uploads are rejected 413 |
| `INSTANT_V2_MAX_BACKUP_BYTES` | 34359738368 (32 GiB) | Configuration value; current alpha restore HTTP routes enforce a fixed 1 GiB ceiling, which this setting does not raise |
| `INSTANT_V2_READ_URL` | `$DATABASE_URL` | Read-plane DSN (instaql refreshes); point at a replica to isolate reads (docs/09 §T2.3) |
| `INSTANT_V2_WRITE_POOL_MAXCONNS` | 32 | Write-pool connection ceiling |
| `INSTANT_V2_READ_POOL_MAXCONNS` | 32 | Read-pool connection ceiling |
| `INSTANT_V2_MAX_QUEUE_DEPTH` | 0 (off) | Shed transacts (429 + Retry-After) when the refresh queue crosses this depth; reopens at half (docs/09 §T2.1) |
| `INSTANT_V2_PG_STATEMENT_TIMEOUT` | `30s` | Per-connection statement ceiling on both pools (`0` disables); one runaway query can no longer pin a pooled conn. Migrations are exempt |
| `INSTANT_V2_PG_LOCK_TIMEOUT` | `5s` | DDL/row-lock wait ceiling on both pools (`0` disables) |
| `INSTANT_V2_PG_IDLE_TX_TIMEOUT` | `30s` | Kills connections left idle inside an open transaction (`0` disables) |
| `INSTANT_V2_WS_COMPRESSION` | `disabled` | permessage-deflate: `no-context-takeover` or `context-takeover` (docs/09 §T2.2) |
| `INSTANT_V2_WS_ALLOWED_ORIGINS` | `*` (development default) | Set explicit application origins for the alpha; wildcard is rejected by qualification |
| `INSTANT_V2_INVALIDATION_BUS` | `none` | `postgres` enables LISTEN/NOTIFY invalidation across peer processes; this config knob exists, but running more than one `instantd` against one DB is not a qualified or supported topology for this single-node alpha (DEC-001; docs/09 §T2.4) |
| `INSTANT_V2_NODE_ID` | hostname | Node identity in logs and `/health` |
| `INSTANT_V2_METRICS_ADDR` | `127.0.0.1:9465` | Prometheus scrape endpoint (`/metrics`); set empty to disable |
| `INSTANT_OAUTH_GOOGLE_CLIENT_ID` | _(required)_ | Google authorization-code client ID |
| `INSTANT_OAUTH_GOOGLE_CLIENT_SECRET` | _(required)_ | Google authorization-code client secret |
| `INSTANT_OAUTH_GITHUB_CLIENT_ID` | _(required)_ | GitHub authorization-code client ID |
| `INSTANT_OAUTH_GITHUB_CLIENT_SECRET` | _(required)_ | GitHub authorization-code client secret |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(unset)_ | Enables OTLP/HTTP trace export for the transact→commit→fanout chain |

## Creating an app

There is no hosted dashboard. Create an app and admin token directly:

```sh
psql "$DATABASE_URL" -c "INSERT INTO apps (id, title, created_at) VALUES (gen_random_uuid(), 'my-app', now()) RETURNING id"
psql "$DATABASE_URL" -c "INSERT INTO app_admin_tokens (token, app_id) VALUES (gen_random_uuid(), '<app-id>') RETURNING token"

# Register the origins your app may redirect to after OAuth
# (exact "scheme://host" match; an empty list permits no redirects):
psql "$DATABASE_URL" -c "UPDATE apps SET redirect_origins = '[\"http://localhost:3000\"]'::jsonb WHERE id = '<app-id>'"
```

(Tools like `cmd/soaksetup` do the same programmatically; a CLI wrapper is on
the roadmap.)

## Pointing client SDKs at your daemon

```js
import { init } from '@instantdb/react';
const db = init({
  appId: '<app-id>',
  apiURI: 'http://localhost:8080',
  websocketURI: 'ws://localhost:8080/runtime/session',
});
```

This configuration is a testing example, not a claim that frozen v1 SDKs work
unmodified. The [actual v1 rehearsal](../plans/next-release/v1-differential-rehearsal-20261002.md)
found discrepancies in 16 of 18 selected scenarios. Current query tuples omit
v1's fourth timestamp element; SDK `serverCreatedAt` ordering, infinite-query
and cursor fidelity are unqualified. Serialized cursor input also differs from
pinned v1. See [compatibility observations](../plans/next-release/v1-compatibility-observations.md).
An owner decision on these discrepancies remains pending; they are not approved
alpha exclusions.

The approved alpha differences remain explicit: no email delivery (503), direct
ID-token sign-in (501), admin presence (501), dynamic view rules, sync/stream
operations or object-store backup (503). HA/read replicas and real-provider
acceptance remain unselected.

## Surfaces

- `POST /runtime/framework/query`, `/runtime/auth/*`, `/runtime/oauth/*`,
  `/runtime/signout` — end-user plane
- `/admin/*` — admin-token plane (`Authorization: Bearer <token>` or
  `X-admin-token:`); bypasses permission checks by design
- `POST /storage/signed-upload-url`, signed PUT/GET, `DELETE /storage/files` — blob plane
- `GET /backup/{app_id}` / `POST /backup/{app_id}/restore` — streaming app dumps
- `ws://host/runtime/session` (+ SSE fallback `/runtime/sse`) — reactive sync

## Backups

Export is a plain NDJSON stream with trailing checksum:

```sh
curl -H "Authorization: Bearer <admin-token>" http://localhost:8080/backup/<app-id> > app.ndjson
```

Restore posts it to an empty target app; a nonempty target is rejected. Native
NDJSON contains SQL records and file metadata, not file bytes: preserve and
restore the durable file volume alongside it. V1 ZIP restoration includes its
file bytes. Known rejections preserve target/source state; an unknown COMMIT
outcome requires reconciliation and a crash may leave orphan blobs. Use an
alpha maintenance window for restore. See the
[restore contract](../plans/next-release/restore-contract.md).

Object storage routes (`PUT /backup/<app-id>/object?key=…`,
`GET /backup/<app-id>/object?key=…`,
`POST /backup/<app-id>/restore-object?key=…`) are disabled for this alpha:
they return the stable explicit `503 {"message":"no object store wired"}`,
construct no object-store backend, create no staging or final backup object,
and require no S3 credentials. They activate only when an S3-compatible
store is wired in a future packet; keys are namespaced under the app id
server-side. All backup routes require the app's admin token. See
`internal/backup` package doc. Public-candidate container and restore acceptance
require their own retained runtime evidence; production recovery is not claimed.
