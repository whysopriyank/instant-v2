# Self-hosting instantd (v2)

This guide replaces every `instantdb.com` assumption in the v1 docs. Nothing here
phones home: the daemon talks to one Postgres and to your clients, nothing else.

## Quickstart

Requirements: Postgres 15+ with `wal_level=logical` (reactive sync streams changes
via logical replication), Go 1.24+ or Docker.

```sh
# 1. Postgres with logical WAL
docker run -d --name instant-pg -p 5432:5432 \
  -e POSTGRES_USER=instant -e POSTGRES_PASSWORD=instant postgres:17 \
  -c wal_level=logical

# 2. Run instantd against it
export DATABASE_URL='postgres://instant:instant@localhost:5432/instant?sslmode=disable'
go run ./cmd/instantd            # or: docker run -p 8080:8080 ghcr.io/<you>/instantd
```

The daemon creates its schema on boot (embedded migrations under an advisory
lock — safe to run multiple replicas against one DB).

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN |
| `INSTANT_V2_HTTP_ADDR` | `:8080` | Listen address (`INSTANT_V2_HTTP_PORT` also accepted) |
| `INSTANT_V2_STORAGE_SECRET` | — (required) | HMAC key signing storage presigned URLs. Startup fails without it; set `INSTANT_V2_INSECURE_DEV_SECRETS=1` to allow the legacy DSN-derived dev fallback |
| `INSTANT_V2_STORAGE_ROOT` | — (required) | Durable file-storage root. Startup fails without it (or on a malformed/unwritable root) unless `INSTANT_V2_INSECURE_DEV_SECRETS=1`, which uses an explicit `./.instant-dev-files` directory — never temp storage. Mount a writable volume here (see below) |

### File storage volume

The daemon runs as UID/GID `65532:65532` and validates its storage root at
startup: the directory must exist (or be creatable), be absolute, and accept
synced writes, otherwise the daemon refuses to start. Mount a persistent
volume owned by `65532` and point `INSTANT_V2_STORAGE_ROOT` at it:

```sh
# host path /srv/instant-files owned by 65532:65532
docker run -v /srv/instant-files:/data:rw -e INSTANT_V2_STORAGE_ROOT=/data …
```

Ephemeral container storage loses blobs on restart; object-backup routes stay
`503` until an S3-compatible store is wired (not selected for the alpha).
| `INSTANT_V2_MAX_SUBS_PER_APP` | 2000 | Live subscription cap per app (0 would mean unlimited) |
| `INSTANT_V2_MAX_WS_CONNS` | 20000 | Concurrent websocket connection cap |
| `INSTANT_V2_MAX_SSE_CONNS` | 10000 | Concurrent SSE stream cap |
| `INSTANT_V2_MAX_UPLOAD_BYTES` | 536870912 (512 MiB) | Per-upload body ceiling; over-cap uploads are rejected 413 |
| `INSTANT_V2_MAX_BACKUP_BYTES` | 34359738368 (32 GiB) | Restore-body ceiling |
| `INSTANT_V2_READ_URL` | `$DATABASE_URL` | Read-plane DSN (instaql refreshes); point at a replica to isolate reads (docs/09 §T2.3) |
| `INSTANT_V2_WRITE_POOL_MAXCONNS` | 32 | Write-pool connection ceiling |
| `INSTANT_V2_READ_POOL_MAXCONNS` | 32 | Read-pool connection ceiling |
| `INSTANT_V2_MAX_QUEUE_DEPTH` | 0 (off) | Shed transacts (429 + Retry-After) when the refresh queue crosses this depth; reopens at half (docs/09 §T2.1) |
| `INSTANT_V2_PG_STATEMENT_TIMEOUT` | `30s` | Per-connection statement ceiling on both pools (`0` disables); one runaway query can no longer pin a pooled conn. Migrations are exempt |
| `INSTANT_V2_PG_LOCK_TIMEOUT` | `5s` | DDL/row-lock wait ceiling on both pools (`0` disables) |
| `INSTANT_V2_PG_IDLE_TX_TIMEOUT` | `30s` | Kills connections left idle inside an open transaction (`0` disables) |
| `INSTANT_V2_WS_COMPRESSION` | `disabled` | permessage-deflate: `no-context-takeover` or `context-takeover` (docs/09 §T2.2) |
| `INSTANT_V2_INVALIDATION_BUS` | `none` | `postgres` enables LISTEN/NOTIFY invalidation across nodes — required when running >1 instantd against one DB (docs/09 §T2.4) |
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
const db = init({ appId: '<app-id>', apiURI: 'http://localhost:8080' });   // REST/WS plane
// websocket lands on ws://localhost:8080/runtime/session automatically
```

The frozen v1 SDKs work unmodified — that compatibility is the project's core
constraint.

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

Restore posts it back. Object storage routes (`PUT /backup/<app-id>/object?key=…`,
`POST /backup/<app-id>/restore-object?key=…`) activate when an S3-compatible
store is wired; keys are namespaced under the app id server-side. All backup
routes require the app's admin token. See `internal/backup` package doc.
