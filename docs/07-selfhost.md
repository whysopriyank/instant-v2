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
| `INSTANT_V2_STORAGE_SECRET` | derived from DSN (dev only) | HMAC key signing storage presigned URLs |

## Creating an app

There is no hosted dashboard. Create an app and admin token directly:

```sh
psql "$DATABASE_URL" -c "INSERT INTO apps (id, title, created_at) VALUES (gen_random_uuid(), 'my-app', now()) RETURNING id"
psql "$DATABASE_URL" -c "INSERT INTO app_admin_tokens (token, app_id) VALUES (gen_random_uuid(), '<app-id>') RETURNING token"
```

(Tools like `tools/soaksetup` do the same programmatically; a CLI wrapper is on
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

Restore posts it back. See `internal/backup` package doc for the wire format.
