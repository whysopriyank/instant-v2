# 03 — Frozen Wire Protocol Contract

**Sources**: `server/src/instant/reactive/session.clj` (op dispatcher ~984, `get-supported-features` 73–107,
`handle-init!` 141–190), `client/packages/core/src/Reactor.js` (_handleReceive 641–929,
init send 1762), `server/src/instant/db/transaction.clj` (tx-steps spec 32–67),
`server/src/instant/admin/routes.clj` (778–822), `server/src/instant/runtime/routes.clj`
(745–776), `server/src/instant/storage/routes.clj` (71–76), `server/resources/migrations/01_bootstrap.up.sql`,
`client/packages/core/src/attrTypes.ts`, `client/packages/core/src/rulesTypes.ts`,
`server/src/instant/model/rule.clj` (286–290), `client/packages/core/src/authAPI.ts`.

All field spellings are **kebab-case** on the WS (e.g. `client-event-id`, `processed-tx-id`).
Any break is a new SDK major.

---

## 1. Transports

- **WebSocket** at `GET /runtime/session` (default `wss://api.instantdb.com/runtime/session`).
- **SSE fallback** at `GET /runtime/sse` (downstream) + `POST /runtime/sse/push` (upstream)
  with handshake returning `{session-id, sse-token, machine-id}`.

A single session negotiates versions on `init`; all subsequent ops carry a UUID
`client-event-id` echoed by replies for correlation.

## 2. Version / feature negotiation

`init` carries a `versions` map keyed by SDK name (`"@instantdb/core": semver`). Server
derives a feature set:

| Feature            | Gate                |
|---|---|
| `skip-attrs`       | `≥ 0.20.4`         |
| `patch-presence`   | `≥ 0.17.5`         |
| `batch-messages`   | `≥ 0.22.75`        |

Responses are shaped behind these flags. Unknown `op`s are **logged and ignored**
(Reactor.js _handleReceive fall-through) — the v1 forward-compat lever. Preserve it.

## 3. Client → server ops (all JSON `{op, ...}`)

| op | Required fields | Notes |
|---|---|---|
| `init` | `app-id`, `refresh-token?`, `versions`, `__admin-token?` | `__admin-token` bypasses permissions. Replies with `init-ok{session-id,auth,attrs,app-status}`. |
| `add-query` | `q`, `client-event-id` | `q` is an InstaQL query (see §6). Replies `add-query-ok` or `add-query-exists`. |
| `remove-query` | `q`, `client-event-id` | Tears down a subscription. Replies `remove-query-ok`. |
| `transact` | `tx-steps`, `client-event-id` | Grammar in §4. Replies `transact-ok{tx-id,isn}` or `transact-error`. |
| `error` | `message` | Client-side error report. |
| `join-room` / `leave-room` | `room-type`, `room-id` | Presence. |
| `set-presence` / `refresh-presence` | `data`, `is-patch?` (behind `patch-presence`) | Ephemeral state. |
| `client-broadcast` / `server-broadcast` | `room-type`, `room-id`, `data` | `server-broadcast` is the server echo. |
| `start-sync` / `remove-sync` / `refresh-sync-table` / `resync-table` | `sync-id`, etc. | Table-level sync streams. |
| `start-stream` / `append-stream` / `subscribe-stream` / `unsubscribe-stream` | `stream-id`, bytes | Byte streams. |

Additionally, the **in-validator injects** `{op:"refresh", …}` internally
(`reactive/invalidator.clj:246`) — never sent by browsers but part of session handling.

## 4. Tx-steps grammar (frozen)

Canonical spec at `db/transaction.clj:32-67`, built client-side in `instaml.ts`:

```jsonc
["add-triple",    entityIdOrLookup, attrIdOrLookup, value, opts?]
["deep-merge-triple", entityIdOrLookup, attrIdOrLookup, valueObj]
["retract-triple", entityIdOrLookup, attrIdOrLookup, value]
["delete-entity",  lookupOrEntityId, etype]
["add-attr",       {id, "forward-identity":[uuid,etype,label],
                    "reverse-identity":[uuid,etype,label],
                    "value-type":"blob"|"ref", "cardinality":"one"|"many",
                    "unique?":bool, "index?":bool, ...}]
["update-attr",    {id, ...patch...}]
["delete-attr",    attrId]
["restore-attr",   attrId]
["rule-params",    { ... }]               // propagated per-transaction, not persisted
```

Lookup refs are encoded **as** `[attrId, value]` arrays (`instaml.ts: extractLookup:120`
↔ server's lookup expansion). Modes `create`/`update` with required-checks are enforced
server-side. Any grammar change is an SDK major.

## 5. Server → client ops

| op | Fields |
|---|---|
| `init-ok` | `{session-id, auth, attrs, app-status}` |
| `add-query-ok` / `add-query-exists` | echoes subscription id |
| `remove-query-ok` | |
| `refresh-ok` | `{computations:[{instaql-query, instaql-result}], processed-tx-id, processed-isn, attrs?}` |
| `transact-ok` | `{tx-id, isn}` (`isn` = incrementing sequence number from wal tailer) |
| `error` | `{status, type, message, hint?}` |
| `presence` / `broadcast` / `stream:*` / `app-status-changed` | ephemeral/stream fan-out |

`refresh-ok` currently ships **full `instaql-result` envelopes** (see §6). A wire-level delta
is an additive v2 optimisation (new feature flag), not a breaking change.

Batching: when the session negotiates `batch-messages`, server may coalesce multiple `refresh`es
into one frame; clients that understand it read an array envelope, others receive singular frames.

## 6. InstaQL surface (must stay identical to v1)

### Query shape (nested-map, GraphQL-like)

```jsonc
{ "posts": { "$": { "where": {...}, "order": {...}, "limit": 10,
                    "offset": 0, "before": cursor, "after": cursor,
                    "first": 10, "last": 10, "aggregate": "count", "fields": [...] },
            "comments": {} } }
```

### `$` options (coercion at `db/instaql.clj:100-133`)

`where`, `order`/`orderBy`, `limit`/`first`/`last`, `offset`, `before`/`after`
(+ `beforeInclusive`/`afterInclusive`), `aggregate` (`"count"` only, admin-only otherwise
raises at instaql.clj:1190), `fields`.

### Where operators (`db/instaql.clj:46-81` + `rulesTypes.ts`)

`$in` (legacy alias `in`), `$not` (`$ne` is an alias of `$not`), `$isNull`,
`$gt`/`$gte`/`$lt`/`$lte`, `$like`, `$ilike`, `$entityIdStartsWith` (dashboard helper),
dotted-path traversal across refs.

### Result envelope

`instaql-result` carries `{data, page-info{startCursor,endCursor,hasNextPage,hasPreviousPage}, aggregate{count?}}`.
Client re-evaluation (`client/packages/core/src/instaql.ts` + `datalog.js:117`) produces
identical envelopes for optimistic updates — dual implementations must agree byte-for-byte
under corpus tests. Reserved namespace `$$ruleParams`.

No `groupBy`. Datalog (`db/datalog.clj`) is internal-only.

## 7. Attr wire shape (attrTypes.ts)

```json
{ "id":"uuid", "value-type":"blob"|"ref", "cardinality":"one"|"many",
  "forward-identity":[uuid,etype,label], "reverse-identity":[uuid,etype,label],
  "unique?":bool, "index?":bool, "required?":bool, "primary?":bool,
  "on-delete": "...", "on-delete-reverse": "...",
  "checked-data-type":"string"|"number"|"boolean"|"date"|"json",
  "indexing?":bool, "setting-unique?":bool }
```

Field spellings include hyphens and question-mark suffixes. Parity is required for `init-ok`
round-trips and for `add-attr`/`update-attr` tx-step payloads.

## 8. Permissions model (model/rule.clj:286-290, rulesTypes.ts)

**Persistence**: one JSONB doc per app — `rules(app_id uuid primary key, code jsonb)`.

```jsonc
{ "posts": { "bind": ["ownerId = \"auth.id\"", ...],
             "allow": {"view":"author.id == auth.id", "$default":"false"},
             "fields": {"secret": "false"} },
  "attrs":      { "allow": { ... } },
  "$default":   { "allow": { "view":"false", "$default":"false" },
                  "fields": {"$default":"false"} },
  "$rateLimits": { "Chat": {"capacity":100, "refill":{"amount":50,"period":"1m"},
                             "type":"greedy"|"interval"} } }
```

Resolution chain: `etype.allow.action → etype.allow.$default → $default.allow.action
→ $default.allow.$default → etype.fallback.action`. `bind` entries are topologically
sorted with cycle detection; reserved namespaces `$users/$files/$default/$streams/$rateLimits`.
Each string value is a CEL expression. CES parameters are bound from auth/data + request
context (`modifiedFields`, `time`/`ip`/`origin` via `db/proto.clj` protobuf).

Client-side `rulesTypes.ts` is the canonical authoring shape; server applies it to
both reads (compiled into query `where` clauses in `db/cel.clj`) and writes
(per-step programs in `permissioned_transaction.clj`).

## 9. REST surface (frozen paths)

### Runtime (`runtime/routes.clj:745-776`) — user-facing

```
POST /runtime/auth/send_magic_code
POST /runtime/auth/verify_magic_code
POST /runtime/auth/sign_out
POST /runtime/auth/refresh_tokens
POST /runtime/auth/sign_in_guest
GET  /runtime/session                  ← WS upgrade
GET  /runtime/sse                      SSE fallback
POST /runtime/sse/push
POST /runtime/signout
POST /runtime/framework/query          HTTP query path (no WS)
GET  /runtime/oauth/{start,callback,token,id_token}
GET  /runtime/openid-configuration
```

Kebab/snake details match `authAPI.ts`: `refresh-token` vs `refresh_token`,
`code_verifier`, `extra_fields` — corpus must assert exact spellings.

### Admin (`admin/routes.clj:778-822`) — authenticated by `__admin-token` or PAT

```
POST /admin/query | /admin/transact | /admin/query_perms_check
                  | /admin/transact_perms_check | /admin/subscribe-query | /admin/sse (+push)
POST /admin/sign_out | /admin/refresh_tokens | /admin/magic_code | ...
GET  /admin/users | DELETE /admin/users
GET  /admin/schema | /admin/soft_deleted_attrs | /admin/rooms/presence
/storage/signed-upload-url | /storage/upload | /storage/signed-download-url
/storage/files (single + bulk delete)
```

Auth: `__admin-token` on WS/skipping all permission checks when passed to `init`;
admin routes keyed by PAT/`__admin-token`.

### Storage (`storage/routes.clj:71-76`) — SDK-coupled

```
PUT  /storage/upload
DELETE /storage/files
POST /storage/signed-upload-url
PUT  /storage/:upload-id/consume-upload-url
GET  /storage/signed-download-url
```

### Dashboard/management (future-facing, not yet frozen for self-host)

`dash/routes.clj:2726-2903` — schema push plan/apply/pull, rule versions, indexing jobs,
backups, webhooks, OAuth apps/service providers. Only the CLI couples here; not in
the frozen compat surface for phase 1–4.

## 10. Auth semantics

- Refresh tokens are **opaque secrets stored hashed**, looked up by re-running an InstaQL
  query on `$userRefreshTokens.hashedToken` (model/app_user.clj:155-176). Not JWT.
- OAuth: bespoke GitHub + generic OIDC-discovery providers; id_token verification
  RS256/ES256 via JWKS (`auth/jwt.clj`), including Apple client-secret minting and
  provider nonce quirks (Google skips nonce check; Apple accepts `sha256(nonce)` at
  auth/oauth.clj).
- PKCE everywhere (migration 15).

## 11. What must NOT change without an ADR

- Any field spelling or `op` name in sections 2–7, 9–10.
- Tx-step tuple shapes.
- Index semantics (`:ea/:eav/:av/:ave/:vae` and flag-column derivation).
- CEL fallback chain or reserved namespaces.
- Whether `rules.code` is one JSONB doc per app.

Any deliberate break requires a bumped SDK major and a corpus version bump (see 05).
