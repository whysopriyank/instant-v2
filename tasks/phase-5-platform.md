# Phase 5 — Platform surfaces

**Read first**: `docs/03-protocol.md` §9 (frozen REST paths), `docs/02-architecture.md` (stack).

Three packages ship in parallel on disjoint paths. None may touch the protocol schema
(that stayed frozen since Phase 0) or the Phase 4 invariants.

## 5A — `internal/adminapi` (owner: `adminapi`)

Admin is keyed by `__admin-token` or PAT and **bypasses all permission checks**
(WS `init` path asserts this same semantic).

- [ ] `POST /admin/{query, transact, query_perms_check, transact_perms_check, subscribe-query, sse (+push)}`
      — each reuses `internal/instaql`/`transact`/`perms` from prior phases; no re-impl.
      `query_perms_check`/`transact_perms_check` dry-run permission evaluation without committing.
- [ ] `POST /admin/{sign_out, refresh_tokens, magic_code, send_magic_code, verify_magic_code, sign_in_guest}`
- [ ] `GET|DELETE /admin/users`, `GET /admin/schema`, `GET /admin/soft_deleted_attrs`,
      `GET /admin/rooms/presence` (admin view of reactive presence store).
- [ ] Token auth: `Authorization: Bearer <admin_token>` + `__admin-token` header compat.
- [ ] Tests: HTTP golden files in `corpus/http/admin-*.ndjson`; `TestAdminBypass` asserts that
      admin writes elide CEL checks even when the rule doc says `false`.

## 5B — `internal/runtimeapi` (owner: `runtimeapi`)

Runtime is the end-user REST plane (every call couples to `authAPI.ts` field spellings):

- [ ] `POST /runtime/auth/{send_magic_code, verify_magic_code, sign_out, refresh_tokens, sign_in_guest}`
      — spelling bridge between kebab-over-WS and snake/kebab-over-HTTP
      (`refresh-token` vs `refresh_token`, `code_verifier`, `extra_fields`) asserted by corpus.
- [ ] `POST /runtime/framework/query` — HTTP query path using the same `instaql→datalog` engine (no second query impl).
- [ ] `POST /runtime/signout`, `GET /runtime/oauth/{start,callback,token,id_token}`,
      `GET /runtime/openid-configuration` (per-app discovery URL passthrough), Apple/Signing flows.
- [ ] `GET /runtime/session` (WS upgrade) and `GET /runtime/sse` delegating to `internal/sync` — no duplicate session state.
- [ ] Tests: corpus HTTP suites `corpus/http/runtime-*.ndjson`; WireMock-style record/restore of JWKS endpoints.

## 5C — `internal/storageapi` (owner: `storageapi`)

SDK-coupled blob routes (`client/...` storage client hits these directly):

- [ ] `POST /storage/signed-upload-url` — returns `{url, id, expires_at}`; policy respects `attrs` for `$files` binding
- [ ] `PUT  /storage/upload` (direct passthrough where provider allows) + `PUT /storage/:id/consume-upload-url`
- [ ] `GET  /storage/signed-download-url`
- [ ] `DELETE /storage/files` (single + bulk) + `PUT/DELETE` per-object ops
- [ ] S3 backend via `aws-sdk-go-v2/service/s3` + `s3-transfer-manager` equiv; presigner with CRT config;
      file→triple linkage for `$files` (file triples created/retracted alongside upload/delete).
- [ ] MIME: stdlib `mime.TypeByExtension` (v1 uses Tika; document the subset delta in an ADR if it matters).
- [ ] Tests: `TestStoragePresignRoundTrip` against a MinIO fixture + `examples/*` that use `$files` replayed against v2.

## Phase 5 exit gate

```
corpus HTTP suites (admin + runtime + storage) replay pass (WireMock + MinIO fixtures)
examples/* that use storage ($files) replay against v2 (frame+HTTP granularity)
field-spelling regression test: authAPI.ts kebab/snake map asserted literally
```
