# 11 — Security Audit & Remediation (2026-08-25)

End-to-end audit executed across three parallel review tracks (authn/authz
trust chain · injection/data-plane · transport/DoS/storage), each verified
line-by-line against source, plus supply-chain (`govulncheck`), schema, and
repo-hygiene lanes. Every reported claim was independently re-verified before
remediation.

## Remediated (this wave)

| ID | Severity | Finding | Fix |
|----|----------|---------|-----|
| H1 | HIGH | Sync plane **failed open** on rule-doc load errors while HTTP planes failed closed | `rulesFor` propagates errors; ops answer `503 rules-unavailable` (4ffc1af) |
| H2 | HIGH | Unvalidated attr metadata amplified into every `init-ok`; anonymous schema writes on rule-less apps | ident grammar (1–256 B, `[A-Za-z0-9_$.-]`) forward+reverse; reverse-side ReservedNamespaces; value-type enum; `checked-data-type` persisted + enforced (24fe7ea) |
| H3 | HIGH | 64 MiB unauthenticated WS frames (~6 GiB/s parse churn per public app-id) | `INSTANT_V2_MAX_FRAME_BYTES` default 4 MiB for WS reads + transact bodies (e8b64e8) |
| H4 | HIGH | Rate limiter keyed on client headers; bucket map grew unboundedly | UUID-validated keys else IP bucket; MaxBuckets cap + idle eviction + SweepLoop; over-cap admits untracked so the cap can't become a lockout lever (e8b64e8) |
| H5 | HIGH | SSE: global-only cap, no deadlines/heartbeat, POST batch bypassed budgets | per-IP caps, 20s heartbeats with write deadlines, init guard, per-message ws-class charging (a9b0635) |
| M1 | MED | OAuth/id_token signup skipped `$users.create` gate | gate runs in `VerifyMagicCodeTrusted` (e8cf6a0) |
| M2 | MED | id_token nonce not enforced when claim absent | absence rejected when a nonce was bound; sha256(nonce) style preserved (e8cf6a0) |
| M4 | MED | Brute-force lockout process-local → N nodes = N× budget; map growth | `auth_throttle` table (migration 005) with atomic upserts shared by all nodes (e8cf6a0) |
| M6 | MED | Re-`init` orphaned prior session's subs/rooms past teardown → cap-exhaustion DoS | replaced sessions detached at swap on WS and SSE (6b96473) |
| M7 | MED | `reverse-identity` bypassed reserved namespaces / non-empty checks | same guards as forward side (24fe7ea) |
| M8 | MED | `checked_data_type` unreachable from wire → DB CHECK inert | wire field parsed, enum-validated, persisted; inserts pre-validated (24fe7ea) |
| M9 | MED | Raw pgx errors (SQLSTATEs, constraint names) returned to clients | `platform.ClientMessage` at WS/HTTP boundaries (92c3411) |
| M10 | MED | Stored XSS via sniffed content-type on same-origin file serving | nosniff; HTML/SVG/etc → octet-stream + attachment (92c3411) |
| M11 | MED | `INSECURE_DEV_SECRETS=no` enabled insecure mode; DSN-derived HMAC key silent | ParseBool strict; loopback-only fallback with WARN (92c3411) |
| SC | MED×5 | govulncheck reachable CVEs incl. pgx SQL-injection GO-2026-5004 | pgx v5.9.2, otel v1.43.0, grpc v1.82.1 (313c913) |

Also landed (LOW): rooms/presence caps (64 rooms, 4 KiB data) · tx-step cap
(10k) · InstaQL depth cap (10) · like-pattern bound (512 B) · value-position
lookups require unique attrs · presign Host sanitization · OAuth exchange
timeouts · explicit `MaxHeaderBytes` · metrics-server `IdleTimeout`.

## Accepted residual risks (documented, revisit triggers noted)

- **Default-open rules** (`no rules ⇒ allow`): v1-parity product semantics.
  CEL eval errors themselves fail closed.
- **Wildcard WS origins** default: safe today (tokens ride frames, not
  cookies); set `INSTANT_V2_WS_ALLOWED_ORIGINS` before cookie auth ships.
- **Per-process limits** (rate limiter, previously lockout): rate budgets are
  still per-node by design; the auth-critical lockout is now shared.
- **No storage quotas**: any app admin can fill the file volume; multi-node
  quota tracking deferred to Phase 6 (shared-state work).
- `email_verified` absent (vs explicitly false) is still treated as verified
  for account synthesis — needs provider-matrix research before tightening.
- Global server Read/WriteTimeout intentionally unset (breaks WS/SSE
  hijacked streams); covered instead by header timeout, frame/body caps,
  WS keepalive, and SSE write deadlines.

## Evidence

Every behavioral fix above carries a pinned regression test proven red
against the pre-fix tree where reproducible (H1, H2, H3, H4-keying,
H5-init-guard, M1, M2, M4, M6); the remainder are guarded by tests written
against the fixed behavior plus line-cited inspection. Full ring at close:
`go vet ./...`, golangci-lint v2.13.1 (0 issues), `go test ./... -race
-count=1 -short -p 1` — all packages green (waltail requires logical
replication locally; enforced in CI).
