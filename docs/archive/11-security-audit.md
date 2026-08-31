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
- **Per-process limits** (rate limiter): traffic budgets remain per-node by
  design; the auth-critical lockout/resend state is shared via Postgres.
- **Rate-limit keying**: non-UUID `app-id` values fall back to the IP bucket,
  but an attacker can still rotate *valid* self-generated UUIDs to get fresh
  buckets. Closing that fully requires server-side app existence checks
  (cache lookup per request) or IP-only keying — revisit with multi-node
  limiter work. Known-app poisoning of a victim's budget is inherent to
  per-app budgets keyed on public ids.
- **auth_throttle growth**: bounded operationally — rows are pruned at boot
  and hourly (24h idle TTL) and deleted on successful verification — not
  structurally (no FK cascade from user deletion). Acceptable at expected
  scale; revisit if per-app cardinality explodes.
- **No storage quotas**: any app admin can fill the file volume; multi-node
  quota tracking deferred to Phase 6 (shared-state work).
- `email_verified` absent (vs explicitly false) is still treated as verified
  for account synthesis — needs provider-matrix research before tightening.
- Global server Read/WriteTimeout intentionally unset (breaks WS/SSE
  hijacked streams); covered instead by header timeout, frame/body caps,
  WS keepalive, and SSE write deadlines.
- Pre-existing (flagged by adversarial review, predates wave): SSE sessions
  receive `Send` but not `SendRaw` on init, so raw-frame fan-out skips them;
  live push over SSE relies on the snapshot path. Tracked for the SSE
  transport follow-up.

## Adversarial review

The wave passed one fresh-context falsification pass (read-only reviewer
with the ledger and full diff). Verdict was REJECT on first pass; both
blocking items were repaired in this wave: (1) the resend-throttle upsert's
RETURNING semantics silently blocked all resends forever — rewritten as a
data-modifying CTE reading the pre-upsert row through the statement
snapshot, service clock threaded into SQL so tests control time, pinned by
`TestResendAllowedAfterCooldownWithoutVerify`; (2) the error-sanitizer had
not been committed on two hottest WS boundaries plus three authn HTTP 500s —
now applied repo-wide at client-facing 4xx/5xx sites. Reviewer-flagged test
gaps closed: strict ParseBool rejection, tx-step cap, InstaQL depth cap,
value-position uniqueness requirement.

## Evidence

Every behavioral fix above carries a pinned regression test proven red
against the pre-fix tree where reproducible (H1, H2, H3, H4-keying,
H5-init-guard, M1, M2, M4, M6); the remainder are guarded by tests written
against the fixed behavior plus line-cited inspection. Full ring at close:
`go vet ./...`, golangci-lint v2.13.1 (0 issues), `go test ./... -race
-count=1 -short -p 1` — all packages green (waltail requires logical
replication locally; enforced in CI).

## Follow-up audit (2026-08-27)

Independent re-audit of every remediation above plus surfaces outside the
original three tracks (backup/restore zip pipeline, presign math, runtimeapi,
rate-limiter internals, CI/release plumbing, workspace hygiene). Verdict: all
remediated items re-verified in code at their call sites; tree green
(`go vet ./...`, `golangci-lint run`, full `-short -race -p 1` suite under live PG).

### Fixed this wave

| ID | Severity | Finding | Fix |
|----|----------|---------|-----|
| F1 | MED | Transient auth artifacts never expired on disk: TTL enforcement existed only lazily-at-consume, so anonymous `POST /runtime/auth/send_magic_code` traffic (plus abandoned OAuth starts) grew the shared triple store indefinitely per public app-id | `authn.SweepExpiredAuthEntities` deletes dead `$magicCodes` / `$oauthRedirects` / `$oauthCodes` entities past (TTL + 1h grace) via whole-entity CTE deletes across all apps; wired at boot + hourly beside `PruneThrottle`; pinned by `TestSweepExpiredAuthEntities` (expired artifact removed, fresh artifact + `$users`/`$userRefreshTokens` survive) |
| F2 | MED | `ci.yml` inherited default GITHUB_TOKEN scopes (no top-level `permissions:` block) | Workflow-wide `permissions: contents: read` |

### Corrected drift (documentation/comment-only, zero behavior change)

- `storageapi` package header described signed-upload-url minting as
  runtime-open; the implementation has been admin-gated (`requireAdmin`)
  all along — the comment understated the shipped control.
- `oauth.go` header still listed the JWKS id_token path as "deferred";
  `idtoken.go` implements it (RS256/ES256 pinned). Only Apple's
  end-to-end token exchange remains unwired; its .p8 signing material ships.
- `adminapi.handleMagicCode` claimed nil-Mailer codes are "logged by
  authn" — verified false; the no-mailer branch logs app-id/email only.
- The metrics server lacked the `IdleTimeout` this document claims was
  landed in the previous LOW tranche; added (120s), making the claim true.

### New residual / notes

- Trusted-proxy rate-limit keying (behind an L7 proxy every caller shares
  the proxy's IP bucket) is tracked as LOW until multi-node limiter work;
  reading spoofable XFF headers today would be strictly worse than the
  conservative collapse. No action taken.
- Workspace hygiene: `.omo/` agent-session artifacts are gitignored as of
  this wave (they previously sat untracked-but-unignored next to the
  tracked tree).
