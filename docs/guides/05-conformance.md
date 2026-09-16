# 05 — Cross-verification Against V1

Behavioral compatibility with v1 is the target. This document distinguishes the
current regression gates from the external evidence still needed to establish
SDK-level conformance.

## 1. Principle

Pinned v1 behavior is the comparison oracle, but its runtime must be provisioned
and verified before a differential run. The local checkout includes
`../instant/self-hosting/docker-compose.local.yml`; its presence is not evidence
that v1 is running. Authored regression expectations and the protocol schema do
not substitute for an actual v1 comparison.

## Current coverage (2026-08-31)

The manifest contains **16 authored v2 regression scenarios and zero recorded v1
oracles**. The original smoke and transact/refresh paths remain stable. The real
v2 replay test creates a private database and seeds each scenario through the
platform migrations and transactor. Package-level tests additionally cover
protocol canonicalization, query behavior, permissions, required attributes,
backups, WS/SSE flows, rooms, delta/incremental refresh, and live pgoutput.

| Dimension | Current status | Limitation |
|---|---|---|
| Protocol/canonicalization | Validated manifest and real v2 WS replay | Narrow implemented-surface coverage, not full v1 parity. |
| Auth/OAuth | Focused and live-DB local-provider tests | Google/GitHub authorization-code lifecycle, PKCE, nonce/JWKS, replay, expiry, and startup configuration are covered locally; real-provider acceptance is absent, and direct ID-token plus Apple/custom flows are unsupported. |
| Queries/transactions/perms | Focused and live-DB tests | Full v1 scenario matrix and JS optimistic-evaluation harness are not checked in. |
| WAL/invalidation | Live PG17 pgoutput test plus direct-notify/bus tests | Production assembly uses post-commit notification; tailer-driven ack ordering is not the running path. |
| Presence/rooms | In-process WS tests | No cross-node ephemeral-state bus; admin presence endpoint currently returns `{}`. |
| Storage/backups | Real-DB and S3/fake-store tests | Broad SDK HTTP corpus remains open. |
| Performance | Wave 5 hardened harness plus named smoke/soak and microbenchmarks | The accepted contract is implemented with deterministic paired artifacts and offline replay; Wave 6 paired live V1/V2 measurement is still required before comparative claims. |

The [corpus inventory](../../corpus/README.md) records exact coverage, normalization,
and opt-in failing reproductions for cursor advancement and aliased forward
relations. The target matrices below do not imply that every listed fixture exists.

## 2. Apparatus

### 2.1 Golden corpus

A manifest indexes deterministic NDJSON scenarios and their fixture profiles:

```
corpus/
  manifest.json
  fixtures/
  00-smoke.ndjson
  01-transact-refresh.ndjson
  02-subscription-lifecycle.ndjson
  ...                         # 16 default scenarios; see manifest.json
```

The checked-in scenario format is a sequence of `meta`, `c2s`, and `s2c`
records. Loading and validation do not perform I/O against a server.
`TestCorpusReplayIntegration` consumes the declared fixture profiles, creates
isolated databases, seeds app/attrs/triples/rules, and drives the real WS handler.
The external CLI does not seed remote endpoints. SDK capture and HTTP scenario
recording remain unimplemented.

### 2.2 `corpusctl` tool (`cmd/corpusctl`)

The current binary validates and replays WebSocket scenarios, not SDK proxy traffic:

- **`validate`** checks the complete inventory offline; it is not replay evidence.
- **`replay`** takes `--target ws://...` and sends the checked-in scenario's
  client-to-server frames to that endpoint, then canonicalizes and compares
  replies with the scenario's expected frames.
- **`differential`** takes `--target ws://...`, `--other ws://...`, and a fresh
  private `--output-dir`. It checks the full manifest v1 pin against the local
  checkout and retains raw/normalized outputs, errors, and revision evidence.
- **`record` is future work**. The current command exits with a diagnostic that
  a live v1 checkout and SDK proxy are required; it does not capture traffic.

`make test-contract` runs offline validation, package contracts, and the actual
isolated v2 replay with `DATABASE_URL` set. The CI workflow invokes that same
target. It does not provision v1 or establish v1 parity. External replay and
differential require equivalent isolated fixtures initialized before each
scenario; local checkout verification does not prove a remote endpoint's revision.

### 2.3 Target coverage dimensions (not a current completion claim)

| Dimension | Representative scenarios |
|---|---|
| Auth | magic-code, guest sign-in, opaque refresh-token lifecycle, and planned OAuth provider/Apple nonce scenarios; current direct/injected id-token tests and Apple signer tests do not constitute builtin auth-code exchange |
| Permissions | each fallback-chain branch, field-level denials, `bind` cycle detection, rate-limit bucket |
| Queries | every InstaQL option, operator, petal combination; `$files`; admin-only `aggregate:count` |
| Transactions | every tx-step op, lookup-ref variants, `create`/`update` modes, missing-attr synthesis |
| Pagination | cursors, inclusive cursors, `before`/`after` stability across savepoints |
| Optimistic updates | client `instaql.ts` local evaluation must match server envelope for same fixtures |
| WAL / invalidation | concurrent transacts invalidating overlapping topics — every subscribed session gets exactly one `refresh-ok` with monotonic watermarks; production currently uses post-commit notify, with pgoutput verified independently |
| Presence/rooms | full-snapshot broadcast fan-out; `patch-presence` remains a documented optimization deviation and cross-node fan-out is unsupported |
| Storage | signed upload/download URLs, file→triple linkage |
| Feature gates | `skip-attrs`/`patch-presence`/`batch-messages` negotiated and obeyed |
| Errors | `error{status,type,message,hint}` shapes preserved |

## 3. How it plugs into the roadmap

| Phase | What is verified | How |
|---|---|---|
| 0 | Fixture replay apparatus | Offline validation, isolated v2 replay, and explicit external replay/differential are implemented; recording and v1 bootstrap remain open |
| 1 | Storage parity | Unit SQL fixtures replayed against fresh PG; valuecodec fuzz vs v1 impl table |
| 2 | Transactor + CEL | Corpus transact+perms scenarios through the packages in isolation |
| 3 | Query engine | Corpus queries through `datalog+instaql`; JS `instaql.ts` harness on same fixtures |
| 4 | Reactive sync | Full-session corpus replay end-to-end (v2 as live server) |
| 5 | Platform APIs | HTTP golden files (`admin`/`runtime`/`storage` routes) |
| 6 | Hardening | Soak + chaos + differential soak (v1 vs v2 side-by-side under load) |

The [roadmap](../plans/04-roadmap.md) describes these target phase gates. Current
CI runs the Make unit, integration, contract, container and existing soak lanes;
the matrix above is not a claim that every target scenario or v1 run is present.

## 4. Reuse of v1 as a verification artifact in-repo

The new repo vendors **no v1 code**, but references it explicitly:

- `V1_REF` defaults to `a4d2ef33`; the corpus manifest pins the full commit
  `a4d2ef33b60f281a437191006e4541d4780f9e4a`. Differential mode checks that pin
  against the checkout at `V1_PATH` (default `../instant`).
- Replay still requires an explicit WebSocket URL. Neither replay nor differential
  discovers, boots, or seeds external servers. CI does not clone or start v1.

## 5. Canonicalization rules (the diff is byte-stable)

The manifest names the existing `canonical-v1` policy. Keys sort, UUID-shaped
string values lowercase, array order stays intact, and exact decimal numeric
comparison preserves distinct large integers. Both modes mask transaction IDs
and string session IDs; differential mode has additional explicit exclusions.
See the [complete policy and its blind spots](../../corpus/README.md#canonicalization-policy)
before interpreting equality. Arbitrary timestamps and session IDs used as object
keys are not masked. Cursor reuse is an opt-in known failing reproduction, not a
passing conformance claim.

## 6. Failure triage protocol

When `make test-contract` or `make replay TARGET="$V2_URL" SUITE=...` reports a delta:

1. Bisect by dimension (query vs transact vs perms) using tagged scenario suites.
2. If pinned v1 and equivalent isolated fixtures are available, run
   `make differential V1_URL="$V1_URL" V2_URL="$V2_URL" DIFFERENTIAL_OUTPUT="$EVIDENCE_DIR" SUITE=...`.
   Preserve the raw/normalized evidence; an unavailable comparison is not a pass.
3. Fix in the owning package (see [ownership map](06-agent-orchestration.md)); **no
   cross-package fix** without an ADR.
4. Before closing the incident, add a minimal regression scenario to `corpus/`.
