# 05 — Cross-verification Against V1

A v2 is only worth more than a tuned Clojure fork if it is **indistinguishable from v1
to the published SDKs**. This document defines the conformance harness that proves that
indistinguishability at every phase, and how it is distributed across sub-agents.

## 1. Principle

V1 is the oracle. It stays runnable (self-hosting compose at
`../instant/self-hosting/docker-compose.local.yml`). V2 is judged against it, not against
a spec. The spec (`protocol.schema.json`) exists to generate types and to keep agents
from inventing drift — not to substitute for behavioral conformance.

## Current coverage (2026-08-28)

The conformance apparatus is present, but the planned corpus is not yet at its
final breadth. The checked-in corpus currently contains the smoke and
transact/refresh scenarios (`corpus/00-smoke.ndjson` and
`corpus/01-transact-refresh.ndjson`). Package-level tests additionally cover
protocol canonicalization, query behavior, permissions, required attributes,
backups, WS/SSE flows, rooms, delta/incremental refresh, and live pgoutput.

| Dimension | Current status | Limitation |
|---|---|---|
| Protocol/canonicalization | Implemented | Corpus remains small. |
| Auth/OAuth | Focused tests | Injected-provider/direct id-token, JWKS, and Apple signer paths are tested; builtin auth-code provider configuration, nonce handling, and Apple end-to-end exchange are deferred. |
| Queries/transactions/perms | Focused and live-DB tests | Full v1 scenario matrix and JS optimistic-evaluation harness are not checked in. |
| WAL/invalidation | Live PG17 pgoutput test plus direct-notify/bus tests | Production assembly uses post-commit notification; tailer-driven ack ordering is not the running path. |
| Presence/rooms | In-process WS tests | No cross-node ephemeral-state bus; admin presence endpoint currently returns `{}`. |
| Storage/backups | Real-DB and S3/fake-store tests | Broad SDK HTTP corpus remains open. |
| Performance | Wave 5 hardened harness plus named smoke/soak and microbenchmarks | The accepted contract is implemented with deterministic paired artifacts and offline replay; Wave 6 paired live V1/V2 measurement is still required before comparative claims. |

The tables below describe the target gate and historical apparatus; they do
not imply that every listed fixture already exists.

## 2. Apparatus

### 2.1 Golden corpus

A directory of deterministic scenarios, each an NDJSON file of ordered JSON frames:

```
corpus/
  01-auth-magic-code.ndjson
  01-auth-refresh-token.ndjson
  01-auth-jwks-rs256.ndjson
  02-perms-allow-view.ndjson
  02-perms-deny-write.ndjson
  03-query-simple.ndjson
  03-query-nested-where-order-limit.ndjson
  03-query-pagination-cursors.ndjson
  04-transact-lookup-ref.ndjson
  04-transact-cascade-delete.ndjson
  05-presence-rooms.ndjson
  05-sync-table.ndjson
  ...
```

The checked-in scenario format is a sequence of `meta`, `c2s`, and `s2c`
records. `meta.seedFixture` is descriptive metadata; the current loader does
not seed apps/attrs/triples/rules or execute REST calls. The current files
contain WebSocket operations and expected reply envelopes. Fixture seeding,
SDK capture, and HTTP scenario recording remain future corpus work.

### 2.2 `corpusctl` tool (`tools/corpusctl`)

The current binary is a WebSocket corpus replayer/differencer, not an SDK proxy:

- **`replay`** takes `--target ws://...` and sends the checked-in scenario's
  client-to-server frames to that endpoint, then canonicalizes and compares
  replies with the scenario's expected frames.
- **`differential`** takes `--target ws://...` and `--other ws://...`, replays
  each scenario against both endpoints, and compares their collected frames.
- **`record` is future work**. The current command exits with a diagnostic that
  a live v1 checkout and SDK proxy are required; it does not capture traffic.

`--v1-path` and `--v1-ref` are advisory flags retained for the future record
workflow and are ignored by current replay mode. Current tests use fake
servers and live service tests use explicitly supplied database/server
endpoints; CI does not run `corpusctl`, clone v1, or boot a v1 testcontainer.

### 2.3 Coverage dimensions (each ≥ 1 scenario before its phase gate)

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
| 0 | Fixture replay apparatus | `corpusctl replay --target ws://...` is implemented; live-v1 recording/replay is future work |
| 1 | Storage parity | Unit SQL fixtures replayed against fresh PG; valuecodec fuzz vs v1 impl table |
| 2 | Transactor + CEL | Corpus transact+perms scenarios through the packages in isolation |
| 3 | Query engine | Corpus queries through `datalog+instaql`; JS `instaql.ts` harness on same fixtures |
| 4 | Reactive sync | Full-session corpus replay end-to-end (v2 as live server) |
| 5 | Platform APIs | HTTP golden files (`admin`/`runtime`/`storage` routes) |
| 6 | Hardening | Soak + chaos + differential soak (v1 vs v2 side-by-side under load) |

Gates in `04-roadmap.md` each reference these runs by name. Per-row CI jobs
are a planned gate; the current CI/test setup does not invoke `corpusctl`,
clone v1, or start a v1 service.

## 4. Reuse of v1 as a verification artifact in-repo

The new repo vendors **no v1 code**, but references it explicitly:

- `V1_REF` records the intended comparison commit (`a4d2ef33`, override via
  future workflow configuration). It is documentation for the planned v1
  differential apparatus; CI does not clone v1 or build truth tables from it.
- `V1_PATH` is an advisory path retained for the future record workflow.
  Current `corpusctl replay` does not discover or boot v1; it requires an
  explicit WebSocket URL via `--target`.

## 5. Canonicalization rules (the diff is byte-stable)

- JSON: `jq -S .` key sort + `encoding/json` normalization on both sides.
- Envelopes: strip machine-local fields (`isn` order is asserted relatively, not absolutely;
  `value_md5` is asserted exactly; timestamps compared within 1s window where present).
- Cursors: opaque strings asserted by re-use round-trip (submit → get page-info → request
  next page with returned cursor → stable result), not string equality.
- Spelling: every `op` name and every `attrTypes.ts` field (hyphens, `?` suffixes) asserted
  literally — the corpus is how we defend against accidental renaming.

## 6. Failure triage protocol

When `replay --target v2` reports a delta:

1. Bisect by dimension (query vs transact vs perms) using tagged scenario suites.
2. Run `differential` against v1 on the failing scenario; the diff output names the exact
   envelope field that diverged.
3. Fix in the owning package (see `06-agent-orchestration.md` ownership map); **no
   cross-package fix** without an ADR.
4. Before closing the incident, add a minimal regression scenario to `corpus/`.
