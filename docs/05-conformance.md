# 05 — Cross-verification Against V1

A v2 is only worth more than a tuned Clojure fork if it is **indistinguishable from v1
to the published SDKs**. This document defines the conformance harness that proves that
indistinguishability at every phase, and how it is distributed across sub-agents.

## 1. Principle

V1 is the oracle. It stays runnable (self-hosting compose at
`../instant/self-hosting/docker-compose.local.yml`). V2 is judged against it, not against
a spec. The spec (`protocol.schema.json`) exists to(generate types and to keep agents
from inventing drift — not to substitute for behavioral conformance.

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

Each scenario encodes: seed fixtures (apps/attrs/triples/rules) + a sequence of client
operations (WS ops, REST calls) + expected reply envelopes. Fixtures are ported from
`server/test` and from `examples/*`.

### 2.2 `corpusctl` tool (`tools/corpusctl`)

Two modes, same binary:

- **record**: sits as a proxy between a stock `@instantdb/core` (pinned SDK versions,
  including pre-0.17.5 and pre-0.20.4 for gate-matrix testing) and a live v1 server.
  Captures every frame bidirectionally, normalizes timing, and emits/updates a scenario file.
- **replay --target {v1|v2|differential}**: drives the same client SDK against a target,
  captures replies, and diffs them against the corpus using **canonical JSON**
  (sorted keys, normalized number formatting, UUID hex-lowercased). `differential` mode
  runs the same scenario simultaneously against v1 and v2 and diffs the two outputs
  without an on-disk golden.

Both modes run deterministically under `go test` via `testcontainers-go` Postgres instances
and in-process `instantd` servers.

### 2.3 Coverage dimensions (each ≥ 1 scenario before its phase gate)

| Dimension | Representative scenarios |
|---|---|
| Auth | magic-code, guest sign-in, opaque refresh-token lifecycle, OAuth OIDC + Apple nonce quirks |
| Permissions | each fallback-chain branch, field-level denials, `bind` cycle detection, rate-limit bucket |
| Queries | every InstaQL option, operator, petal combination; `$files`; admin-only `aggregate:count` |
| Transactions | every tx-step op, lookup-ref variants, `create`/`update` modes, missing-attr synthesis |
| Pagination | cursors, inclusive cursors, `before`/`after` stability across savepoints |
| Optimistic updates | client `instaql.ts` local evaluation must match server envelope for same fixtures |
| WAL / invalidation | concurrent transacts invalidating overlapping topics — every subscribed session gets exactly one `refresh-ok` with monotonic isns |
| Presence/rooms | `patch-presence` vs full, broadcast fan-out |
| Storage | signed upload/download URLs, file→triple linkage |
| Feature gates | `skip-attrs`/`patch-presence`/`batch-messages` negotiated and obeyed |
| Errors | `error{status,type,message,hint}` shapes preserved |

## 3. How it plugs into the roadmap

| Phase | What is verified | How |
|---|---|---|
| 0 | Fixtures deterministic vs v1 | `corpusctl replay --target v1` 100% pass |
| 1 | Storage parity | Unit SQL fixtures replayed against fresh PG; valuecodec fuzz vs v1 impl table |
| 2 | Transactor + CEL | Corpus transact+perms scenarios through the packages in isolation |
| 3 | Query engine | Corpus queries through `datalog+instaql`; JS `instaql.ts` harness on same fixtures |
| 4 | Reactive sync | Full-session corpus replay end-to-end (v2 as live server) |
| 5 | Platform APIs | HTTP golden files (`admin`/`runtime`/`storage` routes) |
| 6 | Hardening | Soak + chaos + differential soak (v1 vs v2 side-by-side under load) |

Gates in `04-roadmap.md` each reference these runs by name. CI enforces them with a job
per row.

## 4. Reuse of v1 as a verification artifact in-repo

The new repo vendors **no v1 code**, but references it explicitly:

- `V1_REF` file pins the commit (`a4d2ef33`, override via env). CI clones v1 at that ref to:
  (a) build value-codec truth tables from `db/model/triple.clj` test utils, and
  (b) drive differential runs where an in-process `instantd` replaces v1's Undertow+Datalog
  while the SDK stays identical.
- `V1_PATH` env var points at the local checkout (default in dev:
  `../instant`). Every `corpusctl replay --target v1` invocation reads this to find the
  comparison server if one is already running; otherwise testcontainers boot a fresh one.

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
