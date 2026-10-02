# Pinned-v1 compatibility observations — 2026-10-02

Status: observed discrepancies, not approved exceptions or passing CF-005
evidence. This note does not change the release selection or golden captures.

## Query tuple timestamps

Candidate `4a2e0fcc6a0c239c71d1bc72fe4653c96ed52996` was compared with
official v1 source `a4d2ef33b60f281a437191006e4541d4780f9e4a`, image
`sha256:3f3488b753c739f8ae3c35fa9c2b5848f16687e8687d9f50fbd01dd789403fac`.
The live `04-query-filter` capture has complete two-frame streams, but its
returned title triple differs: v1 sends `[entity, attr, "beta", 1790955108122]`;
the candidate sends `[entity, attr, "beta"]`. Raw evidence is retained on the
owned qualification host at
`/home/bigbeast/iv2q-public-alpha-20261002/differential/evidence/attempt3/raw/04-query-filter.ndjson.evidence.json`.
Artifact SHA-256:
`2d7e82b0aa468651eb7c09042ce09d74bee138c6e3c0856c9f6e3147e45e7401`.
Candidate binary SHA-256:
`bcd6ccb860e134107d55f6d96f0c16ef6a4434393d47a20c119c33f4faf04130`.
The fixtures' timestamps were not fixed equal across engines, so this capture
proves the missing field; it cannot prove equality of timestamp values.
The producer retained seed bodies and full SQL rows including `created_at`;
its equivalence check covers logical attributes, triple values/index flags
and rules while excluding server-generated timestamps.

The pinned upstream `server/src/instant/db/datalog.clj` maps tuple component 3
to `:created-at`; `reactive/query.clj:76` collects the original triples into one
deduplicated node. The SDK's `model/instaqlResult.js` preserves entire tuples;
`store.ts:421` reads element 3. `instaql.ts:630` compares an ID ordering cursor
using its timestamp, and `SyncTable.ts:168` reads the ID triple's element 3 as
`serverCreatedAt`. These source consumers establish a real timestamp contract,
not an ignorable wire annotation. No SDK execution was performed for this note.

In the candidate, `internal/instaql/query.go` already fetches each triple's
`created_at`, but retains only the ID triple's timestamp for server-side
pagination. `internal/sync/nodelist.go` reconstructs three-element triples
from entity JSON. Both initial and shared refreshes use this projection.
Server-side ordering tests therefore do not establish client-side ordering
or pagination compatibility. A general frozen-SDK, infinite-query,
`serverCreatedAt` ordering/cursor, or four-tuple parity claim is unqualified;
this observation does not imply that ordinary unordered entity queries fail.

The minimum correct future repair must preserve actual stored tuple timestamps
through the executor and both reactive serializers to the node-list projection,
including ID and reference triples, permission filtering and nested results.
It must also preserve them in incremental refreshes or explicitly use the
existing full-refresh fallback: `internal/reactive/incremental.go:339` currently
rejects envelopes containing extra fields. A constant timestamp or current wall
clock would corrupt the contract. Verify with identical fixed timestamps on
both engines, complete raw streams and an SDK ordering/cursor check. No product
change or golden rewrite was made during this investigation.

## Fixture prerequisites and query cursors

The differential producer also observed that posts/admin/relation fixtures
have values and references but no corresponding ID attributes/triples. V1
match-all queries for scenarios 08, 11, 12, 16 and 18 consequently return empty
results while the candidate enumerates entities from other attributes. An
explicit title equality query can still return a v1 triple. Record this as a
fixture-prerequisite discrepancy until the intended scenario contract is
confirmed; adding IDs to only one engine or rewriting golden results would not
establish parity.

Scenario 17 sends a serialized cursor string accepted by the candidate; the
pinned v1 path expects a join-row vector and rejects that input. Cursor input
compatibility remains unqualified. These observations require an owner
decision on the supported comparison scope before any exclusion is accepted
or broader query behavior is changed.
