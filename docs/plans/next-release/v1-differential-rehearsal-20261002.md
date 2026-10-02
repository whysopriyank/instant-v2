# CF-004 / CF-005 actual v1 differential rehearsal — 2026-10-02

This is failed runtime rehearsal evidence, not public-alpha acceptance. The
original 18-scenario run produced **2 agreements, 14 wire/replay failures and
2 physical fixture blocks**. A separately retained common-index prerequisite
rerun completed those two blocked comparisons: both failed. Across the two
attempts all 18 selected scenarios have genuine runtime observations: 2 agree
and 16 fail. No PASS qualification record or final-candidate facts were issued.
CF-004's selected HTTP comparison remains coordinator-owned and is not proved
by these WS observations.

## Immutable inputs and scope

| Input | Actual rehearsal identity |
|---|---|
| Clean candidate source | `4a2e0fcc6a0c239c71d1bc72fe4653c96ed52996` |
| Mounted candidate executable SHA256 | `bcd6ccb860e134107d55f6d96f0c16ef6a4434393d47a20c119c33f4faf04130` |
| Toolchain | Go 1.25.14; CGO disabled |
| Pinned public v1 checkout | `a4d2ef33b60f281a437191006e4541d4780f9e4a` |
| Actual v1 image identity | `ghcr.io/instantdb/server@sha256:3f3488b753c739f8ae3c35fa9c2b5848f16687e8687d9f50fbd01dd789403fac` |
| Corpus manifest SHA256 | `a5fbd18166d683a3ce3f66e338ea8780388c1f4dbca36273bf9662fbbe11afd4` |
| Original complete attempt | `2026-10-02T15:30:51.169946Z`–`15:34:08.245090Z` |
| Common-index rerun | `2026-10-02T15:38:37.733193Z`–`15:38:58.569088Z` |
| Runtime configuration | **Rehearsal unbound**; retained bootstrap SHA256 `79a85f8fd3b208fa3aa743ce7d8cfcce9acca10a47059a448c06d6cf2efac78e` specifies actual generated fixture overrides |

The coordinator's common configuration template identity
`aeb6a02000d8ac779f86a600f5b7009379cb1893237fb31189a93959313428a2`
is not proof that this independently provisioned rehearsal consumed the final
campaign configuration. Likewise, the subsequently staged distribution image
was not this lane's mounted executable runtime. This lane must be rerun against
the final frozen identity/configuration before release qualification.

The worker's runtime resources are exclusively
`iv2q-diff-pa-20261002-{pg,server,net}` on bigbeast; HTTP binds loopback 55482
and PostgreSQL loopback 55483. Candidate is the corpusctl `target`; pinned v1
at loopback 55472 is `other`. Only reserved app
`22222222-2222-4222-8222-222222222222` and generated creator
`11111111-1111-4111-8111-111111111111` were reset. The shared reference stack
was never restarted/stopped and the separately owned restore source app was
not modified. Cleanup is tracked separately; retaining this report does not
assert resources have been removed.

## Fixture and comparison contract

Both engines receive the same candidate fixture through real admin WS init
(`__admin-token`) and transact, then rules are installed, matching the package
bootstrap order. Omitted `required?` is made explicitly false on both sides;
the transformed actual seed input and emitted seed replies are retained.
Schema, values, rules and physical index flags are compared before each replay.
Server-generated `created_at` is retained in full SQL snapshots but is excluded
from this prerequisite equivalence check; no fixed timestamp equality claim is
made. See [query timestamp observations](v1-compatibility-observations.md).

Original fixtures 14/15 supplied unique attributes with `index? false`:
v1 retained false/`ave=false`; v2 forced true/`ave=true`. They originally stopped
before wire replay. The separate rerun supplies explicit `index? true` for
unique attributes on **both** engines and retains the transformed inputs and
equivalence checks. Replay scenario bytes remain unchanged.

`corpusctl --mode differential` from the clean candidate performs real replays
and compares both collected streams using the existing `canonical-v1` policy.
Authored expected frames supply scheduling/frame-count requirements; they are
not a captured v1 oracle. No additional masking or normalization was introduced.
Nested volatile error metadata remains visible in the retained deltas.

## Complete selected inventory

Frame counts are candidate/v1. A completed frame inventory with a nonempty
delta is a failure. This table reports observed differences and does not by
itself decide which engine's behavior should become the accepted product contract.

| Scenario | Frames | Actual result / principal difference |
|---|---:|---|
| 00-smoke | 2/2 | Agreement under existing canonical policy |
| 01-transact-refresh | 4/3 | v1 rejects the replay's conflicting forward identity as `record-not-unique`; no fourth refresh frame arrives within 12s. Candidate accepts and refreshes. The selected smoke fixture already declared this attribute; package bootstrap also loads it. |
| 02-subscription-lifecycle | 5/5 | Candidate duplicate/removal acknowledgments omit v1's `q`; removal omits explicit null event ID. A subsequent narrow source fix was independently reviewed, but is outside this immutable rehearsal. |
| 03-protocol-errors | 6/6 | Error status/type/message/hint/original-event differ; v1 accepts `{}` tx-steps as an empty transaction while candidate rejects. |
| 04-query-filter | 2/2 | Candidate emits 3-element triples; v1 includes fourth `created_at` element. |
| 05-permission-deny | 3/3 | Candidate `403/transact-error`; v1 `400/permission-denied` with hint/original-event. |
| 06-room-boundaries | 6/6 | Room error envelopes differ; absent presence is `{}` versus null; leave acknowledgment event ID differs. |
| 07-reinitialize | 4/4 | Candidate accepts repeated init/reset; v1 rejects repeated init and preserves the subscription. |
| 08-transact-retract | 6/6 | Null event ID/acknowledgment differences; candidate enumerates title-only entity, v1 root query returns empty. |
| 09-old-sdk-init | 1/1 | Agreement under existing canonical policy |
| 10-transact-rollback | 3/3 | Unknown-attribute error envelope differs (`403/transact-error` versus `400/sql-raise`); final empty query agrees. |
| 11-query-pagination | 3/3 | Candidate returns title-only records/pages; v1 root query is empty for this fixture. |
| 12-query-aggregate | 2/2 | Candidate returns count 2/title records; v1 returns empty result for this fixture. |
| 14-transact-lookup | 3/3 rerun | Original physical seed block; common-index rerun differs on null event ID and title-only root query. |
| 15-transact-required | 3/3 rerun | Original physical seed block; common-index rerun differs on error envelope and fourth timestamp element; original required values remain returned. |
| 16-exact-number | 3/3 | Null event ID differs; candidate returns number entity, v1 root query is empty for title-only schema. This does not prove v1 numeric corruption. |
| 17-after-cursor | 4/4 | Root pagination differs; v1 rejects serialized string cursor, expecting a join-row vector. |
| 18-forward-relation | 2/2 | Candidate returns title/name/ref records; v1 root query is empty for this fixture. |

The title-only/root-query observations require a fixture/contract decision:
most authored fixtures lack an `id` attribute/triple, while the required fixture
has one and v1 enumerates it. This is not authorization to silently add IDs,
alter replay inputs, mask application rows or label these failures as agreements.

## Private retained evidence

Base directory on bigbeast:
`/home/bigbeast/iv2q-public-alpha-20261002/differential/evidence`.
It retains exact candidate `manifest.json`, fixture files, `V1_REF`, build/binary
identity, generated-owned runtime inspection and v1 image identity. Raw wire
files contain only this generated fixture's credentials/data; they remain
private runtime artifacts rather than promoted public goldens.

- `attempt3/results.json`, `provenance.json`, producer `seed.go`, `run.py`,
  `bootstrap.sh`, per-scenario `*-seed-{v1,v2}.ndjson`, transformed
  `*.seed-input.json`, full `*.sql-state.json`, `*-fixture-equivalence.json`,
  actual differential logs and `raw/<scenario>.ndjson.evidence.json`.
- `attempt3/artifact-sha256.json` SHA256
  `88d51340f6ab975b6bad427f4f31b58bafcb176ceb5019bb264f45d3178970a3`.
- `attempt4-index-prerequisite/` retains corresponding actual outputs for
  14/15 plus `seed.go` and `run-extra.py`; its `artifact-sha256.json` SHA256
  is `a2cc3fe6d6146059d050c4f22a825d8cbf63b1ed229883eddbf2fc60a807d496`.
- 04's raw artifact SHA256:
  `2d7e82b0aa468651eb7c09042ce09d74bee138c6e3c0856c9f6e3147e45e7401`.
- Initial fixture setup failures and `attempt2/`'s invalid positional CLI
  invocation are preserved. They are producer failures, not runtime parity
  verdicts, and are not counted as additional selected scenario results.

The public qualifier currently requires every selected scenario, complete
streams, empty deltas and bound final identities. These observations do not
satisfy that contract. Resolving the selected compatibility contract and
rerunning a final immutable candidate remain required; no comparative
performance claim follows from this correctness rehearsal.
