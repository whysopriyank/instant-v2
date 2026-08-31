# Corpus contracts and evidence

`manifest.json` is the deterministic WS scenario, fixture, owner, normalization,
and coverage inventory. The 18 default scenarios are **authored v2 regression
expectations**, not recorded v1 oracles. The two original scenario paths remain
stable. Scenarios are still NDJSON `meta`/`c2s`/`s2c` records; loading is recursive
and sorted. Every scenario must start with one named meta record and contain
both client inputs and expected server outputs. A receive step is a barrier
before the following send step. Grouped-send files retain their grouped order.

## Gates

From the repository root:

```sh
go run ./cmd/corpusctl --mode validate --corpus corpus
go test ./internal/corpus ./cmd/corpusctl -race -count=1
INSTANT_TEST_INTEGRATION=1 go test ./internal/corpus -run '^TestCorpusReplayIntegration$' -race -count=1 -v
```

The last command requires `DATABASE_URL` for a PostgreSQL role with CREATEDB.
It creates a private database **for every scenario**, applies the actual platform
migrations, creates the fixture app, executes `fixtures/*.json` `txSteps` through
the real transactor, installs declared rules/admin tokens, and drives the real
WS handler, query executor, permission gates and reactive notifier. It does not
reset shared `public` schemas. Missing integration prerequisites fail; unit mode
skips the live lane explicitly. Offline validation is not replay evidence.

The fixture UUIDs/admin token are deterministic local test data, not credentials
for a real service. `relations.json` seeds the forward-relation regression.

## Coverage

| Scenario | Exact exercised contract |
|---|---|
| 00 | Anonymous init attrs/auth/status and empty query |
| 01 | In-band add-attr, triple commit and full subscribed refresh |
| 02 | Duplicate query, remove, re-add |
| 03 | Pre-init, missing fields and malformed tx-step container errors |
| 04 | Equality filter selects the matching seeded entity |
| 05 | Seeded view denial and rejected update |
| 06 | Room input/membership errors, empty presence resync and leave |
| 07 | Re-init releases the old query registration |
| 08 | Add, read, unsubscribe, retract, empty read |
| 09 | Old SDK init still includes attrs |
| 10 | Multi-step failure leaves no partial triple |
| 11 | Two limit/offset pages with exact cursor and boundary metadata |
| 12 | Admin-authenticated aggregate count of two seeded records |
| 14 | Unique lookup changes the original entity |
| 15 | Required-field retraction fails and preserves original data |
| 16 | `9007199254740993` survives transaction, storage and query |
| 17 | Reusing an emitted cursor advances to the second entity |
| 18 | Aliased cardinality-one forward relation retains reference/child projection |

The manifest records 22 narrowly covered surfaces, seven gaps and two unsupported
surfaces, with zero v1 captures. Remaining gaps include the full
cardinality/merge/cascade matrix, dynamic permission
bindings/fallbacks, token auth, multi-client room fanout, delta refresh, SSE and
HTTP/SDK capture. Sync/stream acknowledgement placeholders and cross-node rooms
are explicitly unsupported, not asserted as implemented parity.

## Promoted query regressions

The former `after-cursor` and `forward-relation` known-gap cases now run as
scenarios 17 and 18 in the normal integration replay. Their client inputs and
expected server outputs are unchanged; only paths and suite metadata changed.
The old opt-in runner was removed, so default integration runs cannot silently
skip these repaired contracts. Historical red evidence remains in the older
quality reports; fresh execution is recorded in
[implementation pass 1](../docs/plans/implementation-pass-1.md).

These two narrow fixtures do not establish general query or v1 parity.

## Canonicalization policy

`canonical-v1` names the existing comparison policy, not a v1-conformance claim.
Traversal is independent of field policy. Object keys sort, UUID-shaped string
values lowercase, and array order is retained. Numbers use exact decimal semantic
equality: `1`, `1.0`, `1e0` are equal; distinct large integers and fractional digits
remain distinct; signed zero is equivalent to zero. Huge exponents are normalized
symbolically without allocating their expanded decimal representation.

Both modes mask `tx-id`, `processed-tx-id`, and string-valued `session-id`.
Differential mode additionally retains the pre-existing exclusions:

- Replace present `attrs` with `<attrs>`; presence versus absence still differs.
- Remove `processed-isn`, `isn`, `trace-id`, `server-hostname`, `server-port`.
- Project `auth.app` to `id`, change null `auth.admin?` to false, and null
  `result-meta` to an empty object.

No new ignored fields were added. These historical key-based rules apply
recursively, so application payload fields with those same names can be masked.
That is a known comparison blind spot, not proof of equality for those fields.
Session IDs in object keys and arbitrary timestamps are **not** masked. The
replayer compares exactly the declared frame count and does not prove absence
of later unsolicited frames after its final expected step.

## External replay and pinned-v1 differential

```sh
go run ./cmd/corpusctl --mode replay --corpus corpus/00-smoke.ndjson --target "$V2_URL" --output-dir "$EVIDENCE_DIR"
go run ./cmd/corpusctl --mode differential --corpus corpus/00-smoke.ndjson --target "$V2_URL" --other "$V1_URL" --v1-path ../instant --output-dir "$EVIDENCE_DIR"
```

Provision and reset equivalent **isolated** fixtures on each external server
before each scenario. The CLI does not seed or reset external systems; use one
file at a time when fixtures differ or a scenario mutates state. The local Go
fixture bootstrap above is v2-only, not a v1 bootstrap implementation.

Differential mode requires a full manifest v1 pin matching the checkout HEAD and
`--output-dir`; it fails on either transport/decode failure, even when both
servers failed identically. Zero selected scenarios is an error. `record` remains
unimplemented and fails explicitly.

Evidence files are write-once, mode 0600, and retain raw received text, normalized
frames, errors, delta, fixture ID, local v1/v2 revisions and v2 dirty state. Raw
frames may contain auth/application information: keep the directory private and
inspect/redact before sharing. Local revision checks do **not** establish which
revision a remote endpoint is running. A designated comparison run must also
record endpoint deployment/configuration, equivalent fixture initialization,
and accepted-difference rationale externally. Captures are never automatically
promoted to oracle entries. `v1-capture` metadata requires a full matching ref,
a nonempty checked-in raw evidence file, and exact canonical agreement with the
scenario's expected outputs; metadata cannot prove where a capture originated.

### Local readiness inspected 2026-08-31

- `../instant` was clean at `a4d2ef33b60f281a437191006e4541d4780f9e4a`, matching
  the abbreviated root `V1_REF` and the full corpus manifest pin.
- Java 26.0.2.1, Clojure CLI 1.12.5.1664 and `migrate` were available. Docker was
  not on PATH; the compose v1 service at `127.0.0.1:8888` was not listening.
- The pinned local compose file requires its PG17/pg_hint_plan image with
  logical WAL, MinIO/bucket setup and server config. The pinned Dockerfile uses
  Java 26, builds the standalone jar, runs `tasks/bootstrap-for-oss` (including
  migrations/config), then starts `instant.core`.
- No v1 service, equivalent v1 fixture bootstrap or designated v1/v2 differential
  run was executed. Dependency/JAR build readiness was not established. These
  remain external prerequisites, so full v1 parity remains unverified.
