# Corpus contracts and evidence

`manifest.json` is the deterministic WS scenario, fixture, owner, normalization,
and coverage inventory. Its `coverage` array is the claim ledger for each
auth/perms/query/refresh/rooms/transaction case, including transport, expected
state, owner, and evidence status. The 18 default scenarios are **authored v2 regression
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

The manifest records 22 narrowly covered surfaces, six gaps and three unsupported
surfaces, with zero v1 captures. The matrix currently has 26 entries: 9 covered,
13 gaps, and 4 unsupported cases. Remaining gaps include the full
cardinality/merge/cascade matrix, token auth, multi-client room fanout, delta
refresh, SSE and HTTP/SDK capture. Dynamic data-dependent view rules are an
explicit DA-004V exclusion: the `ws.permissions.dynamic` surface and
`permissions-ws-dynamic-view-exclusion` row are `unsupported`, and package/DB
tests do not become corpus or release acceptance. Sync/stream acknowledgement
placeholders and cross-node rooms are explicitly unsupported, not asserted as
implemented parity.

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

Both modes mask only frame-root `tx-id`, `processed-tx-id`, and string-valued
`session-id`. Differential mode additionally applies these path-specific rules:

- Replace present frame-root `attrs` with `<attrs>`; presence versus absence still
  differs.
- Remove frame-root `processed-isn`, `isn`, `trace-id`, `server-hostname`, and
  `server-port`.
- Project `auth.app` and `auth.admin?` only when they are direct children of the
  frame-root `auth` object: `auth.app` becomes `{id}` and null `auth.admin?`
  becomes `false`.
- Convert only frame-root null `result-meta` to an empty object.

Application payloads may use the same field names and remain comparison-significant;
the path policy prevents metadata normalization from hiding user data. Session IDs
in object keys and arbitrary timestamps are **not** masked. After the final expected
frame, the replayer observes a bounded 25ms quiescence window and fails on an extra
frame or abnormal disconnect. A silent peer, normal close, or window expiry is
accepted; the finite window does not prove that no later frame can ever arrive.

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
servers failed identically. Zero selected scenarios is an error. WebSocket record
remains unsupported and fails explicitly (`record mode is unsupported for ws`).
CF-002 provides bounded HTTP and SSE recording tooling, requiring an explicit
target, a securely reserved fresh private output directory mode 0700 with a pinned
identity, caller-supplied endpoint/source/fixture identity metadata, and bounded
quiescence/size/timeouts, with caller-owned fixture reset. Output directories are resolved
and reserved via descriptor-relative no-follow traversal (`openat O_NOFOLLOW`, `mkdirat`)
pinning parent and directory file descriptors and `(dev, ino)` identities without path-based
cleanup on reservation failure. Publication is atomic and write-once: evidence is written
to a temporary file via `openat` on the pinned directory file descriptor (mode 0600),
synced, closed, and published via a kernel no-replace rename (`renameatx_np` on Darwin,
`renameat2` on Linux) followed by directory sync. Failed publication never performs
ambiguous named-file deletion: private temporary or
final artifacts may remain as untrusted incomplete evidence, and the operation returns
failure. Only a successful return makes an artifact eligible evidence. Non-Unix
platforms fail closed as unsupported.

Evidence files retain raw received text, normalized frames, errors, delta, fixture
ID, local v1/v2 revisions and v2 dirty state. Raw frames may contain auth/application
information: keep the directory private and inspect/redact before sharing.
Caller-asserted endpoint ID, source ID, fixture ID, and `fixtureReset=caller-owned`
metadata are assertions, not proof of candidate revision, true fixture reset, or
server state. WebSocket recording, client SDK harness bindings, and external
endpoint process/lifecycle management remain unsupported. Do not claim full
candidate binding or real fixture reset proof from plain `--mode record` alone.
Local revision checks do **not**
establish which revision a remote endpoint is running. A designated comparison run
must also record endpoint deployment/configuration, equivalent fixture initialization,
and accepted-difference rationale externally. Captures are never automatically
promoted to oracle entries. `v1-capture` metadata requires a full matching ref,
a nonempty checked-in raw evidence file, and exact canonical agreement with the
scenario's expected outputs; metadata cannot prove where a capture originated.

### Managed local lifecycle (candidate-bound, CF-002)

The supported runnable recorder is `corpusctl --mode managed-record` (see
`cmd/corpusctl/README.md`). It builds the local `instantd` binary from the
exact candidate (clean worktree required), with no caller-supplied binary
override, and launches a loopback-only daemon
against a newly owned `instant_test_*` fixture, derives Git SHA, dirty
state, binary SHA-256, PID/executable, loopback endpoint, secrets-excluded
configuration digest, and Go/OS/arch locally
(`internal/corpus/candidate.go`, `cmd/corpusctl/managed.go`), resets the app
fixture before every mutating scenario with exact precondition/final-state
assertions, captures HTTP (`/health`) and SSE (`/runtime/sse` hello) through
the corpus recorders with raw+canonical retention, and publishes a
checksummed manifest (`VerifyCaptureManifest`) into the caller's fresh 0700
directory with 0600 files. Any process, binary, endpoint, revision,
configuration, or fixture drift fails closed with no eligible manifest.
Pre-reservation failures create no output. Post-reservation failures close only
the descriptor-pinned reservation and may leave private incomplete residue as
untrusted and ineligible; cleanup never deletes through the replaceable output
pathname.
`TestCF002ManagedRecordEndToEnd` drives this same production entry point.
This proves local candidate/process/endpoint identity and owned fixture
reset; it does not prove remote deployment identity, v1 provenance, SDK
parity, or external fixture equivalence. Plain `--mode record` remains
caller-asserted and unverified.

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
