# Phase 05 — Corpus, COPY acceptance, and pinned-v1 compatibility

Phase gate: `SELECTED_SURFACE_CONFORMANT`
Packets: `CF-001` through `CF-005`
Ownership: corpus is single-writer; COPY owns only its storage file/tests.

The objective is not a larger scenario count. It is an evidence-accounted
support matrix in which every selected behavior has a fixture, real transport,
oracle, exact expected state, provenance, and disposition.

## CF-001 — PostgreSQL COPY acceptance

**Goal objective:** `Accept the repaired COPY semantics against an owned PostgreSQL fixture, or enforceably exclude COPY from the release profile.`

Implementation now preserves JSON null and uses deterministic input order for a
cardinality-one winner, but `CopyTriples` has no production caller and no current
PostgreSQL-backed acceptance.

Required evidence:

- actual COPY/insert path round-trips null distinctly from the string `"null"`;
- repeated cardinality-one values choose the declared last input occurrence,
  including equal timestamps and IDs;
- cardinality-many and mixed attributes retain all declared values;
- rollback leaves no partial triples after malformed/conflicting input;
- behavior is compared with the non-COPY storage path where parity is claimed.

Write lease: `internal/storage/copy.go` and focused tests only. Use an explicitly
owned isolated PostgreSQL fixture. Without it, return `BLOCKED`; compile/unit
evidence is not integration acceptance. If COPY is not exposed, DEC-001 and the
release gate must enforce the exclusion.

## CF-002 — Recorder and equivalent-fixture lifecycle

**Goal objective:** `Implement or explicitly exclude corpus recording with candidate-bound source identity, isolated fixture bootstrap/reset, raw capture, normalization, privacy classification, and write-once output.`

Current HTTP/SSE tooling provides transport mechanics, but `corpusctl record`
still reports unavailable and there is no complete SDK/external fixture lifecycle.

Contract rows:

- source revision/deployment and endpoint identity are proven;
- every mutating scenario begins from an equivalent isolated reset state;
- HTTP/WS/SSE/selected SDK frames retain raw and canonical forms;
- volatile-field masking is path-scoped and application payload keys remain
  significant;
- completion/quiescence is bounded and late/error frames are observable;
- output is fresh, private, redacted where needed, atomic, and candidate-bound.

Write lease: `internal/corpus/**`, `cmd/corpusctl/**`, and `corpus/**`. Product
defects found during capture become separate owning packets. External endpoints,
SDKs, or services require explicit authority.

## CF-003 — Selected coverage matrix closure

**Goal objective:** `Close every DEC-001-selected corpus matrix row with real-path positive, denial, boundary, lifecycle, concurrency, exact-state, and provenance evidence.`

Current baseline: 18 authored v2 scenarios; 22 covered surfaces, 7 surface gaps,
2 unsupported; matrix 9 covered, 15 gaps, 1 unsupported; zero v1 captures.

The 15 open matrix families are:

1. HTTP auth/admin/runtime/storage/backup positive and denied behavior.
2. Auth refresh lifecycle and token denial/replay.
3. Dynamic permission binding/fallback.
4. SSE permission lifecycle.
5. HTTP conjunction and multi-client SSE query lifecycle.
6. Delta refresh and full-result convergence.
7. SSE refresh lifecycle/convergence.
8. Room fanout and admin presence lifecycle.
9. Transaction rollback, cardinality, merge/cascade, lookup and concurrency.

For every selected row, record transport, fixture owner/reset, expected final
state, oracle origin (`authored-v2`, `captured-v1`, or approved difference), raw
evidence, and failure classification. Do not mark a row covered because a nearby
unit test exists. Explicit unsupported rows must be enforced in code and docs.

## CF-004 — Pinned-v1 environment qualification

**Goal objective:** `Provision and prove an isolated v1 endpoint at the exact approved ref with equivalent resettable fixtures and endpoint/binary/configuration identity.`

Default ref is `a4d2ef33b60f281a437191006e4541d4780f9e4a`
unless DEC-001 changes it explicitly.

Preflight Java/Clojure, PostgreSQL/logical WAL/required extensions, MinIO if
selected, configuration, ports, bootstrap, health, fixture reset, and served
revision. A local checkout pin does not prove a remote endpoint. Store raw logs
privately and durable redacted provenance/checksums in the repository evidence
summary.

This goal requires authority to run services and use their credentials. If
unavailable, complete preflight only and return `BLOCKED`.

## CF-005 — Differential and accepted differences

**Goal objective:** `Run the selected matrix against the qualified v1 endpoint and exact v2 candidate, then fix or durably classify every divergence without zero-selection, dual-failure, transport, decode, or fixture-mismatch false passes.`

Run `make differential` only with explicit full `V1_PATH`, `V1_REF`, `V1_URL`,
`V2_URL`, suite, and a fresh private output directory. Reset equivalent fixtures
before each mutating scenario. Preserve raw/canonical frames and both endpoint
identities.

Every divergence must become either:

- a new product packet owned by the affected package;
- a corpus/harness defect repaired in this phase;
- an owner-approved difference with stable ID, rationale, affected SDK/surface,
  detection assertion, and release-envelope reference.

One-side or both-side transport/decode failure is not parity. Existing authored
v2 expectations are not v1 oracles. Require a fresh compatibility/release review.

## Phase completion

The gate closes when every selected matrix row has accepted evidence, every
excluded row is enforceable, and the exact compatibility claim matches the
differential evidence. A bounded alpha may complete without frozen-v1 parity only
if DEC-001 and public docs explicitly disclaim it; in that case CF-004/005 reach
packet state `EXCLUDED_APPROVED` through accepted-exception rows, not green parity
evidence.
