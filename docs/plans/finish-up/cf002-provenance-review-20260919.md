# CF-002 provenance review — 2026-09-19

Scope: `internal/corpus/candidate.go`, `candidate_test.go`, `http.go`
(capture/publication paths), `cmd/corpusctl/main.go` (record mode),
`cmd/corpusctl/managed_lifecycle_test.go`, `corpus/README.md`,
`cmd/corpusctl/README.md`, `corpus/manifest.json`.

Verdict: **ACCEPT**

All seven falsification targets were attacked and falsified. The managed
lifecycle derives identity from the built binary, launched process, loopback
endpoint, git state, and owned fixture, and re-verifies it before, during,
and after capture; the legacy caller-asserted path keeps its documented
limitation and never claims proof.

1. Caller assertions presented as proof — FALSIFIED.
   `RecordMetadata` still records caller-supplied endpoint/source/fixture
   with `FixtureReset="caller-owned"` (`internal/corpus/http.go:1057-1064`,
   `cmd/corpusctl/main.go:480-488`), and both READMEs still disclaim it as
   assertion rather than proof (`corpus/README.md:142-152`,
   `cmd/corpusctl/README.md:38-42`). The managed path does not use
   `RecordMetadata` at all: it builds `CandidateIdentity` from
   `GitIdentity`, `HashFile`, process handle, loopback check, config digest,
   and fixture handle
   (`cmd/corpusctl/managed_lifecycle_test.go:306-391`), validates formats
   (`candidate.go:189-235`), and re-verifies liveness at four points
   (`managed_lifecycle_test.go:393,428,472,564-568`). No caller string is
   trusted for revision, binary, process, endpoint, or fixture state.
2. Stale or replaced process identity — FALSIFIED.
   `VerifyProcessAlive` rejects dead PIDs via `kill(pid,0)` with
   ESRCH→error and EPERM→alive (`candidate.go:142-161`); the daemon handle
   is held open for the whole test so the PID cannot be reused, and the
   recorded PID is checked before capture (`:344`), before each scenario
   (`:428`), during capture (`:472-477`, handle equality), and
   post-stop binary/revision continuity (`:564-568`). A dead PID
   (`1<<30`) is rejected (`candidate_test.go:TestCandidateVerifyProcessAlive`);
   a tampered PID passes format validation but fails `VerifyLive`
   (`candidate_test.go:TestCandidateValidateAndVerifyLive/bad-pid`).
3. Wrong endpoint or binary — FALSIFIED.
   `RequireLoopbackEndpoint` accepts only loopback IPs/`localhost` and
   rejects wildcard/remote/hostnames (`candidate.go:110-140`); the managed
   baseURL is checked at bind (`managed_lifecycle_test.go:335`) and every
   capture addresses that exact URL (`:438,455`). `VerifyBinaryDigest`
   hashes the file before capture (`:347`), per scenario via `VerifyLive`
   (`:428,472`), and after stop (`:564`); replacement is detected
   (`candidate_test.go:TestCandidateBinaryDigestDetectsReplacement`,
   `.../bad-binary`). A foreign repo directory fails liveness
   (`TestCandidateValidateAndVerifyLive`, last assertion). Endpoint
   mismatch at the transport layer fails with no evidence file
   (`cmd/corpusctl/main_test.go:TestRecordFailurePropagation`,
   `TestRecordMissingMetadataMakesZeroRequests` zero-request legs).
4. Reset omission or cross-run fixture contamination — FALSIFIED.
   `cf002ResetFixture` deletes the app row (cascading to attrs/triples/
   idents) and recreates app+token (`managed_lifecycle_test.go:251-270`);
   it runs before every mutating scenario unless the documented probe
   variable is set (`:417-419`). Exact precondition
   (`apps=1 … attrs=0 triples=0 idents=0 rows=[]`, `:424-427`) and exact
   final state (`attrs=2 triples=2 idents=2` plus literal rows, `:487-490`)
   are asserted per scenario, and cross-scenario equality of both strings
   is asserted (`:525-530`). Live probe `CF002_SKIP_RESET=1` goes red with
   the s2 precondition showing s1's rows (`attrs=2 triples=2 …`,
   observed 2026-09-19); unset runs go green. Each run uses a separate
   owned `instant_test_*` database (observed `instant_test_4099…` and
   `instant_test_2232…`); testkit drops only the owned DB.
5. Incomplete capture accepted as complete — FALSIFIED.
   `CaptureSSEWithOptions` errors when fewer than `recordLimit` records
   arrive (`http.go:389-391,443-444`); the record CLI fails incomplete
   streams with no evidence file
   (`cmd/corpusctl/main.go:596-601`,
   `main_test.go:TestRecordFailurePropagation` SSE leg). The managed SSE
   capture requires exactly 1 hello record with `init-ok` framing
   (`managed_lifecycle_test.go:455-470`); HTTP capture requires 200 plus
   `ok/db` body (`:438-442`). Late/error frames: extra record inside the
   quiescence window errors (`http.go:459-463`), proven by
   `TestCaptureSSEQuiescenceDetectsExtraRecord` and
   `TestCaptureSSEIncompleteStreamFails`; transport errors yield
   non-pass results with deltas (`TestReplayHTTPTransportFailureIsNotPass`,
   `TestReplaySSEDoesNotAcceptEOFAfterParentCancellation`).
6. Raw/canonical divergence — FALSIFIED.
   HTTP retains raw bytes and derives canonical deterministically
   (`managed_lifecycle_test.go:443-449`, double-canonical equality);
   SSE retains raw blocks plus `Normalized` (`:460-470`, raw holds
   `session-id` framing while normalized holds `<session-id>`).
   `NormalizeSSERecord` clones rather than aliasing (`http.go:842-847`);
   replay comparisons derive normalization from `Data`, never trusting a
   supplied `Normalized` field. Masking is path-scoped
   (`canonical.go:102-145`); payload fields id/token/timestamp/cursor/
   email/title at payload paths stay significant in both modes while
   frame-root protocol fields still mask
   (`candidate_test.go:TestPayloadApplicationFieldsRemainSignificant`).
   Live probe: an injected over-broad `title` mask went red in both modes
   (observed 2026-09-19); reverted source goes green.
7. Artifact/manifest checksum mismatch — FALSIFIED.
   `ManifestArtifacts` hashes published bytes (`candidate.go:272-288`);
   `VerifyCaptureManifest` recomputes size+sha per artifact and validates
   the candidate (`:290-314`); the managed run publishes 4 evidence files
   + `manifest.json` through the reserved dir and verifies
   (`managed_lifecycle_test.go:534-556`), with 0600/0700 mode assertions
   (`:410-412,516-520,553-556`) and a write-once republish rejection
   (`:559-562`). Tampered SHA, mutated bytes, empty manifests, escaping
   names, and missing files are all rejected
   (`candidate_test.go:TestCaptureManifestRejectsTamper`).

Recording-profile disposition (no silent waiver): HTTP and SSE recording are
selected and supported; WebSocket recording is explicitly unsupported and
fail-closed (`cmd/corpusctl/main.go:122-125`, stable message
`record mode is unsupported for ws`); direct WS invocation is proven to fail
with no output directory or artifact created
(`managed_lifecycle_test.go:TestCF002WSRecordCreatesNoArtifact`). SDK
recording is not selected (DEC-001 makes no frozen-SDK/parity claim);
external/v1 capture stays owned by CF-004/CF-005 (both BLOCKED). No doc,
manifest entry, or gate claims WS/SDK/v1 recording: `corpus/README.md:123`,
`cmd/corpusctl/README.md:21-23`, manifest v1 (`manifest.go:15-16`
describes WS NDJSON replay inventory, not recording).

Initial findings: payload-significance lacked an explicit `title` pin and
the masking probe initially stayed green because of it; added the `title`
pair to `TestPayloadApplicationFieldsRemainSignificant` (test-only, green).
No production-code repair was needed. Re-review of the four new/affected
tests confirms each fails when its guard is removed (probes above).

## Addendum — runnable recorder re-review (2026-09-19, intermediate 92e3a64)

The ACCEPT above covered a test-only lifecycle: orchestration lived in
`cmd/corpusctl/managed_lifecycle_test.go` and no runnable recorder existed.
This addendum re-reviews the extracted production entry point
`cmd/corpusctl/managed.go` (`corpusctl --mode managed-record`, wired in
`cmd/corpusctl/main.go`) and two direct CLI runs from the clean intermediate
SHA. Prior sections stand; only the new path is re-verified below.

Re-verified against the runnable path:

- Direct CLI, persistent evidence: `go run ./cmd/corpusctl --mode
  managed-record --output-dir /private/tmp/cf002run1` and `.../cf002run2`
  from `92e3a64` both exit 0 with `PASS managed-record 92e3a64…`
  (pids 98966/99253, fixtures `instant_test_4d64ba…`/`instant_test_acb587…`,
  endpoints `127.0.0.1:51496`/`:51584`). Evidence (4 files + manifest)
  remains readable after exit with 0700/0600 modes; both daemons confirmed
  stopped (connection refused); both owned databases dropped (absent from
  `pg_database`); no repository residue.
- Candidate binding: both manifests record `gitSha 92e3a64`,
  `gitDirty false`, 64-hex binary SHA-256 and config digest, PID, loopback
  endpoint, `go1.27.1 darwin/arm64`, and `instant_test_*` fixture DB + app.
  The CLI requires a clean worktree before any build/daemon/fixture work
  (`managed.go`: dirty check precedes all side effects), derives the SHA
  itself, and re-verifies git/binary/process before, during, and after
  capture. A dirty scratch repo is rejected with `clean Git worktree`
  and no output (`TestCF002ManagedRecordRequiresCleanTree`); a wrong-SHA
  manifest copy is detected by the SHA-equality check the e2e asserts.
- Reset equivalence: both runs show identical pre
  (`attrs=0 triples=0`) and identical literal post (`attrs=2 triples=2`
  with exact rows) across the inter-scenario reset; the omission probe
  through the production entry point goes red with no manifest
  (`TestCF002ManagedRecordSkipResetFails`).
- Manifest checksums: every artifact hash/size recomputed and matching in
  both runs; tampered copy (zeroed SHA, lied checksum) detected on both
  fields; reuse of an output dir exits 1 (`already exists`) with the first
  manifest byte-identical (`d4f54400…` before and after).
- Transports unchanged: HTTP+SSE captured raw+canonical (base64 envelope
  decodes to exact raw bytes asserted in the e2e); WS record still
  excluded with no artifact (`TestCF002WSRecordCreatesNoArtifact`
  retained). Ordinary `--mode record` keeps its caller-asserted,
  unverified evidence class; nothing silently upgraded it.

Verdict: **ACCEPT** (re-review). The runnable recorder provides persistent
candidate-bound evidence with the same guarantees the test-only lifecycle
proved. No remote identity, v1 provenance, SDK parity, or external fixture
equivalence is claimed.
