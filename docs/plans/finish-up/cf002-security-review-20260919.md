# CF-002 security review — 2026-09-19

Scope: `internal/corpus/candidate.go`, `http.go` (redaction/publication),
`fs_unix.go` (reservation/publication), `cmd/corpusctl/main.go` (record
mode), `cmd/corpusctl/managed_lifecycle_test.go`,
`internal/corpus/candidate_test.go`, `cmd/corpusctl/main_test.go`.

Verdict: **ACCEPT**

All seven falsification targets were attacked and falsified. Evidence stays
private and write-once; redaction is header-scoped so payloads cannot be
hidden by name; untrusted endpoint data never controls filesystem paths;
failure never yields eligible evidence.

1. Secret leakage — FALSIFIED.
   `ConfigDigest` hashes only caller-provided non-secret fields over sorted
   `key\x00value\x00` pairs and never receives secret values
   (`candidate.go:90-108`); the managed digest passes presence bits only
   (`storage-secret-is-set=true`, no secret;
   `managed_lifecycle_test.go:350-359`). The daemon log is asserted to name
   the storage root and never contain the secret
   (`:570-578`). Record/SSE evidence redacts `Authorization`, `Cookie`,
   `Set-Cookie` by default plus repeatable `--redact-header`
   (`cmd/corpusctl/main.go:43-49`); redaction is asserted on request and
   response headers and on normalized headers
   (`main_test.go:TestRecordHTTPSuccessAndRetention:353-362`,
   `TestHTTPEvidenceRedactsDefaultAndOverrideHeaders`,
   `TestSSEEvidenceIncludesHTTPExchanges:183-194`). Bodies stay raw inside
   the private directory by design and the READMEs warn they may carry
   auth/application data (`corpus/README.md:139-141`). The digest unit test
   asserts no secret substring appears in the digest
   (`candidate_test.go:TestCandidateConfigDigestExcludesSecrets`).
2. Over-broad or under-broad redaction — FALSIFIED.
   Over-broad: `RedactExchange` touches only named headers and clones
   bodies byte-for-byte (`http.go:1029-1048`); canonical masking is
   path-scoped to frame-root protocol fields (`canonical.go:102-145`),
   and seven payload field pairs (id/token/timestamp/cursor/email/title,
   plus nested/array shapes) are proven significant in both plain and
   differential modes
   (`candidate_test.go:TestPayloadApplicationFieldsRemainSignificant`).
   Under-broad: default credential headers plus configurable extras are
   redacted in HTTP and SSE evidence paths listed above; an unredacted
   custom header stays visible only when the operator does not request it,
   which is the documented private-evidence posture, not a bypass.
3. Symlink/TOCTOU output replacement — FALSIFIED.
   Reservation walks descriptor-relatively with `O_NOFOLLOW`, pins parent
   and directory fds plus `(dev,ino)`, and rejects symlink components,
   parent redirection, existing paths, and traversal
   (`fs_unix.go:49-193`); every write re-verifies pinned identity before
   temp creation and after hooks (`:298-325`). Proven by
   `TestReserveOutputDirSymlinkAndParentRedirectionRejection`,
   `TestReserveOutputDirDirectorySwapRejection`,
   `TestReserveOutputDirConcurrentAndDuplicate`,
   `TestReserveOutputDirMkdiratEEXISTFailClosed`,
   `TestWriteEvidenceSwapBeforeTempCreateRejection`
   (`internal/corpus/http_unix_test.go`), and at the CLI layer by
   `TestRecordOutputFreshnessAndWriteOnce` legs 3–5 (symlink dir, symlink
   parent child, `../escape`). The managed run reserves a fresh 0700 dir
   and asserts its mode (`managed_lifecycle_test.go:405-412`).
4. Unsafe cleanup — FALSIFIED.
   Reservation failure performs no path-based cleanup
   (`fs_unix.go:146-151`); failed publication never deletes ambiguous
   named files and residue stays private untrusted
   (`fs_unix.go:379-394`, `http.go:911-915`); the managed lifecycle stops
   the daemon with bounded SIGTERM/wait/kill and removes only the owned
   storage root, output/log temp dirs, and the testkit-owned database
   (`managed_lifecycle_test.go:165-189` cleanup, `:580-581` ownership
   note). `TestWriteEvidenceDirSyncFailureLeavesUntrustedArtifact` and
   `...ReplacementPreserved` pin the no-deletion semantics.
5. Output overwrite — FALSIFIED.
   Publication uses same-directory 0600 temp + fsync + kernel no-replace
   rename (`renameatx_np`/`renameat2`) + dir sync
   (`fs_unix.go:327-394`); existing names fail write-once
   (`:379-383`). Proven by `TestWriteEvidenceIsPrivateAndWriteOnce`,
   `TestWriteEvidenceConcurrentWriteOnce`,
   `TestWriteEvidenceInjectedFailuresNoPartialArtifact`,
   `TestRecordOutputFreshnessAndWriteOnce` (duplicate run on existing dir
   fails), and the managed manifest republish rejection
   (`managed_lifecycle_test.go:559-562`). Evidence files are asserted
   0600 (`:516-520,553-556`).
6. Untrusted endpoint data controlling filesystem paths — FALSIFIED.
   Scenario IDs are validated filename-safe before any path use
   (`http.go:986-999`); evidence paths are joined under the reserved root
   and containment-proven (`:1003-1020`, `ReservedDir.EvidencePath`
   `:905-907`); artifact names with separators are rejected
   (`candidate.go:274-276,295-297`). Proven by
   `TestValidateScenarioIDAndEvidencePath`,
   `TestReplaySSERejectsUnsafeScenarioIDBeforeNetwork` (rejected before
   any network call), `TestSSEEvidenceRejectsUnsafeScenarioID`, and
   `TestCaptureManifestRejectsTamper` (escaping name rejected). SSE/HTTP
   response bodies are content, never paths.
7. Evidence publication after failure — FALSIFIED.
   Only a successful return makes an artifact eligible
   (`corpus/README.md:133-137`, `cmd/corpusctl/README.md:28-36`).
   Transport/incomplete/extra-record failures return nonzero with no
   evidence file (`main_test.go:TestRecordFailurePropagation` asserts
   absence of both http and sse evidence after failure;
   `TestRecordMissingMetadataMakesZeroRequests` asserts zero requests when
   metadata/output checks fail). The managed run verifies the manifest
   only after all captures succeed and fails the test on any capture,
   publish, or verification error (`managed_lifecycle_test.go:434-556`);
   failed publication leaves residue untrusted and ineligible
   (`fs_unix.go:390-392`).

Boundary: local candidate/process/endpoint identity and fixture reset are
proven for the managed loopback path only. No remote deployment identity,
v1 provenance, SDK parity, or external fixture equivalence is claimed.
`record --transport ws` stays an enforced exclusion with no artifact
(`managed_lifecycle_test.go:TestCF002WSRecordCreatesNoArtifact`).
Non-Unix platforms fail closed as unsupported
(`http_other_test.go`, `fs_other.go`).

Initial findings: none requiring repair. The `title` payload pin added
during provenance review (test-only) also serves this review's over-broad
redaction leg; re-review confirms it fails when the guard is removed and
passes otherwise. No production-code change, no secret touched, no residue.

## Addendum — runnable recorder re-review (2026-09-19, intermediate 92e3a64)

The ACCEPT above covered a test-only lifecycle. This addendum re-reviews the
runnable `corpusctl --mode managed-record` path (`cmd/corpusctl/managed.go`)
and its two direct CLI runs. Prior sections stand.

Re-verified against the runnable path:

- Output handling: directories are reserved fresh with mode 0700
  (descriptor-relative no-follow reservation, fail-fast freshness pre-check
  plus authoritative `ReserveOutputDir`); evidence and manifest are 0600;
  publication is fsynced atomic no-replace; output inside the repository is
  refused before any work (it would dirty the candidate); symlinked output
  is rejected with nothing written under the link
  (`TestCF002ManagedRecordRejectsSymlinkedOutput`, target dir observed
  empty); reuse is refused with the first evidence byte-identical.
- Secret exclusion: manifests and logs carry no secret values — config
  digest uses presence bits only, the admin token lives only in memory and
  request headers, and both run manifests were scanned for the storage
  secret and the database URL (absent). Database errors use static messages
  because pgx errors may embed credentials (`managed.go`); the secret value
  is asserted absent from the daemon log. Raw SSE hello retains its
  short-lived session token inside the private 0700/0600 evidence only,
  matching the documented private-evidence posture.
- Owned-resource cleanup: daemon stopped via bounded SIGTERM/wait with
  port-down confirmation (both runs connection-refused after exit, no
  `instantd-managed` process remains); owned `instant_test_*` databases
  dropped on success and failure paths (both absent afterwards; unrelated
  pre-existing test databases untouched); storage root, build, and log
  temp dirs removed. Failure paths remove the freshly reserved output dir
  itself, so unsuccessful runs leave no eligible manifest (proven by the
  omitted-reset probe: no `manifest.json`).
- Safe database scoping: the owned name is always generated
  `instant_test_*`; no flag accepts a database name, so out-of-scope names
  are structurally impossible; `--database-url`/DATABASE_URL is an admin
  connection string that is never recorded, logged, or printed.
- Endpoint restriction: the daemon binds `127.0.0.1` by construction with
  no flag to change it; `RequireLoopbackEndpoint` gates the recorded URL;
  loopback is asserted in both run manifests.
- Failure non-publication: every identity/fixture/capture/publication
  error returns nonzero before manifest publication (dead-binary probe
  `/bin/echo`: `never became healthy`, no output dir; missing-DATABASE_URL
  probe: immediate usage failure). Only the final verified manifest makes
  evidence eligible.

Verdict: **ACCEPT** (re-review). The runnable recorder preserves the
private write-once posture with no secret leakage, no unsafe cleanup, and
no eligible evidence on failure.

## Corrective addendum — post-reservation cleanup race (2026-09-19, implementation 5b7d30b8)

The runnable-recorder ACCEPT above missed an unsafe-cleanup gap. Its failure
defer called `os.RemoveAll(absOut)` after descriptor-relative reservation. A
concurrent actor could rename the pinned directory and replace the visible
pathname, causing failure cleanup to delete unrelated replacement contents.
The earlier finding and verdict are preserved as history, but the claim that
managed failure cleanup was safe was incorrect.

Repair and falsification:

- Policy A (before reservation): validation, candidate build, database setup,
  and capture are buffered in memory. A failure creates no output path.
- Policy B (after reservation): cleanup closes only `ReservedDir`; it never
  removes through `absOut`. Failed publication has no eligible manifest, while
  private incomplete residue may remain explicitly untrusted and ineligible.
  This matches the lower-level no-ambiguous-deletion publication policy.
- `TestCF002ManagedRecordFailurePreservesVictimUnderReplacement` pauses after
  reservation, renames the pinned directory aside, replaces the visible output
  pathname with a victim directory containing a sentinel, and injects failure.
  It proves nonzero exit, byte-identical victim survival, no eligible manifest
  at either path, and ineligible residue preservation in the pinned directory.
  The regression would fail under the removed `os.RemoveAll(absOut)` behavior.

Fresh verdict: **ACCEPT**. Pathname-based post-reservation deletion is removed;
the Policy A/B boundary and deterministic replacement-race regression repair
the unsafe-cleanup defect. Private residue is not eligible evidence, and no
secret, remote, publication, or external-fixture claim is broadened.
