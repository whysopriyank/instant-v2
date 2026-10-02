# QR-003R public alpha qualification contract

Implementation ledger, 2026-10-02. Baseline `421d69f5ab4c1ed4ff524b7d8c6a51873e50a610`;
the pre-existing untracked `docs/plans/readiness-review-20261001.md` is owner work.
Owned scope: `cmd/qualify/**`, release gate and its contract tests,
`scripts/qualify/**`, this document. No commits, remote operations or live
campaign execution are authorized to this worker. Parent owns the approved
`DEC-002-single-node-public-alpha-20261002` envelope and live campaigns.

| ID | Desired invariant / real path | Planned observation and red expectation | Scope / goal | Status |
|---|---|---|---|---|
| R1 | DEC-001 schema-1 manifest/records retain current selection and acceptance | Existing manifest/record/package and production-shell gate contract checks stay green | Existing gate; FIX | GREEN |
| R2 | DEC-002 public profile requires native, recovery, soak, container, restore and v1 differential; performance only when candidate policy selects it | Real shell gate accepts a complete hermetic contract fixture; rejects missing lanes/records, old selection and wrong decision | Public gate schema; FIX | GREEN (contract) |
| R3 | Every public record binds candidate, binary, configuration, image and its fixture identity; all referenced bytes verify | Mutation tests reject wrong identities and changed/missing/unsafe artifacts | Record/manifest/gate; FIX | GREEN (contract) |
| R4 | New record producers reject synthetic, zero/skipped/failed/unasserted runtime evidence | Packet-specific source adapters plus rejection tests, runtime facts from separately owned campaign producers | Three new required lanes; optional performance fails closed until adapter exists; FIX | PARTIAL (adapters checked; live producer proof pending) |
| R5 | Campaign freshness, no selected skips, no zero tests, clean candidate and child failure propagation remain enforced | Existing contract checks plus public-profile identity/freshness mutations | Existing checks; FIX | GREEN (contract) |
| R6 | Public campaign assembly passes explicit profile/record arguments without provisioning unknown resources | Focused script argument/assembly checks; old default retained | Scripts; FIX | PARTIAL (arguments/build contract checked; live assembly pending) |
| R7 | Runtime acceptance requires real retained outputs from one immutable public candidate | Parent-run owned-host container/restore/differential + existing three lanes; unavailable locally | No fabricated PASS; FIX | PARTIAL |

Verification: exact new red tests, same tests green plus clean rerun,
`go test ./cmd/qualify -race -count=1 -short`, release shell contract suite,
`bash scripts/qualify/test-campaign.sh`, formatting, build and bounded static
checks. Parent owns full-repository checks and independent review. This worker
cannot claim COMPLETE while R7 or required independent review remains missing.

## Interface review

The proposed schema extends the existing evidence model; producer inputs and
packet-specific assertions are under parent review before implementation.
DEC-001 remains the default; DEC-002 is selected explicitly, not by weakening
the old profile. Public readiness is not established by a hand-written PASS
record or synthetic benchmark.

## Public runtime facts producer input

`qualify container|restore --evidence-root ROOT --facts RELPATH --out FILE`
reads the immutable facts file, rejects unsupported/synthetic observations,
and derives a lane result. The facts file must itself be included in the
record's artifact inventory; the gate replays the assertions from it.

Facts schema (unknown fields rejected):

```json
{
  "schema_version": 1,
  "measurement_class": "live",
  "identity": {
    "candidate_sha": "40 lowercase hex",
    "binary_sha256": "64 lowercase hex",
    "configuration_sha256": "64 lowercase hex",
    "endpoint_sha256": "64 lowercase hex",
    "image_digest": "sha256:64 lowercase hex (distribution OCI image)",
    "fixture_id": "owned fixture descriptor ID",
    "fixture_sha256": "64 lowercase hex"
  },
  "started_at": "RFC3339 UTC",
  "finished_at": "RFC3339 UTC",
  "observations": {}
}
```

Container observations: `uid` = 65532, `health_status` = 200,
`ready_status` = 200, `tls_verified`, `transact_acked`,
`subscription_converged`, `port_released` all true; `drain_seconds` between
0 and 30; nonempty 64-hex `object_sha256_before` equals
`object_sha256_after`. The runtime producer, rather than this offline
adapter, owns the actual startup, restart, TLS handshake and protocol probes.

Restore observations: `formats` contains exactly two rows (`format` =
`v1zip` or `v2ndjson`), each with `triple_count` >= 1000000,
`duration_seconds` > 0 and <= 600, `logical_sha256_expected` equal to
`logical_sha256_actual`, and `objects_sha256_expected` equal to
`objects_sha256_actual` (all nonempty 64 hex). `rejections` contains exactly
five rows with IDs `truncated`, `corrupt`, `wrong-app`, `oversized`,
`non-empty`; every row has a nonempty `error`, equal 64-hex
`target_sha256_before`/`target_sha256_after`, and equal 64-hex
`source_sha256_before`/`source_sha256_after`. Target hashes cover logical
state and objects, not table counts alone.

## Independent restore architecture review (2026-10-02)

The nonauthor reviewer inspected `internal/backup/import.go`,
`internal/backup/v1zip.go`, `internal/storageapi/backend.go` and object-store
callers before implementation. Prewrite/sync new blobs, then SQL commit is
safe for known precommit rejection if archive validation precedes writes,
only newly created keys are cleaned up, cleanup uses a bounded fresh context
and directory fsync, and cleanup errors are surfaced. Existing blobs must
never be replaced/deleted. PostgreSQL's nontransactional `setval` must be
replaced by transactional identity `RESTART`, under restore locks, without
moving the sequence backwards.

A lost SQL COMMIT reply can mean committed rows: delete-on-error would lose
objects belonging to committed metadata. Keep blobs and expose explicit
commit-outcome-unknown/reconcile semantics. A process crash can leave
unreferenced blobs. Without a cross-store journal this does not satisfy an
unconditional crash-atomic restore claim. Parent has selected the bounded
policy: known rejections preserve target/source; unknown commit outcomes are
classified separately, retained for reconciliation; crash orphans are an
explicit limitation. The global restore table locks are an alpha restore-only
ceiling, not general write-path synchronization.

[PostgreSQL 17 RESTART semantics](https://www.postgresql.org/docs/17/sql-altersequence.html)
confirm RESTART is transactional and blocks concurrent sequence allocations,
unlike `setval`. This review does not prove the worker's eventual code or
runtime fault checks; those require their own evidence.

## Selection and assembly

The committed `qualification-policy.json` selects the exact successor decision,
public profile, `performance` (`not_selected` by default, or `artifact`) and
`release_version` (`v0.1.0-alpha.1`). The shell gate builds its verifier from
`git archive` of the clean candidate, rejects archived symlinks, disables
external Go workspaces/GOFLAGS overlays and uses readonly module selection. The public distribution binary uses
`scripts/qualify/build-candidate.sh single-node-public-alpha [OUTPUT]`, with
`CGO_ENABLED=0`, readonly module selection and `-trimpath -ldflags '-s -w -X main.version=<approved release_version>'`.
Campaign build accepts `--profile single-node-public-alpha` and performs the
same build twice; native/recovery/soak use the retained binary identity.

Without performance, public handoffs are the original 28 plus `CF-004`,
`CF-005`, `DA-009`, `DA-010`, `OP-004`, `OP-004D`, `OP-006`, `QR-003R`,
`QR-004` (37 total). Selecting performance adds `EV-007` and `QR-002` (39).
FR-003 publication is excluded to avoid a circular publication gate. Selected
performance currently fails closed: no source adapter has been implemented,
so it cannot accept a hand-authored or synthetic PASS. A future comparative
claim must reuse the benchmark report eligibility checks before selection.

`assemble.sh` adds `--profile`, `--fixtures FILE`, `--image-digest sha256:...`.
The fixture map keys exactly match selected external-record names, each value
is `{ "id": "owned descriptor ID", "sha256": "descriptor SHA256" }`.
Every lane must include the actual descriptor bytes in its artifact inventory,
and its `cleanup.complete` and `lane.json` must exist. New lane directories are
`container`, `restore`, `v1_differential`; native's map key is `native_linux`.
The distribution image digest is separate from the existing qualification
image ID. Manifest construction uses `--policy` pointing to the candidate
source-controlled policy, plus the three new `--*-record` flags.

`qualify v1-differential --evidence-root ROOT --facts RELPATH --out FILE`
adapts existing `corpusctl differential` raw outputs. Facts observations are
`{ "corpus_manifest_artifact": "v1_differential/manifest.json", "evidence":
["v1_differential/00-smoke.ndjson.evidence.json", "..."] }`. The retained
manifest must byte-match the candidate's `corpus/manifest.json`, and every
candidate scenario must appear exactly once with its registered fixture,
full v1 pin, clean candidate v2 SHA, no transport error or delta. The adapter
recomputes canonical-v1 differential normalization from both raw frame streams
and compares them. Both engines must also retain exactly the required
server-frame count from each candidate scenario; equal truncated streams
are rejected without treating the authored frames as a v1 oracle. Facts, manifest, and every raw scenario output must be
hash-bound in the external record. An authored golden or scalar PASS is
insufficient. Parent remains responsible for equivalent isolated fixtures and
real execution, not merely constructing this identity wrapper.

## Executed verification and remaining proof

RED: before implementation, `TestPublicNativeRecordIdentity` rejected
`--profile` as an unknown flag. The container negative test exposed an omitted
`drain_seconds` incorrectly defaulting to zero; the observation now requires
an explicit field. A bounded overlay of the original baseline production
shell rejects the complete public contract fixture with `manifest schema,
selection, identity, or handoff inventory is invalid`; the actual updated shell
accepts it. This overlay is test evidence only and changes no candidate files.

GREEN: `go test ./cmd/qualify -race -count=1 -short` covers the original
qualifier and new public identity, container, restore, differential, full
manifest selection and production-shell checks. The public shell uses the
actual compiled verifier, stubbing only command execution inside temporary
hermetic fixtures. Tests reject missing new records, wrong images/fixtures,
synthetic observations, missing artifacts, changed bytes, missing packets,
unapproved performance selection, old profile/decision, changed differential
raw normalization, nonempty differential delta, dirty v2 capture and symlink
parents. These test fixtures are never runtime qualification evidence.

`CGO_ENABLED=0 GOCACHE=/tmp/instant-public-qualify-gocache bash
scripts/test-quality-release-gate.sh` passes 45 checks, including the real
reactive hermetic logs and their injected required-skip rejection. The default
CGO run on this macOS host fails in the local linker (macOS 27 SDK `.tbd`
`arm64e.x1` architecture unsupported), including with explicit Apple clang;
this is an environment limit, not a passing native Linux test. The pure Go
shell run does not replace the required owned-host native campaign.

`bash scripts/qualify/test-campaign.sh` passes 27 dry argument/resource-safety
checks, including explicit public profile and unknown-profile rejection.
`go vet ./cmd/qualify`, shell syntax checks and scoped `git diff --check` pass.
Logs retained locally under `/tmp/instant-public-*`; parent owns permanent
campaign retention. No commits, remote actions, publication or runtime load
were performed by this worker. Required parent-owned live container, restore,
v1 differential, native/recovery/soak campaign, immutable candidate retention
are still missing from this ledger. Independent feature review approved the
frame-count fix and its integration; R7 remains PARTIAL;
no production/public alpha acceptance is asserted here.

Independent review identified and fixed an initial CF-005 false acceptance:
equal nonempty but truncated streams could pass. The required-count rejection
first failed against the implementation (`exit=0`), retained in
`/tmp/instant-public-truncated-red.log`. The adapter now loads the exact
candidate scenario and requires both raw and normalized streams on both
engines to equal `len(ExpectedS2C())`; the complete contract fixture uses
candidate scenario frame shapes/counts. Independent feature review rechecked and approved this fix and its integration.
Post-fix clean reruns: qualifier race package PASS 13.372s, shell suite PASS
45/45 (CGO disabled for the known local SDK limit), vet PASS. Logs:
`/tmp/instant-public-qualifier-reviewed-race.log` and
`/tmp/instant-public-gate-reviewed.log`. R7 remains PARTIAL; none of these fixtures is runtime campaign proof.

Strict repository lint then identified unchecked test setup operations and two
superseded qualifier helpers. The bounded repair makes every new test
filesystem/JSON/hash operation fail explicitly on error, uses a tagged fixture
switch, and removes `recordPathForPacket`/`checkPacketSet` after graph lookup
(no indexed nodes) and source lookup confirmed no callers. The generalized
helpers retain the DEC-001 and public acceptance behavior.
Scoped `/Users/priyank/go/bin/golangci-lint run ./cmd/qualify/...` reports
`0 issues` with CGO disabled and task-specific `/tmp` caches;
`/tmp/instant-public-qualifier-lint.log`. Post-repair qualifier race package
passes 13.007s; `/tmp/instant-public-qualifier-lintfix-race.log`. Live campaign
and assembly proof remain PARTIAL. No candidate commits or remote actions.

Post-lint-repair actual production shell suite passes 45/45;
`/tmp/instant-public-gate-lintfix.log` (CGO disabled for the local SDK limit).

## Native package-failure blocking fix (2026-10-02)

Baseline candidate `4a2e0fcc6a0c239c71d1bc72fe4653c96ed52996`.
The owned Linux run exposed package setup/build failures omitted from native
qualification verdicts because `runNative` discarded the parser's existing
`packageFailed` map. Scope is only native qualifier source/tests and this
ledger. Planned invariant: both owned-DB and hermetic package failures prevent
PASS even when every named platform test passes; named test counts retain
their existing meaning. Actual CLI regressions cover `--log`, `--run` owned-DB
failure, and `--run` hermetic failure. Expected RED is unsafe PASS; package
checks, scoped lint and an independent re-review follow the minimal fix.
Runtime campaign proof remains PARTIAL and must use the successor candidate.

Native guard verification: RED actual CLI `--log`, owned-DB `--run`, and
hermetic `--run` all returned unsafe PASS with seven required platform tests
passing plus an unrelated package failure; retained in
`/tmp/instant-native-package-failure-red.log`. The minimal fix retains both
existing parser maps and passes their combined package-failure count to the
verdict independently of failed named tests. A package failure now yields
FAIL with `failed=0`, `package_failed=1`, and the original seven selected
identities. No parser change or new production abstraction was introduced.
Focused regression GREEN; native/parser/platform race checks PASS 1.887s;
complete qualifier race suite PASS 12.274s, including the independently owned
valid release-tag fixture. Scoped golangci-lint reports `0 issues`; diff check
passes. Logs `/tmp/instant-native-package-failure-final-race.log` and
`/tmp/instant-native-package-failure-final-lint.log`. Independent review and
fresh successor-candidate runtime qualification remain parent-owned. Live
campaign readiness is still PARTIAL; prior candidate output is not promoted.

## Explicit no-v1-parity public scope (2026-10-03)

Baseline `4d4d5fed9402484d17d0983b5e9971ffbcf094c6`, owned qualifier/policy/gate files initially clean. Owner explicitly selected “Document explicit compatibility limits; no v1 parity claim.” Scope is the existing public selection contract; legacy profile and selected v1 parity remain strict. Five live records remain mandatory. No historical differential result is promoted to PASS.

| ID | Invariant / real path | Planned evidence | Expected RED | Status |
|---|---|---|---|---|
| PV1 | Approved candidate policy external_v1=not_selected omits only CF-004/005 and v1_differential; exact35packets/5records | Actual verify-public CLI and production shell no-parity case | Current strict public policy rejects selection | PENDING |
| PV2 | Missing/unknown external selection, missing approval/owner scope, lane contradiction, unexpected parity record/fixture/packet fail closed | Focused verifier/shell negative cases | Rejection guards | PENDING |
| PV3 | external_v1=run still requires full37packets and real differential; legacy unchanged | Existing public selected and legacy package/shell cases plus missing differential regression | Missing differential fails | PENDING |

Tests first, minimum adaptation to existing selection function/manifest/verifier/gate. Source-owned policy carries exact EXCLUDED_APPROVED approval and explicit owner scope; no CLI automatic fallback. Runtime qualification and truthful packet handoffs remain parent-owned PARTIAL until successor evidence exists.

PV1 RED was the actual public production shell + verify-public CLI rejection of the new approved external_v1 field (`/tmp/instant-public-alpha-no-parity-red.log`). The existing selection function now requires run|not_selected; not_selected requires source-policy EXCLUDED_APPROVED and the exact owner sentence, omits only CF-004/005 and v1_differential identity, and retains all five mandatory live records. Manifest CLI rejects a supplied unselected record; immutable-candidate verifier rejects contradictory lanes, missing/unknown selection, incorrect/missing approval or owner scope, extra parity record/fixture/packet, and missing mandatory restore. Selected run still requires real differential evidence and37packets. No historical PASS is imported.

Contract GREEN: actual public shell selected and approved-unselected paths; unapproved exclusion rejected; exact no-parity manifest CLI35handoffs/5records/5fixtures PASS0.683s. Whole qualifier race suite PASS17.135s (`/tmp/instant-public-alpha-no-parity-final-race.log`), including legacy schema1 and selected differential rejection guards. Initial new manifest fixture collided with its own existing handoff outputs; corrected fixture --handoff-dir to generated-handoffs, preserving create-exclusive production behavior. Scoped strict lint0issues, vet, shellsyntax, formatting and diffcheck PASS. Root independent source review found no blocker; artifact worker review and actual successor runtime evidence remain parent-owned. Campaign readiness remains PARTIAL until current candidate live evidence and grounded handoffs exist.
Existing production shell regression suite also PASS48/48 (`/tmp/instant-public-alpha-no-parity-legacy-shell.log`), including tag identity and actual public verifier paths. Author-owned source and ledger final; no commits or remote actions.
