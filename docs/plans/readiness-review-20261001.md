# Readiness review — 2026-10-01

Reviewed source: `421d69f5ab4c1ed4ff524b7d8c6a51873e50a610` on `main`, initially clean.
Accepted candidate: `f7dc5b10327a3b6a31d540c426e62d710d4078af`.
This report adds no product changes, changes no release selection, and grants no external authority.

## Assessment

**The project has passed its selected internal single-node alpha gate. It has not published a public alpha and is not production-qualified.**

This is an advanced implementation and qualification stage. The backend is real: PostgreSQL catalog/triple storage and migrations, transactions, CEL write permissions, query execution, WS/SSE subscriptions, delta/incremental refresh, single-node rooms, local durable files, admin/runtime APIs and database backup import/export exist. It is beyond a skeleton or prototype. The remaining work combines specific missing behavior with compatibility, operations and release evidence.

HEAD differs from the accepted candidate in seven documentation files only. The reviewed product source therefore matches the accepted candidate. Acceptance remains bound to the original candidate and binary; a rebuild from HEAD embeds a new revision and cannot reuse the old campaign as a new release acceptance.

| Milestone | Current judgment |
|---|---|
| Internal/source alpha for controlled single-node testing | Accepted within DEC-001 scope on September 26 |
| Published public alpha | Not shipped; bounded next milestone proposed in DEC-002 |
| General InstantDB v1 replacement | Not demonstrated; selected differences and unsupported behavior remain |
| Single-node production | Not qualified; operating budgets, restore, supported auth/compatibility and rollout proof remain |
| HA/read replicas | Not selected or qualified; conditional correctness gaps remain |

There is no defensible completion percentage or calendar ETA from the available evidence. Packet counts give equal weight to very different work and include deliberate exclusions. Public alpha is a defined next release; production first needs an explicit supported scope and acceptance criteria.

Authority: [accepted envelope](../reference/release-envelope.md), [finish-up manifest](finish-up/program-manifest.md), [acceptance evidence register](finish-up/evidence-alpha-20260926f.md), [proposed DEC-002](next-release/dec-002-selection.md). DEC-002 awaits owner confirmation and has not superseded DEC-001.

## Tests: retained acceptance and fresh checks

The local acceptance archive was inspected directly at `/Users/priyank/Developer/sideproj/instant-v2-evidence/alpha-20260926f/evidence-alpha-20260926f.tgz`. Its archive and manifest SHA256 values match the register. All 49 manifest/record artifact references match their recorded sizes and hashes; the archive contains 60 files. This verifies artifact integrity, rather than relying solely on status prose.

| September 26 accepted campaign | Verified result |
|---|---|
| Native Linux, owned PostgreSQL, race lane | 1,891 named test/subtest outcomes passed, zero skipped, zero package failures; 77 platform checks |
| Separate short/race lane | 1,889 passed, two expected reporting-test skips, zero package failures |
| Recovery | 7/7 passed: crash before commit, after commit, during publication; PostgreSQL restart; idle, moderate and saturated drain |
| Release gate | Recorded success for candidate `f7dc5b1`, with matching candidate/binary/configuration identities |

The two test lanes repeat tests; their pass counts must not be added and presented as unique tests. Recovery establishes the selected restart/drain outcomes, not backup restoration or multi-node recovery.

| October 1 fresh check on reviewed HEAD | Result |
|---|---|
| `CGO_ENABLED=0 GOFLAGS=-json make test-unit` | PASS: 33 package passes, three packages without tests; 1,508 named test/subtest passes, 300 skips, zero failures; race enabled |
| Release-gate contract tests, included above | 44 passed, zero failed |
| Installed `golangci-lint run ./...` | PASS: zero issues |
| `make vet check-generated` | PASS; generated Go/TypeScript protocol artifacts match |
| `CGO_ENABLED=0 go build -o /tmp/instant-v2-review-instantd-20261001 ./cmd/instantd` | PASS |
| `CGO_ENABLED=0 make validate-release` | PASS: 18 authored regression scenarios, 26 coverage rows, zero captured-v1 scenarios |

The fresh unit lane explicitly disables integration and clears database URLs. Its 300 named skips include database integrations and short-mode exclusions, so it is not fresh database-backed qualification. Database/recovery/soak acceptance above comes from the retained Linux campaign. No new long soak, comparative campaign, container, restore or provider acceptance was run; remote GitHub Actions status was not checked.

Initial local attempts were blocked by sandbox socket/shared-memory/cache restrictions and then by the workstation's Apple SDK linker rejecting `arm64e.x1` architecture entries. The documented `CGO_ENABLED=0` configuration completed with race detection. These failed attempts are environment failures, not established product regressions. No SDK, harness or system configuration was repaired.

Fresh logs: `/tmp/instant-v2-review-unit-cgo0-20261001.log`, `/tmp/instant-v2-review-static-20261001.log`, `/tmp/instant-v2-review-lint-20261001.log`, `/tmp/instant-v2-review-corpus-20261001.log`.

## Soak and performance status

“SOAP” is interpreted here as **soak**, the repository's terminology.

| Accepted soak measurement | Result |
|---|---|
| Concurrent sessions | 500 |
| Active write duration | 900 seconds, plus 60-second ramp and 60-second settle |
| Write rate | Eight transactions/second total, not per session |
| Acknowledged / committed / refreshed transactions | 7,200 / 7,200 / 7,200 |
| Dropped / unresolved transactions | 0 / 0 |
| Refresh frames | 3,600,500 including initial snapshots; approximately 4,000/second during writes |
| Transport lag | p50 23.288 ms; p99 48.274 ms; maximum 55.576 ms |
| RSS | Peak 95.95 MiB; final approximately 95.09 MiB |
| Resources | File descriptors settle at 526; PostgreSQL connections remain 24 |

This is positive evidence for **one whole-table query group using delta-refresh at SDK wire version ≥0.23**. Pre-0.23 full-envelope clients at this fanout are explicitly unqualified. It does not prove 5,000 sessions, saturation, diverse queries/permissions, tenant contention or multiday memory stability. The original 5k ×30-minute aspiration was not a requirement of the accepted alpha.

Lag is explicitly labeled `transport_diagnostic` with `semantic_claim:false` in [cmd/soak/main.go](../../cmd/soak/main.go). It correlates a writer transaction with that writer session's refresh watermark, rather than independently proving content convergence on all 500 recipients. The retained events contain progress/lifecycle records, not an exported per-transaction ledger; the gate's transaction counts derive from successful harness quiescence certification in [cmd/qualify/soak.go](../../cmd/qualify/soak.go). RSS/descriptors/database connections are sampled; CPU, goroutine and queue-depth time series are absent. These limits bound the claim rather than invalidate the selected alpha acceptance.

**Comparative performance: the supported harness and report tooling exist; qualified v1-versus-v2 measurement is outstanding.** The unit lane exercises benchmark tooling, and the performance workflow uses a small v2 smoke plus synthetic pairs. Neither establishes a performance advantage. Historical August comparisons used old revisions, short/single runs and transport-frame accounting; v1 coalescing could look like loss, and the raw comparative bundle is not retained. No current “N× faster” claim is supportable.

The [binding benchmark contract](../reference/13-benchmark-contract.md) requires semantic recipient/convergence accounting, equivalent isolated fixtures, pinned served identities, seven balanced seeded AB/BA pairs per selected cell, retained failed outcomes and paired confidence intervals. DEC-002 proposes resource efficiency and convergence latency, plus a cold-start/idle-RSS amendment. Its narrowed measurement selection must be reconciled with the existing broader contract before measurement; no post-hoc selection of favorable cells.

## Concrete missing or limited product behavior

| Area | Current implementation and missing work | Milestone consequence |
|---|---|---|
| Dynamic read permissions | [instaql/query.go](../../internal/instaql/query.go) rejects non-admin dynamic view rules before fetching protected rows; literal open/closed rules work | Explicit alpha exclusion; implement if per-user/per-record read authorization is promised |
| Runtime magic-code email | [daemon assembly](../../cmd/instantd/routes.go) never supplies a `Mailer`; [authn.go](../../internal/authn/authn.go) fails before generating a code; HTTP returns 503 | Real delivery adapter/configuration is missing; supplying provider credentials alone is insufficient. Admin code generation through a capture mailer is separate and works |
| Direct ID-token sign-in | [authn/http.go](../../internal/authn/http.go) deliberately returns 501 pending a server-issued one-time nonce lifecycle | Unimplemented acceptance path; authorization-code OAuth is a separate implemented path |
| v1 backup files | [backup/v1zip.go](../../internal/backup/v1zip.go) skips `files/` blobs while importing metadata | DA-009 must restore bytes and verify links/hashes before promising complete v1 migration |
| Restore target policy | [backup/import.go](../../internal/backup/import.go) and import helpers permit merging/upserts into nonempty apps | DA-010 selects explicit refusal of nonempty targets. Existing SQL transactions/checksum rollback work; import is not wholly non-atomic |
| Sync and streams | [sync/session.go](../../internal/sync/session.go) returns unsupported responses for dedicated sync/stream operations | Excluded from current and proposed alpha; working query subscriptions/delta-refresh are different features |
| Admin presence | [adminapi/schema.go](../../internal/adminapi/schema.go) returns stable 501 | Positive admin presence view is absent; WS room presence exists |
| Multi-node rooms | [sync/rooms.go](../../internal/sync/rooms.go) keeps state in a process map; full snapshots rather than patch-presence | Cross-node collaboration absent; patches remain a compatibility/bandwidth difference |
| Object/S3 backup assembly | [daemon routes](../../cmd/instantd/routes.go) mount backup without an object store; object routes remain 503 | Deliberate exclusion; local durable storage is implemented |
| Multi-node invalidation recovery | [invalidation.go](../../cmd/instantd/invalidation.go) supervises listener reconnection, but retains one publisher connection and only logs later failures | OP-001 required before promising peer convergence through PostgreSQL outages |
| Read replicas | Reader/writer pools exist without a qualified replica-visibility contract | OP-002 must define a barrier, primary fallback or explicit consistency policy before replica claims |

WAL tailing is a separate implemented/tested component, not the daemon's serving path. Current serving uses post-commit notifier calls and an optional LISTEN/NOTIFY peer bus. Integrating WAL is not automatically necessary for a single-node production product; the chosen serving path must meet its declared recovery/consistency contract.

Persisted rules are **not generally missing**: current admin permission checks, signup, WS manager and runtime transactions resolve persisted rules. Older README/header text implying unwired rules should not become an implementation backlog item without a concrete uncovered path.

## Ordered roadmap

1. **Reconcile and confirm public-alpha scope.** Amend DEC-002's selected T1/T2 wording for magic-code email, direct ID-token sign-in and admin presence: implement them or register explicit supported differences. Reconcile the selected performance matrix with the benchmark contract. Then update record contracts and gate profile (`QR-003R`).
2. **Finish container distribution and qualification (`OP-004D/OP-004`).** Existing scratch image, CA roots and non-root user are useful foundations. Add the selected built-in healthcheck, OCI metadata and reference compose deployment; correct alpha tags. Qualify the exact image with PostgreSQL, persistent storage, TLS, transact/subscription and SIGTERM cleanup.
3. **Prove selected v1 compatibility (`CF-004/CF-005`).** Boot the official pinned v1 with proven served identity and equivalent fixtures. Capture allowed/denied selected surfaces and run differential checks. Fix discrepancies or approve exact differences. Eighteen authored v2 regressions do not establish v1 parity.
4. **Complete restoration (`DA-009/DA-010 → OP-006`).** Restore v1 blobs, refuse nonempty targets under the selected policy, then prove fresh-target logical/object equality and unchanged-target rejection of corrupt, truncated, wrong-app and oversized input. Record realistic RPO/RTO.
5. **Run qualified comparative measurement (`EV-007 → QR-002`).** Freeze workloads first; measure semantic convergence, CPU and RSS, plus selected cold start/idle RSS, with the agreed paired methodology. This is required for the proposed performance claim, not inherently for publishing an alpha without one.
6. **Finish publication tooling (`QR-004`).** Add the proposed publish workflow, signing/SBOM/provenance and post-publish digest verification; dry-run first. Current `.goreleaser.yaml` is scaffolding and still includes `latest`, contrary to the proposal's alpha-only policy. No publish workflow or reference compose directory currently exists.
7. **Qualify one new immutable candidate.** Rerun existing selected Linux/recovery/soak lanes plus newly selected container/restore/compatibility/performance lanes. Bind all records to the same source, binary, configuration and campaign.
8. **Publish on explicit grant (`FR-003`).** The proposal names `v0.2.0-alpha.1`, GHCR and GitHub Releases. Selection is not publication authority. Public/private visibility and protected tags remain owner decisions.

For **production**, define a separate supported profile after these results. Prioritize backup/restore and upgrade operations, supported authentication, permission/compatibility behavior for target apps, representative longer-load/capacity evidence, monitoring/operator runbooks and actual rollout/rollback proof. HA, replicas, S3, dynamic views and streams become required only when the chosen product promises them. Billing/dashboard and multi-cloud clustering are explicit project non-goals, not missing production prerequisites.

## Documentation findings and follow-up

These findings are recorded here for prioritization; no source or existing policy was changed.

- [README](../../README.md) still implies permission persistence is unwired and benchmark methodology is pending; current September closures supersede parts of that wording.
- [Original roadmap](04-roadmap.md) still says two scenarios and recorder future work. Current inventory has 18 authored WS scenarios and bounded HTTP/SSE recording tooling; WS recording remains unsupported. Old unchecked phases are not the current execution ledger.
- [Self-host guide](../guides/07-selfhost.md) describes serving through logical replication and says frozen SDKs work unmodified. The daemon uses post-commit notification, and v1 parity is expressly unclaimed. Its quickstart also needs reconciliation with current required storage configuration before a public release.
- DEC-002 selects admin presence and magic-code/id-token contracts while carrying forward exclusions/unclaimed provider behavior. This is a scope inconsistency to resolve before the next gate, not evidence that those features are already implemented.
- Benchmark contract and proposed narrower DEC-002 matrix need an explicit reconciled selection before comparative claims.
- The graph index reports August 24 metadata and excludes `cmd/instantd`; changed split files are not reliably represented. Graph discovery was attempted first, then conclusions were checked against current source. Refresh the index before relying on it for later exhaustive reviews.

Deferred lifecycle/origin, storage reconciliation and modularity debts remain in [phase 09](finish-up/09-deferred-tech-debt.md). Promote them only when the selected production scope or fresh evidence makes them material; do not turn release preparation into general refactoring.

## Review boundary

This is a source-backed readiness and roadmap review of central serving, auth, permission, storage, recovery, test and release paths, with retained artifact verification and fresh hermetic/static checks. It is not exhaustive proof that every code path is defect-free. No product fixes, remote campaigns, publication, deployment or external messaging occurred.
