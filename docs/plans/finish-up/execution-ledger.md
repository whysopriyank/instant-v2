# Finish-up execution ledger

Active objective: verify and repair the implementation, commit accepted work,
then complete selected finish-up phases in dependency order.

Baseline: `26a1caf9856110b711315aaed4c5cbeaec3bbc36` on `main`.
The initial working tree contains 13 user-owned modified files spanning realtime,
storage, tests, and decision documents. Preserve these changes while repairing
the remaining defects. No push or deployment is selected by this objective.

| ID | Invariant / production path | Planned evidence / red expectation | Scope | Status |
|---|---|---|---|---|
| RT-001 | Permission revocation coordinates with initial answers, queued delivery, refresh fan-out and publication | Deterministic revocation at publication boundaries; current partial-send exception does not establish original acceptance | reactive, sync, daemon assembly; architecture review first | PARTIAL |
| RT-002 | Failed delivery has an explicit recovery outcome and reconnect establishes matching state/watermark | Actual notifier retry and transport reconnect tests, including same-tx corrective delivery | reactive, sync | PARTIAL |
| RT-003 | Queue depth one reopens after drain | Existing deterministic gate tests; rerun focused tests | reactive/config | PENDING |
| DA-001-R | Failed root confirmation cannot be trusted by a later upload, even if directory cleanup fails | Inject root-sync failure and failed cleanup, then retry; current Stat shortcut bypasses confirmation | storageapi backend and durability tests | PENDING |
| F-001/F-002 | Current candidate evidence and decision selections are consistent | Reconcile canonical ledger after accepted code; distinguish pending policy edits from approval | finish-up and reference docs | PARTIAL |

Verification starts with exact regression tests, then affected package race tests,
static checks and build. Database/corpus acceptance must run against an explicitly
owned test database; skipped tests are not acceptance. High-risk changes receive
independent review before completion. Remaining selected packets are enumerated
in `program-manifest.md`; this ledger does not replace or narrow that program.

## H-00 baseline (2026-09-07T15:51:40Z, go1.27.0 darwin/arm64, HEAD 26a1caf main)
- Porcelain: 13 modified tracked + untracked execution-ledger.md + session-recovery-patch-contract.md (preserved, no reset/stash).
- Diff fingerprint (pre-repair): cb9674d2f612ab57e5ccadc98b276729d3b3739fdb4e65676d5acf8a1a1deaf6.
- Graph: codebase-memory-mcp instant-v2 ready but excludes cmd/instantd + docs by design; used direct source reads for H-01 paths.
- PostgreSQL: Homebrew 17.11, wal_level=logical, superuser priyank, owned isolated DBs via testkit (DATABASE_URL postgres://priyank@localhost/postgres, CREATEDB). No default DB claimed disposable; fixtures create/drop instant_test_* only.
- Corpus: manifest 18 scenarios; integration via INSTANT_TEST_INTEGRATION=1 + DATABASE_URL (per corpus/README + Makefile test-contract, no --suite flag invented).

## H-01 authorization publication (design REJECT→repair→re-review; residual WS wire window pending, not accepted)
- Design: revocation linearization = observed Gen epoch at next refresh/attach boundary (not DB commit, not completed barrier). Wire bytes irrecoverable. Partial-generation allowance (WS at most one superseded envelope, uncertified, healed via retry at same txID; SSE zero via dequeue drop) REMAINS PENDING owner ratification.
- Repairs: ClearServedState atomic + clear-then-bump; RefreshGate/attach I/O outside groupsMu (cap recheck); version-bound fast path; WS/SSE/admin initial-answer fail-closed on nil snapshot + post-render Gen recheck (S1 fix); SSE sseEvent queue with Store.Get dequeue drop + SendRawGen/SendGen (fixed lost SendRaw across SSE init); admin SSE via snapshotOrRefresh flight; WS 10s write timeout (wsWriteTimeout); splice apply outside sub.mu with Gen rechecks + commit guard.
- Matrix: H-01a Emit check (TestEmitRejectsSupersededGeneration) GREEN; H-01b mid-fan-out (TestSwapMidFanOutStopsSpreadAndRefusesCommit 1-frame pinned as pending + TestMidFanOutSwapHealsThroughNotifierRetry healing) GREEN; H-01c Publish refusal (same test commit leg) GREEN; H-01d flight (TestFlightRejectsSupersededRefresh) + fast-path miss (TestH01dFastPathRacingClearIsNotReused) + post-publish fail-closed decision (TestH01dPostPublishSwapFailsClosed) GREEN; H-01e splice bail (TestH01eSpliceOverlappingRevokeBails) GREEN; H-01f SSE queue (TestH01fQueuedSSEEnvelopesDroppedAfterRevoke, 5 stale, Store wiring pinned) GREEN; H-01g blocked-writer independence + detach (TestH01gBlockedWriterDoesNotStallOtherGroups) + 10s bound GREEN (attach rule-load stall fixed); H-01h fail-closed (TestSyncFailsClosedOnRulesLoadError + TestRealtimeRuleOutageDropsAndRecovers + TestSupersededGenerationDrops, live) GREEN.
- Reviews: independent architecture/security REJECT with 7 must-fix (initial queue/mat/slow/rule-only/partial-smuggle); post-repair re-review caught S1 fallback (fixed to fail-closed); final re-review notes only residual WS single-envelope + rule-only indefinite as pending (no approval invented).

## H-02 recovery/watermarks/client (partial, WS live pinned; SSE live gap remains)
- TestMidFanOutSwapHealsThroughNotifierRetry (real notifier+retry, fake transport) GREEN -race x2.
- TestLiveReconnectConvergesAfterDrop (live PG loopback, two members, drop, rejoin to equal result+watermark, liveness both directions) GREEN -race.
- TestSupersededGenerationDrops (barrier-parked refresh, deny persist, superseded drop) GREEN live.
- Gaps: no live SSE reconnect leg; delta-member live reconnect not separately pinned (splice bail unit + steady-deny live cover oracle, not live delta reconnect); same-tx corrective-frame client application requires pinned SDK version (slice-collecting bytes is not client proof) — recorded, not claimed.

## H-03 storage (no code change needed; barrier coverage added)
- 5 required tests GREEN -race x2 + full storageapi package GREEN -race.
- New TestDiskBackendSecondUploadWaitsForRootConfirmation (barrier entry signal, 500ms non-ack bound, release completes both) GREEN -race x2 — pins cross-goroutine fsync ordering the 32-writer success test does not.
- Rollback trace (calls==2) holds; external/precreated-dir trust + single-process createMu documented in code (backend.go confirmAppDir EEXIST path); multi-process shared-root out of scope, not claimed as crash proof.

## H-04 verification (owned fixtures, no push/deploy)
- Focused storage (6 tests incl. new barrier) -race x2 GREEN; focused sync Swap/MidFanOut -race x2 GREEN; H01 matrix -race x2 GREEN; H01e -race x2 GREEN.
- Six packages -race GREEN: sync, reactive, storageapi, cmd/instantd, backup, config. go vet (same six) clean. git diff --check clean.
- Live: INSTANT_TEST_INTEGRATION=1 DATABASE_URL=postgres://priyank@localhost/postgres TestLiveReconnectConvergesAfterDrop + TestSupersededGenerationDrops GREEN; RT-001a/b/e + steady-deny GREEN live.
- Corpus: validate GREEN; TestCorpusReplayIntegration 05-permission-deny PASS; 11/17 pagination FAIL preserved pre-existing (instaql/CF territory, untouched by this patch; ledger records stash-proven pre-existing, not re-proven here to preserve dirty tree).

## H-05 docs (no new approval invented)
- Preserved prior dirty decision edits (02-realtime RT-001a pending-ratification bound; envelope SSE-correction pending + approval-scoping sentence) without broadening approval.
- This ledger + truth-ledger appendix record actual dirty fingerprint, pending WS single-envelope + rule-only-indefinite decisions, and live vs hermetic scope. No self-approval of missing security evidence; no inherited green from different tree.

## H-06 review/commit (independent, local only)
- Reviewer inputs: requirement table, exact diff, raw outcomes above, skipped (SSE live reconnect, delta live reconnect, SDK corrective-frame proof), draft handoff. Findings repaired: S1 fallback, SSE Store wiring, attach missing Unlock (deadlock), splice/mat ordering, WS timeout.
- Commit: stage reviewed realtime + storage + tests together (realtime shared interfaces + daemon wiring + tests land together); storage barrier test lands in same train (accepted with realtime; separately reviewable). No git add . indiscriminate; inspect diff --cached --check + staged diff. No push/deploy.
