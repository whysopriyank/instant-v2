# RT-001 independent security review - 2026-09-17

Candidate SHA: bd7191508907014a161e7ea2f55b88b4ffff46e1 (main, clean).
Reviewer: rt001-security-reviewer (read-only reviewer/security_reviewer).
Close-out: docs/plans/finish-up/execution-ledger.md (RT-001 close-out).

Recovery: no durable raw review output exists for the close-out citation
(docs/plans/finish-up/rt001* absent; only unrelated review-fix-85298d3.md
for the earlier 85298d3 A1/A2/S1 scope). Ledger states 10 targets for the
11-item contract. Fresh independent read-only review of the exact accepted
candidate, not a reconstruction. Close-out count corrected 10 to 11.

Scope: groups.go 1-586, rebind.go 1-176, session.go 1-321,
session_query.go 1-231, sse.go 1-575, admin_sse.go 1-254,
reactive.go (Frame 25-42, Subscription 44-103, lease 105-122,
snapshot/clear 141-201, Store 203-315, drain/stamp 767-845),
incremental.go (epoch/bail 184-318, seed 323+), routing.go,
refresh.go 1-106, routes.go 77-180, runtime.go 111-140, invalidation.go,
ws.go (bound 20-21, initial-answer guard 260-325, send 190-240),
RT-001 regression/live tests.

Verdict: ACCEPT. All eleven targets falsified (defect not demonstrable).
No unresolved blocker. No implementation change requested or made.
Residual single superseded WS envelope is the ratified bounded exception
(DEC-001-rt001-bounded-rebinding-20260917), not a finding.

## Falsification table

| # | Target | Result | Evidence |
|---|--------|--------|----------|
| 1 | stale allow-gate reuse | FALSIFIED | rebind.go:42-61 reload newest LOADED doc each generation; rebind.go:58-60 load error fails closed; refresh.go:45-47 pre-run rebind fail-closed; refresh.go:62-75 post-run GateHash recheck drops superseded; routes.go:127-139 production mirror; reactive.go:780-785 epoch stamped before compute; groups.go:154-159 Emit admission refuses stale; groups.go:545-546 per-member WS check; rebind.go:148-167 SpliceAuthorize fail-closed; incremental.go:227-228,247-249,258-260,265-267,280-282,310-312 epoch bails; session_query.go:18-24 never reuse blindly. |
| 2 | more than one superseded WS envelope | FALSIFIED | groups.go:539-549 fan-out loop, decisive 545-546 return errGenerationSuperseded stops spread; groups.go:154-159 Emit admission; reactive.go:819-831 Emit failure retries never publishes; reactive.go:833-841 refused Publish retries; groups.go:140-144 at most one recall-impossible remainder documented, uncertified, healed same txID; ws.go:305-309,315-319 initial-answer close; sse.go:485-498 SSE mirror. |
| 3 | stale snapshot or watermark commit | FALSIFIED | rebind.go:82-92 clear-then-bump; rebind.go:125-131 rebind twin; reactive.go:186-201 atomic ClearServedState; rebind.go:96-118 PublishGeneration under delivery lease, decisive 110-116 snapshot-then-watermark; groups.go:387-413 flight, decisive 403-410 epoch check under groupsMu; reactive.go:149-162 conservative SnapshotPair order; reactive.go:812-845 commit order; routes.go:99-141 production twin. |
| 4 | stale queued SSE delivery | FALSIFIED | sse.go:54-64 sseEvent carries epoch plus exact sub; groups.go:531-537 SendRawGen carries epoch; sse.go:364-372,373-381 enqueue with guard; sse.go:70-115 dequeue holds generation lease through bounded write plus flush, decisive 102-113 drops superseded; sse.go:303-312 drop-tolerant loop; admin_sse.go:180-225 coordinated flight, decisive 204-212,223-225 guarded initial send; admin_sse.go:238-253 stream guard; sse.go:470-498 pre-enqueue recheck. |
| 5 | fail-open on rule lookup errors | FALSIFIED | rebind.go:36-39,58-60 load error returns err, old gate never served; refresh.go:42-47,69-74 rerr drops generation; routes.go:112-117,132-138 production snapshot path; session_query.go:139-143 attach-time 503 rules-unavailable; groups.go:197-200 version-bound reload error to errRulesReload; session_query.go:161-164 maps to 503; session.go:187-200 fail-closed invariant; sse.go:463-469 initial-refresh failure ends stream. |
| 6 | mixed-generation group membership | FALSIFIED | groups.go:92-113 key mixes admin bit; groups.go:184-222 join reloads fresh doc, decisive 214-219 rebinds only if cur2 equals cur; rebind.go:120-131 rebindGroupLocked swap plus clear plus bump; groups.go:342-349 epoch-checked fast path; groups.go:351-414 single-flight one outcome; rebind_group_test pins TestVersionStaleJoinRebindsGroupBeforeAdmission, TestVersionMismatchedJoinRebindsGroup; live TestSteadyDenySecondCommitDoesNotLeak. |
| 7 | late allow replacing newer deny | FALSIFIED | groups.go:201-222 delivery lease plus under-groupsMu identity recheck, decisive 214-219 completed swap wins; rebind.go:66-70 converge on intervening gate; rebind.go:71-78 content gate; rebind.go:46-51,62-65 order delivery lease then groupsMu; pinned by TestAdmissionDelayedLoadPreservesNewerGate (blocked allow, intervening deny, deny preserved). |
| 8 | teardown plus admission resurrection | FALSIFIED | groups.go:309-324 DetachAll sets closing before snapshot, decisive 313-316; groups.go:168-177 entry check; groups.go:204-213 post-IO check; groups.go:250-259 final commit check plus rollback; session.go:148-152 closing flag; groups.go:263-273 cap-failure rollback; pinned by TestAdmissionRejectsClosingSessionAfterBlockedLoad (zero group, Store, cap, Subs residue) and TestAdmissionRecreatesGroupRemovedDuringLoad. |
| 9 | cancellation or subscription-pointer ABA | FALSIFIED | reactive.go:61-66 Gen monotonic never reused; rebind.go:88-92 bump-last; reactive.go:111-122 WithCurrentGeneration refuses cancelled or mismatched; reactive.go:448 cancelled predicate; reactive.go:270-307 Remove marks cancelled closes cancelCh hook outside lock, decisive 290-304; reactive.go:244-252 re-arm only nil or cancelled; reactive.go:84-86,644,659,776-797 timers plus drain revalidate; groups.go:123-161 fresh Subscription per group no pointer reuse; sse.go:102-113 exact pointer check. |
| 10 | lock-order inversion | FALSIFIED | order delivery lease then groupsMu then sub.mu: rebind.go:46-51,62-65,104-105,123-124; reactive.go:105-107 never wait on delivery under registry lock; reactive.go:186-201 swap takes sub.mu once under groupsMu; groups.go:201-205 admission delivery then registry; groups.go:281-307 detach registry then group then session; groups.go:351-364,416-433 flight lock never across groupsMu or refresh IO; groups.go:403-410 check-then-commit; incremental.go:188-250,305-312 snapshot sub.mu release across authorize plus IO; sse.go:102-113 per-sub lease only; reactive.go:300-305 Remove hook outside store lock. No reverse acquisition read. |
| 11 | unbounded IO while holding global registry lock | FALSIFIED | rebind.go:46-58 rule load outside locks; groups.go:194-200 unlock across rulesFor; session.go:190-200 loader; groups.go:375-397 refresh outside groupsMu; reactive.go:767-845 drain outside registry; groups.go:145-148 no registry lock across writes; groups.go:79-90,442-446 member snapshot; groups.go:517-563 send loop unlocked; sse.go:68,70-95 encode before lease bounded 10s write plus flush; ws.go:20-21,190-240 bounded 10s write shed wedged peers; groups.go:566-575 failMember; pinned TestH01gBlockedWriterDoesNotStallOtherGroups. |

## Non-blocking findings

- Rule-doc cache has no TTL; freshness from Invalidate (catalog_cache.go:76)
  plus per-generation reload (rebind.go:42-61). Consistent with ratified
  observed-swap boundary (linearize at observed swap, not DB commit). Noted.
- Legacy nil-hook fallbacks keep pre-RT-001 semantics, safe only because
  production injects hooks: nil rebind (refresh.go:20-23,53-57), nil Publish
  (reactive.go:842-845), nil Authorize (incremental.go:40-54). Test-only;
  production wiring routes.go:148-157 and runtime.go:129-135. Keep test-only.
- Incremental.materialize (incremental.go:323+) seeds without own epoch check;
  relies on outer Emit/Publish refusal plus Authorize bail. Optional future
  defense-in-depth Gen guard. No leak observed.

## Consistency

- program-manifest RT-001 remains COMPLETE / ACCEPTED_BOUNDED_REBINDING.
- RT-002 remains PARTIAL.
- Release-envelope policy unchanged by this artifact.
- Implementation changes requested: none. Read-only review, zero repair cycles.
