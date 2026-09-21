# 2A-6 — Cardinality row manifest-scope decision (narrow expectedState)

**Status:** decided. Owner-selected Option A (approve narrowing) for packet
CF-003-2A6, recorded 2026-09-21 in the finish-up working session in response
to the packet's BLOCKED handoff (independent review ses_f3cbbdf0dffeNx6ifrgVAybOaP).

**Parent scope:** `docs/plans/finish-up/followup-packets.md` FU-02 (Option A
report-only, decided in `fu02-recorder-scope-decision.md`). That decision
authorizes single-flow flips by checked-in raw capture + exact corpus replay,
never by report-only text change, and reserves `expectedState` rewrites to
owner decisions (FU-03 precedent). This file is the owner decision the
2A-6 BLOCKED verdict requires. No other row is affected.

**Decision:** coverage row `transactions-cardinality-boundary` is narrowed to
the evidenced SSE behavior. Exact replacement `expectedState`:

> cardinality-one final-value convergence and exact validation denial preserve final state

The row keeps its id, family (`transactions`), case (`boundary`), surface
(`ws.transact.extended`), owner, `transport: sse` (WS variant remains excluded
by enforcement), `status: covered`, regression oracle, and evidence
(`transactions-cardinality-boundary.json`). The row `note` carries the explicit
residual sentence: merge/cascade/required legs remain unflipped as future work
(no accepted assembled SSE-capturable leg proves deep-merge preservation
convergence, cascade-delete convergence, or required-field denial over the
single-subscriber SSE path; merge and delete exist only as live
`/admin/transact` boundary evidence in the neighboring
`transactions-rollback-error` replay; required retraction is WS corpus evidence
only and WS recording stays excluded by enforcement).

**Why narrowing, not expansion or split:** the SSE cardinality + validation-
denial evidence is exact and complete for the narrowed claim (ACCEPTed in
substance by independent review, including the terminal probe); no assembled
SSE legs exist for the dropped behaviors, so expansion would require new
implementation scope outside FU-02 report-only; a split row was considered and
rejected as unnecessary churn while the residual is tracked here, in the row
note, and in the execution ledger.

**Non-goals (preserved):** no other row change; no WS evidence or claim; no
v1-parity claim; no recorder change; no multi-client scope; no product change.
