# CF-003 — Manifest-standard decision (13 gap rows stay gap)

**Status:** decided. All 13 `corpus/manifest.json` coverage rows stay
`gap`. CF-003 stays `PARTIAL /
HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING`
(`program-manifest.md` line 35, untouched).

**Precedent:** `docs/plans/finish-up/fu03-admin-presence-decision.md`
(FU-03, Option 1). That packet rewrote one row's `expectedState` to
the enforced 501 exclusion at HEAD `239be84` and kept the row `gap`.
This packet rewrites nothing: no `expectedState` text, no status, no
oracle field. It records the standard under which any future flip is
judged.

**Decision (conservative, evidence-preserving):** no gap→covered flip
without checked-in raw capture evidence + corpus replay under the
"exact" oracle standard. Package/route tests — including the accepted
assembled-route legs — are durable local proof but are not corpus
acceptance and cannot flip a row by themselves.

**Proof (durable local, accepted on `main`, cited not duplicated):**
the per-gap assembled-route mapping lives in
`docs/plans/finish-up/execution-ledger.md:904-1047` (13 rows, each
with its exact oracle test + acceptance SHA, missing-flip statement,
and why-gap-remains). That mapping is the cited evidence here; this
file duplicates none of its 13 rows.

**Flip routing (future packets only, none started here):**
- Multi-client rows (`rooms-fanout-positive`,
  `query-concurrency-gap`, `refresh-convergence-concurrency`) flip
  only via the FU-01 recorder path: `followup-packets.md` FU-01
  (multi-client capture contract) → managed-record multi-client
  capture + checked-in NDJSON + corpus replay.
- Single-flow rows (HTTP/SSE assembled legs) flip only after the
  FU-02 scope decision (`followup-packets.md` FU-02, Option A
  report-only vs Option B managed-record extension), then by
  checked-in raw capture + corpus replay of the already-accepted
  leg — never by report-only text change.
- Captured-v1 rows (any claim of v1 parity on any of the 13) flip
  only via FU-04 external evidence: CF-004 pinned-v1 environment +
  CF-005 differential, both `BLOCKED / EXTERNAL_EVIDENCE`
  (`followup-packets.md` FU-04). Every checked-in scenario oracle
  today is `regression` from the authored v2 contract, not captured
  v1.
- `rooms-presence-lifecycle` is already decided under FU-03 Option 1
  (`expectedState` = enforced 501 exclusion, row stays `gap`); a
  positive-surface flip would require FU-03 Option 2 implementation
  + capture, not this packet.

**Non-goals (preserved):** no `corpus/manifest.json` edit, no
`program-manifest.md` edit, no code/test change, no capture run, no
NDJSON, no CF-002/004/005 change, no push/deployment/publication/tag.
