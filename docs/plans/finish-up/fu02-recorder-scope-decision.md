# FU-02 — Recorder scope decision (Option A: report-only single-flow capture)

**Status:** decided Option A. No `corpusctl` recorder behavior change is
authorized by this packet. WS recording stays excluded by enforcement.

**Parent packet:** `docs/plans/finish-up/followup-packets.md` FU-02
(proposed). That file is untouched by this packet; no terminal row
(`program-manifest.md`, `corpus/manifest.json`, `execution-ledger.md`
verdicts) is flipped and no CF-002/004/005 material is touched.

**Owner decision (this invocation, DEC-001 single-node-alpha finish-up):**
single-flow HTTP/SSE flips proceed as report-only scope changes —
checked-in raw capture + exact corpus replay of the already-accepted
assembled leg, with no `corpusctl` behavior change. No general
single-flow recorder expansion unless evidence proves it is necessary.

**Constraint (reaffirmed, not a task):** WS recording stays excluded by
enforcement. CF-002 `record --transport ws` fails closed with no
artifact (`TestCF002WSRecordCreatesNoArtifact`); no WS NDJSON leg may
be authored or accepted under this decision. WS gap rows
(`refresh-delta-boundary` WS variant, `transactions-cardinality-boundary`
WS variant, WS room lifecycle cross-reference) cannot flip while this
enforcement stands.

**Single-flow legs in scope (all accepted on `main`, none flipped here):**
- `auth-http-refresh-lifecycle` (`TestCF003AssembledAuthRefreshBatchAndSignout`).
- `auth-http-denied-error` (`TestCF003AssembledAuthMagicCodeDenied`).
- `query-conjunction-gap` (`TestCF003AssembledQueryConjunctionAndOrder`).
- `refresh-sse-lifecycle` (`TestCF003AssembledSSERefreshAndReconnect`).
- `transactions-rollback-error` (assembled transaction matrix +
  transaction transport matrix).
- `transactions-cardinality-boundary` SSE/HTTP variants only (WS variant
  excluded by the constraint above).
- `transactions-lookup-lifecycle` (`TestCF003AssembledSSELookupLifecycle`).
- `transactions-concurrency-gap` HTTP convergence (start barrier
  encourages but does not force DB-level overlap; boundary evidence
  only, not a fault-scheduling campaign).

Each flip additionally requires its checked-in raw capture leg + corpus
replay under the "exact" oracle standard (whole-result `reflect.DeepEqual`
with lengths before elements, exact float `processed-tx-id` compare,
exact status/body strings, exact session/presence snapshots) — never a
report-only text change. Captured-v1 claims on any row still await
FU-04 external evidence (CF-004/005, both `BLOCKED / EXTERNAL_EVIDENCE`).
Multi-client rows (`rooms-fanout-positive`, `query-concurrency-gap`,
`refresh-convergence-concurrency`) are out of this packet; they route via
the FU-01 managed multi-client recorder path. `rooms-presence-lifecycle`
is already decided under FU-03 Option 1 (enforced 501 exclusion).

**Non-goals (preserved):** no recorder code, no capture runs, no NDJSON
authored, no manifest edit under this file. No WS scope change. No
multi-client contract (FU-01) and no v1 parity (FU-04). No rewriting of
`expectedState` (FU-03, already decided Option 1).
