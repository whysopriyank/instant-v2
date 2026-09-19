# CF-003 / CF-004 / CF-005 bounded follow-up packets (proposed, not started)

Scope: docs-only definition of the structurally-blocked remainder after
`302b138` ("CF-003: honest closure mapping, gaps stay structural").
No code, no tests, no `corpus/manifest.json` edit, no program-manifest
status change is made or authorized by this file.

Terminal state preserved (see `program-manifest.md` + `execution-ledger.md`):
CF-002 `COMPLETE / ACCEPTED_CANDIDATE_BOUND_LOCAL_CAPTURE`;
CF-003 `PARTIAL / HTTP_SSE_PERMISSION_MULTICLIENT_TRANSACTION_ROOM_DELTA_ACCEPTED_MATRIX_PENDING`;
CF-004/005 `BLOCKED / EXTERNAL_EVIDENCE`.
Nothing in this file flips any of those rows.

Oracle standard referenced below ("exact") means: whole-result
`reflect.DeepEqual` (lengths before elements where stated), exact float
`processed-tx-id` compare, exact status/body strings, exact
session/presence snapshots. A subset/ID-only check does not satisfy it.
Per-gap assembled-leg mapping lives in the `execution-ledger.md`
"CF-003 honest closure" section; this file defines only what a future
bounded packet would need, not that any of it is done.

---

## FU-01 — Multi-client stream capture contract

**Status:** proposed, not started. No work selected or begun here.

**Goal:** Define the raw-evidence contract whose existence would make it
possible, in a future packet, to flip these three gap rows by replay
(not by package/route tests): `rooms-fanout-positive`,
`query-concurrency-gap`, `refresh-convergence-concurrency`.

**Scope (if ever selected):**
- Transports: SSE multi-client only. HTTP single-flow and WS are
  explicitly out of this packet (WS is excluded by enforcement, see
  FU-02 constraint).
- Assembled legs already accepted on `main` that this contract would
  need to reproduce via capture + replay:
  - `rooms-fanout-positive`: `TestCF003AssembledRoomFanoutSSE` @ `58dab88`
    (ordered presence fanout, resync convergence, set-presence fanout,
    peer-only broadcast with exact sender session, leave → exact
    one-member snapshot; lengths-before-elements DeepEqual).
  - `query-concurrency-gap` + `refresh-convergence-concurrency`:
    `TestCF003AssembledSSEQueryConcurrencyOrderedSnapshot` @ `778d348` +
    `19ec8e9` (two SSE subscribers through a concurrent barrier observe
    one identical ordered whole-result snapshot; exact float watermark
    compare; one committed admin change converges both to identical
    refresh frames at the exact tx with exact final titles).
- Oracle standard: "exact" as defined above; each captured refresh must
  carry its exact triggering transaction ID and float watermark, and
  replay must reproduce the identical ordered whole-result snapshot on
  every selected subscriber.

**Non-goals:**
- Not a recorder implementation, not a capture run, not a manifest flip.
- Not WS capture (excluded by enforcement).
- Not single-flow HTTP/SSE report-only flips (those belong to FU-02).
- Not v1 parity (that belongs to FU-04).
- Not a concurrency fault-scheduling campaign beyond what replay proves.

**Acceptance criteria (for a future packet, none satisfied here):**
1. A written multi-client capture contract exists: subscriber count,
   shared-room/shared-query topology, barrier or ordering rule,
   per-subscriber raw stream retention, canonical derivation, and the
   exact replay oracle (whole-result DeepEqual + exact float watermark
   + exact tx ID per refresh).
2. The contract states how late-join, resync, leave, and quiescence are
   captured and distinguished from absence of evidence (bounded quiet
   period, not an unbounded wait).
3. The contract is compatible with CF-002 provenance/security invariants
   (candidate-bound, fresh private redacted atomic write-once output,
   no secret or database URL in manifests).

**Estimated shape (if ever executed):**
- Docs: 1 contract doc + ledger appendix entry.
- Tests: new managed-record multi-client test(s) + replay leg(s);
  existing single-flow tests untouched.
- Manifest rows: 3 gap rows (`rooms-fanout-positive`,
  `query-concurrency-gap`, `refresh-convergence-concurrency`) each gain
  a checked-in NDJSON leg + replay; no other row changes.

---

## FU-02 — Recorder scope extension decision

**Status:** proposed, not started. No scope change made here.

**Constraint (recorded, not a task):** WS recording stays excluded by
enforcement. CF-002 `record --transport ws` fails closed with no
artifact (`TestCF002WSRecordCreatesNoArtifact`); no WS NDJSON leg may
be authored or accepted via any packet defined here. WS gap rows
(`refresh-delta-boundary` WS variant, `transactions-cardinality-boundary`
WS variant, WS room lifecycle cross-reference) cannot flip while this
enforcement stands.

**Goal:** Obtain an explicit owner decision on whether single-flow
HTTP/SSE flips may proceed as report-only scope changes (checked-in raw
capture + corpus replay of the already-accepted assembled leg, no
recorder behavior change) versus as a managed-record extension
(new supported single-flow capture path in `corpusctl`), with WS
remaining excluded either way.

**Single-flow candidate legs this decision would govern** (all accepted
on `main`, none flipped here):
- `auth-http-refresh-lifecycle` (`TestCF003AssembledAuthRefreshBatchAndSignout` @ `a686613`).
- `auth-http-denied-error` (`TestCF003AssembledAuthMagicCodeDenied` @ `356d68d`).
- `query-conjunction-gap` (`TestCF003AssembledQueryConjunctionAndOrder` @ `31d5539` + `b293a5c`).
- `refresh-sse-lifecycle` (`TestCF003AssembledSSERefreshAndReconnect` @ `ec78ad7`).
- `transactions-rollback-error` (`TestCF003AssembledTransactionMatrix` @ `ec78ad7` + `TestCF003AssembledTransactionTransportMatrix` @ `43ed443`).
- `transactions-cardinality-boundary` SSE/HTTP variants (@ `43ed443` + `ab0d75b` + `e2d97ae` + `dcc8373` + `255e28b`).
- `transactions-lookup-lifecycle` (`TestCF003AssembledSSELookupLifecycle` @ `63b82d0` + `2340195` + `f904b42`).
- `transactions-concurrency-gap` HTTP convergence (@ `ec78ad7` + `43ed443`; start barrier encourages but does not force DB-level overlap).

**Non-goals:**
- No recorder code, no capture runs, no NDJSON authored, no manifest
  edit under this file.
- No WS scope change (excluded by enforcement, above).
- No multi-client contract (FU-01) and no v1 parity (FU-04).
- No rewriting of `expectedState` (that belongs to FU-03).

**Acceptance criteria (for the decision, none satisfied here):**
1. Owner selects exactly one: (A) report-only — single-flow HTTP/SSE
   legs may flip named gap rows to covered by checked-in raw capture +
   corpus replay with no `corpusctl` behavior change; or (B)
   managed-record extension — a new bounded recorder packet is required
   first, and no gap row flips until that recorder packet is ACCEPTed.
2. The decision explicitly re-affirms WS exclusion by enforcement.
3. The decision lists which of the 8 candidate legs above are in scope
   for the selected path (all, subset, or none).

**Estimated shape (after decision, if ever executed):**
- Option A (report-only): docs (decision record + per-leg mapping) +
  checked-in NDJSON capture(s) + corpus replay test(s); manifest rows
  flip gap → covered per listed leg only; no `corpusctl` change.
- Option B (managed-record extension): 1 recorder implementation packet
  (flags/behavior/tests/docs/reviews) followed by per-leg capture +
  replay packets; manifest rows flip only after the recorder packet is
  ACCEPTed.

---

## FU-03 — Admin-presence surface decision

**Status:** proposed, not started. No `expectedState` rewrite made here.

**Goal:** Resolve the explicit recorded divergence for
`rooms-presence-lifecycle`: manifest `expectedState` still reads
"admin presence view reflects join, update and leave lifecycle"
(positive surface) while product enforces `GET /admin/rooms/presence`
→ stable 501 unsupported (`TestCF003AssembledAdminPresenceExclusion`
@ `951175c`: exact 501 to an authorized caller, still 501 with live WS
room presence behind it, 401 missing/foreign without disclosing state).
Row stays `gap` until the owner selects one of the two options below.

**Non-goals:**
- No `expectedState` text changed by this file.
- No positive admin-presence implementation, no capture, no manifest
  flip under this file.
- No change to the enforced 501 behavior under either option until a
  selected follow-up packet implements its acceptance criteria.

**Option 1 — Rewrite `expectedState` to the 501 exclusion.**
- Acceptance criteria: owner approves exact replacement text stating
  the admin-presence surface is unsupported and returns stable 501;
  the replacement text cites the enforced behavior (authorized 501,
  live-presence-behind-it 501, missing/foreign 401 without disclosure);
  a future docs-only packet applies the text change + replay/contract
  evidence that the 501 exclusion holds, with no positive-surface claim.
- Estimated shape: docs (decision record + manifest-notes entry) +
  1 manifest `expectedState` text edit + exclusion replay/assertion;
  no product-code change (behavior already enforced).

**Option 2 — Select a positive admin-presence surface.**
- Acceptance criteria: owner selects the exact positive surface
  (routes, auth, join/update/leave lifecycle semantics, exact oracle);
  a future implementation packet builds it behind the selected routes;
  a future capture packet checks in raw admin-presence capture + corpus
  replay proving join/update/leave convergence under the "exact"
  oracle standard; only then does the row flip to covered.
- Estimated shape: 1 implementation packet (product code + tests +
  reviews) + 1 capture/replay packet (NDJSON + replay) + manifest row
  flip; strictly larger than Option 1.

---

## FU-04 — CF-004/005 external evidence (out of local scope)

**Status:** proposed, not started. Explicitly out of local scope.

**Goal (stated for completeness, not selected):** Name the
authority/fixture a future CF-004/005 effort would need before any
captured-v1 flip on any of the 13 gap rows.

**What is needed (none present or claimed here):**
- CF-004: a qualified pinned-v1 runtime + service authority capable of
  producing captured-v1 oracles (every checked-in scenario oracle today
  is `regression` from the authored v2 contract per
  `corpus/manifest.json` `oracle.kind`; no captured v1 oracle exists).
- CF-005: a differential harness + independent review comparing pinned
  v1 behavior against the candidate on the selected rows, per the
  `program-manifest.md` evidence levels (qualified v1 runtime;
  compatibility + independent review).

**Non-goals (explicit):**
- No v1 environment is provisioned, pinned, or qualified by this file.
- No external endpoint is contacted; no frozen-v1 parity is claimed.
- No differential is run; no independent review is requested.
- No gap row flips to a captured-v1 oracle under any FU-01/FU-02/FU-03
  packet; all such flips await CF-004/005 `BLOCKED / EXTERNAL_EVIDENCE`
  rows turning green through their own authority-gated packets.

**Acceptance criteria:** none satisfiable locally. A future CF-004
packet must present the qualified v1 runtime + authority evidence; a
future CF-005 packet must present the differential + independent review.
Until then every captured-v1 flip remains blocked.

**Estimated shape (if ever authorized externally):**
- Docs: authority/fixture qualification record + ledger entries.
- Tests/fixtures: pinned-v1 fixture + differential legs (outside local
  scope; not estimated further here).
- Manifest rows: affected gap rows gain captured-v1 oracles only after
  both CF-004 and CF-005 acceptance; no local packet may pre-flip them.

---

## What this file does not change

- `corpus/manifest.json`: untouched; all 13 coverage rows stay `gap`.
- `program-manifest.md`: untouched; CF-002 stays COMPLETE, CF-003 stays
  PARTIAL, CF-004/005 stay BLOCKED, all other rows unchanged.
- Code and tests: untouched; no new implementation, no new capture, no
  NDJSON authored.
- Ledger history: preserved; the pointer appendix (if added alongside
  this file in the same commit) cites this file without altering any
  recorded verdict.
