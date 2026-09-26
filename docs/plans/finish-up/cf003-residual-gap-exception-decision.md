# CF-003 — Residual-gap exception decision (owner-accepted, single-node alpha)

**Status:** decided. CF-003 closes as `COMPLETE /
ACCEPTED_WITH_OWNER_EXCEPTIONS_19_COVERED_3_EXCEPTION_4_UNSUPPORTED`
for the single-node alpha. The three remaining `gap` rows are accepted
exceptions, not covered. No row flips to `covered`. No `expectedState`
text changes.

**Parent packet:** `docs/plans/finish-up/contracts/cf003-closure.md`
(CF-003 closure). Prior standards preserved, not rewritten:
`cf003-manifest-standard-decision.md` (13 gap rows stay gap without
checked-in raw capture + exact replay), `fu02-recorder-scope-decision.md`
(Option A report-only; WS recording excluded by enforcement),
`fu03-admin-presence-decision.md` (Option 1 stable 501 exclusion, row
stays `gap`), 2A-8 owner DEFER (2026-09-21, concurrency stays gap),
FU-01 2B close-out (2026-09-23, 19 covered / 3 gap / 4 unsupported).

**Owner decision (2026-09-26, delegated to coordinator):** the three
remaining `gap` rows are accepted exceptions for the single-node alpha.
The 4 `unsupported` rows stay unsupported and remain enforced. This
decision authorizes no product-code change, no capture run, no NDJSON,
no manifest-status flip, no `expectedState` rewrite, and no v1-parity,
provider, multi-node, replica, performance, publication, deployment, or
canary claim.

| Row | Basis already decided |
|---|---|
| `refresh-delta-boundary` | WS recording excluded by enforcement (FU-02 Option A) |
| `rooms-presence-lifecycle` | FU-03 Option 1: admin presence is a stable 501 exclusion |
| `transactions-concurrency-gap` | 2A-8 owner DEFER (2026-09-21): no deterministic capture primitive |

**Exact claim the alpha does NOT make (per row):**

- `refresh-delta-boundary` (`expectedState`: "delta and full refreshes
  converge to the same canonical result"): the alpha does NOT claim
  WS delta/full convergence through corpus evidence. The assembled
  delta leg (`TestCF003AssembledDeltaCrossTransportConvergence` @
  `6bd719e`) is durable local proof only; no WS NDJSON leg exists and
  none may be authored while the WS-recording exclusion stands.
- `rooms-presence-lifecycle` (`expectedState`: "GET
  `/admin/rooms/presence` is unsupported and returns stable 501"): the
  alpha does NOT claim a positive admin-presence join/update/leave
  lifecycle surface. It claims only the enforced 501 exclusion, proven
  by `TestCF003AssembledAdminPresenceExclusion` @ `951175c`. The row
  stays `gap` (no `covered-by-exclusion` status pattern exists).
- `transactions-concurrency-gap` (`expectedState`: "concurrent writes
  retain transaction ordering and final state"): the alpha does NOT
  claim deterministic concurrent-write ordering/final-state through
  corpus replay. The 2A-8 probe (30 fresh-DB histories → 4 distinct
  exact-byte outcomes) proved no true byte sequence exists to replay;
  the start-barrier assembled leg is boundary evidence only.

**Enforcement point (fail closed):**

- `docs/reference/release-envelope.md` carries the explicit
  accepted-exception table (`## CF-003 residual gap exceptions`) with
  exactly the three rows above.
- `corpusctl --mode validate-release` rejects the release when any
  corpus manifest coverage row has status `gap` that is not listed in
  that table, and rejects a table entry that names a row which is not
  `gap` (stale exception) or does not exist. Table parsing reuses the
  existing envelope text-parsing approach (read file, locate the
  section header, parse the markdown table's first column); no new
  envelope format is invented.
- `cmd/corpusctl` hermetic tests pin: current corpus + envelope pass;
  an extra unlisted `gap` fails; a listed row that is `covered` fails;
  a listed row that does not exist fails; removing one of the three
  entries fails. Each negative test asserts its error text.

**Non-goals (preserved):** no `corpus/manifest.json` edit, no
`corpus/*.json|ndjson` addition, no `internal/**`, `scripts/**`, or
`Makefile` change, no `expectedState`/status/oracle edit, no capture
run, no CF-002/004/005 change, no push/deployment/publication/tag.

**Coordinator review repair (2026-09-26):** the original enforcement covered
only the 3 coverage rows; the 6 `gap` *surfaces*
(`http.auth-admin-runtime-storage-backup`, `sse`, `ws.auth.tokens`,
`ws.reactive.delta`, `ws.rooms.fanout`, `ws.transact.extended`) were
ungoverned. They are now listed with their exact non-claims in
`docs/reference/release-envelope.md` § "CF-003 surface gap exceptions" and
enforced by the same fail-closed `validate-release` check (unlisted gap
surface, stale or unknown listed surface, or missing section → reject).
Corpus surface statuses and notes are unchanged.
