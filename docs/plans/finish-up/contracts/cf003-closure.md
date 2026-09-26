# Contract — CF-003 closure (owner-accepted residual gaps)

Repo: /Users/priyank/Developer/sideproj/instant-v2 (branch main, clean at start).
Read first: `docs/plans/finish-up/00-operating-contract.md`,
`docs/plans/finish-up/05-compatibility.md` (CF-003),
`docs/plans/finish-up/cf003-manifest-standard-decision.md`,
`docs/plans/finish-up/fu02-recorder-scope-decision.md`,
`docs/plans/finish-up/fu03-admin-presence-decision.md`,
the last two sections of `docs/plans/finish-up/execution-ledger.md`,
`docs/reference/release-envelope.md`, `corpus/manifest.json`,
`cmd/corpusctl` (the `validate-release` mode and its tests).

## Owner decision to record (2026-09-26, delegated to coordinator)

CF-003 closes as `COMPLETE / ACCEPTED_WITH_OWNER_EXCEPTIONS` for the
single-node alpha. The three remaining `gap` rows are **accepted exceptions**,
not covered:

| Row | Basis already decided |
|---|---|
| `refresh-delta-boundary` | WS recording excluded by enforcement (FU-02 Option A) |
| `rooms-presence-lifecycle` | FU-03 Option 1: admin presence is a stable 501 exclusion |
| `transactions-concurrency-gap` | 2A-8 owner DEFER (2026-09-21): no deterministic capture primitive |

The 4 `unsupported` rows stay unsupported and must remain enforced.
No row flips to `covered`. No `expectedState` text changes.

## Required rows

R1. New decision file `docs/plans/finish-up/cf003-residual-gap-exception-decision.md`
    (same style as the other decision files): the table above, the exact
    claim the alpha does NOT make for each row, and the enforcement point.

R2. Enforcement in code, fail closed: `corpusctl --mode validate-release` must
    reject the release when **any** corpus manifest row has status `gap` that
    is not listed in an explicit accepted-exception table in
    `docs/reference/release-envelope.md`, and must reject an exception-table
    entry that names a row which is not `gap` (stale exception) or does not
    exist. Parse the table the same way the existing envelope parsing works
    (reuse, don't invent a new format). Add the table to the envelope with the
    three rows.

R3. Tests in `cmd/corpusctl` (hermetic): (a) current corpus + envelope pass;
    (b) an extra unlisted `gap` row fails; (c) a listed row that is `covered`
    fails; (d) a listed row that does not exist fails; (e) removing one of the
    three table entries fails. Show each negative test failing for the right
    reason (error text asserted).

R4. `docs/plans/finish-up/program-manifest.md` row 22 →
    `COMPLETE / ACCEPTED_WITH_OWNER_EXCEPTIONS_19_COVERED_3_EXCEPTION_4_UNSUPPORTED`.
    Update the CF-003 status mention in `docs/plans/finish-up/README.md`
    status line only if it states CF-003 as partial. Update any *current* doc
    (not historical reports) that says CF-003 is partial — grep for it.

R5. Append a handoff section to the end of
    `docs/plans/finish-up/execution-ledger.md` in the existing handoff format
    (Status / Ledger table / Changes / Verification with raw command outcomes /
    Not run / Scope audit).

## Verification you must run and paste raw results of

```
go build ./...
go test ./cmd/corpusctl/... -race -count=1
go run ./cmd/corpusctl --mode validate --corpus corpus/
make validate-release
bash scripts/test-quality-release-gate.sh
git status --porcelain
```

## Leases / limits

Write only: `cmd/corpusctl/**`, `docs/reference/release-envelope.md`,
`docs/plans/finish-up/**`, and current docs that mis-state CF-003.
Do not touch `corpus/*.json|ndjson`, `internal/**`, `scripts/**`, `Makefile`.
Do NOT commit, push, tag, or run anything against remote hosts. Leave changes
in the working tree. End your reply with a short list of changed files and the
verification outcomes.
