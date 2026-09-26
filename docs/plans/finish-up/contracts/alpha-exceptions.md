# Contract — AX-001 alpha exception record

Owner direction 2026-09-26 (see `docs/plans/finish-up/completion-run-20260926.md`):
the first release is an **explicit alpha** (`single-node-alpha`). The packets
below are **not claimed** for this release. They are not complete, not
accepted, and not excluded-as-unsupported features — they are claims the alpha
does not make. This packet records that decision and makes every current doc
and the release envelope say so consistently.

| Packet | New manifest state | What the alpha does NOT claim |
|---|---|---|
| DA-006B provider acceptance | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | Google/GitHub OAuth verified against real providers (local contract DA-006A is accepted; provider round-trip unverified) |
| DA-008B magic-code provider | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | real email delivery of magic codes |
| CF-004 pinned-v1 environment | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | any v1 runtime comparison |
| CF-005 differential | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | v1 wire/behaviour parity or drop-in replacement |
| OP-004 container runtime | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | qualified container image (gate lane `container: not_selected`) |
| OP-006 backup/restore drill | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | a drilled restore RPO/RTO (DA-003 fail-closed backup remains accepted) |

Rows already `NOT_SELECTED` (OP-001/002, QR-002, QR-004, FR-003/004) keep
their state; add the same "not claimed for alpha" wording in the decision
file. TD-001..005 stay `DEFERRED`.

## Required rows

R1. `docs/plans/finish-up/alpha-exception-decision.md` — the table above,
    owner/date, and the rule: none of these may be claimed in any current doc
    until its packet is re-selected and completed on a new candidate.

R2. `docs/plans/finish-up/program-manifest.md` — update the six rows' state
    cells exactly as above; add a one-paragraph note under the table pointing
    at the decision file.

R3. `docs/reference/release-envelope.md` — add (or extend) a
    "Not claimed in this alpha" section listing the six packets with the claim
    text. Keep any existing parsed tables byte-compatible with
    `cmd/corpusctl --mode validate-release` (run it).

R4. Current public/operator docs (`README.md`, `UPGRADE.md`,
    `docs/README.md`, `docs/guides/07-selfhost.md`,
    `docs/reference/quality-scorecard.md`, `docs/reference/quality-verification.md`,
    `docs/plans/04-roadmap.md`, `docs/plans/finish-up/README.md`): the release is
    labelled **alpha** wherever release status is stated; no sentence implies
    production readiness, provider-verified OAuth/email, v1 parity, container
    qualification, or restore drill. Grep for `production-ready`, `production
    ready`, `v1 parity`, `drop-in`, `parity`, `provider`, `restore`,
    `container` and fix only false/overbroad claims. Historical reports stay
    untouched (they are labelled historical).

R5. Append a handoff section to `docs/plans/finish-up/execution-ledger.md`
    (existing format) listing every edited sentence (file:line before → after).

## Verification (paste raw output)

```
make validate-release
go run ./cmd/corpusctl --mode validate --corpus corpus/
bash scripts/test-quality-release-gate.sh
git diff --stat
```

## Leases / limits

Docs only (the files above plus `docs/plans/finish-up/**`). No code, scripts,
Makefile, corpus, or workflow changes. Do NOT commit.
