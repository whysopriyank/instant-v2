# Repair contract — QH-001 R1 (coordinator review findings)

Scope: the QH-001 working tree in this directory (uncommitted). Same leases as
`qh001-qualification-harness.md`. Still no product-code change, no commit, no
remote hosts. The coordinator will run the lanes on real Linux hosts after
this repair, so every verdict must be derived from observation — **a
hard-coded `true` for any verdict field is a defect**.

## F1 (blocker) — campaign.sh build lane cannot write

`build` lane mounts `/src:ro` then runs `make build` (writes `bin/instantd`).
Fix: mount `/src` read-write for the build lane only (other lanes stay `:ro`).
Record the `make build` output (`src/bin/instantd`) as THE candidate binary;
recovery/soak must execute exactly that file (copy it to `evidence/candidate/`
and run from there), not a separately built `tools/instantd`. The helper
binaries (qualify, soak, soaksetup) may stay separately built.

## F2 (blocker) — git "dubious ownership" inside the image

The container runs as root over a host-uid checkout; Go VCS stamping calls
git and fails (the FR-002 gate also runs `make build` in this image). Add
`RUN git config --system --add safe.directory '*'` to the Dockerfile. Do not
use `-buildvcs=false` (it would diverge from the gate's `make build`).

## F3 (critical) — recovery verdicts are not derived from observation

Rewrite the outcome drivers in `cmd/qualify/recovery.go` so each property is
measured, never assigned:

1. **Pipelined writers.** Add a non-blocking submit path: send `transact`
   frames without waiting, track each by client-event-id in a per-tx ledger
   (submitted → acked(server tx id) | error | unknown-at-crash). Use ≥2
   writer sessions.
2. **Subscribers.** Every crash/restart outcome runs ≥2 subscriber sessions
   with an `add-query` over the fixture attribute, recording every
   `add-query-ok` / `refresh-ok` result.
3. **Oracle comparison = convergence.** After recovery, subscribers
   reconnect/resubscribe, and PASS requires each subscriber's final query
   result (the set of values it reports) to equal exactly the DB oracle's
   value set for that app/attr. Comparing the oracle with itself is not
   convergence.
4. `crash-before-commit`: SIGKILL only once ≥1 submitted tx is still
   un-acked (assert from the ledger; if the condition is not reached within
   a bound, the outcome FAILs "precondition not reached" — never PASS).
   Verify: every acked value present; for every un-acked tx the value is
   either present or absent (single-triple tx) — AND add a multi-triple tx
   shape (≥3 triples per tx, distinct attrs or entities) so atomicity is
   actually checkable: all of a tx's triples present or none. Count partial
   txs from the DB; >0 → FAIL.
5. `crash-after-commit`: SIGKILL immediately after receiving an ack for which
   at least one subscriber has NOT yet received the refresh containing that
   value (assert from subscriber observations; bounded attempts, else FAIL
   "precondition not reached"). After restart the value is present and
   converged subscribers see it.
6. `crash-during-publication`: SIGKILL while ≥1 refresh-ok has been received
   for the current write wave and ≥1 subscriber is still missing it (i.e.
   mid fan-out); use ≥4 subscribers and a burst of writes. Same bounded
   precondition rule.
7. `postgres-restart`: subscribers + pipelined writers active during the
   restart; instantd is NOT restarted (assert same PID before/after). RTO =
   DB-ready → first new acked write AND all subscribers converged. Every
   acked value present; every errored tx absent-or-atomic; subscribers
   converge to the oracle.
8. Drain outcomes: send **SIGTERM** (`syscall.SIGTERM`), not SIGINT.
   `drain-moderate` / `drain-saturated` use pipelined writers across
   multiple sessions; saturated must submit faster than acks return
   (demonstrate from the ledger: max outstanding un-acked ≥ 8 at SIGTERM).
   Record for every tx submitted before exit: acked+present, or explicit
   error / socket close with its observed close code (capture the real
   websocket close status), or never-acked → must be absent-or-atomic in DB.
   Every acked tx (from the client ledger, not a pre-snapshot) must be in the
   DB after exit. Record real exit status and real close codes.
9. Remove every hard-coded verdict assignment (`Converged = true`,
   `UnackedAtomic = true`, `InFlightResolved = true`, `CloseCodes =
   []string{"normal"}`, etc.) — each must come from a computed check.
10. Write a per-outcome artifact with the ledger (submitted/acked/errored
    ids, server tx ids), subscriber final sets, oracle set, partial count,
    precondition evidence, timings, exit status, close codes. The record's
    `artifacts` must include these files.

Tests: extend `verdict_test.go` with injected observations for each new
failure mode (precondition not reached, subscriber diverged, partial
multi-triple tx, acked-but-missing from client ledger, SIGTERM exit > 30 s,
PID changed during postgres-restart). Where possible, unit-test the ledger /
convergence comparison functions directly with fakes.

## F4 (major) — soak mapping guesses the schema

`deriveLedgerCount` guesses field names and treats any non-empty state as
submitted and refreshed as acked. Bind strictly to `cmd/soak`'s real output:
read `CompletenessManifest` (`cmd/soak/evidence.go`) and the per-tx ledger
artifact it lists (check `cmd/soak/ledger.go` for the exact event/state
names). Require `status=="complete"`, `completed==true`, `summary.success`.
Define and document: committed = distinct server tx ids acknowledged;
acknowledged = entries in state acknowledged (or later states that imply an
ack, listed explicitly from ledger.go); refreshed = entries whose own refresh
was observed; dropped/unresolved = from summary AND recomputed from ledger —
mismatch → FAIL. Unknown field/state names → FAIL (no fallback guessing).
Update soak tests to use a fixture produced in the real `cmd/soak` evidence
format (generate it by calling the real evidence writer in the test if it is
reachable; otherwise a fixture that mirrors the struct tags exactly, with a
test that fails if the struct tags change).

## F5 (minor)

- `instantd.env` uses `INSTANT_V2_INSECURE_DEV_SECRETS=1`. Replace with
  per-campaign generated secrets passed via environment at start time (never
  written to the env file or evidence), using whatever variables
  `internal/config` requires for non-dev startup; keep the env file
  non-secret. If config requires secrets in-file, stop and report instead.
- Delete the stray built binary `./qualify` at the repo root.

## Verification (paste raw)

```
gofmt -l cmd/qualify
go vet ./cmd/qualify/...
go test ./cmd/qualify/... -race -count=1
bash scripts/qualify/test-campaign.sh
bash scripts/test-quality-release-gate.sh
grep -n '= true$' cmd/qualify/recovery.go
git status --porcelain
```

Also run the recovery driver **locally** once against a local disposable
PostgreSQL if `psql`/`pg_ctl` are available (`CGO_ENABLED=0` build of
instantd), and paste the lane JSON — or say explicitly it was not run.
Update the QH-001 ledger section with an R1 subsection.
