# Qualification campaign (QH-001 harness)

How the coordinator runs the DEC-001 single-node-alpha Linux qualification
campaign with the harness built by contract
`docs/plans/finish-up/contracts/qh001-qualification-harness.md`. This packet
builds the harness only; it does not run the campaign and makes no
product-code change.

## What the harness produces

Three gate-schema external records plus the campaign manifest that
`scripts/quality-release-gate.sh` consumes:

| Lane | Packet | Record details |
|---|---|---|
| `native` | OP-003 | `{native, platform_checks}` from owned-DB + hermetic `go test -json` |
| `recovery` | OP-005 | `{max_rto_seconds, max_drain_seconds, outcomes[7]}` exact-state verdicts |
| `soak` | QR-001 | `{sessions, active_seconds, committed, acknowledged, refreshed, dropped, unresolved}` from soak evidence |

Canonical identity shared by all lanes of one campaign:

- `endpoint_sha256 = sha256("ws://instantd:8080/runtime/session")`
  (`qualify` computes this when `--endpoint-sha` is omitted).
- `configuration` = per-campaign non-secret env file
  `<workdir>/qualify/instantd.env` (DSN injected via environment only, never
  written into the file or printed by any subcommand). The file carries no
  secrets: per-campaign secrets (`INSTANT_V2_STORAGE_SECRET`,
  `INSTANT_V2_STORAGE_ROOT`, the four `INSTANT_OAUTH_*` values required
  whenever `DATABASE_URL` is set) are generated fresh by `campaign.sh` for
  each invocation and passed to lane processes via the environment at start
  time only. `qualify recovery/soak` refuse non-dev startup without them
  and refuse `INSTANT_V2_INSECURE_DEV_SECRETS` outright.
- THE candidate binary is `make build` output (`src/bin/instantd`),
  recorded by the build lane at `evidence/candidate/instantd`. The build
  lane mounts `/src` read-write (all other lanes stay `:ro`); recovery and
  soak execute exactly `evidence/candidate/instantd`, and later lanes
  refuse to run when its sha256 differs from the build lane's recorded sha.
  (`bin/` is gitignored, so the candidate tree stays clean; the FR-002 gate
  re-runs `make build` in the same image, which is why `gate.sh` also mounts
  `/src` read-write and why the image marks every checkout
  `safe.directory` instead of using `-buildvcs=false`.)

## Prerequisites (coordinator checks on the real Linux host)

- Ubuntu, Linux 6.8, x86_64, Docker 29, **no Go on the host**. The host runs
  unrelated containers: the harness only creates/removes `iv2q-<campaign>-*`
  resources (anything else is refused by `require_prefix`), uses a private
  Docker network, publishes only `127.0.0.1:55432` (postgres),
  `127.0.0.1:18080` (recovery candidate), `127.0.0.1:18081` (soak
  candidate), and removes everything on exit (trap), success or failure.
- A `git bundle` of exactly one clean commit; the harness clones it to
  `<workdir>/src` and asserts `git rev-parse HEAD == CANDIDATE_SHA` and a
  clean tree before every lane.
- The assumptions below under "Determinism" hold (verified by the build
  lane's double-build check; a mismatch fails the lane).

## Exact commands

```sh
CANDIDATE=<40-char sha>
CAMPAIGN=<id, [a-z0-9-], e.g. alpha-20260920>
HOST_ID=<uname -n or asset id>
WORKDIR=<private dir, e.g. /srv/qualify/$CAMPAIGN>

git bundle create /tmp/candidate.bundle HEAD   # on a clean $CANDIDATE checkout
scp /tmp/candidate.bundle host:$WORKDIR/../candidate.bundle

# 1. build (image, double-build determinism check, binary sha)
bash scripts/qualify/campaign.sh --campaign "$CAMPAIGN" --candidate "$CANDIDATE" \
  --bundle "$WORKDIR/../candidate.bundle" --workdir "$WORKDIR" --lane build --host-id "$HOST_ID"

# 2. native (OP-003 lane result; go test runs INSIDE the qualification image)
bash scripts/qualify/campaign.sh --campaign "$CAMPAIGN" --candidate "$CANDIDATE" \
  --bundle "$WORKDIR/../candidate.bundle" --workdir "$WORKDIR" --lane native --host-id "$HOST_ID"

# 3. recovery (OP-005 lane result; qualify runs on host so --pg-restart-cmd
#    executes in host context; candidate + DB stay owned/prefixed)
bash scripts/qualify/campaign.sh --campaign "$CAMPAIGN" --candidate "$CANDIDATE" \
  --bundle "$WORKDIR/../candidate.bundle" --workdir "$WORKDIR" --lane recovery --host-id "$HOST_ID"

# 4. soak (QR-001 lane result; ~15 min active plus ramp/settle)
bash scripts/qualify/campaign.sh --campaign "$CAMPAIGN" --candidate "$CANDIDATE" \
  --bundle "$WORKDIR/../candidate.bundle" --workdir "$WORKDIR" --lane soak --host-id "$HOST_ID"

# 5. records (one per lane; --cleanup complete only because each lane wrote
#    evidence/<lane>/cleanup.complete after verified removal)
BIN_SHA=$(cat "$WORKDIR/evidence/candidate/binary.sha256")
CFG_SHA=$(sha256sum "$WORKDIR/qualify/instantd.env" | awk '{print $1}')
FIXTURE="instant_bench_qh001_<campaign>"   # owned DB identity per lane logs
for LANE in native recovery soak; do :; done  # placeholders; real commands:
qualify record --lane-result "$WORKDIR/evidence/native/lane.json" \
  --campaign "$CAMPAIGN" --candidate "$CANDIDATE" --binary-sha "$BIN_SHA" --config-sha "$CFG_SHA" \
  --fixture "$FIXTURE" --host-id "$HOST_ID" --host-os linux --host-kernel "$(uname -r)" \
  --host-arch amd64 --host-runtime "go1.25.14" --cleanup complete \
  --artifact "native/lane.json:<size>:<sha>" --out "$WORKDIR/evidence/records/native.json"
# ... same for recovery/lane.json -> records/recovery.json and soak/lane.json -> records/soak.json
# The recovery record MUST list the per-outcome evidence files as artifacts
# (evidence/recovery/recovery-<outcome>.json for all 7 outcomes: ledger with
# submitted/acked/errored ids + server tx ids, subscriber final sets, oracle
# set, partial count, precondition evidence, timings, exit status, close
# codes). The gate checks every artifact path/size/sha, and the outcomes
# entries themselves carry exactly {id,result} (close codes live only in
# those files, never in details).
for O in crash-before-commit crash-after-commit crash-during-publication postgres-restart drain-idle drain-moderate drain-saturated; do :; done

# 6. manifest (handoffs from the coordinator-authored input file; refuses
#    wrong packet sets and non-GREEN states, then self-validates)
qualify manifest --evidence-root "$WORKDIR/evidence" --campaign "$CAMPAIGN" --candidate "$CANDIDATE" \
  --binary "candidate/instantd:<size>:$BIN_SHA" --configuration "qualify/instantd.env:<size>:$CFG_SHA" \
  --campaign-started-at "<RFC3339 UTC>" --native-record records/native.json \
  --recovery-record records/recovery.json --soak-record records/soak.json \
  --handoffs handoffs-input.json --out manifest.json

# 7. gate (validates pre-existing evidence; never provisions it)
bash scripts/qualify/gate.sh --campaign "$CAMPAIGN" --candidate "$CANDIDATE" --workdir "$WORKDIR"
```

## Budgets (declared, frozen for the campaign)

- Recovery RTO (DB back → first acked write + converged subscribers): ≤ 3600 s
  (integer seconds, ceil). Drain exit: ≤ 30 s with listen-port release,
  recorded exit status, and every in-flight request either acked+durable or
  failed with an explicit error/close code (observed close codes recorded).
- Drain load levels: idle 0 tx/s (one serving probe, then SIGTERM once both
  subscribers show its refresh); moderate 8 tx/s sustained (20 s settle
  before SIGTERM); saturated unthrottled flood (≫64 tx/s) until the ledger
  demonstrates submit-faster-than-ack (max outstanding un-acked ≥ 8) with
  live delivery to both subscribers — flood size, max outstanding, close
  codes, and timings are all recorded in the per-outcome artifact.
- Soak: `-sessions 500 -duration 1020s -ramp 60s -settle 60s
  -global-tx-rate 8 -max-p99-lag 10s -sdk-version 0.23.0` (cmd/soak's
  duration spans ramp and settle, so 1020 s gives 900 s of active writes;
  `active_seconds` = duration − ramp − settle; `-sdk-version` negotiates
  delta-refresh, recorded as `workload.sdk_version`); RSS sampled via `ps`, 1 GiB ceiling
  enforced by the existing soak gate semantics; RSS/open-fds/PG-connections
  sampled every 10 s to `soak-timeseries.jsonl`.
- Soak mapping rule: counters come strictly from the soak's own
  completeness manifest (`status == "complete"`, `completed == true`,
  `summary.success == true`) plus the events/ledger artifact its
  `artifacts` list names, using cmd/soak's exact field and `EntryState`
  names (`submitted/acknowledged/refreshed/resolved/terminal`; unknown
  names fail the lane). `committed` = distinct acknowledged server tx ids;
  `acknowledged` = entries in ack-implying states; `refreshed` = entries
  with their own refresh observed; `dropped`/`unresolved` come from the
  summary cross-checked against the ledger (mismatch fails). Without
  per-tx entries the mapping uses the manifest's summary-certified counts
  (`success == true` certifies quiescence: every submitted tx acked and
  resolved, zero drops) and records the certification basis in the lane
  result (`derived_from_ledger`, kept out of gate details whose keys are
  exact).

## What each outcome proves (and does not prove)

- `crash-before-commit`: writers in flight, SIGKILL with ≥1 submitted tx
  un-acked. Proves every acked tx present exactly and every un-acked tx
  all-or-nothing (full triple set per tx in PostgreSQL, zero partial). Does
  not prove multi-node publisher behavior (OP-001, excluded).
- `crash-after-commit`: SIGKILL after acks, before refresh delivery. Proves
  acked state durable and resubscribed clients converge to the DB oracle.
- `crash-during-publication`: SIGKILL under active refresh delivery + write
  load. Proves post-restart convergence, no acked tx missing.
- `postgres-restart`: DB container restarted via `--pg-restart-cmd` with
  instantd untouched. Proves writes resume, acked txs present, subscribers
  converge; measures RTO. Does not prove replica/lag policy (OP-002,
  excluded).
- `drain-idle/moderate/saturated`: SIGTERM at 0/8/64 tx/s. Proves exit
  ≤ 30 s, port release, acked durability, explicit in-flight accounting.
  Does not prove behavior beyond the saturated rate.
- Soak: proves 500 sessions × 900 s with per-tx accounting
  (ack == committed == refreshed, zero dropped/unresolved). This is the
  alpha envelope, not the 5k×30m DEC-001 ceiling (see QR-001).
- Native: proves the reviewed platform-sensitive list (process identity,
  `/proc`, descriptors, signals, advisory locks, replacement/atomic write,
  cleanup) passed natively on Linux with zero skips. Any listed category
  without a passing test, or any non-Linux/cross-compiled execution, FAILS.

## Determinism assumptions (build lane verifies, coordinator must preserve)

`make build` (`go build -o bin/instantd ./cmd/instantd`) must be
byte-identical across the build lane's two builds and the gate's rebuild:
same qualification image digest
(`golang:1.25-bookworm@sha256:3b4a…36437`, GOTOOLCHAIN=local), same fixed
`/src` mount path, same clean VCS state at the same commit, no
VCS/timestamp stamping in ldflags. Go build IDs hash content (not mtimes),
so checkout time is irrelevant. If the host substitutes any of these, the
build lane fails closed before evidence exists.
