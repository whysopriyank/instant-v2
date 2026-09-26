# Contract — QH-001 Linux qualification harness

New bounded packet (assembly gap): nothing in the repo produces the three
external records that `scripts/quality-release-gate.sh` requires
(`native_linux` → OP-003, `recovery` → OP-005, `soak` → QR-001), nor the
campaign manifest it consumes. This packet builds that harness. It does **not**
run the qualification campaign (coordinator does that afterwards on the real
hosts) and makes **no product-code change** (`internal/**`, `cmd/instantd/**`
are read-only; if you believe a product change is needed, stop and report it).

Read first: `docs/plans/finish-up/00-operating-contract.md`,
`docs/plans/finish-up/06-topology-recovery.md` (OP-003, OP-005),
`docs/plans/finish-up/07-qualification-release.md` (QR-001, QR-003),
`scripts/quality-release-gate.sh` (THE schema authority — read every jq
predicate), `scripts/test-quality-release-gate.sh`, `scripts/quality-soak.sh`,
`scripts/soak-process-identity.sh`, `cmd/soak/**` (ledger + evidence output),
`cmd/soaksetup/**`, `cmd/chaos/**` (process identity, instantd start/stop,
waitHealth — reuse ideas, do not modify), `Makefile`, `Dockerfile`,
`.github/workflows/ci.yml` (pinned tool installs).

## Execution model (the coordinator will run it this way)

- Target hosts: Ubuntu, Linux 6.8, x86_64, Docker 29, **no Go on the host**.
  Hosts run unrelated containers: every resource you create is named
  `iv2q-<campaign>-*`, uses a private Docker network, publishes no ports
  beyond 127.0.0.1, and is removed on exit (trap), success or failure.
  Never stop/modify anything not carrying the prefix.
- Candidate transport: coordinator copies a `git bundle` of one clean commit to
  the host; the harness clones it to `<workdir>/src` and asserts
  `git rev-parse HEAD == CANDIDATE_SHA` and clean status.
- Toolchain image `scripts/qualify/Dockerfile` (the "qualification image"),
  FROM `golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437`
  (Go 1.25.14, GOTOOLCHAIN=local); installs `jq`, `make`, `git`, `procps`,
  `postgresql-client`, and golangci-lint v2.13.1 via the exact curl +
  `sha256sum -c` install used in `.github/workflows/ci.yml`. It must be able to
  run the full `make test-release` gate later (needs git, jq, shasum, make,
  golangci-lint). The source is always mounted at the fixed path `/src`
  (reproducible build path).
- Database: `postgres:17@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232`
  with `-c wal_level=logical`, as a sibling container on the private network,
  fresh per lane.
- Candidate binary: built once per campaign inside the qualification image
  with exactly `make build` in `/src` (the gate later re-runs `make build` in
  the same image and compares sha256 — it must be byte-identical; verify this
  by building twice in your hermetic reasoning and document the determinism
  assumptions: same image digest, same path, same clean VCS state).

## Deliverables

### D1. `cmd/qualify` (Go, stdlib + existing deps only)

Subcommands, each writing machine-readable JSON and never printing DSNs or
secrets:

- `native` — runs, inside the qualification image against the owned DB:
  `INSTANT_TEST_INTEGRATION=1 go test ./... -race -count=1 -json` (the same
  selection as `make test-integration`) plus the hermetic short lane; parses
  `go test -json` final actions per (package,test). `selected_count` = passed,
  `skipped_count` = skipped in the owned-DB run (must be 0 for PASS; any fail
  → FAIL). `details.platform_checks` = number of passed tests whose package or
  file is platform-sensitive (process identity, `/proc`, descriptors,
  signals, advisory locks, filesystem replacement/atomic write, cleanup) —
  select them by an explicit, reviewed list in code, and make the command
  FAIL if any listed test did not run and pass. `details.native=true` only
  when `runtime.GOOS=="linux"` and the binary under test is not
  cross-compiled (GOOS/GOARCH == host).
- `recovery` — drives the **candidate binary** (`--instantd PATH`) against the
  owned DB and executes exactly these 7 outcomes, each on a fresh app/fixture,
  each producing an exact-state verdict (not liveness/counts):
  - `crash-before-commit`: writers submit transactions; SIGKILL instantd while
    ≥1 submitted tx has no ack. After restart: every acked tx present exactly;
    every un-acked tx is atomically all-or-nothing (verify each tx's full
    triple set in PostgreSQL); zero partial.
  - `crash-after-commit`: SIGKILL immediately after acks are received and
    before the matching refresh reaches subscribers. After restart: acked
    state present; resubscribed clients receive a result equal to the DB
    oracle.
  - `crash-during-publication`: SIGKILL while subscribers are actively
    receiving refreshes under write load. After restart + reconnect: every
    client's final result equals the DB oracle; no acked tx missing.
  - `postgres-restart`: restart the DB container (command supplied via
    `--pg-restart-cmd`, run by the host orchestrator) while clients and
    writes are active, **without** restarting instantd. Writes resume, acked
    txs present, subscribers converge to the oracle. Measure RTO (DB back →
    first successful acked write + converged subscribers).
  - `drain-idle`, `drain-moderate`, `drain-saturated`: SIGTERM at 0 load,
    moderate load, and saturated load (declare the concrete rates in code).
    PASS requires: process exits ≤30 s, listen port released, every acked tx
    durable, every in-flight request either acked+durable or failed with an
    explicit error/close code (record observed close codes), exit status
    recorded.
  Output `details = {max_rto_seconds, max_drain_seconds, outcomes:[{id,result}]}`
  with integer seconds (ceil). Any budget miss / exact-state mismatch → that
  outcome FAIL and the record `result` FAIL. Reuse `cmd/soak` ledger ideas
  (per-tx accounting); you may factor shared client/ledger code into a new
  `internal/qualifyclient` package ONLY if needed — do not change `cmd/soak`
  behaviour.
- `soak` — seeds via the existing `cmd/soaksetup` flow and runs the existing
  `cmd/soak` binary against the candidate instantd with
  `-sessions 500 -duration 900s` (ramp/settle/tx-rate: choose and declare
  alpha budgets; keep `-max-p99-lag` explicit), using `soak-process-identity`
  sampling pre/post like `scripts/quality-soak.sh`. Sample instantd RSS,
  open fds, goroutines (if exposed) and PG connections every 10 s to a
  time-series artifact. Map soak evidence into
  `details = {sessions, active_seconds, committed_transactions,
  acknowledged_transactions, refreshed_transactions, dropped_transactions,
  unresolved_transactions}` from the soak's own evidence file — never
  recompute optimistic numbers; if the soak's evidence lacks a field, derive
  it from its per-tx events ledger and document how.
- `record` — assembles one gate-schema external record from a lane result:
  exactly the keys the gate requires (`artifacts, binary_sha256, campaign_id,
  candidate_sha, cleanup, configuration_sha256, details, endpoint_sha256,
  finished_at, fixture_id, host{arch,id,kernel,os,runtime}, packet, result,
  schema_version, selected_count, skipped_count, started_at`), artifact paths
  relative to the evidence root with size + sha256. `cleanup` is `complete`
  only when the orchestrator reports verified removal. For recovery/soak
  `selected_count` = number of outcomes / transactions checked;
  `skipped_count` = 0.
- `manifest` — assembles the gate manifest (`schema_version 1`,
  decision `DEC-001-single-node-alpha-20260905`, profile
  `single-node-alpha`, lanes exactly as the gate's `expected_lanes`,
  `provider_evidence: "not_selected"`, candidate binary/config artifacts,
  `endpoint_sha256`, three `external_records`, handoffs). Handoffs come from
  an explicit coordinator-authored input file (packet → state, ledger ref,
  finished_at); the tool writes one handoff file per packet containing the
  packet, state, candidate sha, campaign id, ledger reference and, for
  OP-003/OP-005/QR-001, the record path+sha. It must refuse to write the
  manifest if the packet set differs from the gate's `expected_packets`, or
  any state other than `GREEN` (or `ACCEPTED_EXCEPTION` for DA-004V).
  After writing, it self-validates by running the same jq predicates (or an
  equivalent Go check) — and the coordinator will run the real gate.

Canonical identity shared by all lanes of one campaign:
`endpoint_sha256` = sha256 of the literal canonical endpoint string
(`ws://instantd:8080/runtime/session` — document it); `configuration` = one
non-secret env file `qualify/instantd.env` generated per campaign (DSN
injected separately, never written into it).

### D2. `scripts/qualify/` host orchestration (bash, `set -euo pipefail`)

- `Dockerfile` (qualification image, above).
- `campaign.sh --campaign ID --candidate SHA --bundle FILE --workdir DIR
  --lane {build|native|recovery|soak} --host-id NAME` — creates prefixed
  network/containers, runs the lane in the qualification image, writes
  evidence under `DIR/evidence/`, verifies cleanup (no `iv2q-<campaign>-*`
  containers/networks/volumes remain; instantd pid gone; port free) and only
  then marks cleanup complete. `build` lane produces `bin/instantd` +
  its sha256 and the image digest; later lanes refuse to run if the binary
  sha differs from the build lane's recorded sha.
- `gate.sh --campaign ID --candidate SHA --workdir DIR` — runs
  `make test-release` (i.e. `scripts/quality-release-gate.sh`) inside the
  qualification image with a fresh owned DB, `RELEASE_GATE_MANIFEST` pointing
  at the assembled evidence dir (mounted read-only, outside `/src`), and
  captures the full log.

### D3. Tests (hermetic, run on macOS too)

- `cmd/qualify` unit tests: go-test-json parsing (pass/fail/skip, subtests),
  record schema output (feed produced records through the **real** gate jq
  predicates by shelling to `jq` with the predicate text extracted from
  `scripts/quality-release-gate.sh` — or duplicate it and add a test that
  fails if the gate script's predicate text changes), manifest refusal
  cases (missing packet, extra packet, non-GREEN state, binary sha mismatch),
  outcome verdict logic with injected fake observations (partial tx → FAIL,
  drain 31 s → FAIL, missing outcome → FAIL).
- `scripts/qualify/test-campaign.sh`: shellcheck-clean; dry-run argument
  validation and prefix-safety tests (refuses empty campaign id, refuses
  names without prefix).
- Extend `scripts/test-quality-release-gate.sh` ONLY if you add a seam — prefer
  not to touch the gate.

### D4. Docs

`docs/guides/15-qualification-campaign.md`: exact commands for the
coordinator (bundle → copy → build → native → recovery → soak → manifest →
gate), budgets declared, what each outcome proves and does not prove.

## Verification you must run and paste raw results of

```
go build ./...
go vet ./cmd/qualify/...
go test ./cmd/qualify/... -race -count=1
bash scripts/qualify/test-campaign.sh
bash scripts/test-quality-release-gate.sh
INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./... -race -count=1 -short 2>&1 | tail -30
git status --porcelain
```

(`golangci-lint` may be absent locally; if present run `golangci-lint run ./cmd/qualify/...`.)

## Leases / limits

Write only: `cmd/qualify/**`, `internal/qualifyclient/**` (new, optional),
`scripts/qualify/**`, `docs/guides/15-qualification-campaign.md`, and append
one handoff section to `docs/plans/finish-up/execution-ledger.md`.
Read-only: everything else (especially `internal/**`, `cmd/instantd/**`,
`cmd/soak/**`, `cmd/chaos/**`, `scripts/quality-release-gate.sh`, `Makefile`,
`corpus/**`). No network access to remote hosts. Do NOT commit, push or tag.
End with: changed files, verification outcomes, and an explicit list of any
assumption the coordinator must check on the real Linux host.
