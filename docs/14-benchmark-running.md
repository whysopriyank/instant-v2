# Running the benchmark harness

This is the operator guide for Wave 5 and the handoff into Wave 6. The binding
experiment contract is [`13-benchmark-contract.md`](13-benchmark-contract.md).
Wave 5 provides the harness, artifact model, and guard rails. Wave 6 performs
the comparative measurement and is the earliest point at which a performance
headline can be published.

## Safety and prerequisites

Run benchmarks on a dedicated, quiet host where possible. The server and
collector defaults bind to `127.0.0.1`; keep them loopback-only unless the
qualification record explicitly documents an isolated network. Install the Go
toolchain required by `go.mod`, Docker, PostgreSQL 17, `curl`, `jq`, and the
usual POSIX command-line tools.

Every database must be newly provisioned and named `instant_bench_<run-id>`.
Use a distinct database for each target and pair. The benchmark tools reject
credential-bearing DSNs and names that look production or shared. Supply
credentials through `DATABASE_URL` in the environment, never as a flag or
artifact field. A reset additionally requires `BENCHMARK_MARKER`; setup stores
that marker and reset verifies the exact resolved database identity before it
drops the public schema.

The historical 5,000-session/30-minute soak is a V2-only resource guard. It is
not a V1/V2 comparison and cannot justify a speedup claim.

## Build and focused checks

From the repository root:

```sh
make bench-acceptance
make build-bench
```

The binaries are written under `bin/`: `instantd`, `benchrun`, `benchreport`,
`soaksetup`, and `soak`. Keep each output bundle under
`benchmarks/results/<bundle-id>/` or another dedicated, private directory.
Artifact paths are constrained to the resolved bundle root, symlinks and path
traversal are rejected, logs are bounded/redacted, and checksums are written
before a raw bundle is accepted.

## Bounded V2 smoke

Create a disposable PostgreSQL instance and set its marker:

```sh
export DATABASE_URL='postgres://instant@127.0.0.1:5432/instant_bench_smoke?sslmode=disable'
export BENCHMARK_MARKER="smoke-$(date -u +%Y%m%dT%H%M%SZ)"
make bench-smoke
```

Start the server in another terminal with its loopback default, then capture
structured events and human-readable logs:

```sh
export BUNDLE="benchmarks/results/smoke-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BUNDLE"
bin/soak \
  -url ws://127.0.0.1:8888/runtime/session \
  -app "$APP" -attr "$ATTR" \
  -sessions 300 -ramp 10s -settle 30s -duration 60s \
  -global-tx-rate 8 -max-p99-lag 10s \
  -events "$BUNDLE/soak-events.jsonl" 2>&1 | tee "$BUNDLE/soak.log"
```

The CI short soak uses the same shape with 150 subscribers, a 10-second ramp,
30-second settle, 60-second measurement, 8 transactions per second, a 20
second convergence grace, a 10-second p99 budget, and a 1 GiB RSS ceiling.
It is a semantic correctness/resource guard only. It does not compare V1 and
V2 and does not run the 24-cell/7-pair matrix on pull requests.

`soaksetup` emits `APP` and `ATTR` values for this current CLI. Preserve the
captured setup output next to the JSONL event stream. If a future pair runner
adds qualification or target records, retain those records even when a target
fails; do not turn a failed target into zero delivery.

## Offline metadata and reports

Create a deterministic synthetic pair bundle and verify it offline:

```sh
export BUNDLE="benchmarks/results/manual-$(date -u +%Y%m%dT%H%M%SZ)"
SYNTHETIC=1 SEED=17 PAIR=pair-1 FAMILY=H-append SCALE=300 make bench-run
make bench-verify
```

`benchrun -synthetic` writes the full synthetic pair shape—targets, seven AB/BA
attempts, ledgers, summary, report, raw index, and checksums—but it is not a
live V1/V2 measurement. Make refuses an implicit mode: use `SYNTHETIC=1` for
acceptance or `BENCH_CONFIG=/path/config.json` for a live pair.

For an explicit live-config run:

```sh
export BENCH_CONFIG="$PWD/benchmarks/config/live-config.example.json"
export BUNDLE="benchmarks/results/wave6-h-append-300-$(date -u +%Y%m%dT%H%M%SZ)"
make bench-run
make bench-verify
```

To include the historical V2 reference, provide three targets in the live
config with ids `v1`, `v2_reference`, and `v2_current`. The runner detects this
shape, writes seven seeded target-order blocks (21 target runs), and reports
the independent comparisons `v1-v2_current` and `v2_reference-v2_current`.
The historical target is qualified by the same gate; if it fails, its target
record and all seven failed attempts remain in the bundle and no comparison is
promoted to an eligible claim. Exact per-target revisions are persisted in
`manifest.json`, each target record, each run record, and both comparison
headers in the offline report.

`benchrun -config` loads strict JSON, constructs the WP5-A target adapters, and
fails before writing a success bundle when validation or qualification cannot
prove the target. A complete redacted, non-runnable-until-provisioned example is
[`benchmarks/config/live-config.example.json`](../benchmarks/config/live-config.example.json),
with matching metadata examples beside it. It requires these top-level fields:
`pair_id`, non-zero `seed`, contract `family`, positive `scale`, `output`,
`authorization_signature`, `fixture_path`, `fixture_hash`, and `fixture`.
`fixture_path` is absolute and names canonical fixture evidence whose SHA-256
equals `fixture_hash`. Query labels and transaction
attribute UUIDs are separate namespaces:
use `query_entity`, `query_bucket_attr`, and `query_rank_attr` for query labels,
and `value_attr_id`, `bucket_attr_id`, and `rank_attr_id` for provisioned UUID
attribute IDs. These identifiers must be recorded in the fixture evidence.

Each target requires `id` (`v1` or `v2`), `kind`, `transport`, `session_url`,
`health_url`, `app_id`, reviewed `revision`, `dirty_tree_hash` (empty for a
clean target),
`database_name`, `postgres_version`, `invalidation_mode`, `metadata_file`,
`provisioned_marker`, non-shell `provision_command` argv,
`provision_command_sha256`, fixed `admin_token_env`, `refresh_token_env`,
`database_url_env`, `process_pid_env`, `runtime_token_env`,
`process_executable_path`, `process_executable_sha256`, and `probe_entity_id`.
`C-process-cold` additionally requires an absolute `process_pid_file`; the
provisioner atomically replaces it with the new PID and the collector rereads
it after each provision.
`provision_command[0]` must be an absolute executable path and its SHA-256 must
match `provision_command_sha256`; shell wrappers are rejected. Database names must
begin with `instant_bench_`; the fixed marker must exactly equal the database
name. URLs must be loopback and use the transport's required scheme. V1 must
declare `output_plugin: "wal2json"`.

For complete provenance, `metadata_file` should contain matching `revision`,
`database_name`, `postgres_version`, `invalidation_mode`, and (for V1)
`output_plugin`, plus the observed `dirty_tree_hash`. The current loader
validates all of those metadata fields; the live database
identity/version/marker are independently checked. It uses a fixed metadata query against
`instant_bench_metadata` (there is no configurable `marker_query`): it checks
the connected database name, stored database name, PostgreSQL version, and the
fixed provisioned marker. `provision_command` is an argv array executed
directly—and must provision the exact database, marker, and canonical fixture
before qualification. Before every execution the runner rehashes, parses, and
deterministically reconstructs the fixture, then passes only fixed
`BENCH_FIXTURE_PATH`, `BENCH_FIXTURE_SHA256`, `BENCH_FAMILY`, `BENCH_SCALE`, and
`BENCH_SEED` fields alongside the fixed database/target environment.
The fixed environment fields must use `BENCH_<ID>_DATABASE_URL`,
`BENCH_<ID>_ADMIN_TOKEN`, `BENCH_<ID>_REFRESH_TOKEN`,
`BENCH_<ID>_RUNTIME_TOKEN`, and `BENCH_<ID>_PID` (for `v1`/`v2`). They carry
variable names only, never `NAME=value` literals or secret values.

`admin_token_env` and `refresh_token_env` are environment-variable names, not
secret values. If configured, export those variables in the runner environment
before invoking `make bench-run`; do not place secrets in JSON, shell command
arguments, or artifacts. The recognized local operator names in the example are
`BENCH_V1_ADMIN_TOKEN`, `BENCH_V1_REFRESH_TOKEN`, `BENCH_V2_ADMIN_TOKEN`, and
`BENCH_V2_REFRESH_TOKEN`; export them only in the trusted local runner that
executes the live config. Before provision, `benchrun` verifies the config's
`authorization_signature` using the out-of-band
`INSTANT_BENCH_TRUSTED_APPROVAL_PUBLIC_KEY`. The checked-in performance workflow never injects
these secrets and always runs synthetic acceptance plus bounded V2 smoke. The
live config may also include loopback `admin_base_url` and `versions`. `versions` is the provenance map for toolchain/protocol/schema/
fixture/config/executable hashes; top-level `v1_sha`, `v2_sha`, `source_tree`,
`schema_hash`, `fixture_hash`, `config_hash`, `host_id`, and `executables` carry
the corresponding pair/host evidence. A clean target leaves `dirty_tree_hash`
empty; any non-empty value is retained as dirty evidence and blocks claim
qualification. Live configs are therefore operator-controlled through local
`make bench-run` invocations, not workflow inputs.

The collector fields are `process_pid_env` (or C's dynamic
`process_pid_file`),
`process_executable_path`, `process_executable_sha256`, `runtime_endpoint`,
`runtime_token_env`, and `collector_interval_millis` (default 1,000 ms).
`process_executable_path` must be absolute and its SHA-256 must match the
target's expected live PID executable hash recorded in `executables`.
Process/runtime/database collector credentials and PIDs are resolved through
fixed environment-variable names.
An omitted process or runtime collector is recorded as `unsupported`; it is
never converted to a zero measurement. Transport bytes are likewise
`unsupported` unless isolation makes them trustworthy.

The fixed lifecycle is 30 s ramp, 30 s settle, 60 s warm-up, 180 s measurement,
and 30 s convergence grace for H/X/M/O/S/R. C-process-cold is exactly 8,192
entities across 64 cohorts, with no warm-up; it measures readiness and initial
convergence, then 60 s at 8 tx/s. T-saturation is exactly 8,192 entities and
4,096 fixed operations through eight closed-loop writers, with no rate control
and a 600 s hard limit. These C/T limits are part of the family identity and
must not be substituted with the bounded workflow's 300-session smoke.
The performance workflow intentionally does not claim to execute these full
cells: it allows only a bounded smoke duration and subscriber choices. Use the
contract-specific executor settings for C/T before treating a run as Wave 6
evidence.

Raw ledger bundles can be large because every affected-recipient coverage is
retained. JSONL is written atomically as a bounded stream rather than buffered
in memory. With the default writer budget, the runner derives conservative
per-file and whole-bundle limits from mutation, query, recipient, and 14-attempt
cardinality; an explicit operator cap is never raised. Provision enough local
disk for that derived matrix before starting a live run. Exceeding either bound
fails the attempt and removes the partial artifact instead of truncating it.

## Approval, unsigned diagnostics, and budgets

Live-config authorization uses an Ed25519 `authorization_signature` verified
against the out-of-band `INSTANT_BENCH_TRUSTED_APPROVAL_PUBLIC_KEY` environment
variable before any provision command runs. The config cannot choose its own
trust root. After a successful run, sign the immutable content root locally:

```sh
export INSTANT_BENCH_APPROVAL_PRIVATE_KEY='hex-or-base64-private-key'
bin/benchrun -approve -output "$BUNDLE"
```

Offline report verification must use the trusted public key and verify both
checksums/content root and the detached signature. An unsigned or invalidly
signed bundle remains useful diagnostic evidence, but is claim-ineligible.
Never put a private signing key in the repository, config, workflow, or
artifact.

For an approved bundle, set `INSTANT_BENCH_TRUSTED_APPROVAL_PUBLIC_KEY` in the
offline verification environment and rerun `benchreport` against the immutable
raw bundle. Without that key, the report remains diagnostic rather than a
trusted claim artifact.

The current safety budgets are explicit: each artifact/config/metadata file is
limited to 128 MiB, individual log artifacts to 8 MiB, retained frame metadata
to 8,192 frames, and cumulative evidence to 10,000,000 frames or 8 GiB of
payload bytes per run. JSONL records are capped at 1 MiB per line. Exceeding a
budget is a failed attempt, not truncation that can be treated as complete
evidence; optional retained samples may be truncated while their cumulative
counts and digest remain recorded.

The checked-in performance workflow invokes only the synthetic pair mode,
invokes `soak` for a bounded live V2 smoke, and invokes `benchreport` for
offline replay. It fails if
setup, smoke, artifact verification, or report generation fails, and uploads the
bundle even on failure. Its final claim check rejects any report marked
eligible: this workflow is not the Wave 6 claim run.

## Target qualification for Wave 6

Freeze and record the exact commits, dirty-tree hashes, executable hashes,
fixture/schema/config hashes, PostgreSQL versions/settings, host limits, seed,
run order, and endpoint under test. V1 must pass a live refresh probe with a
healthy `wal2json` output plugin. V2 must record its real production
invalidation mode (post-commit notification/direct invalidation, with optional
peer bus). A separately verified `pgoutput` tailer is useful diagnostic
evidence, but must not be reported as V2's production driver.

Application payload bytes are mandatory. Transport bytes require an isolated
network namespace/cgroup; otherwise record them as `unsupported`, retaining the
reason. Missing, zero, not-applicable, failed, and unsupported values are
distinct states. Reports must preserve every attempted pair and can only read
the immutable local raw bundle after target teardown.

## Wave 6 qualification and claim rule

Wave 6 runs exactly seven seeded AB/BA paired attempts for each H/X/M/O/S/R/C/T
family at 300, 1,000, and 2,000 subscribers. It records semantic convergence,
coalescing, drops, submit-to-cover, acknowledgement-bounded commit-to-cover,
resource metrics, application bytes, and target failures. It uses paired
log-ratio aggregation with at least 10,000 bootstrap resamples plus the paired
sign result.

Do not publish “faster”, “lower”, or “improved” until the relevant cell has
complete provenance, all seven attempted outcomes, no correctness failure, a
reported unit/denominator/sample count, and a claim-gate-passing confidence
interval and effect direction. Historical frame counts and old V1/V2 ratios are
diagnostic context only.

## Cleanup and retention

Retain the raw bundle, `checksums.sha256`, `raw-index.json`, generated report,
setup output, and failure logs before tearing down targets. The CI workflow
retains its bundle artifact for 14 days; the explicit performance workflow
retains it for 30 days. Copy claim-producing Wave 6 bundles to durable review
storage according to the project retention policy.

After retention is confirmed, stop only the disposable server/container and
reset only the explicitly marked `instant_bench_` database:

```sh
bin/soaksetup -database-url "$DATABASE_URL" \
  -marker "$BENCHMARK_MARKER" -reset
```

Never reset a shared/public/production database, never alter shared PostgreSQL
configuration, and never use a failed-run deletion or replacement to improve a
headline.
