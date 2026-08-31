# Benchmark artifacts and commands

This directory contains the versioned benchmark schemas and local result
bundles. The accepted methodology is in
[`docs/reference/13-benchmark-contract.md`](../docs/reference/13-benchmark-contract.md). Wave 5
hardens collection and reporting; Wave 6 is the first comparative measurement
wave. A bundle produced before Wave 6 is diagnostic evidence unless it passes
the complete claim gate.

For the credit-efficient V1 smoke, one-attempt triad smoke, detached full run,
and checkpoint prompts, use
[`docs/plans/15-wave6-execution.md`](../docs/plans/15-wave6-execution.md) and
[`benchmarks/scripts/wave6-orchestrate.sh`](scripts/wave6-orchestrate.sh).
The controller requires the canonical H-append/300 triad timings (30/30/60/180/30),
proves the dedicated loopback-only Linux namespace, snapshots and rehashes its
inputs from stable file descriptors, holds the adjacent output ownership lock
through launch, caps/redacts controller logs, and validates exact 21-run
execution progress without making a claim-eligibility decision. Linux artifact
status reads are pinned to an opened bundle directory and reject symlink or
replacement races. Linux output-lock creation is also dirfd-based with
`O_CREAT|O_EXCL|O_NOFOLLOW`; a missing or unowned output lock is a status error.

## Prerequisites

- Go version from `go.mod`.
- Docker with a local PostgreSQL 17 image for the live V2 smoke.
- `curl`, `jq`, and standard POSIX tools.
- A loopback-only server and a disposable PostgreSQL database whose name starts
  with `instant_bench_`.

The benchmark tools reject credential-bearing or production-looking DSNs. Keep
credentials in `DATABASE_URL` in the process environment; do not put them in a
command line or an artifact. The default server and collector endpoints bind
to loopback. Do not point these commands at a shared, staging, or production
database.

## Local commands

Run the focused acceptance suite:

```sh
make bench-acceptance
```

Create a deterministic synthetic pair bundle and render its offline report:

```sh
export BUNDLE="benchmarks/results/manual-$(date -u +%Y%m%dT%H%M%SZ)"
SYNTHETIC=1 make bench-run
make bench-verify
```

`bench-run` invokes the deterministic `benchrun -synthetic` executor only when
`SYNTHETIC=1` is explicit. It exercises the pair/ledger/report path with fake
targets for harness acceptance; it is not a live V1/V2 measurement. Without
either `SYNTHETIC=1` or `BENCH_CONFIG=/path/config.json`, Make refuses an
implicit benchmark mode. The explicit performance workflow uses synthetic
acceptance only, combines it with a bounded live V2 smoke, and stores the smoke
events inside the checksummed raw bundle. Trusted live V1/V2 runs are local and
operator-controlled; the workflow never injects their credentials.

For a live pair, pass a strict JSON config explicitly; it is never inferred or
silently replaced with synthetic mode:

```sh
export BENCH_CONFIG="$PWD/benchmarks/config/live-config.example.json"
export BUNDLE="benchmarks/results/wave6-h-append-300-$(date -u +%Y%m%dT%H%M%SZ)"
make bench-run
make bench-verify
```

`live-config.example.json` is a checked-in redacted contract example, not a
ready-to-run target: replace its reviewed revisions, PostgreSQL version,
metadata/provisioner paths, and environment mappings with values for the
isolated targets. The config's `output` is used only when no `-output` override
is supplied; the Make target supplies the bundle output explicitly. Keep live
config and metadata files private and free of credential values.

For an operator-invoked smoke, first create a fresh database and set an
unambiguous marker. The marker is persisted by `soaksetup` and is required for
any reset:

```sh
export DATABASE_URL='postgres://instant@127.0.0.1:5432/instant_bench_local?sslmode=disable'
export BENCHMARK_MARKER="manual-$(date -u +%Y%m%dT%H%M%SZ)"
make bench-smoke
```

Start `bin/instantd` with its loopback defaults in a separate terminal, then
use the `APP` and `ATTR` values emitted by `soaksetup`:

```sh
mkdir -p "$BUNDLE"
bin/soak \
  -url ws://127.0.0.1:8888/runtime/session \
  -app "$APP" -attr "$ATTR" \
  -sessions 300 -ramp 10s -settle 30s -duration 60s \
  -global-tx-rate 8 -max-p99-lag 10s \
  -events "$BUNDLE/soak-events.jsonl" 2>&1 | tee "$BUNDLE/soak.log"
```

This is a V2 correctness/resource smoke, not a V1/V2 speedup measurement.
For a full synthetic report, use `SYNTHETIC=1 make bench-run` to create a fresh
raw bundle and `make bench-report` after the run. For a live pair, set
`BENCH_CONFIG=/path/config.json` instead. Reports read only local artifacts and
must be reproducible with both targets unavailable.

## Live-config contract and fixed family limits

The redacted example under `benchmarks/config/` shows the current strict JSON
shape; the corresponding structural schemas are
[`schema/live-config.schema.json`](schema/live-config.schema.json) and
[`schema/fixture.schema.json`](schema/fixture.schema.json). Top-level `pair_id`, non-zero `seed`, `family`, positive `scale`,
`output`, `authorization_signature`, and `fixture` are required for a live run.
Query labels and transaction attribute UUIDs are separate: use `query_entity`, `query_bucket_attr`, and
`query_rank_attr` for query labels, `id_attr` for the provisioned identity
attribute label, and `id_attr_id`, `value_attr_id`, `bucket_attr_id`, and
`rank_attr_id` for provisioned UUID attribute IDs. The identity attribute is a
unique cardinality-one blob attribute named `id`; its value is the entity UUID.
The checked-in canonical UUID allocation reserves `...000100` for `id`,
followed by `...000101`, `...000102`, and `...000103` for value, bucket, and
rank.

Each target requires `id`, `kind`, `transport`, loopback `session_url` and
`health_url`, UUID `app_id`, reviewed `revision`, `database_name` with the
`instant_bench_` prefix, `postgres_version`, `invalidation_mode`,
`metadata_file`, `provisioned_marker`, top-level absolute `fixture_path` and
matching canonical `fixture_hash`, absolute direct executable
`provision_command` argv, its `provision_command_sha256`, `database_url_env`,
and `probe_entity_id`. The live process also requires absolute
`process_executable_path` plus `process_executable_sha256`, anchored to the
target entry in `executables`. V1 additionally requires
`output_plugin: "wal2json"`. `database_url_env` names an environment variable
containing the disposable loopback DSN; it is never a DSN value in JSON.
`admin_token_env`, `refresh_token_env`, `runtime_token_env`, and
`process_pid_env` must use the fixed names `BENCH_<ID>_ADMIN_TOKEN`,
`BENCH_<ID>_REFRESH_TOKEN`, `BENCH_<ID>_RUNTIME_TOKEN`, and `BENCH_<ID>_PID`;
`database_url_env` is `BENCH_<ID>_DATABASE_URL`. The checked-in example uses
loopback admin/runtime endpoints and contains no token values.

Before every provision, the runner verifies and reconstructs the canonical
fixture, then passes its path/hash plus family, scale, and seed through fixed
`BENCH_FIXTURE_PATH`, `BENCH_FIXTURE_SHA256`, `BENCH_FIXTURE_ID_ATTR`,
`BENCH_FIXTURE_ID_ATTR_ID`, `BENCH_FAMILY`, `BENCH_SCALE`, and `BENCH_SEED`
fields. The provisioner must create the identity attribute and seed its value
equal to each entity UUID before the benchmark opens subscriptions.
`C-process-cold` must also configure an absolute `process_pid_file`; the
provisioner atomically writes the fresh PID there and the collector rereads it
for every measured boundary.

The marker is fixed: `provisioned_marker` must equal the exact database name.
The runner uses its built-in metadata query against
`instant_bench_metadata` and compares the connected database, stored database
name, PostgreSQL version, and marker. There is no configurable `marker_query`
field in the current CLI. A non-empty `dirty_tree_hash` means dirty revision
evidence and blocks qualification; a clean target records it as empty while
the reviewed revision and executable/schema/fixture/config hashes remain in
the provenance record. Optional process/runtime collectors use
`process_pid_env` (or C's required dynamic `process_pid_file`),
`process_executable_path`, `runtime_endpoint`,
`runtime_token_env`, and `collector_interval_millis` (default 1,000 ms).
Missing collectors are recorded as `unsupported`, never as zero.

The contract's fixed lifecycle values are 30 s ramp, 30 s settle, 60 s warm-up,
180 s measured phase, and 30 s convergence grace for H/X/M/O/S/R. C-process-cold
uses 8,192 entities and 64 cohorts, no warm-up, readiness plus initial
convergence, then 60 s at 8 tx/s. T-saturation uses 8,192 entities, 4,096
fixed operations, eight closed-loop writers, no rate control, and a 600 s hard
limit. These values describe the contract cells; changing them changes the
cell identity.

Before provisioning a live config, set the out-of-band
`INSTANT_BENCH_TRUSTED_APPROVAL_PUBLIC_KEY`; `benchrun` verifies the config's
`authorization_signature` against it. The config cannot choose the trust root.
After an integrity-checked run, sign its immutable content root locally:

```sh
export INSTANT_BENCH_APPROVAL_PRIVATE_KEY='hex-or-base64-private-key'
bin/benchrun -approve -output "$BUNDLE"
```

Offline trusted verification checks the content root, checksums, and detached
Ed25519 signature. Unsigned or invalidly signed bundles remain diagnostic
evidence only and are claim-ineligible; never store a private signing key in
the repository or artifact.

Set `INSTANT_BENCH_TRUSTED_APPROVAL_PUBLIC_KEY` when rerunning `benchreport`
offline against an approved immutable raw bundle; without it, the report stays
diagnostic.

The safety budgets are 128 MiB per artifact/config/metadata file and 8 MiB per
log. Live evidence uses a signed, workload-derived bound. For `Q`
queries/subscribers, `M` measured plus warm-up mutations, recipient bound `P`,
and behavior duration `D` seconds:

```text
F = M*P + 2*84*(2*Q + 2) + 4*Q + 4*Q + 21 + M
    + (T ? 16 : 0)
    + (R ? 4*ceil(Q/10)*floor(D/30) : 0)
    + 128
B = max(8 GiB, F*16 MiB)
```

`P=Q` is the safe default; only verified canonical bucket-only X/C/T fixtures
use `P=ceil(Q/cohorts)`. The derived byte bound is overflow-checked and must
stay below 64 TiB. Independent hard ceilings remain 10,000,000 frames and
8,192 retained frame-metadata samples. An all-zero budget requests derivation;
an explicit non-zero operator cap is preserved and never raised. The manifest
stores the triple and offline verification recomputes it for every run.
H300 may therefore exceed the old 8 GiB floor for legitimate cumulative
full-snapshot payload traffic; payloads are counted/hashed and released rather
than retained in memory. JSONL records remain limited to 1 MiB per line, and
budget overflow fails the attempt without erasing observed counts or digest.

## Qualification and metrics

Before a Wave 6 pair, qualify both targets and retain the qualification output:

- V1 must use a healthy `wal2json` output plugin and replication path.
- V2 records its actual production invalidation mode (currently post-commit
  notification/direct invalidation, with optional peer bus). An independent
  `pgoutput` tailer is not evidence that production V2 is pgoutput-driven.
- Both sides use newly provisioned databases, equivalent fixtures, fixed
  revisions, and recorded schema/config hashes.
- The bundle records the host, PostgreSQL/toolchain versions, limits, command
  line, seed, run order, and target endpoint.

Application payload bytes are required. Transport/network bytes are reported
as `unsupported` unless an isolated network namespace or cgroup makes them
trustworthy; unsupported is never silently converted to zero. Raw bundles,
including failed attempts, are retained and checksummed. Do not delete a
failed attempt to improve a report.

No public claim such as “faster”, “lower”, or “improved” may be made before
Wave 6 has run the fixed H/X/M/O/S/R/C/T matrix at 300, 1,000, and 2,000
subscribers with seven seeded AB/BA pairs and passed the claim gate.

## Cleanup

Only reset a benchmark database after verifying its exact name and marker. Use
the explicit reset command and the same marker used during setup:

```sh
bin/soaksetup -database-url "$DATABASE_URL" \
  -marker "$BENCHMARK_MARKER" -reset
```

Stop the loopback server and remove only the disposable PostgreSQL container
or database that you created. Preserve the result bundle before cleanup. Never
run a reset against a database without the `instant_bench_` prefix and an
already-recorded marker.
