# Wave 6 staged execution and monitoring

Wave 6 uses the smallest contract scale, **300 subscribers**, for both live
smokes. Scale 300 is large enough to exercise the real H-append fan-out and V1
legacy refresh decoder; scales 1000 and 2000 would add cost without improving
the preflight decision.

The smoke and final measurement have deliberately different meanings:

1. `benchsmoke -targets v1` runs one diagnostic V1 attempt.
2. `benchsmoke -targets all` runs one diagnostic attempt for V1, historical V2,
   and current V2.
3. Only after both pass does `benchrun` start the complete seven-block,
   21-attempt schedule.

Smoke reports are never approved and never support performance claims. The full
schedule remains one detached process and one immutable bundle. The 4.5-hour
and later 2–10-hour windows are monitoring checkpoints, not separate benchmark
runs; stopping and joining independent partial bundles would invalidate the
balanced order and detached approval.

## Build

From a clean reviewed harness revision:

```sh
go build -o /absolute/task/path/benchsmoke ./cmd/benchsmoke
go build -o /absolute/task/path/benchrun ./cmd/benchrun
sha256sum /absolute/task/path/benchsmoke /absolute/task/path/benchrun
```

Do not place authorization keys, database URLs, or tokens in command arguments,
the repository, the state directory, or agent prompts.

## Start inside the isolated namespace

Prepare the exact target binaries, three disposable databases, signed live
configuration, fixed `BENCH_*` environment variables, and loopback-only network
namespace as described in [the benchmark-running guide](../guides/14-benchmark-running.md).
Then enter that namespace and run:

```sh
export WAVE6_CONFIG=/absolute/task/path/live-h300-triad.json
export WAVE6_STATE_DIR=/absolute/task/path/controller-state
export WAVE6_BENCHSMOKE=/absolute/task/path/benchsmoke
export WAVE6_BENCHRUN=/absolute/task/path/benchrun

/absolute/harness/path/benchmarks/scripts/wave6-orchestrate.sh start
```

The orchestrator refuses a non-H300 or non-triad config, non-canonical timing
or warmup settings, a host-network namespace, mismatched signed target and
collector namespace identities, any external interface or IPv4/IPv6 route, a
symlink/non-directory output, a nonempty full output, or a failed smoke. It
opens each source through the stable snapshot helper (`O_NOFOLLOW` on Linux),
copies and hashes from that descriptor, and rehashes the immutable snapshots
immediately before each invocation. It executes only the snapshots and holds
an exclusive lock adjacent to the output from the emptiness check through
child launch. Linux lock acquisition is performed through a held parent
directory descriptor with `O_CREAT|O_EXCL|O_NOFOLLOW`; that descriptor is
inherited by the locked controller, wrapper, and benchrun child without
becoming an artifact. It stores only hashes, status, sanitized
diagnostic JSON, and bounded stdout/stderr paths. The full process is detached
only after both smokes pass, in its own process group; TERM/INT/HUP are
forwarded and the wrapper waits for the child before atomically recording its
exit and finish times. Remote execution fails closed off Linux: status and
progress validation reads JSON, JSONL, ledger, and frame artifacts through
descriptor-pinned, no-symlink component walks with fstat identity/size checks;
the recorded output lock must remain a root/task-owned regular file.

## Cheap status check

Run this from the same host account; it does not build, test, restart, approve,
or mutate the bundle:

```sh
WAVE6_STATE_DIR=/absolute/task/path/controller-state \
  /absolute/harness/path/benchmarks/scripts/wave6-orchestrate.sh status --json
```

The first check is due about 4.5 hours after launch. Later checks use
`next_recommended_check_seconds`, clamped between 2 and 10 hours based on the
observed completed-attempt rate. Status counts only the exact canonical
21-run triad schedule and validates run schema/class, target revisions,
schedule blocks, and sibling evidence. It reports execution progress only; it
never infers claim eligibility. A completed process still requires detached
approval and independent offline report/checksum/content-root verification.

## Prompt for the execution agent

Use this prompt in the main benchmark task after the signed configuration and
isolated namespace exist:

> Execute Wave 6 through `benchmarks/scripts/wave6-orchestrate.sh` using the
> reviewed clean harness commit and exact target revisions already approved in
> the task. Build `cmd/benchsmoke` and `cmd/benchrun` into the unique remote
> task directory and record their SHA-256 hashes. Run the orchestrator inside
> the dedicated loopback-only, non-host Linux network namespace with secrets
> supplied only through the existing environment. The orchestrator must first
> pass one V1 H300 diagnostic attempt, then one diagnostic attempt for each of
> V1, historical V2, and current V2. Do not start the full run if either smoke
> fails. If both pass, let the orchestrator launch the unchanged seven-block,
> 21-attempt H300 schedule as one detached process and return the absolute state
> directory. Do not poll continuously, modify the signed configuration, reuse
> partial databases, combine bundles, approve partial output, or make a
> performance claim. Schedule the first read-only status check for 4.5 hours
> after `full.started_at_epoch`. At each check, run only the orchestrator’s
> `status --json`. If it is running, report attempts completed, failures,
> elapsed time, and ETA, then schedule the next check using
> `next_recommended_check_seconds`. If it failed or was interrupted, report the
> first sanitized blocker and do not restart automatically. If it completed 21
> passing attempts, report completion to this task, stop further monitoring,
> and wait for explicit authorization before detached approval and final
> offline verification.

## Prompt for the scheduled monitor

> Read the Wave 6 controller state using only
> `wave6-orchestrate.sh status --json` with the recorded absolute state
> directory. Do not run builds, tests, benchmark commands, approval, cleanup, or
> restarts. If `phase` is `running`, report completed attempts out of 21,
> failures, elapsed time, and estimated remaining time, then keep monitoring at
> the returned `next_recommended_check_seconds`. If `phase` is `failed` or
> `interrupted`, report the state and ask the main task to inspect the sanitized
> failure; never restart automatically. If `phase` is `completed`, report it to
> the main task and stop this monitor. Treat missing state as a setup blocker,
> not permission to create or launch a run.
