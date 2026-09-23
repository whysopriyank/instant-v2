# Quality-run delivery and database incident follow-up

Historical (baseline `5f78ca0877c1a8c04b173e5c502e8c948ecfb965`, 2026-08-31) —
incident record preserved as-is; not current-candidate status.

Date: 2026-08-31. Source baseline:
`5f78ca0877c1a8c04b173e5c502e8c948ecfb965`, branch `main`.

Status: **RESOLVED FOR PRE-RELEASE DEVELOPMENT** by the user's 2026-08-31
disposition: no pre-incident backup/baseline exists, the program has not been
publicly released, and historical restoration/production backup work is not
required. Keep the current development schema and documented uncertainty. The
user authorized reviewing relevance and committing the useful existing edits.
This is not proof of no historical impact or production readiness.

## Contract and result ledger

| ID | Required invariant / observation | Result | Remaining condition |
|---|---|---|---|
| D1 | Preserve the mixed working tree and distinguish existing changes from this follow-up | GREEN: private checkpoint retained; relevant pre-existing edits reviewed and included under user authorization | No unrelated user source removed; generated/private files excluded |
| D2 | Observe the affected database without migrations, DDL, DML, slot changes, or application-row disclosure | GREEN: explicit loopback endpoint, read-only transactions, schema-only dump and row counts | Current snapshot is not a pre-incident baseline |
| D3 | Resolve historical uncertainty without unsafe reversal | ACCEPTED_EXCEPTION: user confirms no baseline exists and declines historical recovery/production-backup work for this unreleased program | Preserve current schema; historical delta remains unknown, not a development blocker |
| D4 | Preserve the original verification cluster and prove it is stopped | GREEN: `pg_ctl status` reports no server; `pg_controldata` reports shut down; server log hashed and copied | Intentional retention, not an unattended running service; deletion requires exact approval |
| D5 | Revalidate the repaired fixture pool/DSN identity and owned cleanup away from the affected DB | GREEN: hermetic testkit lane plus two race-enabled live isolation executions on a new dedicated cluster | This proves the existing regression contract, not historical absence of side effects |
| D6 | Keep generated root executable out of source delivery without masking command sources | GREEN: anchored `/corpusctl` ignore; executable retained | No binary deletion or broad cleanup performed |
| D7 | Commit relevant implementation with proportionate checks | GREEN: implementation checkpoint `eb88232`; policy/closure recorded in the following documentation commit | Push/PR/deployment are outside this task; production certification is not required merely to save development progress |

## Confirmed current database state

The inspected endpoint was explicitly `127.0.0.1:5432`, database `postgres`, role
`priyank`, PostgreSQL 17.11, data directory
`/opt/homebrew/var/postgresql@17`. Connections used `psql -X -w`, a five-second
connection timeout, ten-second statement timeout, and
`default_transaction_read_only=on`. The metadata/count snapshot used one
repeatable-read read-only transaction. No application values were queried.

- Sixteen public tables exist: fifteen application tables plus
  `goose_db_version`.
- Goose records version 0 initialization at `2026-08-31 01:55:04.735879`, followed
  by versions 1–6, all applied, ending at `01:55:04.980299`.
- The database reports `Asia/Kolkata`; the recorded `tstamp` values do not carry
  an explicit offset. Their timing is consistent with the disclosed incident.
- All fifteen application tables contained zero rows at the follow-up snapshot.
- Two pre-existing inactive logical slots, `aggregator` and `invalidator`, were
  observed. Neither was modified, activated, nor deleted.
- `internal/platform/migrate.go` and the six embedded migrations are unchanged
  from the recorded source HEAD. That source comparison does not establish the
  database's pre-incident schema.

The version timestamps and contemporaneous missing-table errors in the local
server log strengthen the evidence that the misdirected migration call applied
the application schema in the incident window. They do not prove every object's
prior state, the absence of historical application data, or that reversal is safe.
The second migration invocation need not have applied the migrations again.

No previous database dump/baseline was found in the bounded repository/document
inspection. Backups elsewhere were not exhaustively searched. PostgreSQL's
current statement logging is `none`, so the server log is not a complete DDL
audit trail. **Do not run down migrations, DROP SCHEMA, table deletion, or slot
cleanup as an inferred recovery action.**

## Private evidence and retention

The durable local evidence directory is:

`/Users/priyank/Developer/sideproj/instant-v2/.git/quality-incident-20260831/`

It is mode 0700, with evidence files mode 0600, and is outside the tracked source
tree. It is local preservation, not an off-host backup. It contains a read-only
audit SQL file, its metadata/count output, a schema-only dump without ownership
or grants, and a copy of the original dedicated-server log. No application-data
dump was taken. Do not attach raw logs to a public PR without inspection/redaction.

| Artifact | SHA-256 |
|---|---|
| `catalog-audit.sql` | `a99a7074cdaa9ce47c37019b2317ad06eb03e14551209444093d2ee909921771` |
| `catalog-audit.txt` | `f6726a05c01de1c6aa751b92b69c554adaaf459ab04284247ff76f594ec63a01` |
| `postgres-schema.sql` | `d3cd332543c61db04a809c19841002fca10838c07a55df926bf1d1ba86559cc4` |
| `dedicated-server.log` | `7ab0e3d70cb22a6683cd1280c49e3a7fab67663f7b91431ced7001ed2663f83a` |
| `postgres-schema-after.sql` | `4d2cc51e459330450259755e82ae8f64dcb5b108293f5b905890074602467f69` |
| `working-tree.patch` | `3afce625aadd1a8f0dc5587c168682aa483cd8b2cff050a05207052b65d6f4e8` |
| `untracked-source.tar` | `b3539586fc20c8792a13882d89c53abcefe0d28d7b4464932f415e146bf3d1e3` |

The preservation patch includes tracked changes against the named HEAD; the
archive includes 253 untracked source/document/fixture files from the explicit
`cmd`, `internal`, `corpus`, `docs`, `scripts`, and `examples` allowlist. These are
an intermediate local recovery checkpoint taken during this follow-up, not a
pristine pre-refactor snapshot, a final release artifact, or a commit. Later
documentation-only finalization is not retroactively included. Do not apply the
patch or extract the archive over a working tree without a separate recovery plan.

A second schema-only dump after fixture verification compared identically to the
first after removing only pg_dump's random `\restrict`/`\unrestrict` guard lines.
This is bounded evidence that the inspected schema did not change during this
follow-up, not evidence of its state before the original incident.

| Local resource | Verified disposition |
|---|---|
| `/tmp/instant-quality-pg.9QIyWF/data` | Original verification cluster, port 55487, shut down; parent directory approximately 103 MB retained |
| `/tmp/instant-quality-pg.9QIyWF/server.log` | Original retained; matching copy preserved in private evidence directory |
| `/tmp/instant-quality-tools.aTcn07` | Approximately 60 MB of verification tooling retained; no deletion needed for incident containment |
| `/tmp/instant-quality-followup-pg.A4e6za/data` | Fresh follow-up cluster, port 55488, started solely for fixture checks, then shut down cleanly; parent approximately 55 MB retained |
| `/tmp/instant-quality-incident-evidence.Vehsfa` | Initial private collection directory retained; named evidence files copied to the durable local directory above |

Retention is intentional. Do not restart the original cluster merely to clean it
or run more tests. New verification should use newly owned disposable resources.
After the owner accepts the evidence and retention decision, cleanup can target
each exact directory separately; no recursive `/tmp` or repository cleanup.

## Fresh regression evidence

These checks were executed for this follow-up, not copied from the previous run:

1. `INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./internal/testkit -race -count=1 -v`
   — exit 0; nine top-level tests passed; the live isolation test correctly skipped.
2. `INSTANT_TEST_INTEGRATION=1 DATABASE_URL='postgres://priyank@127.0.0.1:55488/postgres?sslmode=disable' go test ./internal/testkit -run '^TestPostgresIsolationIntegration$' -race -count=2 -v`
   — exit 0; two top-level isolation executions and two nested executions passed.
   The test checks pool identity, returned-DSN identity, exact cross-fixture data,
   and second-fixture cleanup.
3. Read-only follow-up-cluster audit — only the bootstrap `postgres` database
   remained outside templates, no replication slots, and no
   `public.goose_db_version` in that bootstrap database.
4. Explicit `pg_ctl` shutdown of that newly created cluster — exit 0;
   `pg_controldata` subsequently reported `shut down`.
5. Root `corpusctl` was an untracked Mach-O executable and was not ignored before
   this follow-up. The new anchored ignore excludes only that root artifact;
   `cmd/corpusctl` source remains visible.
6. `git diff --check` and a bounded local Markdown check passed: five touched
   documents, 24 local links, no missing targets or unmatched fenced blocks.

The original cluster was not restarted, and the affected developer cluster
received only read-only queries/schema export. No product Go code or migrations
were edited during this follow-up.

A fresh-context, read-only Sol reviewer found no blocking issue in this bounded
follow-up: metadata, the initial four evidence hashes/private permissions, HEAD/
index status, source ignore and stated uncertainty were checked. The reviewer did
not rerun tests or query the database; its PASS is for the handoff's accuracy and
safety, not historical database restoration or production acceptance.

## Database closure decision

The user confirmed no pre-incident backup/schema baseline exists and the program
is unreleased. Close the incident for development by retaining the current
database, accepting the historical uncertainty and keeping the fixed fixture
regression. No further baseline search, production backup project, database
migration/reversal or recovery drill is required or authorized for this checkpoint.
Existing stopped resources/evidence may remain; deletion is unnecessary to proceed.
This disposition does not prove the original operation had no effect. Any later
requested removal still requires an exact target/ownership check.

## Delivery closure decision

The working tree contains pre-existing user changes, refactor moves, fixes, tests,
generated protocol changes, and new documents. The index was empty at inspection.
Tracked-only diff statistics omit untracked destination files and must not be
presented as net code deletion.

The user authorized including relevant pre-existing work and committing coherent
batches as progress is made. Preserve useful README/project history, reactive
queue/routing changes, storage write simplification, canonical sync projection
and fixtures, and the query-plan benchmark source. Keep historical performance
plans as deferred context, not active instructions. Exclude generated executables,
dependencies, private evidence and raw result bundles. No user source is deleted.

The [implementation-first policy](../plans/production-completion-roadmap.md#current-authority--implementation-first)
now controls execution. Do not fabricate a pure-rename history or rewrite `main`.
Push/PR/deployment remain outside this task; deferred production checks do not
block a clearly labeled development commit.

## Committed development checkpoint

Implementation commit: `eb88232` (`refactor: consolidate implementation and
repository layout`) on `codex/quality-implementation-checkpoint`. This is an
honest integrated change, not a pure rename: shared helpers, package splits,
consumers, tests, command moves and document links are interdependent.

Two Luna agents independently checked pre-existing-edit relevance and delivery
inventory. They recommended inclusion of the product changes and their tests/
historical plans, with no blocking integration issue identified. No new product
or benchmark-hardening change was made while preparing the commits.

Fresh minimum checks for this commit:

- `INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL= go test ./... -short -count=1`
  — exit 0; 31 packages passed, four had no tests; live integration disabled and
  no benchmarks requested. This run did not enable the race detector.
- `make check-generated` — exit 0; protocol outputs match their generator/schema.
- `git diff --cached --check` — exit 0 before the implementation commit.
- Staging inventory: only explicitly reviewed source/document paths; no binary
  entries or private evidence. Generated root executables remain ignored locally.

The following documentation-only commit records the user's incident disposition,
Luna-heavy routing, deferred hardening and incremental commit policy. Its checks
are local Markdown links/anchors and whitespace, not a repeated product test run.
No database connections, migrations, cleanup, production backup project, live
benchmarks, soak, release certification, push or deployment occurred in this
commit-preparation task. Earlier evidence above retains its original scope.
