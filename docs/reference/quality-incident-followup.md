# Quality-run delivery and database incident follow-up

Date: 2026-08-31. Source baseline:
`5f78ca0877c1a8c04b173e5c502e8c948ecfb965`, branch `main`.

Status: **PARTIAL**. Containment, current-state inspection, evidence preservation,
and fixture regression checks are complete. Historical database restoration is
not established or authorized. Commit scope and external delivery remain open.
This is not a production-readiness declaration.

## Contract and result ledger

| ID | Required invariant / observation | Result | Remaining condition |
|---|---|---|---|
| D1 | Preserve the mixed working tree and distinguish existing changes from this follow-up | GREEN: initial HEAD, branch, status and tracked diff inspected; index empty; 397 changed/untracked file entries before follow-up; private tracked patch and allowlisted untracked-source archive saved | Obtain approval for exact commit contents; do not stage all files indiscriminately |
| D2 | Observe the affected database without migrations, DDL, DML, slot changes, or application-row disclosure | GREEN: explicit loopback endpoint, read-only transactions, schema-only dump and row counts | Current snapshot is not a pre-incident baseline |
| D3 | Establish the incident's historical schema/data delta before proposing reversal | BLOCKED: no trustworthy pre-incident snapshot supplied or identified in the bounded repository/document search | User supplies baseline/backup or explicitly accepts preserving the current schema; never infer an empty historical DB from empty current tables |
| D4 | Preserve the original verification cluster and prove it is stopped | GREEN: `pg_ctl status` reports no server; `pg_controldata` reports shut down; server log hashed and copied | Intentional retention, not an unattended running service; deletion requires exact approval |
| D5 | Revalidate the repaired fixture pool/DSN identity and owned cleanup away from the affected DB | GREEN: hermetic testkit lane plus two race-enabled live isolation executions on a new dedicated cluster | This proves the existing regression contract, not historical absence of side effects |
| D6 | Keep generated root executable out of source delivery without masking command sources | GREEN: anchored `/corpusctl` ignore; executable retained | No binary deletion or broad cleanup performed |
| D7 | Deliver reviewable commits/PR/CI/deployment with an explicit scope and target | BLOCKED: no stage/commit/push/PR/deploy performed | Confirm local commit scope first; external publication and deployment need separate target/approval |

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

The user can supply a known pre-incident schema dump/backup with its capture time
and identity. Compare schema metadata first; inspect application data only if
needed and authorized. Restore a supplied backup only into an isolated comparison
environment, never over the live default database as a diagnostic step.

If no historical baseline exists, the honest alternative is owner acceptance of
the current preserved schema and the documented historical uncertainty. Record
that as acceptance of a residual incident limitation, not proof of no impact.
If removal is requested, first establish dependencies/ownership and an exact
recovery plan with backup and approval. Empty current tables alone do not permit
deleting them.

## Delivery closure decision

The working tree contains pre-existing user changes, refactor moves, fixes, tests,
generated protocol changes, and new documents. The index was empty at inspection.
Tracked-only diff statistics omit untracked destination files and must not be
presented as net code deletion.

Do not manufacture a supposedly clean pre-refactor snapshot or rewrite `main`.
Before local commits, approve the exact file/hunk scope, including whether to
include the older README/reactive/storage/sync/performance-plan/query-benchmark
edits. The [production roadmap](../plans/production-completion-roadmap.md)
specifies atomic delivery units and verification. Push, PR creation and deployment
remain separate actions with explicit destinations. Unresolved production gates
must remain visible in the PR and release decision.
