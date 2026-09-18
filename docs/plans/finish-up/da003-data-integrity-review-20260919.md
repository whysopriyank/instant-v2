# DA-003 data-integrity review — 2026-09-19

Scope: `internal/backup/backup.go`, `export.go`, `import.go`,
`import_records.go`, `codec.go`, `http.go`, `http_objects.go`,
`objectstore.go`, `da003_failclosed_test.go`,
`da003_extra_contract_test.go`, `http_test.go`, `import_test.go`,
`import_boundary_test.go`, `export_test.go`, `objectstore_http_test.go`,
`cmd/instantd/routes.go`, `daemon_object_disabled_test.go`,
`runtime_cf003_test.go`.

Verdict: **ACCEPT**

All seven falsification targets were attacked and falsified. Where the
existing battery left a contract unpinned, a focused test was added inside
`internal/backup/**`; production behavior required no change (new tests
green on first run against owned PostgreSQL).

1. Partial publication — FALSIFIED.
   `handlePutObject` streams `Export` into a private staging key
   (`internal/backup/http_objects.go:76-97`), promotes only after both
   `Put` and `Export` succeed (`:127-134`), and deletes staging on every
   failure path (`:104-106,116-118,128-130`). `Put` prefix failure
   preserves final and cleans stage
   (`da003_failclosed_test.go:323-358`); short-success (truncated upload
   with nil Put error) fails export, returns 502, promotes nothing, keeps
   final (`:360-388`); success uses exactly one staging Put, one
   staging→final promotion, one staging delete
   (`:161-227`, puts[0] private, promotions=={[staging,final]}).
2. Accepted truncation or checksum mismatch — FALSIFIED.
   Export always ends with `{"kind":"checksum","sha256","records"}`
   (`export.go:98-104`, records=sum of logical records). Import verifies
   sha over all preceding bytes (`import.go:92-94`), verifies
   records==logical (`:95-97`), rejects missing trailer
   (`:146`), and rejects valid-extra-after-checksum (`:101-103`).
   Corrupt value rejected with checksum error, target counts unchanged
   (`import_test.go:63-104`); truncated (dropped trailer) rejected, fresh
   DB commits 0 apps/0 triples (`:109-135`); read-failure after checksum
   rolls back, no app row (`import_boundary_test.go:20-38`);
   extra valid line after checksum rejected, 0 apps/0 triples
   (`da003_extra_contract_test.go:23-50`); lied records count (valid sha)
   rejected, 0 apps/0 triples (`:52-102`); stored object artifact itself
   carries verifiable trailer with records==counts sum and non-empty sha
   (`da003_failclosed_test.go:191-213`).
3. Commit before full validation — FALSIFIED.
   `Import` opens one tx (`import.go:35-39`, deferred rollback), checks
   header/app binding before any row (`:44-71`), buffers triples without
   commit, validates checksum+records+extra-content before flush/commit
   (`:82-110`), commits once (`:107-110`). No early commit path exists;
   truncation/corruption/extra/records-mismatch/read-failure tests above
   all observe zero committed rows on fresh targets.
4. Target mutation after failed restore — FALSIFIED.
   Same-tx rollback plus idempotent upserts guarantee atomicity. Corrupt
   and truncated object restores return 400 with target triples/attrs
   unchanged (`da003_failclosed_test.go:508-597`); corrupt re-import into
   the live DB leaves the next export byte-identical, not merely
   count-identical (`da003_extra_contract_test.go:178-202`);
   cross-app and foreign-attr/triple imports write zero rows into the
   victim (`import_test.go:140-277`).
5. Source-artifact deletion or mutation — FALSIFIED.
   `handleRestoreObject` only `Get`s then `Import`s; no Put/Promote/Delete
   on the restore path (`http_objects.go:149-176`). Failed corrupt and
   truncated object restores leave source bytes identical via snapshot
   (`da003_failclosed_test.go:538-541,584-587`).
6. Ambiguous promotion outcome reported as success — FALSIFIED.
   `Promote` documents unknown publication status on error
   (`objectstore.go:15-18,78-87`); `handlePutObject` maps promotion error
   to 502 with explicit "publication status unknown", cleans staging, and
   never reports success (`http_objects.go:127-134`). Pinned by
   `TestPutObjectPromotionFailureReportsUnknownStatus`
   (`da003_failclosed_test.go:421-452`, 502 + "status unknown", final
   preserved). Cleanup-failure after successful promotion still returns
   200 (published truth) with explicit log, no retry-unsafe signal
   (`:283-321`).
7. Cancellation bypassing cleanup bounds — FALSIFIED.
   Staging cleanup runs on `context.WithoutCancel` with a 5s timeout
   (`http_objects.go:18-20,82-86`); canceled-request Put failure still
   performs exactly one staging delete on a non-canceled context with a
   deadline (`da003_failclosed_test.go:390-419`).

Integrated local contract (no object store wired): assembled daemon keeps
local `GET /backup/{app}` (NDJSON + checksum trailer validated by
`cf003CheckDump`), local `POST /backup/{app}/restore` round-trip, stable
object-route 503s, and unauthorized 401s
(`cmd/instantd/runtime_cf003_test.go:210-244`,
`daemon_object_disabled_test.go:27-137`,
`routes.go:221-228` with `S3` nil). In-memory/fake-S3 object tests prove
internal staging/promotion/rollback semantics only; no real S3/provider
behavior is claimed.

Initial findings: extra-after-checksum (valid line), records-count lie
(valid sha), traversal/escape namespace, and byte-exact rollback had no
dedicated pins. Repair: added
`internal/backup/da003_extra_contract_test.go` (four tests, all green on
first run, no production edit). No red-before-production-fix was needed;
the gap was missing evidence, not a defect. Re-review of the four new
tests confirms each fails if the corresponding check is removed
(structural code inspection: delete `:95-97` breaks records test; delete
`:101-103` breaks extra test; delete `scopeObjectKey` checks breaks
traversal test; early commit would break exact-dump test).
