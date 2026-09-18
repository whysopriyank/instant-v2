# DA-003 security review — 2026-09-19

Scope: `internal/backup/http.go`, `http_objects.go`, `import.go`,
`import_records.go`, `objectstore.go`, `da003_failclosed_test.go`,
`da003_extra_contract_test.go`, `http_test.go`, `import_test.go`,
`import_boundary_test.go`, `cmd/instantd/routes.go`
(assembles `backup.Handler` with `a.cats.CheckAdminToken`, `S3` nil),
`daemon_object_disabled_test.go`, `runtime_cf003_test.go`.

Verdict: **ACCEPT**

All seven falsification targets were attacked and falsified. Authorization
precedes every side effect; tenant and key namespaces are enforced; no
credential or dump bytes leak on denial.

1. Nil-auth panic or fail-open — FALSIFIED.
   `ServeHTTP` rejects nil `AdminTokenCheck` with 500 before routing,
   UUID parsing, auth, DB, or store work (`http.go:64-69`, logs without
   token). Pinned for the base route (`http_test.go:110-123`, no panic,
   500) and for all three object routes (`da003_failclosed_test.go:121-155`,
   no panic, 500 each, zero puts/gets).
2. Unauthorized data disclosure or mutation — FALSIFIED.
   Token extraction mirrors adminapi (`http.go:223-232`); `AdminTokenCheck`
   runs before the action switch (`:84-95`), so export/restore/object
   handlers are unreachable on 401. Bad-token export is 401
   (`http_test.go:21-46`); bad-token restore is 401 with triples/attrs
   unchanged (`:128-153`); bad/missing-token PUT/GET/restore-object are
   401 each, GET emits no `kind` bytes, zero puts/gets
   (`da003_failclosed_test.go:457-502`); assembled-mux hermetic matrix
   pins backup 401 with exact body and no files
   (`runtime_cf003_test.go:109-124,126-133`).
3. Cross-app restore — FALSIFIED.
   `Import` binds the dump header app to the authenticated route app
   before any write and aborts with `ErrAppMismatch`
   (`import.go:56-71`, `ErrAppMismatch` at `:22`). Cross-app import
   returns `ErrAppMismatch` with zero rows in the victim
   (`import_test.go:140-174`); foreign attr ids (public via init-ok) are
   rejected without mutating the owner (`:179-231`); foreign triple attrs
   without a matching attr record are rejected with zero writes
   (`:236-277`); HTTP `writeImportError` maps mismatch to 403
   (`http.go:183-195`).
4. Public access to private staging keys — FALSIFIED.
   Staging prefix `.instant-backup-staging/` is rejected by
   `scopeObjectKey` (`http_objects.go:25-36`), staging keys embed
   `staging/<app>/<rand16>` (`:141-147`), PUT writes only to staging then
   promotes (`:76-139`), success deletes staging (`:135-137`), every
   failure deletes staging (`:104-130`). Direct GET/PUT/restore-object
   with a staging key is 400 with zero store calls
   (`da003_failclosed_test.go:251-281`); successful PUT leaves no staged
   object (`:161-227`, snapshot of staging key absent).
5. Key traversal or namespace escape — FALSIFIED.
   `scopeObjectKey` rejects empty, `..`-containing, leading-`/`, and
   staging-prefixed keys, then prefixes with `<caller-app>/`
   (`http_objects.go:25-36`). Traversal battery (`..`, `../evil`,
   `a/../b`, `dumps/../../escape`, `/etc/passwd`, `/absolute`) is 400 on
   PUT/GET/restore-object with zero puts/gets/promotions/deletes and no
   dump bytes (`da003_extra_contract_test.go:104-176`); a key naming a
   foreign app id is stored strictly under the caller prefix
   (`:163-176`, response key `caller/foreign`, no write outside prefix).
6. Credential/token disclosure — FALSIFIED.
   Token-check failures log only `"err"` from the checker, never the
   token (`http.go:86-94`); missing-config logs a static string
   (`:66`); S3/promotion/cleanup failures log `err` only
   (`http_objects.go:78-80,105-136,151-167`); `NewS3Store` errors carry
   endpoint/bucket probe text, never secret material
   (`objectstore.go:49-70`); disabled daemon wires no object store and
   the log contains no S3 bucket wiring
   (`daemon_object_disabled_test.go:106-111`); 401/400/502 bodies are
   fixed messages without tokens, keys, or secrets (see tests above).
7. Authorization after side effect — FALSIFIED.
   Order in `ServeHTTP` is fixed: nil-check → route parse → UUID parse →
   method check → token check → action (`http.go:64-120`). No Pool, S3,
   export, import, or filesystem call precedes the 401. Proven by zero
   store calls on missing-config object routes (`da003_failclosed_test.go:150-154`),
   zero puts/gets on unauthorized object routes (`:497-501`), unchanged
   DB on unauthorized restore (`http_test.go:144-152`), and zero files on
   hermetic denials (`runtime_cf003_test.go:126-133`). Disabled runtime
   (`S3` nil, `routes.go:224-228`) still enforces auth first: object
   routes 503 only after auth passes
   (`objectstore_http_test.go:44-52`,
   `daemon_object_disabled_test.go:70-94` with auth header).

Boundary: local NDJSON export/restore is accepted; runtime object-backup
routes remain deliberately unwired (stable 503 `{"message":"no object
store wired"}`, `http_objects.go:63-65,150-153`,
`import_boundary_test.go:40-53`). In-memory/fake-S3 tests prove internal
failure semantics only; no real S3, provider credential, external network,
recovery campaign, Linux/container qualification, or production/release
acceptance is claimed.

Initial findings: traversal/escape had code enforcement but no dedicated
pin; added `TestObjectKeysRejectTraversalAndEscape`
(`da003_extra_contract_test.go:104-176`, green first run, no production
edit). No other repair needed. Re-review confirms the new test fails if
`scopeObjectKey` checks are removed and passes with them.
