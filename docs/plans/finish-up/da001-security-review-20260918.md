# DA-001 security review — 2026-09-18

Scope: `internal/storageapi/backend.go`, `signing.go`, `authorization.go`,
`control.go`, `transfer.go`, `root_durability_test.go`,
`symlink_escape_test.go`, `storageapi_test.go`, `atomicity_test.go`,
`internal/config/config.go`, `config_test.go`, `cmd/instantd/routes.go`,
`runtime.go`, `serve.go`, `main.go`, `daemon_restart_test.go`,
`daemon_startup_failure_test.go`, `daemon_object_disabled_test.go`,
`internal/backup/http.go`, `http_objects.go`, `docs/guides/07-selfhost.md`.

## Initial verdict: REJECT (repaired, see below)

One load-bearing gap was found: `DiskBackend.Delete` omitted the
symlink-dir rejection enforced by Put/Open, allowing a planted
`root/<app-uuid> -> /outside` link to direct `os.Remove` outside the root.

- Put blocked via `confirmAppDir` rejectSymlinkDir
  (`internal/storageapi/backend.go:179,196-198`, impl `:340-352`).
- Open blocked via dir + file Lstat checks (`backend.go:319-324`).
- Delete NOT blocked: `backend.go:354-367` (pre-repair) called `file()`
  then `os.Remove(p)` with no Lstat guard.

## Repair

`Delete` now serializes on `createMu` (`backend.go:355-356`) and calls
`rejectSymlinkDir(filepath.Dir(p))` before each `Remove` (`:368-370`).
`TestDiskBackendSymlinkAppDirRejected`
(`internal/storageapi/symlink_escape_test.go:17`) now asserts Put, Open,
AND Delete all refuse with `symlink` in the error (`:30-44`) plus
victim-file survival across a second refused Delete (`:59-66`).

## Re-review verdict: ACCEPT

Focused re-review of the Delete repair confirms:

- Delete holds `createMu`, guards each key independently, joins errors;
  traversal via Delete blocked by `splitKey` uuid/uuid + `path.Clean`;
  bulk Delete checks each key; file-symlink Delete unlinks safely;
  no nested `createMu` acquisition (flat lock, no deadlock).
- All other targets hold:
  1. Traversal — ACCEPT (`backend.go:112-133`, `http_objects.go:26-36`,
     `root_durability_test.go:275-285`).
  2. Symlink — ACCEPT after repair (see above).
  3. Writable-root validation — ACCEPT (`backend.go:50-92`,
     `routes.go:51-54`, `config.go:176-186`).
  4. Secret exposure — ACCEPT (fingerprint presence-bit only
     `config.go:295-305`; logs root+fingerprint only `routes.go:55-56`;
     daemon tests assert secret absent).
  5. Unauthorized storage access — ACCEPT (admin gating
     `authorization.go:19-55`; HMAC `signing.go:17-37`; filename binding;
     `TestStorageAdminGate`).
  6. Disabled object routes — ACCEPT (S3 nil → stable 503
     `http_objects.go:63-66,150-153`; `routes.go:224-228`; no files delta;
     storage + local backup still work).
  7. Unsafe cleanup — ACCEPT after repair (see above; all other cleanup
     paths correctly scoped).
  8. Startup failure — ACCEPT (nonzero exit, diagnosable, no listener,
     no residue; loopback guard strict).

The original REJECT finding is preserved above; the repair and re-review
ACCEPT close it.
