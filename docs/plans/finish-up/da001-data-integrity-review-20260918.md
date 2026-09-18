# DA-001 data-integrity review — 2026-09-18

Scope: `internal/storageapi/backend.go`, `root_durability_test.go`,
`symlink_escape_test.go`, `atomicity_test.go`, `internal/config/config.go`,
`config_test.go`, `cmd/instantd/routes.go`, `daemon_restart_test.go`,
`daemon_startup_failure_test.go`, `daemon_object_disabled_test.go`,
`internal/backup/http_objects.go`, `http.go`, `docs/guides/07-selfhost.md`.

Verdict: **ACCEPT**

All nine falsification targets were attacked and falsified (no target holds
as a data-loss, durability, or escape violation):

1. Restart/reopen loses bytes/metadata — FALSIFIED.
   `Put` does CreateTemp→Copy→Sync→Close→Rename→syncDir
   (`internal/storageapi/backend.go:238-259`); `PutIfAbsent` does
   O_EXCL→Copy→Sync→Close→syncObjectDir (`backend.go:273-308`).
   Real multi-process restart A/B/C with distinct PIDs, same DB+root+secret,
   new ports (`cmd/instantd/daemon_restart_test.go:370-479`) proves exact
   bytes and content-type survival across two restarts.
2. fsync/confirmation ordering early-ACK — FALSIFIED.
   Every Put serializes on createMu through confirmAppDir
   (`backend.go:235-237,270-272`); barrier test proves no early ACK
   (`root_durability_test.go:226-271`).
3. Failed root confirmation trusted on retry — FALSIFIED.
   Rollback on sync failure (`backend.go:208-218`); re-sync of unconfirmed
   (`:173-178`); `TestDiskBackendFailedRootSyncRollsBack`,
   `TestDiskBackendFailedCleanupDoesNotBypassRootSync` (calls==2).
4. Concurrent first uploads diverge — FALSIFIED.
   32-way converge (`root_durability_test.go:134-167`); createMu + uploadKeyLock.
5. Pre-existing overwritten by retry — FALSIFIED.
   Prod uses PutIfAbsent O_EXCL only (`transfer.go:96`); cleanup only when
   created (`transfer.go:63-71`); `TestDiskBackendPutIfAbsentPreservesExistingObject`.
6. Partial-write residue — FALSIFIED.
   Put temp defer Remove; PutIfAbsent removeOnError join; oversize/metadata
   rollback; probe cleanup; staging cleanup (`http_objects.go:82-137`).
7. Failed start creates partials/starts notifier — FALSIFIED.
   mountRoutes validates storage before maintenance/transports/routes
   (`routes.go:32-56,74-233`); failure matrix asserts nonzero exit, no
   listener, no probe residue (`daemon_startup_failure_test.go:167-204`).
   Non-blocking observation: `runDatabase` starts notifier/bus before
   mountRoutes (`runtime.go:71-80`); idle notifier performs no writes and
   the process exits with no listener — hardening note only.
8. Fingerprint changes/leaks — FALSIFIED.
   sha256(root+quota+presence-bit) only (`config.go:295-300`); secret value
   never mixed in; daemon logs assert fingerprint present, secret absent
   (`daemon_restart_test.go:379-383,438-442`).
9. Cleanup outside owned root/DB — FALSIFIED.
   splitKey uuid/uuid (`backend.go:111-124`); symlink defense via Lstat
   (`:179-181,196-198,319-324,368-370` after Delete repair); traversal
   battery; Delete joins per-key errors; backup scopeObjectKey; teardown
   limited to owned root + instant_test_* DB.

Docs (`docs/guides/07-selfhost.md`) accurately describe required root,
persistence across restart, ephemeral warning, stable 503 disablement, and
OP-003/OP-004 separation with no overclaim.

## Addendum — 2026-09-19 exact $files metadata across restarts

The earlier review's row 1 cited the HTTP download `Content-Type` response
comparison (`daemon_restart_test.go` A-vs-B header equality) as corroborating
metadata survival. That comparison did not independently prove stored
metadata: `fileGet` derives the download `Content-Type` by sniffing object
bytes (`internal/storageapi/transfer.go:169-189`), not by reading the
persisted `$files` `content-type` triple. The earlier ACCEPT evidence for
bytes, startup refusal, 503 disablement, fingerprinting, and cleanup stands;
this addendum closes only the stored-metadata gap.

New assertion (`cmd/instantd/daemon_restart_test.go`):

- `da001FileMeta` (`:342-349`) captures all six persisted fields: `Path`,
  `ID`, `Size`, `ContentType`, `LocationID`, `KeyVersion`.
- `da001ReadFileMeta` (`:354-418`) queries the owned test fixture directly
  (`platform.LoadAttrCatalog` + `storage.FetchTriples` for the file entity),
  maps label→value without relying on map printing or row order, normalizes
  `json.Number` numerics, and fails if any of the six fields is missing.
- `da001RequireFileMeta` (`:420-425`) uses field-by-field struct equality, so
  an omitted, changed, or wrong-object field fails.
- Object one via A validates all six against upload expectations
  (`:507-511`: path `docs/restart-one.txt`, id, size, content-type
  `text/plain; charset=utf-8`, location-id, key-version 1).
- Object one A→B asserts exact equality after a distinct process restart
  (`:547-548`); exact blob bytes retained alongside.
- Object two via B validates all six (`:558-562`).
- Objects one A→C and two B→C assert exact equality after the second
  distinct restart (`:585-588`); exact-byte comparisons retained.
- Entity-scoped fetch isolates objects, so cross-object association fails.

Focused evidence: `TestDA001DaemonRestartPreservesObjects` passes twice
under `-race` with the owned database (zero skips). The HTTP download
`Content-Type` header is no longer asserted as metadata proof.

Verdict: **ACCEPT** — all six `$files` fields are proven exactly unchanged
across distinct daemon A/B/C restarts, alongside exact blob bytes.
