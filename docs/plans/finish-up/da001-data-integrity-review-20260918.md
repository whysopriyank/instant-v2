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
