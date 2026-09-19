# FU-03 — Admin-presence surface decision (Option 1: enforced 501 exclusion)

**Status:** decided Option 1. `corpus/manifest.json`
`rooms-presence-lifecycle` `expectedState` rewritten to the enforced
501 exclusion; `status` stays `gap`.

**Parent packet:** `docs/plans/finish-up/followup-packets.md` FU-03
(proposed). That file is untouched by this packet; no terminal row
(`program-manifest.md`, `execution-ledger.md` verdicts) is flipped and
no CF-002/004/005 material is touched.

**Decision (conservative, enforcement-preserving):** no positive
admin-presence surface is built. Production enforces
`GET /admin/rooms/presence` → `501 unsupported` in
`internal/adminapi/schema.go` (`handlePresence` returns exact body
`{"message":"admin presence is unsupported","type":"unsupported"}`),
so the manifest must not keep claiming a positive
"admin presence view reflects join, update and leave lifecycle".

**Proof (exact oracles, no new code/tests):**
`cmd/instantd/runtime_cf003_presence_exclusion_test.go::TestCF003AssembledAdminPresenceExclusion`
@ `951175c` proves over production-mounted routes: authorized caller
gets exact 501 + `application/json`; still exact 501 with live WS room
presence behind it (non-vacuous); missing/foreign credentials get exact
401 without disclosing presence state; unknown app gets exact 404.

**Manifest change (single row, this packet only):**
- `rooms-presence-lifecycle`: `expectedState` →
  `"GET /admin/rooms/presence is unsupported and returns stable 501"`;
  `status` stays `gap`; `oracle`/all other fields untouched; `note`
  cites the test + HEAD (see manifest). Rewritten at HEAD
  `99c89f5`.
- Why `gap` is kept: `followup-packets.md` FU-03 authorizes only the
  `expectedState` text change + exclusion evidence, not a covered flip;
  `corpus/manifest.json` has no `covered-by-exclusion` status pattern
  (only `gap`/`covered`/`unsupported` across 26 rows), so no status
  pattern applies. A flip to `unsupported` or `covered` would be a
  separate exclusion-row decision and is explicitly not made here.

**Non-goals (preserved):** no positive admin-presence implementation,
no capture run, no NDJSON, no code/test change, no other row change, no
CF-002/004/005 change, no push/deployment/publication/tag.
