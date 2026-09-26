# Alpha exception decision — AX-001

Owner direction 2026-09-26 (see `docs/plans/finish-up/completion-run-20260926.md`):
the first release is an **explicit alpha** (`single-node-alpha`). The packets
below are **not claimed** for this release. They are not complete, not
accepted, and not excluded-as-unsupported features — they are claims the alpha
does not make.

## Alpha exception table

| Packet | New manifest state | What the alpha does NOT claim |
|---|---|---|
| DA-006B provider acceptance | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | Google/GitHub OAuth verified against real providers (local contract DA-006A is accepted; provider round-trip unverified) |
| DA-008B magic-code provider | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | real email delivery of magic codes |
| CF-004 pinned-v1 environment | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | any v1 runtime comparison |
| CF-005 differential | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | v1 wire/behaviour parity or drop-in replacement |
| OP-004 container runtime | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | qualified container image (gate lane `container: not_selected`) |
| OP-006 backup/restore drill | `NOT_SELECTED / OWNER_ALPHA_EXCEPTION` | a drilled restore RPO/RTO (DA-003 fail-closed backup remains accepted) |

## Already `NOT_SELECTED` rows

The following rows keep their existing `NOT_SELECTED` state; they are likewise
not claimed for this alpha: OP-001 publisher recovery, OP-002 replica
visibility, QR-002 comparative performance, QR-004 publish/sign/SBOM, FR-003
publication, FR-004 canary/rollback. No sentence in any current doc may claim
them for the alpha.

## Deferred rows

TD-001, TD-002, TD-003, TD-004, TD-005 stay `DEFERRED`. This decision does not
promote, complete, or accept any of them.

## Rule

None of the packets listed above may be claimed in any current doc until its
packet is re-selected and completed on a new candidate. A new candidate with
fresh evidence and (where required) a new owner decision is required before
any of these claims may be stated.
