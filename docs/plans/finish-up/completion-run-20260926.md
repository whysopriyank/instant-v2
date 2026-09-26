# Completion run — 2026-09-26

Coordinator: Claude Opus 5.5 (Claude Code). Implementation: OpenCode CLI,
default model `opencode/muse-spark-1.3-contributor-free`, xhigh effort.
Review: coordinator (non-author) after every packet; repairs are either small
coordinator fixes or a written repair contract handed back to OpenCode.

## Owner direction (2026-09-26, recorded verbatim in substance)

- Finish the finish-up program as an **explicit alpha** (`single-node-alpha`);
  no v1-parity, production-ready, provider, or performance claim.
- OAuth / email provider acceptance (DA-006B, DA-008B): **skip** — not claimed.
- Pinned-v1 runtime (CF-004/005): coordinator's choice → **not claimed** for
  the alpha (no v1 drop-in parity claim).
- Environment runs use owned hosts `bigbeast`, `cutiepie`, `cutiewhy`
  (Ubuntu, Linux 6.8 x86_64, Docker). Coordinator decides without further
  owner review.

## Target: `FIRST_RELEASE_ACCEPTED` as `single-node alpha accepted`

The composed gate (`scripts/quality-release-gate.sh`) already fixes the
required inventory: GREEN handoffs for 28 packets (incl. CF-003, OP-003,
OP-005, QR-001) plus three external records (`native_linux`, `recovery`,
`soak`) bound to one candidate SHA/binary/config/endpoint. Container,
external-v1 and performance lanes are `not_selected`.

## Packet sequence

| # | Packet | Owner | Exit |
|---:|---|---|---|
| 1 | CF-003 closure — owner-accepted residual-gap exceptions, enforced by `validate-release` | OpenCode | CF-003 `COMPLETE` |
| 2 | QH-001 qualification harness — Linux campaign scripts emitting gate-schema `native_linux` / `recovery` / `soak` records + campaign manifest assembly | OpenCode | hermetic contract tests green |
| 3 | OP-003 native Linux run (bigbeast, containerised pinned Go toolchain, owned PostgreSQL container) | coordinator via harness | record PASS |
| 4 | OP-005 crash/bounce/drain run (bigbeast) | coordinator via harness | record PASS, 7 outcomes |
| 5 | QR-001 soak ≥500 sessions × ≥900 s (cutiewhy) | coordinator via harness | record PASS |
| 6 | Alpha exception record: DA-006B, DA-008B, CF-004/005, OP-004, OP-006, QR-002/004, FR-003/004 not claimed | OpenCode (docs) | enforced in envelope/docs |
| 7 | FR-002 immutable acceptance on bigbeast (same toolchain as evidence) | coordinator + independent reviewer | gate exit 0 |

Product defects found during 3–5 stop the run and become a new bounded packet;
evidence restarts on the new candidate.

## Host hygiene

The hosts run unrelated workloads. Every campaign uses uniquely prefixed
containers/networks/volumes (`iv2q-<campaign>-*`), binds only loopback or an
internal Docker network, and removes everything it created. Pre-existing
containers are never touched.
