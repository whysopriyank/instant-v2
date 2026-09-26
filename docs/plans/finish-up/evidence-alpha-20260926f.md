# Evidence register — single-node alpha acceptance (campaign `alpha-20260926f`)

This file records where the qualification evidence behind the FR-002
single-node-alpha acceptance lives, how its copies relate to each other and to
the repository, and how to verify them. The evidence itself is **not** in git
(49 MB, includes the candidate binary); only its identity is recorded here.

## Chain of identity

```
source commit  f7dc5b10327a3b6a31d540c426e62d710d4078af   (the accepted candidate)
  └─ binary    sha256 a10a6ef56f94076db6253f18fb360bf15974b5e701f6132a255123b9595767e2
               built by `make build` in qualification image
               sha256:01096c3eb3c42cb37a657ff6b867ecb03eafd5db3c4a8314a3e14dade91919f4
               (scripts/qualify/Dockerfile, Go 1.25.14); the gate rebuilt it byte-identically
  └─ config    candidate/instantd.env sha256 aeb6a02000d8ac779f86a600f5b7009379cb1893237fb31189a93959313428a2
               (non-secret: HTTP addr, storage root, invalidation bus)
  └─ records   records/native.json   (OP-003) sha256 cbf5e5323034cf5424c770c443ed72eac2aed9470f934ed52386a1b99b4dcd7b
               records/recovery.json (OP-005) sha256 0669859a6c8ae2499a345e24fb5c1c77e37d401072d14eec6e01b4fc35f58a91
               records/soak.json     (QR-001) sha256 22ba8de6645ae1ac84472fa148ac4528746c9cccc7dd26b2a3f14524b7bd28a7
               each record names the campaign, candidate, binary and config sha, and
               hashes every per-lane artifact it was derived from
  └─ manifest  manifest.json sha256 5be877e3ea934684d67523cb19f306c7bf1766261442327946754b035cb88213
               (28 handoffs from fr002-handoffs.json, 3 external records, lane selection,
               campaign_started_at 2026-09-26T09:14:38Z)
  └─ gate      gate.log sha256 896d57c6b872b1946ee5bc6a6a0416995bd751d752d7af3fdc35ee618bca3e79
               last line: "release-gate: selected single-node-alpha checks passed for f7dc5b10…"
  └─ archive   evidence-alpha-20260926f.tgz sha256 a11eb9b3c00143426e8a72a1f3c84ad8883d42f4b8af08869732aceb958fa16b
               (tar of the whole evidence/ tree, 60 files, taken after the gate ran)
  └─ recorded  commit 758a40d (post-acceptance status; not part of the candidate)
               and the FR-002 handoff in execution-ledger.md
```

The candidate commit is what was qualified. Commits after it (status docs,
this register) do not change the qualified binary, but **any new code commit
is a new candidate and needs a new campaign** — the binary embeds the git
revision, so these records cannot be reused for it.

## Copies

| # | Location | Role | Contents |
|---|---|---|---|
| 1 | `cutiewhy:~/iv2q-alpha-20260926f/work/evidence/` | Primary (as produced; the gate read this tree read-only at `/evidence`) | 60 files, unpacked |
| 2 | `cutiewhy:~/iv2q-alpha-20260926f/harness/` | The exact candidate checkout the lanes and gate ran from | git clone of the candidate bundle |
| 3 | `bigbeast:~/iv2q-evidence-archive/alpha-20260926f/` | Off-host archive | `.tgz` + `.sha256` |
| 4 | Owner workstation `~/Developer/sideproj/instant-v2-evidence/alpha-20260926f/` (outside the repo) | Local archive | `.tgz` + `.sha256` |

Copies 3 and 4 are byte-identical to each other (archive sha above) and were
checked against copy 1 on 2026-09-26: the manifest inside the archive hashes
to the manifest sha above, and the archive holds the same 60 files. Copy 1 is
authoritative if they ever disagree; a disagreement means tampering or
corruption, and the acceptance should then be re-derived from a new campaign.

Superseded attempts `alpha-20260926a`…`e` also remain on the hosts under
`~/iv2q-alpha-20260926{a..e}`. They are diagnostic history only (see the
execution ledger) and carry no acceptance weight; they may be deleted.

## Layout of the evidence tree

```
evidence/
  candidate/            instantd, binary.sha256, image.digest, instantd.env
  candidate-build1.txt  two independent builds of the candidate, both a10a6ef5…
  candidate-build2.txt  (build determinism check)
  build/                cleanup.complete
  native/               gotest.json (go test -json), lane.json, cleanup.complete
  recovery/             recovery-<outcome>.json ×7 (ledger, oracle, preconditions,
                        close codes), lane.json, cleanup.complete
  soak/                 soak-events.jsonl (+ .complete, .manifest.json),
                        soak-timeseries.jsonl, soak.log, lane.json, cleanup.complete
  records/              native.json, recovery.json, soak.json (gate-schema records)
  handoffs/             one handoff file per packet (28)
  handoffs-input.json, manifest.json, gate.log
```

## Verifying

```sh
# archive integrity
shasum -a 256 -c evidence-alpha-20260926f.tgz.sha256
# manifest identity inside the archive
tar xzf evidence-alpha-20260926f.tgz -O evidence/manifest.json | shasum -a 256   # → 5be877e3…
# re-run the gate against the evidence (Linux host with Docker, candidate checkout):
bash scripts/qualify/gate.sh --campaign alpha-20260926f \
  --candidate f7dc5b10327a3b6a31d540c426e62d710d4078af --workdir <dir containing evidence/>
```

The gate enforces a maximum campaign age (`campaign_max_age_seconds` = 86400
in this manifest, i.e. 24 h from 2026-09-26T09:14:38Z), so re-running it after
that is expected to fail with "campaign timestamp is stale or in the future";
that is a freshness rule, not a sign the evidence changed. Use the hashes
above to establish integrity after that window.

## Retention

Keep copies 3 and 4 for as long as the alpha acceptance is referenced by any
release note or status document. Copy 1 may be removed once 3 and 4 are
confirmed. Nothing here was pushed, tagged, published, or deployed.
