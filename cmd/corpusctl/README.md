# corpusctl

Validate and replay the WS corpus; see [corpus contracts](../../corpus/README.md)
for the exact gates, fixture bootstrap, coverage, normalization and evidence
limitations.

`--mode validate --corpus corpus` is offline inventory validation.
`TestCorpusReplayIntegration` is the real isolated-PostgreSQL v2 replay gate.
`--mode replay --target ...` uses an already seeded external endpoint.
`--mode differential --target ... --other ... --output-dir ...` additionally
requires a pinned v1 checkout and equivalent externally initialized fixtures.
No command fabricates recordings or implicitly bootstraps remote databases.

For a single declared HTTP exchange, use `--transport http` with a JSON file
encoding `corpus.HTTPExchange`. For an SSE GET/POST session, use
`--transport sse` with a `corpus.SSEScenario`. These replay paths retain raw
responses/stream blocks, compare JSON through the shared canonicalizer, and
optionally write private write-once evidence with `--output-dir`; they do not
seed or reset fixtures.

CF-002 scope is bounded HTTP and SSE capture (`--mode record` with `--transport http`
or `--transport sse`). Record mode remains explicitly unsupported for WebSocket
(`--transport ws`). Recording requires an explicit `--target`, a securely reserved
fresh private output directory mode 0700 (`--output-dir`), and caller-supplied
endpoint (`--endpoint-id`), source (`--source-id`), and fixture (`--fixture-id`)
identity metadata. Output directories are resolved and reserved via descriptor-relative
no-follow traversal (`openat O_NOFOLLOW`, `mkdirat`) pinning parent and directory
file descriptors and `(dev, ino)` identities without path-based cleanup on reservation
failure. Publication is atomic and write-once: temporary evidence files are created
via `openat` on the pinned directory file descriptor (mode 0600), synced, closed,
published atomically via a kernel no-replace rename (`renameatx_np` on Darwin,
`renameat2` on Linux), followed by directory sync. Failed publication never performs
ambiguous named-file deletion: private temporary or
final artifacts may remain as untrusted incomplete evidence, and the operation returns
failure. Only a successful return makes an artifact eligible evidence. Non-Unix
platforms fail closed as unsupported.

Limitations: Caller-supplied endpoint ID, source ID, fixture ID, and
`fixtureReset=caller-owned` metadata are unverified caller assertions, not proof
of candidate revision, true fixture reset, or server state. `corpusctl` does not
provision, verify, or reset external fixtures. WebSocket recording, client SDK
harness bindings, and external endpoint process/lifecycle management remain
unsupported. Do not claim full candidate binding or real fixture reset proof.
