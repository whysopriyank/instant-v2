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
