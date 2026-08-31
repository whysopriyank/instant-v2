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
