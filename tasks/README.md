# Task backlogs

One file per roadmap phase. Each task names the suites from `docs/05-conformance.md`
that constitute done, and the exclusive package set it may touch. No task may move
the protocol schema or the interfaces in `docs/02-architecture.md` §4 — that belongs
to the main orchestrator's single-commit interface phase-opener.

Work each task with: `go test ./internal/<owner> -race -count=1 -run TestCorpus`
plus the cited `corpusctl` suite (slice, not whole corpus). Capture new regression
scenarios in `corpus/` before closing.

See `docs/06-agent-orchestration.md` for the parallelism and escalation model.
