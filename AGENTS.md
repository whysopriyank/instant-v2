inherits: ../.codex/AGENTS.md

# instant-v2 — durable-agent catalog

This repo keeps `AGENTS.md`'s durable-agent catalog. The CLI harness may not have
experiment agents `trial_luna_*` etc. registered; map as follows:

- `trial_luna_scout`  → `scout`
- `trial_luna_worker` / `trial_luna_deep_worker` → `implementation_worker`
- `trial_luna_test_worker` → `test_worker`
- `trial_luna_reviewer` → `reviewer`
- `trial_terra_analyst` / `_integrator` / `_debugger` → `debugger`
- `trial_sol_architect` / `_gate` / `_security` → `reviewer` / `security_reviewer`

Concretely, when dispatching a task batch, omit `agent` only when the spawn-policy
default matches the task; otherwise prefer the most specific agent above. See
`docs/guides/06-agent-orchestration.md` for the exclusive ownership map and day-to-day
`context`/`task` contract.
