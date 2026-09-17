# Repository context

This is the design repository for BigBrotr's fresh Rust implementation. The
current stage is research, architecture, product design, and implementation
planning. Do not begin product implementation until the owner starts that stage.
Isolated research experiments are authorized and must be clearly labeled. The
owner explicitly authorizes substantial, long-running, realistic experiments on
`ssh tux` when scale can change a critical decision; research is not limited to
small probes. Preserve the local legacy repository and unrelated workloads.
The owner explicitly retired and authorized deletion of all pre-existing
BigBrotr resources on Tux on 2026-09-17; do not reuse those environments. New
research must create separately inventoried resources from scratch.

The neighboring local Go repository and local historical projects are reference
material and must remain read-only. Their process rules and past technology
decisions do not constrain this new design. Preserve useful product intent and
explicitly explain changes to it.

Write repository content and commit messages in English. Keep the product name
BigBrotr; `bigbrotr-rs` is the repository identifier. Record user decisions and
unresolved assumptions in `docs/planning/questions.md`, and keep the current
mandate in `docs/planning/mandate.md`.

Base claims on source inspection, documented guarantees, or recorded experiments.
Separate measured results from extrapolations and proposals. A passing test is
evidence for the behavior it exercises, not proof of overall production fitness.
Review code, contracts, operations, and actual user journeys together.

Use coherent commits for reviewable work. Keep reproducible experiment sources
and concise evidence; downloaded checkouts, build caches, and raw generated data
belong under ignored `.research/` and must be inventoried. Never commit secrets,
live credentials, or unrelated host data. Do not add arbitrary line-count,
coverage-percentage, or repeated-clean-review requirements.

## Implementation handoff

Use [the agent execution guide](docs/implementation/agent-handoff.md) as the
single pipeline for task selection, dependency research, independent fixtures,
practical execution, review, documentation and coherent commits. Root AGENTS.md
is the agent entrypoint; do not maintain a competing agent.txt instruction set.
The current specification is authoritative over historical review suggestions.

Before product work, consult the readiness register. During implementation,
audit every new persistent boundary and each end-to-end milestone; test actual
processes, engines and clients where those guarantees matter. Do not translate
unchecked planned commands or small research probes into claimed release evidence.
