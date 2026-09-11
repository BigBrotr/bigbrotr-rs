# Repository context

This is the design repository for BigBrotr's fresh Rust implementation. The
current stage is research, architecture, product design, and implementation
planning. Do not begin product implementation until the owner starts that stage.
Small, isolated research experiments are authorized and must be clearly labeled.

The neighboring Go repository and historical projects are reference material and
must remain read-only. Their process rules and past technology decisions do not
constrain this new design. Preserve useful product intent and explicitly explain
changes to it.

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
