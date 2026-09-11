# BigBrotr

Design and research for a clean Rust implementation of BigBrotr: a Nostr archive,
relay observatory, analysis platform and configurable publication/read system.

**Planning stage only.** Product implementation has not begun. The repository is
private and temporarily named `bigbrotr-rs`; the product is **BigBrotr** and the
intended final GitHub repository name is `bigbrotr`. The neighboring Go repository
remains untouched by this work.

Start with [the implementation plan](PLAN.md). It links the complete architecture,
all service contracts, dashboard/private administration, website, configuration,
capacity model, security, recovery and practical validation programme.

[Mandate](docs/planning/mandate.md) ·
[Questions and decisions](docs/planning/questions.md) ·
[Six research revisions](docs/planning/process.md) ·
[Experiment results](docs/research/experiment-summary.md) ·
[Historical source inventory](docs/research/source-inventory.json)

Research scripts and probes are under `experiments/`. Downloaded sources,
intermediate data and build material stay in ignored `.research/`; tracked
reports distinguish source inspection, measurements and proposed behavior.
