# BigBrotr

Design and research for a clean Rust implementation of BigBrotr: a Nostr archive,
relay observatory, analysis platform and configurable publication/read system.

**Planning stage only.** Product implementation has not begun. The repository is
private and temporarily named `bigbrotr-rs`; the product is **BigBrotr** and the
intended final GitHub repository name is `bigbrotr`. The neighboring Go repository
remains untouched by this work.

Start with [the plain-language guide](docs/start-here.md), then
[the implementation plan](PLAN.md). It links the complete architecture,
all service contracts, dashboard/private administration, website, configuration,
capacity model, security, recovery and practical validation programme.

[Mandate](docs/planning/mandate.md) ·
[Questions and decisions](docs/planning/questions.md) ·
[Research revisions and full-source recheck](docs/planning/process.md) ·
[Experiment results](docs/research/experiment-summary.md) ·
[Historical source inventory](docs/research/source-inventory.json)

Research scripts and probes are under `experiments/`. Downloaded sources,
intermediate data and build material stay in ignored `.research/`; tracked
reports distinguish source inspection, measurements and proposed behavior.

The [detailed phase/task/step plan](docs/implementation/README.md),
[field contracts](docs/specification/data-contracts.md) and
[coverage ledger](docs/implementation/coverage.md) are the implementation handoff.
The [five-round expansion review](docs/reviews/09-expanded-plan-rounds.md) records
what changed and which practical results remain owed by the future product.

The latest [complete-document audit](docs/reviews/11-whole-repository-audit.md)
records full coverage of its 70-file Markdown baseline, concrete corrections,
the table/interface/naming handoff and a controlled document-layout experiment.
[Decision confidence](docs/planning/decision-confidence.md)
states which choices are justified and which target-scale claims remain untested.
