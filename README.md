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

The earlier [complete-document audit](docs/reviews/11-whole-repository-audit.md)
records full coverage of its 70-file Markdown baseline, concrete corrections,
the table/interface/naming handoff and a controlled document-layout experiment.
[Decision confidence](docs/planning/decision-confidence.md)
states which choices are justified and which target-scale claims remain untested.

The [current readiness register](docs/planning/specification-readiness.md) closes
S01–S08 on paper and identifies the practical gates that only actual adapters,
engines and packaged clients can satisfy. The [agent execution guide](docs/implementation/agent-handoff.md)
provides task selection, dependency research, independent fixtures, practical
checks, periodic audits and coherent commit discipline. Root [AGENTS.md](AGENTS.md)
is the single agent entrypoint.

The [architecture challenge](docs/reviews/13-architecture-alternatives.md) is
historical advice; adopted changes now live in the current contracts. The
[independent closure review](docs/reviews/14-independent-closure-review.md)
records concrete cross-document corrections. Optional MCP and experimental
ContextVM are specified alongside HTTP, ArchiveRelay and DVM through one read core.

The [large Tux storage and recovery report](docs/research/tux-storage-qualification-20260917.md)
adds 20-million-event engine experiments, signed readback and actual restore
evidence. It states their limits explicitly; it is not fleet or product acceptance.

The current [full review and deployment handoff](docs/reviews/16-complete-handoff.md)
covers its complete 106-file Markdown baseline, then reconciles independent roles,
local/S3 storage, useful incremental analytics, compatible assertions and agent
resumption. Start installation design with the
[deployment contract](docs/specification/deployment-contract.md); the
[archive placement contract](docs/specification/archive-storage-contract.md)
explains exactly when S3 is useful. The additional
[Tux object-storage and CH report](docs/research/tux-object-storage-20260917.md)
records executed engine checks and their limits.
