# BigBrotr

BigBrotr is a verifiable Nostr archive and relay observatory: full signed events,
optional provenance, useful analytics, and configurable read/publication surfaces.
This repository contains the design, research and detailed implementation plan for
its fresh Rust implementation, including dashboard, private administration and
website/documentation/blog.

**Planning is complete for implementation handoff; product implementation has
not begun.** This is not a runnable release. The repository remains private and
temporarily named `bigbrotr-rs`; the product name is **BigBrotr**. The eventual
GitHub name `bigbrotr` is a separate transition. The neighboring Go checkout
remains read-only.

## Start here

| Reader's purpose | Read next |
|---|---|
| Understand the product and data flow | [Plain-language guide](docs/start-here.md), then [system design](docs/architecture/system-design.md) |
| Understand what is stored and exposed | [Storage catalog](docs/specification/storage-catalog.md), [interfaces](docs/specification/interface-catalog.md) and [data fields](docs/specification/data-contracts.md) |
| Implement the project | [AGENTS.md](AGENTS.md) → [current checkpoint](docs/implementation/progress.md) → [agent execution guide](docs/implementation/agent-handoff.md) → [PLAN.md](PLAN.md) and [phase/task/step index](docs/implementation/README.md) |
| Develop, test and release | [Development and delivery](docs/engineering/development-and-delivery.md): Mac/Tux, CI, Docker Hub, coherent commits and existing planning checks |
| Understand installation and placement | [Deployment contract](docs/specification/deployment-contract.md) and [local/S3 archive contract](docs/specification/archive-storage-contract.md) |
| Distinguish decisions from unexecuted qualification | [Readiness register](docs/planning/specification-readiness.md), [decision confidence](docs/planning/decision-confidence.md) and [experiment summary](docs/research/experiment-summary.md) |

Current contracts define the selected behavior; task files define how to implement
and demonstrate it. Historical reviews explain changes and do not supersede those
contracts. The plan has 123 tasks and 756 numbered steps, including ten planning
tasks; no future product step is complete merely because its specification exists.

## Working and delivery model

Use the Mac as the primary editing/Git workspace and isolated exact-revision
checkouts on `ssh tux` for native Linux builds, practical faults and larger runs.
The workflow is portable to another developer machine or Linux lab. GitHub Actions
runs isolated CI; Docker Hub distributes application images. Standard single-host
Docker Compose with local archive is the main installation path. S3 and split
roles use ordinary explicit endpoints when needed. No custom cluster manager is
required. All initial Cargo packages stay unpublished; branches and parallel work
are optional tools, while coherent commits and substantive review are required.

Those are specifications for the next stage. This repository does not yet ship
Cargo application packages, product CI workflows, Docker images or installation
bundles. Existing check commands and their Python/Node preparation are documented
in [planning verification](docs/engineering/development-and-delivery.md#checks-available-in-the-planning-repository).

## Repository map and evidence

`docs/product/` explains intent; `docs/architecture/` describes responsibilities;
`docs/specification/` owns precise contracts; `docs/engineering/` owns implementation
and operational practices; `docs/implementation/` contains the executable work
breakdown and one resumption checkpoint. `docs/planning/`, `docs/reviews/` and
`docs/research/` preserve decisions, audits and attributable evidence.
`experiments/` contains research probes, not production adapters.

Downloaded sources, caches and raw generated data belong under ignored
`.research/` and the [laboratory inventory](docs/research/laboratory-inventory.json).
Tracked reports state exact methods and limits; a result from a small model or
component engine cannot certify future product behavior or busy-fleet capacity.

The [latest handoff audit](docs/reviews/17-development-and-delivery-handoff.md)
records concrete corrections and the final validation scope. It builds on
[Review 16's complete baseline reading](docs/reviews/16-complete-handoff.md),
the [architecture challenge](docs/reviews/13-architecture-alternatives.md), the
[20-million-event storage/recovery study](docs/research/tux-storage-qualification-20260917.md)
and the [local/S3 and ClickHouse experiments](docs/research/tux-object-storage-20260917.md).
The [mandate](docs/planning/mandate.md), [decision ledger](docs/planning/questions.md),
[coverage ledger](docs/implementation/coverage.md), [research process](docs/planning/process.md)
and [historical source inventory](docs/research/source-inventory.json) preserve
context without creating a competing implementation plan.
