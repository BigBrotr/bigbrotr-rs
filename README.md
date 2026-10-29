# BigBrotr

BigBrotr is a verifiable Nostr archive and relay observatory: full signed events,
optional provenance, useful analytics, and configurable read/publication surfaces.
This repository contains the implementation, design, research and detailed task plan
for its fresh Rust implementation, including dashboard, private administration and
website/documentation/blog.

**Product implementation began with the owner's authorization on 2026-09-17.**
The current CLI provides help/version/build identity, bounded independent
[signed-event verification](docs/reference/protocols.md),
[static configuration](docs/reference/configuration.md) and explicit initial
[PostgreSQL control migrations](docs/operations/schema-upgrades.md), with
[one-time Control initialization](docs/operations/control-initialization.md) and
accepted-configuration inspection.
The development CLI also provides explicit [archive and coordinator
initialization](docs/user/archive-initialization.md) and finite
[retained-receipt inspection](docs/user/archive-receipts.md). These commands have
separate prerequisites and qualification scopes. The explicit initial
Control/Ingest/Sync/Archive composition supports controlled signed acquisition and
publication; ordinary Control supports private history and signed-event export.
The [first-event tutorial](docs/tutorials/first-event.md) runs a development
candidate outside the checkout, using explicitly declared repository clients and
fixtures. The scoped [M1 milestone](docs/implementation/README.md#coherent-implementation-milestones)
is qualified: signed admission, Archive, private history/export and same-data
restart, including separate dependency outages and truthful capability refusals.
Continuous Index succession, broader recovery/readiness, full installation and
the remaining product capabilities retain their implementation and qualification
gates. This is not a production release or a source-free first-event distribution.
The repository is public and named `bigbrotr-rs`; the product name is **BigBrotr**.
Public source availability does not imply production readiness, an official
release or published packages. A future repository rename is a separate decision.

## Start here

| Reader's purpose | Read next |
|---|---|
| Understand the product and data flow | [Plain-language guide](docs/start-here.md), then [system design](docs/architecture/system-design.md) |
| Understand what is stored and exposed | [Storage catalog](docs/specification/storage-catalog.md), [interfaces](docs/specification/interface-catalog.md) and [data fields](docs/specification/data-contracts.md) |
| Implement the project | [AGENTS.md](AGENTS.md) → [current checkpoint](docs/implementation/progress.md) → [agent execution guide](docs/implementation/agent-handoff.md) → [PLAN.md](PLAN.md) and [phase/task/step index](docs/implementation/README.md) |
| Develop, test and release | [Build instructions](docs/development/build.md), [verification](docs/development/verification.md) and [development and delivery](docs/engineering/development-and-delivery.md) |
| Understand installation and placement | [Deployment contract](docs/specification/deployment-contract.md) and [local/S3 archive contract](docs/specification/archive-storage-contract.md) |
| Distinguish decisions from unexecuted qualification | [Readiness register](docs/planning/specification-readiness.md), [decision confidence](docs/planning/decision-confidence.md) and [experiment summary](docs/research/experiment-summary.md) |

Current contracts define the selected behavior; task files define how to implement
and demonstrate it. Historical reviews explain changes and do not supersede those
contracts. The plan has 123 tasks and 756 numbered steps, including ten planning
tasks; no future product step is complete merely because its specification exists.

## Working and delivery model

Build and verification procedures must work from a clean checkout with declared
dependencies and obtainable inputs, independently of a particular developer host,
agent session or private laboratory. Required reusable tools belong in the
maintained repository; caches are optional accelerators. The repository is still
closing that delivery requirement for the complete selected P00–P10/M1–M5 scope,
including optional capabilities. GitHub Actions provides the selected CI workflow
and Docker Hub is the selected application-image destination. Standard single-host
Docker Compose with local archive is the main installation path. S3 and split
roles use ordinary explicit endpoints when needed. No custom cluster manager is
required. All initial Cargo packages stay unpublished; branches and parallel work
are optional tools, while coherent commits and substantive review are required.

The unpublished Cargo application, CI entrypoint and development candidate bundle/
Compose recipes are present; full release and clean-install qualification remain
incomplete. Internal pre-production iterations are not supported public versions;
necessary current integrity, restart, replay and recovery guarantees still apply.
Follow
[build instructions](docs/development/build.md) and
[practical verification](docs/development/verification.md). The
[checkpoint](docs/implementation/progress.md) distinguishes executed outcomes
from configured workflows and remaining gates. Planning checks and their
Python/Node preparation remain documented in
[planning verification](docs/engineering/development-and-delivery.md#checks-available-in-the-planning-repository).

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
