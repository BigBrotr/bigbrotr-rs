# Research and revision process

Each revision changes the current design documents; this log records what new
evidence caused the change. The old documents are historical sources, not rules
for the new repository. Six revisions are requested by the owner, not a rule that
production quality can be proved by counting reviews.

| Revision | Main investigation | Expected design consequence | State |
|---|---|---|---|
| 1 | Recover vision, original phases, service responsibilities, and product gaps | Requirements, historical crosswalk, scope and questions | [Completed](../reviews/01-vision-recovery.md) |
| 2 | Define service contracts, configuration, ownership and capabilities | Complete service catalog and lifecycle model | [Completed](../reviews/02-contracts-and-simplification.md) |
| 3 | Challenge storage, protocol and Rust library choices with practical probes | Data model, durability protocol and dependency decisions | [Completed](../reviews/03-storage-and-sdk-experiments.md) |
| 4 | Examine failure, security, resource exhaustion and recovery | Operational contracts and fault-validation programme | [Completed](../reviews/04-failures-and-growth.md) |
| 5 | Design and challenge installation, dashboard, website, documentation and contributor journeys | Complete product delivery and maintenance plan | [Completed](../reviews/05-complete-product.md) |
| 6 | Reconcile all contracts, evidence, decisions and implementation dependencies | Implementable roadmap and final outstanding risks | [Completed](../reviews/06-reconciliation-and-delivery.md) |

Research experiments may overlap document revisions when dependencies allow.
Only experiments run and inspected count as evidence. Readiness remains scoped:
an archive prototype does not certify the future collector, and a plan does not
establish production capacity.

Downloaded references, source snapshots and build material remain in the ignored
`.research/` directory. Tracked source manifests identify what was actually read.
Reports retain commands, versions, fixture definitions, limits, outcomes and
limitations sufficient to reproduce the relevant observation.

All six are design/research revisions. Browser journeys, website builds and the
future product acceptance suite are planned, not executed by these revisions.

## Additional full-source and feature recheck

After the six baseline revisions, the owner requested another complete historical
reread and explicit decisions for features/data/analytics. The
[coordinating review](../reviews/07-vision-and-final-recheck.md) records the full
original vision, consolidation plan and all 34 surviving phase-0–9 Markdown files,
with three bounded phase reports and seven named supplemental sources. The source
inventory now includes 31 previously missing files; this corrects the earlier
README-only capture without retroactively overstating its coverage.

This pass added concrete product/data decisions, search and server reconciliation,
analytical definitions and graph costs; simplified premature crate boundaries;
and repaired first-launch, observation, protocol, permissions and packaging
acceptance details. Each phase report distinguishes preserved intent, deliberate
changes and evidence still owed by the implementation. Mechanical artifact checks
verify the written handoff; no new product runtime or capacity result is claimed.

## Selective full-file implementation walkthrough

The owner's next request added substantive actual Go code reading. Four bounded
walkthroughs follow acquisition/transport, storage/replay, analytics/publication
and actual CLI/read/lifecycle paths, with complete selected test-file readings.
The [coordinating review](../reviews/08-go-code-walkthrough.md) links exact hashed
inventories, observed behavior, static-inference limits and changes integrated
into each owning Rust contract. This is separate from the full phase Markdown
coverage, and makes no complete-Go-audit or newly executed product-test claim.
