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

## Five-round detailed implementation expansion

The owner then requested a true phase/task/step execution design and challenged
permanent archive fields. The [new review](../reviews/09-expanded-plan-rounds.md)
records five substantive expansion/correction rounds: producer coverage, retained
field meaning, crash/authority boundaries, physical resource assumptions, and
packaged cross-component journeys. [The detailed plan](../implementation/README.md)
and [coverage ledger](../implementation/coverage.md) make the actual work explicit.

This pass includes a real local DuckDB/Parquet representation probe and a complete
pinned Pensieve schema/decoder/license inspection. It corrects assumptions about
physical document deduplication and protobuf field presence. No product code,
future acceptance scenario, real-relay capacity or paid-service result is claimed.
Review count is requested iteration, not a substitute for evidence.

## Complete current-repository sweep

The owner's subsequent question exposed a scope distinction: prior changed-file
rereads were not a final sweep of every current document after all corrections.
[Review 10](../reviews/10-whole-repository-audit.md) records the new complete
64-Markdown baseline review, exact Git-verified read manifest, domain critiques,
new producer and recovery contracts, simplifications, and actual Parquet target
counterexample. Targeted follow-up critiques corrected weaknesses in newly added
contracts too. Historical reviews remain dated evidence, not current defaults.
[Decision confidence](decision-confidence.md) explicitly separates selections,
measured properties and still-unqualified target-scale behavior.

## Newcomer handoff and another full baseline reread

[Review 11](../reviews/11-whole-repository-audit.md) covers all 70 baseline Markdown
files through complete assigned reads, reconciled by exact Git hashes. It closes
newly found cross-contract gaps and adds a newcomer guide plus storage, interface,
naming, read-view and assertion-binding specifications. Targeted counter-reviews
also corrected those additions: derived state and NEG ownership, header/member
separation, nonrecursive archive receipts and early shared migration ownership.
The controlled document-layout experiment preserves an initial ordering confound
and its corrected comparison. No product implementation or capacity result is
claimed; old review findings remain dated evidence with current dispositions in
the coordinator report.

## Specification readiness before implementation

[Review 12](../reviews/12-specification-readiness.md) is a focused follow-up to
the owner's request for the remaining paper work. It identifies unresolved
same-event visibility transitions, exact persistent encoding and incomplete
cross-contract composition; it distinguishes them from physical selections
that need practical evidence. The [readiness register](specification-readiness.md)
and P00-T06–T10 state concrete closure outputs. This pass assesses and plans that
closure; it does not claim to have completed the new tasks, reread every document
again, or implemented/tested the product.

## Challenging architecture choices, not only specification completeness

[Review 13](../reviews/13-architecture-alternatives.md) responds to the owner's
request for choices that could improve the final product. Three separately
tasked AI reviewers examined storage, runtime/analytics and product/interfaces.
The coordinator read their reports and challenged duplicate-proof semantics,
traversal guarantees, overlap output preservation and the true cost of replacing
archive authority. Their independent reports remain visible, including proposals
the coordinator does not recommend adopting now.

This pass uses targeted contract inspection, primary documentation and checked
arithmetic. The current linear sweep defaults imply 52 historical deliveries per
new event under the explicitly idealized complete-sweep model; this is not a
public-network measurement. No product, database or capacity benchmark was run,
and this is not a fresh full-document audit. Recommendations are advisory for
discussion. Selected architecture documents and P00 completion status stay
unchanged; adopted changes would require coherent contract/task reconciliation.
