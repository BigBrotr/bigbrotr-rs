# Implementation work breakdown

This is the detailed phase → task → step plan for BigBrotr. It describes future
product work; unchecked steps are not claims of implementation. The Go checkout
and `../BIGBROTR-THE-PLAN.md` remain read-only, unfinished reference material.
Findings from captured Go code are design lessons, not a verdict on the other
agent's eventual implementation.

The former phase table in [PLAN.md](../../PLAN.md) was an overview, not a complete
execution breakdown. These files supply concrete deliverables, paths, contracts,
failure cases, documentation and practical evidence. Stable task IDs connect
producers/consumers; prose and fixtures explain meaning beyond the IDs.

## Normative specification and reading order

Use [the agent execution guide](agent-handoff.md) for the implementation/audit pipeline.
Use the single [current checkpoint](progress.md) to resume, after verifying its
claims against Git and actual process/evidence state.
Use [development and delivery](../engineering/development-and-delivery.md) for
Tux-only authoring and isolated execution, existing planning checks, CI, Docker
Hub and commit policy.
Begin with [the newcomer guide](../start-here.md), then the
[storage catalog](../specification/storage-catalog.md),
[interface catalog](../specification/interface-catalog.md) and
[naming contract](../specification/naming.md). The
[read-view contract](../specification/read-view-contract.md) and
[assertion bindings](../specification/assertion-bindings.md) refine their owning
P05/P06/P07 semantics; their exact task mappings are part of the handoff below.

1. [Data fields, meaning, location and retention](../specification/data-contracts.md).
2. [Component contracts, shared types, state machines and evidence runner](../specification/component-contracts.md).
3. [Import format and source contracts](../specification/import-formats.md).
4. [Selected DVM application profile](../specification/dvm-profile.md), when that optional interface is enabled.
5. The owning architecture/service specification linked by each phase.
6. The phase's task, its specific prerequisites and practical completion evidence.

[Deployment composition](../specification/deployment-contract.md) and
[archive placement](../specification/archive-storage-contract.md) refine P01/P02/
P03/P04/P08: native/Compose, fixed-list acquisition, conditional dependencies,
single/split machines, local/S3 credentials, backup and relocation. They introduce
no new product/task hierarchy or mandatory object-store service.

Architecture prose defines product semantics. Tasks define how to build and
demonstrate them. Generated schemas/reference/CLI help will be implemented from
the same source contract; they cannot silently diverge. A later design change
updates the contract, affected tasks, fixtures and user-facing explanation.

The [coverage ledger](coverage.md) maps every retained family, responsibility,
public/private consumer and deliberate exclusion to actual task IDs.

The current breakdown contains 123 tasks and 756 numbered steps: ten planning
tasks and 113 future implementation tasks. These counts identify scope and are
not quality or test-coverage targets.

The [readiness register](../planning/specification-readiness.md) identifies
paper closure and remaining runtime gates under P00-T06–T10. P00-T01–T05 remain records of earlier
completed work; they are not evidence that the later findings are already fixed.
Close meaning/authority/persistent-interface decisions on paper. Resolve physical
choices with their defined experiments and private coding details during review.

## Phase files

| Phase | Responsibility and reviewable output |
|---|---|
| [00 — Specification and evidence](00-specification.md) | Historical context, exact data/component contracts, useful-feature coverage, five substantive reconsiderations and written handoff |
| [01 — Foundation](01-foundation.md) | Actual executable, lossless model/protocol/config, budgets, authority, internal RPC and independent practical harness |
| [02 — Storage](02-storage.md) | PG control, synchronized admission, portable documents/dictionaries, transactional archive, immutable serving, replay, retention and recovery |
| [03 — Acquisition](03-acquisition.md) | Fair sessions, live/history/NEG, scope/completion evidence and resumable imports |
| [04 — Relay lifecycle](04-relay-lifecycle.md) | Enrollment/discovery, guarded transport, validation, all measurement families and evidence lifecycle |
| [05 — Analytics](05-analytics.md) | Current state, exact facts, provenance-derived statistics, graph/ranks and explainable relay selection |
| [06 — Read surfaces](06-read-surfaces.md) | Shared authorized reads, search/count/cursors, ArchiveRelay, NIP-77 and optional DVM/MCP/experimental ContextVM |
| [07 — Publication](07-publication.md) | Purpose-bound signer, durable exact-byte per-target delivery, NIP-66/85 and protocol compatibility |
| [08 — Operations](08-operations.md) | Complete CLI/management, packaging/configuration, export/backup/restore/migration, alerts and release evidence |
| [09 — Product interfaces](09-product-interfaces.md) | Complete public dashboard and private administration, actual browser journeys and accessibility |
| [10 — Website](10-website.md) | Product pages, versioned tutorials/reference, blog, releases, adoption and eventual repository transition |

No phase is less important. Numbering groups responsibility and historical
traceability; it does not force completion of an entire phase before an independent
task in another may start. Early vertical slices prevent years of isolated
components that have never been wired together.

## Task anatomy and execution evidence

Each task states proposed files, consumed/produced contracts, prerequisites,
numbered actions, concrete errors/recovery, documentation and at least one
distinguishing practical scenario. Test paths and shell commands name artifacts
to implement, not files claimed to exist today. Use checkboxes only for work
actually completed, and retain a task's evidence with the tested release identity.

The common invocation and evidence schema are owned by P01-T08. Each scenario
must run the real packaged entrypoint and the actual engine/client when that
boundary matters. An independent expected set/digest/state and a deliberate
wrong interpretation make the oracle meaningful. Validate fixture preconditions
so an empty dataset cannot satisfy a supposed billion-event query test.

Unit tests, property-based tests, fuzzing, real-engine integration, protocol
interoperability, fault/recovery, load/soak, browser/accessibility and clean-install
journeys each have a purpose. Do not add every test category to every small task
as a ritual; cover the concrete risk and the actual route from caller to effect.
For example, a mocked outbox result cannot prove exact-byte retry after a process
dies, while a pure formula oracle is appropriate for a deterministic graph step.

## Cross-phase dependency edges

The task-specific dependencies remain authoritative. These edges resolve the
important apparent cycles:

* P03-T02 session/admission implementation can precede P02-T04, but its step 7
  archive-reader acceptance requires that real reader. P03-T08 supplies the import
  preflight core to P03-T09; T09 adds decoding/dry-run, and T10 adds durable import
  and closes their deferred successful-import scenarios. Those acceptance steps
  cannot block the earlier cores on which they depend.

* P01-T01–T08 provide independent foundations. P01-T09 is the integrated first
  useful event journey after early storage/read slices; it does not block P02.
* P02-T01 provides minimum endpoint enrollment before acquisition. P04 later
  extends that registry without redefining endpoint identity.
* P01-T04's static configuration core (steps 1–5) precedes P01-T05/T06 and
  P02-T01. P02-T01 supplies PG migrations/CAS, after which T04 step 6 and its
  managed/declarative persistence tests finish. Whole-task T04 completion is not
  a prerequisite for the schema it needs.
* P04-T02 implements guarded transport early, using P01 protocol/policy/runtime
  primitives; P03 socket collectors consume it.
* P07's signer/outbox primitives can run after their model/control prerequisites,
  before P04 active probes or P06 DVM delivery need them. Passive monitoring,
  HTTP and archive storage do not wait for publication enablement.
* P05 independent target generations consume P02 coherent cuts. Basic full-event
  reads need no social graph; a broken social scorer cannot stop relay statistics
  or already committed event reads.
* P08 backend management contracts precede P09 private-browser consumers.
  A small local frontend/read slice can validate onboarding earlier; complete
  browser coverage still belongs to P09.
* Website information architecture and examples are planned now. P10 verifies
  the complete built site/tutorials against the real released behavior.

Do not give a task a blanket “all previous phases” prerequisite where its actual
contract is narrower. Conversely, do not mark an enabled feature ready because
its consumer exists while the concrete producer/adapter remains unwired.

## Coherent implementation milestones

| Milestone | Actual demonstration | Required closure |
|---|---|---|
| M1: one verifiable event | Clean isolated bundle admits, archives, reads and exports one complete fixture, then recovers it after restart | Relevant P01/P02 slices plus concrete minimal read/export adapters; no fake success ports |
| M2: controlled fleet | Mixed live/backfill/monitor/import, scoped evidence and fair overload behavior over real sockets | P03/P04 + owner/frontier/recovery contracts, provenance on/off |
| M3: useful research and clients | Current/fact/rank results explained and cross-surface queries/reconciliation/publication validated | Required P05/P06/P07 tasks; optional interfaces disabled or complete, never half-advertised |
| M4: operable distribution | Clean installation, configuration, backup/restore/upgrade, alerts and split-role operation | P08 and dependent practical fault/capacity evidence |
| M5: complete product | Public/private browser journeys, complete website/docs/blog/download and real tutorial execution | P09/P10 plus final supported-feature/workload evidence |

Milestones are evidence collections, not a mandatory number of clean reviews.
Commit coherent reviewed outcomes, including their documentation and validation.
Do not commit every checkbox mechanically.

## Performance and capacity decision discipline

Semantic design is selected here. Performance-dependent physical constants have
an explicit starting value or baseline, alternatives, measured quantities and a
decision task (especially P02-T09). No code-free document can establish future
Rust binding performance, real WAN behavior or 100k-relay host capacity.
An implementation measurement may overturn a physical choice; record the reason
and update dependent plans before propagating the change.

Capacity variables are independent: relay connections R, delivered events D,
unique bodies U, first provenance P, actual measurements M, document versions C,
tags T, current graph V/E, queries Q and maintenance/recovery amplification.
Repeated deliveries cost network/verification but do not create eternal archive
rows. At finite disk/memory/IO the supported behavior is measured capacity plus
bounded deferral/refusal, never hidden data loss or “unlimited” scaling.

## Planning completeness versus production readiness

The five focused expansion/review passes and their actual changes are recorded
in [the revision dossier](../reviews/09-expanded-plan-rounds.md). Completion of
that planning work means the selected product has an explicit build/validation
path. It does not mean the future implementation or every performance assumption
has already passed. Product work begins only when the owner starts that stage.
The current readiness register links the selected rules and independent fixtures
closing the later paper ambiguities. Actual adapter/release gates remain separate;
no planned test becomes executed merely because its task is well specified.
