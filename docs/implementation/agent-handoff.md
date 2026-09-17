# Implementing BigBrotr from this specification

This is the execution guide for a new implementation agent. The repository is
currently a private **design repository**. Its research programs are experiments,
not production adapters. Product implementation begins only after the owner
explicitly starts that stage. The neighboring Go checkout remains read-only.

## Establish the baseline

Read root [AGENTS.md](../../AGENTS.md), [Start here](../start-here.md),
[PLAN.md](../../PLAN.md) and the [phase index](README.md). Then read the current
[readiness register](../planning/specification-readiness.md) and the task's
linked contracts. Historical reviews explain decisions; they do not override the
current owning specification. Resolve an actual conflict before propagating it
into code, tests, generated schemas or documentation.

The key contracts have distinct ownership:

| Concern | Authoritative specification |
|---|---|
| Product intent and exclusions | [Feature decisions](../product/data-and-feature-decisions.md) |
| Record meaning, fields and lifecycle | [Data](../specification/data-contracts.md) and [storage catalog](../specification/storage-catalog.md) |
| Identity, eligibility and portable bytes | [Identity and encoding](../specification/identity-and-encoding.md) |
| Authority, recovery, resources and configuration transitions | [Operations contract](../specification/operations-contract.md) |
| Public resources and modes | [Interface catalog](../specification/interface-catalog.md) and [read views](../specification/read-view-contract.md) |
| DVM versus MCP/ContextVM | [DVM profile](../specification/dvm-profile.md) and [gateway contract](../specification/gateway-contracts.md) |
| Analytical definitions and costs | [Analytics](../architecture/analytics-detail.md), [derivation](../architecture/derived-read-publication.md), [assertion bindings](../specification/assertion-bindings.md) |
| Useful workload acceptance | [Capacity](../engineering/capacity-and-benchmarks.md) and the task's practical scenario |
| Code boundaries and dependency policy | [Rust and repository](../engineering/rust-and-repository.md) |

Do not scaffold every proposed folder or crate before its first consumer exists.
Do not copy the research Python or a state model into a product implementation
merely because it passes its own fixture. Generated APIs, DDL and serializers
must express the selected contracts; their names are not a license to invent
new data meanings during implementation.

## Execute a coherent task slice

1. Select a task whose concrete prerequisites exist. List its consumed/produced
   contracts, incompatible outcomes and practical evidence before changing code.
   Use the cross-phase dependency edges; avoid blanket phase barriers.
2. Inspect the relevant existing code and the exact upstream dependency version.
   Pin the tested dependency set, including native engines/extensions. Read the
   behavior that matters: a method name or successful ACK is not a guarantee.
3. Write an independent expected-result fixture and at least the discriminating
   failure cases the task requires. Keep the oracle independent of the production
   reducer/query. Cover complete signed bytes and real ownership boundaries where
   relevant; do not certify crypto using fake signatures.
4. Implement the smallest complete responsibility with idiomatic Rust, explicit
   errors, bounded work and cancellation. Keep operational state separate from
   retained scientific evidence. Update generated contracts, configuration help,
   user documentation and migrations with their actual behavior.
5. Run appropriate unit/property/fuzz/integration tests and the real packaged
   practical scenario. Missing infrastructure, empty fixtures and unimplemented
   scenarios are `NOT_RUN`, never success. Include refused and cancelled work in
   load reports. Do not repeat broad suites without a new change or concern.
6. Review the complete diff for correctness, complexity, ownership, allocations,
   I/O, observability, exposure and maintenance. Re-read the written contract
   against the implementation and actual output. Fix findings and rerun affected
   checks; a passing test suite does not replace this review.
7. Commit a coherent outcome with evidence and documentation. Record what passed,
   what was not exercised and any remaining release gate. Mark only genuinely
   completed steps. Never mark the whole phase complete from one happy path.

The proposed practical runner is
`python3 tests/acceptance/run.py CASE --binary dist/bigbrotr --compose packaging/compose/acceptance.yaml --evidence PATH`.
It is a **future P01-T08 deliverable**, not an existing command that can be cited
as already passing. The current repository check is
`python3 experiments/verify_artifacts.py`; it checks artifact structure, not
product correctness or semantic completeness.

## Review at boundaries that can amplify mistakes

| Boundary | Audit before dependent work spreads |
|---|---|
| First event end to end | Actual signed event enters, survives restart, is read and exported with exact fields; invalid-ID/signature controls fail |
| New persistent format or identity | Golden bytes, restore/import mapping, compatibility refusal and old-format fixtures; backup/replay closure |
| New durable effect/acknowledgement | Kill at every ambiguity boundary, restart owners in another order, replay retries and positively fence stale owners |
| New source or scope | Untrusted input bounds, private/public arrival order, provenance off, same-ID variants, SSRF/route and scheduling rules |
| New derived generation | Incremental result versus full independent recomputation, retractions/expiry, dirty-only work, partial materialization and activation |
| New public surface | Real client, resource/auth parity, output limits, cancellation, slow consumer, revocation and disabled-capability behavior |
| Claimed performance improvement | Equal semantics/data/loss model, aged state and mixed load, peak resources, maintenance debt, errors and useful completion |
| Distribution milestone/release | Clean install and browser journeys, usable docs, upgrade/restore, offline extension bundle, supported architecture and protection class |

These are periodic audits tied to risk, not an arbitrary required number of clean
reviews. Use an independently tasked reviewer when available and useful; label
AI review as AI review. Ask it to find counterexamples and missed work, not to
approve a predetermined conclusion. Record findings and their disposition.

## Handle decisions and discoveries honestly

Semantic choices are made before affected code. Physical engine settings and
bindings use the bounded decision records and early risk slices; do not build
two complete alternative products. A failed engine guarantee may reopen an
architecture decision. A small performance difference need not justify another
database, process, language or compatibility surface.

When new evidence changes a choice, update its owning specification, affected
tasks, fixtures, migration/compatibility consequences and user explanation in one
reviewable change. Preserve historical evidence as historical evidence. Avoid a
new parallel master plan, hidden TODO authority or a second agent instruction file.

The final implementation report identifies the exact tested revision, supported
deployment/workload envelope and remaining gates. “All tests passed,” “100k
connections,” and “zero loss” are incomplete claims without the exercised
boundary, offered workload and failure class.
