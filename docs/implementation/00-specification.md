# Phase 00 — Specification and evidence dossier

This phase is the present planning work. Its outputs are written contracts,
reviewable task plans and isolated research evidence. It creates no product
workspace/crates/services. Implementation phases remain unchecked until executed.

P00-T01–T05 record earlier completed planning passes. They are not a declaration
that every cross-contract question is closed forever. The current
[readiness assessment](../planning/specification-readiness.md) identifies further
paper closure recorded in P00-T06–T10 below. Checked steps have their actual
contracts, examples and reviews; adding a task alone never completes it.

## P00-T01 — Reconcile historical intent and moving implementation

**Files:** existing source inventory, historical phase rereads, code walkthroughs,
`docs/planning/mandate.md`, `docs/product/requirements-and-history.md`.
**Input/output:** original vision/phases/THE PLAN and current user context →
source-attributed retained/changed/removed intent with honest scope.

- [x] **Step 1:** Confirm THE PLAN's external path and hash against the already read capture.
- [x] **Step 2:** State that its Go work is unfinished; static code observations concern the
  captured version, not the eventual corrected product.
- [x] **Step 3:** Preserve useful product purposes without inheriting Go technology/knobs.
- [x] **Step 4:** Identify where the previous Rust overview lacked sufficient execution
  detail and carry those gaps into explicit task/specification work.
- [x] **Step 5:** Preserve source/read/execution distinctions; a read test is not a run test.

## P00-T02 — Exact data and component contracts

**Files:** `docs/specification/{data-contracts,component-contracts,import-formats,dvm-profile}.md`.
**Output:** field types, null/zero states, identity/retention/exposure, interfaces,
authority, failures and deliberate exclusions.

- [x] **Step 1:** Define full signed event fields, optional provenance and first-observation
  corrections without permanent duplicate counts or passive last-delivery times.
- [x] **Step 2:** Define actual measurements/documents/claims separately from operational
  logs/control, including interrupted execution uncertainty and disabled families.
- [x] **Step 3:** State canonical/pending/serving/control locations and reference closure for
  standalone research exports versus deployment backups.
- [x] **Step 4:** Specify component states, shared types, transport limits and source cuts;
  resolve conflicting names/defaults and actual producer/consumer dependencies.
- [x] **Step 5:** Select an honest explicit DVM profile/payment boundary rather than claim
  an existing NIP defines arbitrary resource queries or recurring execution.
- [x] **Step 6:** Run isolated representation/counterexample probes where they can validate
  design assumptions; record their limits and unexpected results.

## P00-T03 — Expand the full product into executable work

**Files:** `docs/implementation/01-foundation.md` through `10-website.md`,
their index and the requirement-to-task coverage ledger.

- [x] **Step 1:** Give each meaningful feature an owning task with exact proposed files and
  consumed/produced contract, not merely a service heading.
- [x] **Step 2:** Expand implementation, error/recovery, configuration/documentation and
  practical verification into concrete numbered steps.
- [x] **Step 3:** Identify cross-phase early slices, especially transport, signer/outbox,
  minimal registry and read/export integration.
- [x] **Step 4:** Define one common practical runner/evidence vocabulary and exact scenario
  preconditions/oracles; no required test may succeed by not executing.
- [x] **Step 5:** Map every selected data family, public/private interface, workflow and
  scaling concern to tasks or an explicit reason for exclusion.

## P00-T04 — Five substantive reconsiderations

**Files:** `docs/reviews/09-expanded-plan-rounds.md` and corrected owning docs.

- [x] **Step 1:** Round 1: coverage and decomposition; identify missing producers/consumers.
- [x] **Step 2:** Round 2: field meaning, lifecycle, privacy and useful archival value.
- [x] **Step 3:** Round 3: crash windows, ambiguous external actions, cancellation, ownership
  and actual recovery; compare expected state before/after each boundary.
- [x] **Step 4:** Round 4: cardinality, skew, temporary/native memory, IO/network/compaction
  and worst-case query/graph work; remove unsupported efficiency assumptions.
- [x] **Step 5:** Round 5: clean packaged user journeys, API/browser/docs parity, dependency
  consistency and evidence handoff; reread actual resulting plans.
- [x] **Step 6:** Record what changed and what the evidence does not establish. Five passes
  are requested reconsiderations, not five automatic certificates of perfection.

## P00-T05 — Verify and publish the planning handoff

**Files:** PLAN/index/coverage/review, experiment evidence, clean Git history.

- [x] **Step 1:** Check local references/headings, schemas/evidence and experiment syntax.
- [x] **Step 2:** Verify task IDs/dependencies/coverage against actual plan files; distinguish
  real product tasks from headings and references.
- [x] **Step 3:** Preserve reproducible probes/results and source identity; keep downloaded/
  generated artifacts ignored and inventory their locations.
- [x] **Step 4:** Reread the complete changed documents and resolve contradictions before
  claiming this planning pass complete.
- [x] **Step 5:** Commit/push a coherent English planning chunk to the private temporary
  repository; verify remote identity, visibility and clean local worktree.
- [x] **Step 6:** Explain the actual decisions, validation and remaining empirical limits
  to the owner in Italian. Do not begin implementation as part of the handoff.

Planning delivery evidence: commit `831bed3539111c69dd187f351517c366c388a339`
was pushed to private `BigBrotr/bigbrotr-rs/main`; local HEAD and origin/main
matched and the worktree was clean. The owner received the Italian explanation
of completed planning, corrected assumptions and still-unexecuted product
validation. All phase01–10 implementation checkboxes remain unchecked.

## P00-T06 — Close event scope, identity and encoding contracts

**Files:** `docs/specification/{data-contracts,naming,component-contracts,storage-catalog,import-formats}.md`,
affected current-state/read contracts and `docs/planning/specification-readiness.md`.
**Input/output:** S01/S02 ambiguities → one selected semantic/portable contract,
worked examples and consistent producer/consumer tasks. No product adapters.

- [x] **Step 1:** Enumerate same-event private/public arrival orders, import/network
  transitions, provenance on/off and permitted body versus metadata visibility.
- [x] **Step 2:** Compare one body with scope-qualified eligibility against scoped
  body records; choose the durable authority/fields and replay behavior, including
  which result reports a new eligibility effect for an existing event.
- [x] **Step 3:** Close authorized source population before current winner/deletion
  reduction; distinguish it from post-winner query/expiry rules and write examples
  showing that public results do not depend on private replacements/deletions.
- [x] **Step 4:** Specify qualified identity allocation/restore/import mapping and
  exact digest preimages, domains, versioning and unknown/presence rules; separate
  Nostr canonicalization, typed facts and transport serialization.
- [x] **Step 5:** Write golden byte/digest and logical schema examples with independent
  expected values, including empty fields, extreme integers and same-request conflict.
- [x] **Step 6:** Define compatible/incompatible record and RPC version transitions,
  including refusal before writes and retention of readable old identities.
- [x] **Step 7:** Update every affected admission/storage/derivation/read/export task
  and the readiness disposition; reread their combined examples for contradictions.

## P00-T07 — Close recovery transitions and resource composition

**Files:** existing component, storage, durability, configuration/runtime and
security/recovery contracts; affected P01/P02/P05/P07/P08 tasks.
**Input/output:** S03/S04 → explicit transition/effect tables, resource ownership
and discriminating fault schedules. Engine qualification remains separate.

- [x] **Step 1:** Tabulate authoritative write sets, acknowledgement points, ambiguous
  outcomes, retry lookups and reclaim conditions for each S03 boundary.
- [x] **Step 2:** Walk producer retirement, archive commit, partial projections,
  derived activation and signed delivery through reordered failure/recovery;
  identify forbidden observations and the owner allowed to resolve each state.
- [x] **Step 3:** Trace maximum eligible event/document representations through every
  internal stage and enabled public envelope, including transient memory copies.
- [x] **Step 4:** Specify reservation/transfer order, completion/control reserves,
  pressure watermarks and recovery disk headroom; eliminate circular waiting
  rather than relying on independent per-component byte limits.
- [x] **Step 5:** Work a symbolic and resolved example for full, archive-only and
  split-role recipes, including saturated intake plus archive/recovery progress.
- [x] **Step 6:** Record remaining binding/filesystem/engine uncertainties with their
  early risk experiment, pass/fail consequence and latest safe decision point.

## P00-T08 — Close capabilities, external contracts and operator transitions

**Files:** interface/storage catalogs, configuration/runtime, private administration,
Rust/repository and read/DVM contracts; affected P01/P06/P08/P09/P10 tasks.
**Input/output:** S05/S06/S08 → coherent recipes, field/operation matrices and
upgrade/recovery procedures. Actual generated schemas/builds remain future work.

- [x] **Step 1:** Resolve role/owner/dependency/transport/capability matrices for all
  supported recipes, including stopped owners and conditional analytical inputs.
- [x] **Step 2:** Specify enable/disable/re-enable/backfill behavior for provenance,
  search and targets; reconcile key ownership, defaults, bounds and reload classes.
- [x] **Step 3:** Complete public/private/RPC request/result field tables, lossless
  scalar encoding, defaults, filters/orders, counts, continuations and error mappings.
- [x] **Step 4:** Write representative valid/invalid client transcripts and permission
  outcomes; trace dashboard/admin/website journeys to concrete handlers and producers.
- [x] **Step 5:** Specify supported topology/protection/version matrix and exact
  upgrade/restore/rollback-or-forward-repair order, artifacts, authority and checks;
  include interrupted migration, secret loss and release/dependency ownership.
- [x] **Step 6:** Reconcile changed contracts and task prerequisites; keep optional
  capabilities genuinely optional and remove duplicate mutable authorities.

## P00-T09 — Close workload objectives, fixtures and empirical exceptions

**Files:** capacity/validation contracts, independent fixture descriptions,
decision-confidence/readiness registers and affected phase acceptance tasks.
**Input/output:** S07 and empirical exceptions → reproducible decision programme
with useful outcomes and independently described expected results.

- [x] **Step 1:** Define mixed workload tuples, data distributions/age, query and
  maintenance mix, host/background-load envelope and provenance/graph dimensions.
- [x] **Step 2:** Set justified provisional fairness, freshness, completion, recovery
  and resource objectives; define calibration then target selection where evidence
  is lacking, without relabeling measured results as prior requirements.
- [x] **Step 3:** Write a compact signed-event/evidence scenario linking source cuts,
  visibility, current state, facts, graph, output and portable export expectations.
- [x] **Step 4:** Attach independent oracles, meaningful negative controls and actual
  process/engine/client boundaries to the existing acceptance scenarios.
- [x] **Step 5:** Give each empirical selection its candidates, invariant, workload,
  latest safe decision point and consequence if neither candidate qualifies.
- [x] **Step 6:** Check that useful work, rejected work and starvation are all measured;
  no capacity claim or future acceptance result becomes PASS during planning.

## P00-T10 — Reconcile and assess the completed paper baseline

**Files:** owning contracts, PLAN/index/coverage, readiness/confidence registers,
review and evidence records. **Output:** honest per-contract closure, with only
justified empirical and private implementation choices remaining.

- [x] **Step 1:** Trace every S01–S08 disposition to its actual rule, example/oracle,
  producer/consumer and implementation task; an added checklist is not closure.
- [x] **Step 2:** Reconcile shared names, limits, table ownership, external fields,
  compatibility and phase dependencies across the affected final documents.
- [x] **Step 3:** Walk clean launch, steady overload, dependency outage, recovery,
  upgrade and standalone export using only written contracts; repair missing steps.
- [x] **Step 4:** Reread the final diffs and examples, run artifact/reference checks,
  and record remaining empirical conditions without claiming runtime validation.
- [ ] **Step 5:** Commit/push a coherent paper baseline and explain readiness/limits
  in Italian. Product implementation still requires its separate start instruction.

P00-T06–T09 closure is supported by the identity/operations/workload/gateway
contracts, frozen signed and encoding fixtures, selected dependency research,
read/rank oracle and the two review-14 reports. P00-T10 steps1–4 include the
corrected configuration dependency cycle, single-shard fixture, scoped workload
quotas and catalog/file capture barrier. Structural checks cover links, actual
GitHub fragments and task references; semantic review and engine evidence have
their separate stated scopes. All P01–P10 implementation steps remain unchecked.
