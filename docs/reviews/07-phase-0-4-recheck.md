# Review 7: complete master-plan and phases 0–4 recheck

Date: 2026-09-11. Scope: historical intent and the current Rust design, not an audit
of the Go implementation or evidence that future product tests pass.

The full assigned historical corpus and authorized supplements were reread:
**20 Markdown files, 3,970 lines**, including **14 files / 2,677 lines missing
from the original snapshot**. The coordinator has addressed first-observation
metadata without provenance, deterministic first launch, bounded `once` behavior,
ownership timing, timestamp interval endpoints and controllable blocked-ID
requests in the current baseline. Subsequent baseline repairs also cover Monitor
refresh/retention, runtime reporting, explicit test execution evidence, low-rate
publication acceptance and paid-relay read policy. The superseded acquisition
CLI-status sentence is now aligned with the shared enum (R02); source accounting
is recorded in the reconciled inventory (R07). Product
implementation and empirical acceptance remain future work, with concrete
historical failure cases preserved below.
The tables preserve useful intent and explain replacement of inherited constraints.

## Exact source coverage and snapshot recovery

Source directory in the readonly Go repository:
`/Users/vincenzo/Documents/GitHub/BigBrotr/bigbrotr/docs/superpowers/plans/bigbrotr/`.
Snapshot directory in this repository:
`.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/`.

Before reading, all `.md` files under both master-plan trees were enumerated with
`rg --files -g '*.md'`. The assignment covers `00-MASTER-PLAN.md` and every
Markdown currently present inside the phase-0 through phase-4 directories,
recursively. All six previously captured files byte-matched their readonly
originals at comparison time. The seven omitted phase task files and seven later authorized supplemental files
were copied into new snapshot paths with exclusive creation after explicit coordinator authorization;
source bytes were compared again immediately after copying. No legacy file was
written. No git command was run for this review.

Every file below was read from first to last line in bounded chunks, including
code examples, retrospective corrections, task steps and acceptance text. An
initial combined tool response truncated part of phases 1–2; both full READMEs
were subsequently reread in smaller chunks. A heading/search hit was not counted
as a full read. Line counts use `bytes.splitlines()`.

| Ref | Frozen source path | Lines read | Bytes | Original snapshot |
|---|---|---:|---:|---|
| M | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/00-MASTER-PLAN.md` | 1–216 | 14,760 | Present |
| P0 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-0-spec-completion/README.md` | 1–23 | 1,601 | Present |
| P1 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-1-runtime-foundation/README.md` | 1–252 | 26,175 | Present |
| T1.12 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-1-runtime-foundation/task-12-per-preset-zero-config-e2e.md` | 1–292 | 14,956 | Recovered |
| P2 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-2-storage-spine/README.md` | 1–129 | 27,028 | Present |
| T2.1 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-2-storage-spine/task-01-relayurl-canonicalize.md` | 1–93 | 4,232 | Recovered |
| T2.2 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-2-storage-spine/task-02-relayurl-recompose.md` | 1–83 | 3,890 | Recovered |
| T2.3 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-2-storage-spine/task-03-relayurl-retarget-callers.md` | 1–135 | 8,312 | Recovered |
| T2.4 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-2-storage-spine/task-04-journal-segment-header.md` | 1–44 | 4,185 | Recovered |
| T2.5 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-2-storage-spine/task-05-journal-at-least-once.md` | 1–39 | 3,631 | Recovered |
| T2.6 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-2-storage-spine/task-06-earn-review-journal.md` | 1–36 | 4,013 | Recovered |
| P3 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-3-ingest-path/README.md` | 1–461 | 45,877 | Present |
| P4 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-4-relay-lifecycle/README.md` | 1–212 | 33,947 | Present |

The SHA-256 values below also provide the metadata needed to reconcile the main
[source inventory](../research/source-inventory.json); this review does not edit
that shared inventory.

| Ref | SHA-256 |
|---|---|
| M | `efa3bbe624b0aaf3dc460b5279876e33cde426f501d83ff36a1a79f739982a95` |
| P0 | `8bfbee71f440dad2b6bb55221fc2b7ff7a44a9672bbc9504860a09a79a7f0a89` |
| P1 | `98e3767e2e4f26e151b3169fc3499121fc363081857225b9d81868e8d7e8dc83` |
| T1.12 | `b1a4f88ff48a1df967aea1d19a352d330fd3d206ee72ac918d22022338c9a8b0` |
| P2 | `eac15cfcb58a53ad2053e36d3d5b3a7c5b1f7708948a918146b45d02f451d701` |
| T2.1 | `0f2576cf8893953518c957e5cd94578cbc821ece60f878a43effead1e7d16fa2` |
| T2.2 | `3a3459894b25a707ccd39e8e48af66ed22da48253ef8967c09b23a9a1f7e4a8c` |
| T2.3 | `c32bac126cd4e40703edac9a4e48e4c78759b07d66ba8029ad6672ee5d0b9da8` |
| T2.4 | `97409dd86337be76776d0ecf1ca02bf79f32405c2366f31e816744cbd37bbb2c` |
| T2.5 | `263f8adf3f968ed1b16b14c70db648cc5e25d9d57c485e77bc8d8ecf9fa4e821` |
| T2.6 | `cb982f9d8a402b9af42c7e3bca4fcc212586d1150d69e5659f7834fbb875f3d7` |
| P3 | `05351de1a6e98f38c27925434f21236223ad0d5decebe752411c4a5775332ea5` |
| P4 | `d631f31b6c51f089679a57713214935175315ba2ffc4c9b9a4c39200846155cc` |

The P1/P2 indexes also name retired task files absent from both current trees;
the READMEs themselves explain their retirement. They are not additional files
silently omitted by this capture. Their indexed requirements were read and
mapped below, but retired full texts were not retrieved from git. P3/P4 have no
surviving additional Markdown in their phase directories.

### Authorized supplemental coverage

A bounded follow-up covered the full top-level coverage matrix and every
Markdown file currently present under `serve-wiring/` and `test-infrastructure/`.
All seven were initially missing from the snapshot, captured with exclusive
creation, and compared with unchanged readonly originals before and after copying.
Their **1,955 lines / 161,347 bytes** were all read in bounded chunks, including
late corrections, previously false execution claims and remaining limitations.
Paths below are repository-relative; evidence snapshots are ignored locally, so
these are code paths rather than broken published links.

| Ref | Frozen source path | Lines read | Bytes | SHA-256 |
|---|---|---:|---:|---|
| C | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/coverage-matrix.md` | 1–268 | 27,025 | `89d89c49d5d2fd374c4111827be286050646df736392f540e8c1b2ac566a39c5` |
| W | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/serve-wiring/README.md` | 1–138 | 8,297 | `b559897dae632ba9b88aae3b0790631952db2448e3bbe461f17488fb28a3b0c5` |
| WD | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/serve-wiring/earn-review-dedupe-ds2.md` | 1–126 | 7,658 | `cb126c74311b7a0e43a2b9b58562e315bf5f24bfd52aa0b6e69b8f8ec874f10e` |
| WC | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/serve-wiring/earn-review-storage-clickhouse.md` | 1–148 | 9,140 | `7bdf0148471c8822c9432934361e85651cf243f6af10f00ea62bb9203017aab1` |
| W1 | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/serve-wiring/task-01-serve-entry-point.md` | 1–341 | 21,858 | `c89fab0436d90eb73051eff3752ca3dc7cfc2bbf9c743442e7b0dbacb81cf6b0` |
| TE | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/test-infrastructure/EXCLUSIONS.md` | 1–341 | 47,187 | `22da9da80d39f21abe536dcbcc8c1f8e337b32bbe109596d71df638da7e75354` |
| TN | `.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/test-infrastructure/NONDETERMINISM.md` | 1–593 | 40,182 | `13b3b3bd91ee77f2b73bdf7f1b4f497f4e553e5c16a0dd51857c2141ac29e1e7` |

The combined covered corpus is 20 files / 3,970 lines. The 14 recovered files
account for 2,677 lines; the original six files account for 1,293 lines. No read
of the entire historical source tree is implied. `audit-trail.md` was sized at
2,506 lines / 286,862 bytes and excluded from this bounded supplement after the
coordinator explicitly allowed that progression-history boundary. Top-level
`CONSOLIDATION-2026-08-28.md` and `CORPUS-WALK-2026-08-24.md` were inventoried and
reported, but not read here. Other reviewers own the complete vision and phases
5–9. This review does not recover absent retired texts from git.

## Current-design comparison coverage

The following current files were read completely for this comparison:
[AGENTS.md](../../AGENTS.md), [mandate](../planning/mandate.md),
[PLAN.md](../../PLAN.md), [README.md](../../README.md),
[requirements/history](../product/requirements-and-history.md),
[system design](../architecture/system-design.md),
[data/durability](../architecture/data-and-durability.md),
[storage services](../architecture/storage-services.md),
[acquisition services](../architecture/acquisition-services.md),
[configuration/runtime](../engineering/configuration-and-runtime.md),
[capacity](../engineering/capacity-and-benchmarks.md),
[security/recovery/validation](../engineering/security-recovery-and-validation.md),
[Rust/repository](../engineering/rust-and-repository.md),
[planning process](../planning/process.md) and [questions](../planning/questions.md).
Targeted searches in derived/publication, protocol-evolution and product-interface
files only established adjacent references; this review does not claim a fresh
full read of those larger files.

The design is being coordinated concurrently. The coordinator's baseline repairs
were reread before finalizing this review; addressed findings name their actual
contract location. Residual refinements are proposals, not silent edits to another
reviewer's files. “Retained” and “addressed in baseline” mean documented in the
plan, not implemented or tested.

## Master plan and phase 0 dispositions

| Source requirement | Current destination | Disposition and reason |
|---|---|---|
| M goal, 15 named stateful services plus runtime; P0 coherent specification before implementation | [Required outcomes and phase map](../product/requirements-and-history.md#required-outcomes), [implementation sequence](../../PLAN.md#implementation-sequence) | **Retained.** All original service responsibilities are named; ingest authority/archive maintenance have explicit additional ownership. Website and private management extend the historical scope. No product implementation is inferred from plan completion. |
| M Go single static binary, no RPC, one mandatory infrastructure stack | [Service/process boundaries](../architecture/system-design.md#service-and-process-boundaries), [optional topology](../architecture/system-design.md#what-stays-optional) | **Changed.** Rust is selected, the same application supports separate roles, typed authenticated internal RPC is allowed, and archive-only can omit CH. Process/crate/service identities are independent; useful isolation survives without inherited packaging laws. |
| M permanent custom journal as truth; compressed lossless signed event fidelity | [Four frontiers](../architecture/system-design.md#four-different-frontiers), [archive publication](../architecture/data-and-durability.md#archive-publication-and-reclamation) | **Changed.** Synchronized Rocks admission/pending and transactional DuckLake/Parquet archive replace custom permanent segments. Full signed fields/signature are invariant; wire whitespace/extensions are separately attributed optional evidence. A byte-identical wire claim cannot be inferred from valid signed-event serialization. |
| M cursor never ahead of durable storage; P0 acceptance before unsafe dependent work | [Admission transaction](../architecture/data-and-durability.md#admission-transaction), [failure matrix](../engineering/security-recovery-and-validation.md#failure-matrix-and-operator-response) | **Retained with sharper boundaries.** Admission, archive, queryability and observed coverage are separate. A network checkpoint can cite synchronized local admission without pretending that local durability is replicated protection or CH visibility. |
| M TDD, reuse-must-be-earned, independent review, no inert config, real release scenarios | [Delivery discipline](../../PLAN.md#delivery-and-review-discipline), [independent oracles](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles) | **Retained intent.** Review actual entry points, dependencies and failure semantics. **Rejected** fixed coverage floors, blanket exported-comment quotas, banned ordinary vocabulary, repeated clean-review counts and unconditional task/phase rituals. These cannot establish product correctness. |
| M single fixed phase order and no later phase before a predecessor's full acceptance | [Implementation sequence](../../PLAN.md#implementation-sequence) | **Changed.** Preserve dependency order and early storage/protocol experiments; permit thin CLI/read/restore slices early. Import begins in phase 3 and operations finish in phase 8. This reduces the historical risk of discovering unwired code only at packaging. |
| M CI claims corrected after Linux path and single-platform container failures | [Maintenance/release](../engineering/rust-and-repository.md#maintenance-and-release) | **Retained lesson.** Pin tested dependencies, compile explicit amd64/arm64/MSRV targets, use multi-platform image-index digests and execute clean release-bundle journeys. Prior CI anecdotes are historical assertions, not runs performed in this review. |
| M trace every decision/test to work or explicit deferral; P0 zero orphan assertion | This review, [requirements/history](../product/requirements-and-history.md), [PLAN](../../PLAN.md) | **Addressed for this assigned corpus.** The earlier phase summary was too coarse to establish full Markdown coverage. This full-text crosswalk and fourteen recovered source entries restore the assigned scope, coordinated with the vision/other-phase reviews. Its boundary does not become a universal “zero omissions” claim or a code audit. |

## Phase 1 dispositions

| Source requirement | Current destination | Disposition and reason |
|---|---|---|
| P1 D-H5/D-D6: strict typed settings, useful defaults, unknown-key rejection, no inert knobs | [One source of defaults](../engineering/configuration-and-runtime.md#one-source-of-defaults), [shared acquisition config](../architecture/acquisition-services.md#shared-configuration) | **Retained and simplified.** Generated schema/reference/default examples; TOML with explicit precedence and source reporting. Unsupported enabled functionality fails. Test observable configuration behavior rather than invent one superficial assertion per field. Core runtime defaults and finite-run semantics are now addressed in baseline; R02 records the authoritative timing and run-envelope contract. |
| P1 D-A3/A7/A9: five presets, heavy-fields axis, identical infrastructure/default-enabled set | [Profiles/policy history](../product/requirements-and-history.md#profiles-and-policy-history), [recipe matrix](../architecture/system-design.md#recipe-and-capability-matrix) | **Changed by owner context.** All signed fields are retained; provenance/filter/index choices are explicit. LilBrotr may become a clearly explained full-archive/no-provenance nickname, never silently reuse its old lossy meaning. Infrastructure follows selected capabilities, so archive-only is valid. |
| P1 D-D1/H7: dependency/default/off-for-safety classification and zero-config usable instance | [Recipe matrix](../architecture/system-design.md#recipe-and-capability-matrix), [configuration defaults](../engineering/configuration-and-runtime.md#one-source-of-defaults) | **Retained; R01 addressed in baseline.** [Deterministic first launch](../engineering/configuration-and-runtime.md#deterministic-first-launch) names the full-local roles, empty-registry bundled seeding, durable bootstrap receipt, explicit opt-out and actual entry-point acceptance. Required data does not imply co-running its original producer. |
| P1 shared Name/RunOnce, once/loop, injectable clock, no service-specific supervisor logic | [Service/process boundaries](../architecture/system-design.md#service-and-process-boundaries), [lifecycle](../engineering/configuration-and-runtime.md#lifecycle-and-health) | **Changed.** Finite jobs, asynchronous loops and listeners receive suitable contracts. Retain injectable time, cancellation and isolated failures. [Shared once semantics](../engineering/configuration-and-runtime.md#shared-lifecycle-vocabulary-and-once-semantics) now define bounded work units and reject meaningless listener `once` mode. |
| P1 II.9.16 run envelopes, health, metrics and bounded drain | [Lifecycle/health](../engineering/configuration-and-runtime.md#lifecycle-and-health), [acquisition lifecycle](../architecture/acquisition-services.md#boundaries-and-deployment) | **Retained; R02 addressed in baseline.** Shared health, terminal job outcomes, drain, renewal and TTL defaults are explicit. The typed run envelope has bounded progress, source scope/cut and output references; ownership timing changes require restart. |
| P1 lease single-owner, loser waits, crash TTL expiry/reacquire, independent failure budget | [Ownership/failure domains](../architecture/system-design.md#growth-and-failure-domains), [failure matrix](../engineering/security-recovery-and-validation.md#failure-matrix-and-operator-response) | **Strengthened.** Commit fencing and positive old-owner exclusion replace TTL-as-proof. A remote dead relay does not halt healthy peers or poison infrastructure health. Reject one universal five-failure shutdown. The baseline now calls for TTL stalls, drain failure and old-owner resumption. R02 preserves independent lease-clock and local monotonic-clock acceptance detail. |
| P1 private observability and supervisor metric set | [Lifecycle/health metrics](../engineering/configuration-and-runtime.md#lifecycle-and-health), [capacity accounting](../engineering/capacity-and-benchmarks.md#a-workload-model-instead-of-a-relay-count-slogan) | **Retained.** Queue age/bytes, permit ownership, stale frontiers, failures and drain/progress are operational evidence. Per-relay diagnostics use queryable records, not unbounded Prometheus labels. |
| T1.12 real per-preset convergence; annotations admit ticks and resolver transcripts overstated execution | [Packaged entry-point validation](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles), [R01](#repair-dispositions-and-concrete-acceptance) | **Retained outcome, rejected implementation ceremony.** Test actual supported recipes from CLI/Compose with durable fixture collection. Fake service ticks and a configuration set comparison do not demonstrate a working first launch. This review does not independently re-audit the historical Go claims. |

## Phase 2 dispositions

| Source requirement | Current destination | Disposition and reason |
|---|---|---|
| P2 D-R1/R2/R7 and T2.1–T2.3: one relay identity, normalization/recomposition across every caller | [Identity v1](../architecture/acquisition-services.md#identity-v1) | **Retained single authority; changed equivalence rules.** Preserve actual ws/wss, non-root slash, path and query distinctions. Scheme-specific default ports, IDNA and strict IPv6 normalization remain. T2.1 contradicts itself on keeping `wss:80` versus stripping both ports; its self-consistent string tests cannot settle endpoint equivalence. |
| T2.3 unconditional monitor recomposition as wss; permissive canonicalization separately validated in discovery | [Identity and guarded transport](../architecture/acquisition-services.md#endpoint-identity-and-transport) | **Rejected.** Typed validated endpoint identities must not silently change transport or accept arbitrary stripped schemes. All paths use one validated authority and the actual endpoint; test different data/certificates on ws/wss, ports and paths. |
| P2 D-B2 and T2.4 self-describing framing, version/checksum, full-unit corruption detection | [Source encoding](../architecture/data-and-durability.md#dictionary-and-source-encoding-portability), [archive maintenance](../architecture/storage-services.md#archive-publisher-and-maintenance) | **Changed mechanism, retained integrity.** Versioned records/digests plus engine WAL and archive manifests replace BBJ1/CRC segment format. Exercise corrupted metadata, payload, truncation and unsupported versions. T2.4's flipped version can trigger format rejection before seal rejection; require honest classified corruption rather than preserve an inconsistent error expectation. |
| P2 D-B2b/T2.5 faithful duplicate-tolerant replay; consumer idempotency “keyed on event id” | [Typed source cuts](../architecture/storage-services.md#typed-source-cuts-and-projection-identity), [serving publication](../architecture/data-and-durability.md#serving-layout-and-exact-answers) | **Retained, corrected identity granularity.** Bodies deduplicate by canonical event identity; provenance corrections/measurements have immutable source record identities. Generic event-ID dedup would discard distinct useful records. Replays preserve source identities, payloads and ordering metadata across pending/archive. |
| P2 D-B8: compact event IDs and integer relay IDs; bounded lossy edge cache | [Admission transaction](../architecture/data-and-durability.md#admission-transaction), [billion-scale representation](../engineering/capacity-and-benchmarks.md#billion-scale-representation-challenge) | **Changed deliberately.** Exact event and edge admission avoids archiving one edge fact on every resweep. Compact dictionaries/ordinals are recoverable metadata. Pay and measure an extra P-sized inventory; new edges plus earlier corrections are retained, ordinary repeats are suppressed. Provenance-off allocates no edge inventory. |
| P2 R1 / II.9.7-t1: minimum first_seen survives replay with provenance OFF | [Admission owner](../architecture/storage-services.md#durable-ingest-owner), [shared record semantics](../architecture/acquisition-services.md#shared-records-and-invariants) | **Retained; R03 addressed in baseline.** [First observation without provenance](../architecture/data-and-durability.md#first-observation-without-provenance) defines immutable first admission, eligible local receive time, immutable earlier corrections, import separation and provenance-off replay/restore assertions without hidden edge state. |
| P2 D-B11/B12: binary IDs/signatures; slim time/kind/author paths; first-value tag postings with event-ID tie-break | [Data representation](../architecture/data-and-durability.md#permanent-facts-and-rebuildable-representations), [serving layout](../architecture/data-and-durability.md#serving-layout-and-exact-answers) | **Retained intent; physical DDL reconsidered.** Full bodies are not copied into every access path. Exact tag first-values and full body tag ordering survive. Compare chronology-first and ID-first layouts; collision correctness, parts and read amplification decide. Old codec/table naming is not a permanent product knob. |
| P2 D-B7: both provenance orders and earliest observation | [Online indexer](../architecture/storage-services.md#online-indexer), [admission](../architecture/data-and-durability.md#admission-transaction) | **Retained and scoped.** Event→relay and relay→event access paths include observer/auth/origin semantics. A later earlier-time correction stays a separate immutable fact, not a replacing logical edge that erases an old snapshot. |
| P2 D-B6/R10: async ACK/checkpoint, finite dedup tokens, tail reconciliation and failed reindex queue | [Serving persistence](../architecture/data-and-durability.md#serving-persistence-and-incarnation), [online indexer](../architecture/storage-services.md#online-indexer) | **Changed to a concrete protocol.** Deterministic table/partition chunks, durable receipts and a complete typed source cut; synchronous fsync settings initially. Retry copies are logically idempotent beyond engine windows. Checking only whether the body exists cannot prove tags/edges/observations were indexed. Unknown outcome or poison pauses affected progress. |
| P2 D-X8 / t4–t5: hostile timestamps, bounded partitions, future quarantine/promotion | [Shared timestamp contract](../architecture/acquisition-services.md#shared-records-and-invariants), [synchronizer algorithms](../architecture/acquisition-services.md#layer-algorithms) | **Changed.** Preserve valid unsigned 64-bit signed times as integers rather than inherit a smaller CH calendar type. Negative/noninteger/overflow input is rejected; representable future events remain full signed records. Bounded ingestion partitions remove hostile month fan-out. No permanent future-promotion daemon is required merely to repair a schema limitation. R06 closes the after-maximum interval boundary. |
| P2 II.9.7 duplicate, crash-after-ACK, colliding-time, hostile-time and future-policy tests | [Failure programme](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles), [acquisition scenarios](../architecture/acquisition-services.md#practical-acceptance-scenarios) | **Retained with new oracles.** Add forced merges, missing table chunks, replay beyond dedup retention, signed-time extremes and provenance-off first-seen. Future read eligibility follows explicit read policy; it is not evidence that historical coverage reached future time. |
| P2 by-ID p99 <50ms and compression ≥5×; early sort-key spike on realistic unmerged parts | [Capacity programme](../engineering/capacity-and-benchmarks.md#required-workload-programme-before-capacity-claims), [decision points](../../PLAN.md#decisions-deliberately-requiring-implementation-evidence) | **Retained experiment, changed numerical promise.** Preserve early comparison under realistic parts/concurrent work. Report compression including indexes/inventories and publish measured per-release latency envelopes. Synthetic fake-signature files cannot prove a universal 5× ratio or billion-row 50ms service. |
| T2.6 coverage ≥90%, documentation/dead-code review and repeated review approval | [Release discipline](../../PLAN.md#delivery-and-review-discipline), [Rust maintenance](../engineering/rust-and-repository.md#maintenance-and-release) | **Retained review of boundaries and corruption; rejected arbitrary floor.** Checks must reveal actual failure modes. A test repeating duplicate reads under a second name or counting blank godoc lines is not independent correctness evidence. |

## Phase 3 dispositions

| Source requirement | Current destination | Disposition and reason |
|---|---|---|
| P3 D-D7/C2: independent network ingest and indexer, four acquisition layers over one durable sink | [Boundaries](../architecture/acquisition-services.md#boundaries-and-deployment), [layer algorithms](../architecture/acquisition-services.md#layer-algorithms) | **Retained with multiplexing.** Live, historical and recent work share admission and connections where scopes permit; no sequential global RunOnce loop. Historical work yields fairly. Monitor/import use the same durable contract, with independent budget shares. |
| P3 D-C0/C1/C1b/C3: filter-relative cursor, honesty ledger, durable progress | [Progress/evidence](../architecture/acquisition-services.md#progress-and-evidence) | **Strengthened.** Traversed position, acquired evidence and unresolved gaps are separate. No scalar permanent completeness claim. Receipt-backed progress survives restart; a gap remains visible even if traversal moves beyond it. |
| P3 D-X7/A8: normalized filter hash; widening reset, narrowing reuse, oblique reset | [Scope/scheduling](../architecture/acquisition-services.md#scope-and-scheduling) | **Changed conservatively.** New semantic scope creates new evidence; narrowing reuse requires an actual containment proof for filter unions. Request limits/moving windows are not durable selector identity. Reject unsupported search selectors and empty-list ambiguity. |
| P3 D-X3: dropping ephemeral kinds means never requesting them | [Selector policy](../architecture/acquisition-services.md#scope-and-scheduling) | **Rejected unconditional wire claim.** NIP-01 has no general negative-kind filter. Compile a bounded positive filter set or receive-and-drop with an explicit wire-policy label. Default full archive retains ephemeral events. A policy change changes evidence identity. |
| P3 D-C5/t1/t2: silent result caps, inclusive boundary, short/all-seen page discipline | [Historical linear algorithm](../architecture/acquisition-services.md#layer-algorithms), scenarios 11–12 | **Retained cap safety; corrected completion.** An all-seen repeated boundary does not prove exhaustion, especially when “seen” means globally known. Record a saturated-second gap, visit older ranges and subdivide only enumerable dimensions. Empty/EOSE/finish yield qualified evidence, never guaranteed history. |
| P3 D-C4/C4b/#19/t4: bounded relay-scoped negentropy, created_at keys, verified need-list | [Historical negentropy](../architecture/acquisition-services.md#layer-algorithms), scenarios 13–14 | **Retained.** Local witness scope is endpoint/auth, not global body membership or imported claims. Every missing advertised ID requires verified retrieval or explicit exclusion; missing bodies become gaps. A conservative stale reader only costs extra downloads. No mandatory additional ordered P-index is inferred. |
| P3 NEG-ERR blocked treated as result cap; session FrameClosed and signed int64 timestamp assumptions | [Shared records](../architecture/acquisition-services.md#shared-records-and-invariants), [negentropy algorithm](../architecture/acquisition-services.md#layer-algorithms) | **Changed to current protocol limits.** Blocked can be an age/policy restriction and subdivision is bounded. Client algorithm completion and actual served bodies establish the result; NEG-CLOSE alone does not. Full uint64 event range is preserved; max timestamp is an explicit NIP-77 exclusion. |
| P3 D-C10/t9/D-C13: transport-aware probing, timeout not unsupported, optional NIP-11 | [Guarded dialing](../architecture/acquisition-services.md#guarded-dialing-and-budgets), [validator](../architecture/acquisition-services.md#validator-contract) | **Retained, attribution tightened.** Only applicable replies establish unsupported; generic NOTICE text cannot demote unrelated requests. Longer transport-aware retries precede temporary linear fallback. Advertised NIPs/limits never override local safety or become hard dependencies. |
| P3 D-C6/C11/#20/t6/t8: recent sweeps, moving safety lag, hot/warm tiers, adaptive S and bandwidth | [Recent/deep sweeps](../architecture/acquisition-services.md#layer-algorithms), [sync config](../architecture/acquisition-services.md#synchronizer-configuration) | **Retained tiers/budgets; adaptive shrink deferred with reason.** Signed-age percentiles are biased by unseen/backdated events. Use explicit 1h hot/48h warm defaults and visible missed/deferred work; any adaptive alternative needs a comparison against fixed windows and a minimum horizon. Do not silently claim the old adaptive-S acceptance passed. |
| P3 backdated below cursor and re-verification horizon; D-C9 parallel cursors deferred | [Deep re-verification](../architecture/acquisition-services.md#layer-algorithms), [typed cuts](../architecture/storage-services.md#typed-source-cuts-and-projection-identity) | **Retained and explicit.** Stagger capable-relay reconciliation; old linear sampling is labeled sampling. Full linear retraversal is an explicitly budgeted job. Initial bounded sequential intervals avoid unsupported global parallel-completeness claims; shard transitions preserve vector source cuts. |
| P3 D-S2/t5: blocklist-as-owned and zero fetches across two cycles | [Admission/moderation](../architecture/acquisition-services.md#durable-ingest-contract), [negentropy scope](../architecture/acquisition-services.md#layer-algorithms) | **Changed; R05 addressed in baseline.** Durable policy suppression is distinct from possession of a body. Avoid controllable exact-ID refetches, but a linear/live relay can still deliver blocked events because arbitrary ID exclusion is not available. Never turn unverified claimed IDs into permanent ownership. |
| P3 D-C8/S5: per-host/global budgets, one owner, ENOSPC fail-closed | [Shared allocator](../engineering/configuration-and-runtime.md#shared-budget-allocator), scenarios 15–17 | **Retained and expanded.** Multiplex subscriptions; reserve monitor/control/live work, bound bytes/items/CPU, and expose fleet coverage gaps when connections rotate. A physical host envelope bounds all role shares; multiple workers cannot multiply a supposed global limit. |
| P3 D-S1/S4/R5/R6/X4/X6: guarded transport, malicious input, mocks/private lab, self exclusion, AUTH and bans | [Identity/transport](../architecture/acquisition-services.md#endpoint-identity-and-transport), scenarios 2–4/7/10/21 | **Retained with safer scoping.** Pin resolved IP and SNI, enforce redirects/overlay boundaries, use explicit private routes, keep monitoring self endpoints allowed, and scope signers. Public/private mixed lab policy replaces global localhost XOR. |
| P3 D-B13/C12: real firehose ≥5k/s, bytes/relay/day and no unmeasured product claims | [Capacity programme](../engineering/capacity-and-benchmarks.md#required-workload-programme-before-capacity-claims), [network sensitivity](../engineering/capacity-and-benchmarks.md#concrete-network-stress-scenarios) | **Retained measurement intent, changed universal threshold.** Separate deliveries, unique events, new edges and corrections; record actual network bytes by endpoint/layer. Controlled signed workloads establish repeatable capacity; dated public observations assess variability without making release tests depend on public relay uptime. |

## Phase 4 dispositions

| Source requirement | Current destination | Disposition and reason |
|---|---|---|
| P4 II.9.1: curated seed file, comments/blank handling, idempotency, missing/empty hard error | [Seeder](../architecture/acquisition-services.md#seeder-contract), scenario 1 | **Retained.** Version/digest/line evidence, bounded local reading, distinct malformed/no-admissible-input outcomes and resumable database failure. R01 is addressed by the explicit empty-registry bootstrap job and opt-out in the shared runtime contract. |
| P4 D-R4/S1: untested candidate versus Seeder DNS/SSRF pre-filter and unchecked direct relay insertion | [Seeder](../architecture/acquisition-services.md#seeder-contract), [validator](../architecture/acquisition-services.md#validator-contract) | **Changed.** Seeder records candidate/evidence without DNS; the actual dial is guarded by Validator. Operator enrollment does not fabricate reachability. Invalid/out-of-policy literals can be excluded locally. This removes the old tension between “candidate untested” and network prerequisites during seeding. |
| P4 II.9.2: NIP-65, r-tags, NIP-66, NIP-51 and external API sources | [Discoverer](../architecture/acquisition-services.md#discoverer-contract), scenarios 5–6 | **Retained and broadened.** Full retained tags preserve reference positions/markers; scans follow ingestion order, including late imported/backdated events. Sub-record checkpoints preserve long reference tails. Pubkey-only subjects remain non-dialable evidence. |
| P4 D-D9: configurable directory APIs, cooldown, JSONPath and skip-on-error | [Directory loop/config](../architecture/acquisition-services.md#discoverer-contract) | **Retained.** Independently optional, bounded responses/selectors, HTTPS/redirect guard, Retry-After/backoff and source digests. API outage does not block archive discovery. Replayed unchanged responses cannot repeatedly revive candidates. |
| P4 II.9.3: WS/NIP-11 promotion, network domain, retries and exhaustion deletion/ban | [Validator](../architecture/acquisition-services.md#validator-contract), scenarios 7–8 | **Changed.** Require attributable Nostr evidence beyond HTTP 101; NIP-11 remains optional. Transport failure causes backoff/dormancy, not operator ban or erased provenance. Registry errors are not remote-relay failures. Fresh evidence grants bounded revival. |
| P4 D-D10: monitor decay to weekly cadence, instant revival, least-recent selection | [Monitor](../architecture/acquisition-services.md#monitor-contract), scenarios 8–10 | **Retained with anti-storm qualification.** Split frequent liveness/full catalog, bounded evidence-triggered rechecks, and restoration after actual local success. Due-time and completion ages expose when the fleet exceeds budget. |
| P4 D-D11: host checks once per apex/cycle, endpoint checks individually, host tokens/jitter | [Monitor observation scope](../architecture/acquisition-services.md#monitor-contract), scenario 9 | **Changed fact-sharing key, retained efficiency.** Same apex does not imply same backend/certificate. Reuse DNS/Geo within route/resolution context and TLS within hostname/port/IP/policy context; preserve endpoint-specific checks and achieved cadence. |
| P4 catalog: DNS, ASN/Geo, TLS, HTTP/NIP-11, RTT open/read/write, NIP-42/45/50/77 | [Monitor](../architecture/acquisition-services.md#monitor-contract) | **Retained with explicit unknowns.** Typed per-check outcomes/times, empty-relay EOSE distinct from event RTT, advertised versus observed capability, no failed duration as a successful RTT. R04 adds explicit dual-stack and dataset-refresh evidence omitted by the broad scenarios. |
| P4 D-D3: Monitor emits no Nostr EVENT, yet write RTT requires publishing/re-fetch | [Active probes](../architecture/acquisition-services.md#monitor-contract), scenario 10 | **Resolved contradiction.** Passive default signs/sends no EVENT; explicitly enabled active probes use a dedicated scoped signer, bounded PoW/time and OK plus exact-ID readback. NIP-66 announcement publication stays a separate service. No implicit secret or deletion guarantee. |
| P4 D-D8: durable observation stream, Indexer replay, latest per-column PG projection | [Storage owners](../architecture/storage-services.md), [Monitor](../architecture/acquisition-services.md#monitor-contract) | **Retained.** Observations go through durable admission/archive then serving projection, not an unrebuildable CH insert. Stable observer UUID is separate from optional publisher pubkey. Missing required observation sinks cannot silently advance a complete projection frontier. Dedicated-versus-mixed journal directory wording is replaced by typed records/streams. |
| P4 II.9.4 t10/t11: dual-stack A+AAAA and Geo refresh with last-good fallback | [Monitor](../architecture/acquisition-services.md#monitor-contract), [R04](#repair-dispositions-and-concrete-acceptance) | **Retained; R04 addressed in baseline.** [Monitor clarification](../architecture/acquisition-services.md#paid-relays-and-measurement-retention) now specifies independent A/AAAA and corrupt/interrupted refresh fixtures plus source/product/format/schedule validation and last-good behavior. |
| P4 D-B14 / t12: 12-month raw observations, aggregates forever | [Monitor retention](../architecture/acquisition-services.md#monitor-contract), scenario 21 | **Changed to explicit 365-day duration and whole-retention path.** Replay logs/documents/backups must not resurrect expired raw detail. The baseline now explicitly sets indefinite compact aggregate retention with bucket resolution, denominator, method and source-retention coverage; it does not imply deleting CH rows alone implements raw retention. |
| P4 novel-rate scoring proposal, explicitly not approved in the historical plan | [Fair scheduling](../engineering/configuration-and-runtime.md#shared-budget-allocator), [capacity/duplication model](../engineering/capacity-and-benchmarks.md#efficiency-choices-that-matter) | **Not adopted as automatic eligibility.** Novel-body rate depends on collector order, history and other observed relays; low novelty does not remove the value of an independent witness. It may be an attributed diagnostic, but the new plan's fairness/evidence obligations prevent an undisclosed novelty-based exclusion policy. Historical approval wording does not impose a new permission gate on authorized design work. |
| P4 day-0 bundled-seed→candidate→validator→sync and resolved-IP acceptance | [Scenarios 1–4](../architecture/acquisition-services.md#practical-acceptance-scenarios), [packaged release journeys](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles) | **Retained.** Exercise the packaged command/Compose boundary with controlled sockets and real storage, not only constructors/fakes. Verify actual socket destinations, DNS rebinding, correct SNI and forbidden-route rejection. |

## Supplemental coverage matrix, serve wiring and test-infrastructure dispositions

The complete C matrix was read across all areas. Its phase-0–4 and cross-cutting
requirements are mapped here; adjacent phase-5–9 rows were read to catch interface
omissions, while their full service contracts remain the assigned reviewers'
responsibility. Historical checked boxes and transcripts are evidence claims
inside the source, not tests independently rerun during this review.

| Source requirement or revealed failure | Current destination | Disposition and reason |
|---|---|---|
| C Area B D-B2c: time-or-size sealing, with low-rate readers never stranded | [Archive publisher](../architecture/storage-services.md#archive-publisher-and-maintenance), [recovered acceptance](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents) | **Retained through new mechanisms; R09 addressed in baseline.** Admission group sync, archive publication age and online indexing are independent timers/frontiers. One event below every size target must become archived/queryable without a second event. |
| C Area B D-B8 late addition: hot embedded IDs plus cold CH existence checks for billion-key scale | [Exact admission](../architecture/data-and-durability.md#admission-transaction), [representation challenge](../engineering/capacity-and-benchmarks.md#billion-scale-representation-challenge) | **Changed deliberately.** Permanent compact exact event and P-edge inventories prevent analytical lag from creating duplicate admission facts. A bounded cache accelerates, not replaces, exactness. Count on-disk keys separately from cache RAM; benchmark dictionary/LSM/backup costs rather than imply every key must be resident. |
| C D-B2d reported corpus compression below the advertised universal target | [Capacity programme](../engineering/capacity-and-benchmarks.md#required-workload-programme-before-capacity-claims) | **Retained experimental lesson; universal ratio rejected.** A reported 7,353-event corpus changing roughly 1,235 to 590 bytes/event cannot establish 5× compression. Rust Parquet/columnar, edge/index and rebuild measurements must state corpus, representation and all retained bytes. Historical numbers are source reports, not measurements made here. |
| C Area X D-X5: paid relay flagged while best-effort permitted reads continue | [Payment and measurement contract](../architecture/acquisition-services.md#paid-relays-and-measurement-retention), [protocol claims](../architecture/protocol-evolution.md#richer-statistics-benefits-costs-and-exposure) | **Retained; R10 addressed in baseline.** Advertised fees/payment policy is separate from observed operation refusal; public reads can work. No automatic purchase/auth identity, and no global unreachable verdict inferred from one restricted operation. |
| C Areas E/F/G/H/U/V/W and II.9 cross-service test map | [Requirements/history](../product/requirements-and-history.md), [release journeys](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles), aggregate vision/phase reviews | **Cross-read, bounded ownership.** Facts, TrustRank, publishers, APIs/DVM/relay, dashboard, deploy/backup and public value remain named outcomes. This supplement does not replace full phase-5–9 or vision disposition work owned elsewhere. Old exact phase order, separate website repository and stack/preset laws do not override the greenfield mandate. |
| W/W1 real production services unassembled, dependency placeholders, accepted config never consumed | [Implementation sequence](../../PLAN.md#implementation-sequence), [first launch](../engineering/configuration-and-runtime.md#deterministic-first-launch) | **Retained lesson; old late assembly changed.** Build a thin executable path with real adapters early. Required enabled adapters/config must have an actual runtime consumer. Constructor tests, service-name lists and fake successful ticks cannot prove the documented command collects, reads, restores or signs correctly. Test actual dependency outage and socket/listener behavior. |
| WD durable blocked-ID marker exists but its owner never runs; bare ID means “owned” | [Durable ingest](../architecture/acquisition-services.md#durable-ingest-contract), [reread clarification](../architecture/acquisition-services.md#clarifications-from-the-complete-historical-reread) | **Retained wiring requirement; ownership semantics corrected.** Verify the caller and persisted policy effect through CLI acquisition. A denied ID can resolve a controllable need-list as explicit exclusion; it is not evidence of a possessed verified body or witnessed delivery. R05 records the linear-wire limitation. |
| WC duplicated relay-scoped SQL/set-reader implementation and unearned library reuse | [Ordered local-set interface](../architecture/data-and-durability.md#admission-transaction), [online indexer](../architecture/storage-services.md#online-indexer) | **Retained focused boundary; old SQL rules rejected.** One tested endpoint/observer/auth/filter scoped set reader serves reconciliation. Empty scope and invalid ranges fail explicitly. A conservative stale projection can cost downloads but cannot establish false progress. Benchmark actual access paths instead of inheriting a literal no-JOIN rule or duplicated hard-coded predicates. |
| TE build/env/runtime exclusions, inaccurate default-suite claims and missing tool routes | [Required-suite evidence](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents), [release discipline](../engineering/rust-and-repository.md#maintenance-and-release) | **Retained; R08 addressed in baseline.** Inventory selected versus executed versus skipped work, including feature/platform boundaries and auxiliary test crates. A required all-skipped suite is NOT RUN. Clean commands have reproducible dependency routes and correct exit status; source-file counts or a parser that misses runtime gates cannot establish execution. |
| TE runtime skip after wrong product output; partially failing suite described as passing | [Independent oracles](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles), R08 | **Rejected false-pass mechanism.** Missing prerequisite is explicit NOT RUN; an assertion contradicted by product behavior is failure. Report the exact executed scope and failing siblings. Validate a required runner using an intentionally unavailable local prerequisite, without importing arbitrary minimum-test quotas. |
| TN elapsed/calendar conflation, fixed-date fixtures versus real relay age policy, polling deadline misclassification | [Waiting acceptance](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents), R11 | **Retained with two independent dimensions.** Calendar-invariant math does not prove stable performance under load. Pure tests inject time; real engines need a consistent fixture clock basis, finite monotonic waits and diagnostics. A timeout can affect correctness regardless of whether it was described as “just a bound.” |
| TN live-limit decoy becomes ineligible during seeding; waits can pass using previous-run residue | [Live algorithm](../architecture/acquisition-services.md#layer-algorithms), [entry-point oracles](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles), R11 | **Retained non-vacuity lesson.** Isolate run namespaces, prove a decoy meets all other selectors, test newly delivered backdated events and make a never-published event fail finitely. Fresh keys can provide isolation; reproducible recorded entropy or explicit namespaces are valid. Reject a blanket RNG ban and brittle per-line census. |

## Repair dispositions and concrete acceptance

These are contract dispositions and proposed implementation acceptances. They
are not permission requests, product code, arbitrary review gates or claims of
executed tests. Addressed entries remain here so that a reader can follow the
finding to the repaired baseline.

**R01 — Deterministic first launch: addressed in baseline.**
[Deterministic first launch](../engineering/configuration-and-runtime.md#deterministic-first-launch)
names the roles, bootstrap predicate, idempotent source receipt, opt-out, listener
and signer defaults, conditional Ranker readiness and packaged CLI/Compose
acceptance. Clean volumes must lead to real fixture admission/archive/query and
actual dashboard/admin onboarding. Empty-idle health or mocked service ticks
cannot satisfy it. The shared release programme covers provenance-off and
independent role operation; archive-only needs its corresponding supported-recipe
journey without accidentally starting an online indexer.

**R02 — Shared runtime: addressed in baseline.**
[Shared lifecycle vocabulary and once semantics](../engineering/configuration-and-runtime.md#shared-lifecycle-vocabulary-and-once-semantics)
distinguish process health, terminal finite-job outcome and protocol progress.
Bounded work units and listener `once` rejection are explicit.
[Ownership timing and run envelope](../engineering/configuration-and-runtime.md#ownership-timing-and-run-envelope)
adds exact keys, 60s TTL, 20s heartbeat, 5s renewal timeout, 20s safety margin and
30s drain, with validation and controlled restart for timing changes. Commit-time
fencing and positive old-owner exclusion remain independent of timing. Typed
bounded execution envelopes record scope/cut, config, owner/epoch, outcome,
progress and artifacts. The planned tests include TTL stalls, old-owner
resumption, drain failure, clock steps and monotonic elapsed time. Preserve
separate registry-clock and local-clock control in the real ownership fixture;
local injected time alone cannot validate PG lease behavior.
Final coordinating verification replaced the earlier contradictory acquisition
CLI-status sentence with shared health plus activity/reason. Running work is an
activity; backpressure and unavailable dependencies explain degraded health.
This is a documentation repair, not a product-test result.

**R03 — First observation without provenance: addressed in baseline.**
[First observation without provenance](../architecture/data-and-durability.md#first-observation-without-provenance)
now requires immutable `first_admitted_at`, attributable `first_received_at`,
observer/clock metadata and immutable earlier corrections with distinct source
record IDs. Imported assertion time does not become this instance's local
network receive time. Pinned reads filter by source cut before applying minima.
The specified provenance-off replay, duplicate, clock rollback, import-followed-
by-network and restore scenarios preserve O(U) metadata without allocating O(P)
edge state. These scenarios are planned, not executed here.

**R04 — Monitor refresh and retention: addressed in baseline.**
[Paid relays and measurement retention](../architecture/acquisition-services.md#paid-relays-and-measurement-retention)
now specifies per-dataset HTTPS source, protected credentials, format/product
metadata, bounded container allowlist, `mmdb`, 24h refresh checks, atomic
validated install and preserved last-good content. A/AAAA partial success,
different selected-IP TLS, corrupt/truncated/oversized/wrong-product downloads,
interrupted replacement and unavailable stale refresh are explicit fixtures.
Compact aggregates default to indefinite retention (`none`) with resolution,
denominator, method and source-retention coverage; raw detail remains 365d.
Implement the configured-then-invalid startup case and raw-expiry/replay/restore
scenario through the actual adapter and storage owners. These requirements are
documented, with no claim of a passing future Monitor test.

**R05 — Blocked-ID requests: addressed in baseline.**
[Historical-reread clarifications](../architecture/acquisition-services.md#clarifications-from-the-complete-historical-reread)
now limit zero unnecessary refetch to controllable exact-ID requests. Ordinary
linear/live delivery may repeat a blocked body. Acceptance checks bounded
rejection, no retained prohibited body, no false local witness and explicit
policy exclusions; it does not assert impossible zero remote wire delivery.

**R06 — After-maximum interval bound: addressed in baseline.**
[Historical-reread clarifications](../architecture/acquisition-services.md#clarifications-from-the-complete-historical-reread)
define `Finite(u64)` / `AfterMax`, checked inclusive-wire conversion and explicit
empty/inverted/zero cases. Canonical uint64 timestamp validity is independent of
NIP-77's reserved maximum. [Recovered acceptance](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents)
requires both extremes through storage/filter conversion. Include no-wraparound
and finite-subdivision assertions in that implementation scenario.

**R07 — Source accounting: reconciled in the main inventory.**
All fourteen recovered snapshot entries/hashes are in the main inventory, and
this review is linked from the aggregate full-source reread. The tables above supply exact
paths, hashes, bytes and line counts. Record authoritative resolution locations
for residuals after edits. Other phases, vision and the explicitly excluded
progression documents have separate boundaries; this review is not a complete
code audit. Snapshot code paths intentionally avoid links to ignored local files.

**R08 — Test execution evidence: addressed in baseline.**
[Additional recovered acceptance](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents)
now defines required-suite execution accounting and NOT RUN handling. The
historical examples below make the planned acceptance non-vacuous.
The release runner should record selected suites, collected scenarios, executed
outcomes, skipped/ignored/feature-gated cases with reasons, dependency/image
versions and the exact command/configuration. A required suite whose engine is
missing or whose selected scenarios all skip is **NOT RUN**, not passing; fail
that required release job with an actionable prerequisite. Expected required
scenario identities are more meaningful than an arbitrary minimum test count.
Do not turn wrong product output into a skip condition. Reports scope partial
runs exactly, including any failed sibling tests. Keep public-relay observation
availability separate from deterministic release acceptance. Verify the reporter
with a deliberately unavailable local prerequisite and an intentionally excluded
required scenario; neither may be emitted as an executed passing suite.

**R09 — Low-rate bounded publication: addressed in baseline.**
[Additional recovered acceptance](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents)
now explicitly requires the single-event below-threshold case.
Admit one valid event well below archive batch/file targets. With no second event,
it must become archived and queryable according to configured age/flush bounds;
its admission receipt must already be durable. Exercise the packaged roles and
real engines, then restart between admission, archive publication and indexing.
Record which frontier and timer delayed each step. This validates independent
5ms admission-group, 30s archive-delay and 1s index-flush defaults without treating
compressed target size as a condition that can strand an idle tail indefinitely.
The engine/storage contracts already allow small low-rate files. The new
acceptance validates their timers without requiring synchronous Parquet per event.

**R10 — Paid-relay acquisition: addressed in baseline.**
[Paid-relay acquisition](../architecture/acquisition-services.md#paid-relays-and-measurement-retention)
now explicitly scopes declarations/refusals, permitted reads and no purchases.
[Protocol statistics](../architecture/protocol-evolution.md#richer-statistics-benefits-costs-and-exposure)
already preserve advertised auth/payment/write limitations separately from
observations. [Product decisions](../product/data-and-feature-decisions.md)
exclude automatic payments. The acquisition contract now attempts permitted
public reads even when a relay advertises paid writes or optional subscription
plans. Scope an attributable payment refusal to the requested operation and auth
context; distinguish it from timeout, unsupported protocol, NIP-42 challenge and
operator ban. No purchase or secret is inferred. A controlled relay with payment
advertising plus working public read remains readable; a refused restricted
operation reports that result without falsifying global uptime or coverage.

**R11 — Deterministic waiting: retained baseline with detailed acceptance.**
[Independent oracles](../engineering/security-recovery-and-validation.md#test-programme-with-independent-oracles)
and [recovered waiting acceptance](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents)
cover controlled pure time and bounded actual-process waits. Preserve these
supplemental failure cases when implementing that programme.

Use injected time for pure scheduler/retention semantics. Real-engine waits have
an overall finite deadline plus monotonic progress/stall accounting; reset a
stall budget only on genuine tested progress, not arbitrary heartbeat changes.
For tests anchored to a real engine's age policy, capture one clock anchor or
explicitly isolate retention policy; a fixed old signed timestamp must not expire
silently years later. Use isolated fixture namespaces and record randomness
needed to reproduce failure. Demonstrate that an eligible known-stored decoy
would be returned by an incorrect live query and that waiting for a deliberately
unpublished event fails finitely. Calendar invariance does not prove latency
under machine load; report performance measurements separately from correctness.
Do not inherit a brittle per-line nondeterminism census or ban all RNG use.

## Practical verification status

Performed in this review: Markdown file enumeration, readonly original/snapshot
byte comparison, exclusive creation of fourteen newly authorized snapshot files,
SHA-256/byte/line accounting, complete reading of the assigned corpus, current
contract comparison and this written disposition record. All 150 local Markdown
links and heading anchors resolved; all 20 covered snapshots matched readonly
source bytes and their recorded SHA-256 values. No product code, legacy
repository contents, running relay, remote database or git state was changed.
No product test, browser journey, crash drill or capacity benchmark was run.
