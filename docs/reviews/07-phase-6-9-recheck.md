# Full reread and completeness review: original phases 6–9

Date: 2026-09-11. Scope: the four original phase README files and **every Markdown
file actually present beneath those four phase directories** in the read-only Go
checkout. This is a review of the fresh Rust product plan. Historical Go test,
review and gate results are evidence about the earlier project only; none proves
that the Rust product is implemented, reachable or validated.

The core product intent survives: bounded shared reads, real protocol adapters,
publication, complete operational tooling, reproducible exports and a substantial
dashboard. The reread recovered 17 additional source documents, exposed concrete
acceptance cases that a README-only pass misses, and identified deliberate changes
that must not be mistaken for omissions. All services remain equally important;
dependency order does not reduce the finished product's scope or quality.

## Exact source coverage

Both directory trees were enumerated with `rg --files` and a recursive Markdown
file-set comparison. Every file below was read in full, using bounded line chunks;
truncated tool output was followed by a narrower read of the affected interval.
Headings and search hits were navigation aids, not substitutes for reading.

The four README snapshots already existed and matched their source bytes. The
17 other files were copied into previously absent corresponding snapshot paths,
without overwriting existing evidence. Their SHA-256, byte count, line count,
source path and source Git revision are in the
[additional-source manifest](../research/evidence/phase-6-9-extra-sources.json).
Source HEAD was `757a8d899aefb277c0211077bc30ba0c8de68796`, resolved by reading
local Git metadata without running Git. These are hashes of the read-only source
worktree bytes; uncommitted status was not checked, so the revision is context,
not a claim that each file's bytes equal the committed object at HEAD.

Paths below are relative to the ignored local evidence directory
`.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/`. Published readers
use the tracked manifest for source identity; local snapshot paths are not public
repository links.

| Phase | File read completely | Lines | Bytes | Snapshot status |
|---|---|---:|---:|---|
| 6 | `phase-6-read-side/README.md` | 982 | 74,508 | Existing, byte match |
| 6 | `phase-6-read-side/GATE-REPORT.md` | 1,873 | 126,960 | Added |
| 6 | `phase-6-read-side/task-02-earn-review-archive-relay.md` | 1,215 | 81,454 | Added |
| 6 | `phase-6-read-side/task-05-cost-complexity-planner.md` | 1,765 | 79,595 | Added |
| 6 | `phase-6-read-side/task-09-hybrid-current-serving.md` | 837 | 44,339 | Added |
| 6 | `phase-6-read-side/task-12-api-adapter.md` | 2,104 | 109,282 | Added |
| 6 | `phase-6-read-side/task-13-dvm-adapter.md` | 2,961 | 164,172 | Added |
| 6 | `phase-6-read-side/task-14-nip01-differential-conformance.md` | 1,431 | 67,459 | Added |
| 6 | `phase-6-read-side/task-15-phase-gate.md` | 1,127 | 60,590 | Added |
| 7 | `phase-7-publication/README.md` | 296 | 28,820 | Existing, byte match |
| 7 | `phase-7-publication/EARN-REVIEW-nip85-2026-08-28.md` | 682 | 55,914 | Added |
| 7 | `phase-7-publication/GATE-REPORT.md` | 705 | 42,155 | Added |
| 7 | `phase-7-publication/task-10-offsafety-gate-and-ci.md` | 2,253 | 150,706 | Added |
| 8 | `phase-8-ops-packaging/README.md` | 320 | 35,864 | Existing, byte match |
| 8 | `phase-8-ops-packaging/earn-review-cli.md` | 124 | 6,525 | Added |
| 8 | `phase-8-ops-packaging/earn-review-deployments.md` | 627 | 31,000 | Added |
| 8 | `phase-8-ops-packaging/earn-review-deriver-recompute.md` | 275 | 14,059 | Added |
| 8 | `phase-8-ops-packaging/earn-review-klauspost-compress.md` | 76 | 3,776 | Added |
| 8 | `phase-8-ops-packaging/earn-review-takedown.md` | 311 | 17,815 | Added |
| 8 | `phase-8-ops-packaging/task-08-restore-drill-harness.md` | 628 | 38,320 | Added |
| 9 | `phase-9-dashboard/README.md` | 107 | 16,941 | Existing, byte match |
| **Total** | **21 files** | **20,699** | **1,250,254** | **17 newly preserved** |

The additional files total 18,994 lines and 1,094,121 bytes. There are no additional
phase-9 Markdown files in either enumerated tree. Task filenames mentioned by a
README or gate but absent from both scoped directories are not recovered sources;
their existence, full contents or execution cannot be inferred from the index.
This review does not claim a reread of every Go source file or of every original
phase outside 6–9. The parent review owns the original vision and other phases.

Current design crosscheck: the relevant contracts, lifecycle, failure and
acceptance sections of the documents linked below were read against these sources.
The updated search/reconciliation contract and the operational-alert and packaging
amendments were checked after their addition. The earlier complete product-surface
review remains in [product-surface findings](../research/product-surfaces-findings.md).

## How to read dispositions

**Retained** means explicitly required by the fresh plan, not implemented.
**Changed** preserves useful intent with a reasoned different contract.
**Rejected** identifies an obsolete, harmful or unnecessarily restrictive mechanism.
**Gap** means a missing explicit contract or discriminating acceptance case, not a
claim that future code is defective. **Addressed during reread** means another
planning document now contains the repair; implementation evidence is still due.

Historical gate reports contain dated corrections and contradictory older status
paragraphs in the same file. Their old `PASS`, `OPEN`, package counts and exact
line references cannot be treated as present facts. In particular, the phase-6
report eventually adds completed measurements after earlier “not completed”
sections. It also shows that compiling packages and running a conformance-only
host did not initially prove the shipped CLI assembled those services. The fresh
acceptance bar must exercise the actual shipped binary and deployed entry points.

## Phase 6: shared reads, API, relay and Nostr queries

Primary phase source:
`.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-6-read-side/README.md`
(existing snapshot; additional task hashes are in the tracked manifest).
Supplemental task and gate evidence is inventoried above.

| Original requirement or recovered useful intent | Current destination | Disposition and reasoning |
|---|---|---|
| D-G1 / D-D2: one bounded read core; all adapters enter the same admission path | [Shared read core](../architecture/derived-read-publication.md#shared-read-core), [system design](../architecture/system-design.md) | **Retained.** Resource authorization and execution cannot be bypassed through an exported lower-level planner or adapter-specific SQL. A tiny adapter field count is not a security property. |
| D-G2: registry defines filters, sorts, limits, exposure, capabilities, completeness and pagination | [Resource registry](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [dashboard](../product/dashboard-and-website.md) | **Retained.** Generation-aware runtime metadata drives clients. A declaration must lead to a working, bounded resource; an unused registry entry is not delivered functionality. |
| D-G3: plan before expensive storage work; caps alone do not establish cheap access | [Read admission](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [search admission](../architecture/search-and-reconciliation.md) | **Retained and strengthened.** Cheap metadata/statistics reads are legitimate; unbounded estimation scans are not. Runtime scan/memory/time limits remain necessary after admission. Literal SQL predicate order is not proof of physical access efficiency. |
| Task-05/task-15: compare chosen and alternative paths in genuinely different engine regimes | [Validation](../engineering/security-recovery-and-validation.md), [search evidence](../architecture/search-and-reconciliation.md#practical-evidence-required-before-shipping) | **Changed.** Retain varied part counts, revision density, selectivity, skew and concurrent workload. Historical one-coordinate 3/57-part results cannot select the general Rust planner or prove a universal latency threshold. |
| D-G4: latest complete derived output; efficient full signed event lookup | [Generation manifests and reads](../architecture/derived-read-publication.md), [storage contracts](../architecture/storage-services.md) | **Retained.** Validate real body retrieval, ordered tags and signature; complete, empty, missing and failed generations differ. A partial fixture must contain a unique row and a wrong winner so a false fallback is observable. |
| D-G6: current replaceable/addressable winner, correct equal-time tie, deletion-aware views | [Read semantics](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [deletion policy](../architecture/protocol-evolution.md) | **Retained with explicit correction.** Resolve the winner before filters that could reveal an older superseded value. Exercise tags as well as time bounds. Missing a required winner body is a coverage failure, not permission to omit it silently. |
| Hybrid current serving and cadence-derived freshness | [Pinned cuts](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [data and durability](../architecture/data-and-durability.md) | **Changed.** Compatible source cuts and published generation manifests replace an unqualified live/snapshot union. Neither a configured cadence nor “at least as fresh as either input” proves records absent from both inputs were covered. |
| D-G5: efficient COUNT with exact/approximate distinction | [Count contract](../architecture/derived-read-publication.md#resource-registry-and-query-meaning) | **Changed.** `auto` prefers exact when admissible; otherwise use a defensible named estimator. Physical replay duplicates are not an acceptable approximation. Count distinct logical units across overlapping filters, under exactly the visible resource predicate. |
| NIP-45 specialized HLL | [Count contract](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [protocol evolution](../architecture/protocol-evolution.md) | **Retained conditionally on exact protocol fixtures.** Distinct authors and distinct reaction/event IDs are different units; a generic event-ID HLL cannot substitute for the specialized construction. |
| Task-12: strict request parsing cannot broaden a query by dropping malformed constraints | [API](../architecture/derived-read-publication.md#api), [repair cases below](#concrete-repairs-and-discriminating-acceptance-cases) | **Addressed during reread.** The recovered transport-regression section now specifies malformed encoding, invalid/empty values and repeated singleton handling, with shared canonical parsing. Preserve standards-specific ignore rules for Nostr textual extensions separately. |
| Stable pages, honest clamping and tamper-resistant cursors | [Read cursors](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [browser matrix](../product/dashboard-and-website.md#accessibility-and-browser-acceptance) | **Addressed during reread.** The recovered transport-regression section now binds semantic keys/values, source and policy, interprets search through a versioned parser and tests the cursor produced by a clamped response. Equivalent expressions may normalize together; changed literal meaning must not. |
| D-G7: exposure before existence disclosure, quota, CORS and resource capability checks | [Exposure](../architecture/derived-read-publication.md#exposure-limits-and-failures), [private administration](../product/private-administration.md) | **Retained and strengthened.** No API-key exemption from exposure or global resource budgets. Actual trusted proxy peers determine forwarding authority; client source ports cannot create fresh rate identities. |
| Task-02 and task-12: policy narrowing must never turn an empty allowed author set into an omitted author filter | [Policy activation](../product/private-administration.md), [repair cases below](#concrete-repairs-and-discriminating-acceptance-cases) | **Addressed during reread.** Typed `MatchNone` now applies across events/count/search/live/NIP-77/export, with missed-notification and expired-policy-lease acceptance. |
| Slow-client abort, cancellation, backpressure and advertised limits | [ArchiveRelay](../architecture/derived-read-publication.md#archiverelay), [runtime](../engineering/configuration-and-runtime.md) | **Retained.** Bound encoded bytes and global allocations, not just queue item count. A real blocked socket must cancel expensive producer work and release permits/pins. NIP-11 advertisement alone does not establish enforcement. |
| Historical post-EOSE open-but-silent and `limit=0` silence | [ArchiveRelay](../architecture/derived-read-publication.md#archiverelay) | **Rejected.** The selected current contract sends EOSE then a bounded committed-index live feed, including old newly acquired timestamps. An archive-only compatibility limitation must be labeled explicitly. |
| Multiple REQ filters, subscription replacement, COUNT framing | [ArchiveRelay](../architecture/derived-read-publication.md#archiverelay), [cross-surface journeys below](#required-cross-surface-release-journeys) | **Retained.** Add independent wire cases for per-filter historical limits, deduplicated union, same-ID replacement and COUNT lifecycle; generic successful REQ tests do not exercise these distinctions. |
| D-V5 and historical gate's by-ID non-erasure proof | [History/exposure](../architecture/derived-read-publication.md#resource-registry-and-query-meaning), [protocol evolution](../architecture/protocol-evolution.md) | **Changed by owner decision.** Keep full history, but ordinary ID lookup is deletion-aware too. Named authorized history/export proves retention; standard by-ID must not bypass deletion or exposure. |
| D-V1 / D-V3 and provenance fidelity on each surface | [Derived resources](../architecture/derived-read-publication.md), [dashboard](../product/dashboard-and-website.md) | **Retained with honest mapping.** HTTP can return registered provenance resources. A Nostr event body does not acquire provenance fields; adapters disclose their actual supported resources and metadata channel. Do not require fictitious equal fidelity from transports unable to express it. |
| D-V7 / O-V1: actual content search, previously left without a resource or settled engine | [Search and reconciliation](../architecture/search-and-reconciliation.md) | **Addressed during reread.** Search is required for the complete online product, with CH baseline and reasoned Tantivy/OpenSearch alternatives. Disabled deployment capability does not mean the complete product may omit search. |
| NIP-77 archive-server reconciliation, separate from acquisition-client reconciliation | [Archive reconciliation](../architecture/search-and-reconciliation.md#nip-77-on-the-archives-reading-side) | **Addressed during reread.** Same authorized retrievable set as the serving view; bounded pinned negotiation, policy revocation and actual WebSocket client tests. Provenance is not a prerequisite. |
| II.9.14 / D-D9: signed one-shot and recurring Nostr queries, optional pricing | [DVM/query service](../architecture/derived-read-publication.md#dvm--nostr-query-service) | **Retained as product functionality; wire profile changed.** Current NIP-90 status and task kinds require a versioned archive profile before enablement. Kind 5050 is not a generic archive-query registration. Paid mode needs settlement/recovery semantics and remains independent. |
| DVM replay, duplicate request, restart and acceptance-window safety | [DVM ledger](../architecture/derived-read-publication.md#dvm--nostr-query-service) | **Changed and strengthened.** Durable local acceptance, pinned input, signed outbox and replay tombstones replace remote-relay scanning as authority or evictable RAM-only idempotency. One relay ACK is not every destination's delivery receipt. |
| DVM hostile relay hints, URL identity, self-ingestion and signing amplification | [DVM contract](../architecture/derived-read-publication.md#dvm--nostr-query-service), [egress/security](../engineering/security-recovery-and-validation.md) | **Retained with simpler authority.** Configured destinations and typed endpoint identity; no arbitrary request-supplied egress. Disjoint relay lists cannot prevent gossip of the provider's own output; origin classification and kind-specific execution prevent recursion without deleting legitimate archive events. |
| Task-13: whole-job work and signed-envelope budget | [DVM limits](../architecture/derived-read-publication.md#dvm--nostr-query-service), [repair cases below](#concrete-repairs-and-discriminating-acceptance-cases) | **Addressed during reread.** The recovered transport-regression section now reserves aggregate job work and concurrent memory, accounts for complete signed/escaped/framed bytes, bounds feedback and checks actual transport results. |
| Task-14 and gate: independent protocol oracle, actual shipped-host reachability | [Validation](../engineering/security-recovery-and-validation.md), [read/publication acceptance](../architecture/derived-read-publication.md) | **Retained.** Independently construct/sign/check a discriminating corpus; control reference relay versions and behavior. No passing claim when the suite did not run, a conformance-only host served, or a fixture produced no records. |

## Phase 7: interoperable, deliberate publication

Primary phase source:
`.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-7-publication/README.md`
(existing snapshot; additional task hashes are in the tracked manifest).

| Original requirement or recovered useful intent | Current destination | Disposition and reasoning |
|---|---|---|
| D-F0: all signing/publication paths disabled initially; no secret load or background publish loop | [Publication contract](../architecture/derived-read-publication.md), [runtime defaults](../engineering/configuration-and-runtime.md) | **Retained.** Includes DVM announcements/results and Monitor write probes. Test disabled deployment and an explicitly enabled positive path so a disconnected service cannot pass merely by doing nothing. |
| D-F1: full NIP-85 subject families, provider identity and metadata | [Assertion registry/publication](../architecture/derived-read-publication.md), [protocol evolution](../architecture/protocol-evolution.md) | **Retained with current-spec correction.** Cover all applicable 30382–30385 subject families using the proper schema, units and evidence. Kind 10040 is a user's preference list, not the provider's advertisement. Optional kind 0 is informational. |
| D-S3: signer abstraction, restricted file and external signer support | [Key management](../engineering/security-recovery-and-validation.md), [publication](../architecture/derived-read-publication.md) | **Changed deliberately.** Initial restricted file and tested NIP-46 paths; OS keyring/KMS/HSM adapters require actual compatible Schnorr support and tests before advertisement. Distinct algorithms/viewpoints require distinct NIP-85 service keys. |
| D-F2 / D-D3: Monitor measures; separate publisher emits endpoint-specific NIP-66 | [NIP-66 publication](../architecture/derived-read-publication.md), [monitoring](../architecture/acquisition-services.md) | **Retained.** Stored actual observations supply facts and their ages; no implicit probing during publication. Share host facts only within valid resolver/IP/port/SNI context. |
| D-R7: internal/published relay identity correspondence | [Acquisition identities](../architecture/acquisition-services.md), [protocol evolution](../architecture/protocol-evolution.md) | **Changed.** Preserve a full typed endpoint including scheme/path/query instead of reconstructing information discarded from a scheme-less key. One canonical identity contract prevents separate DVM/publisher normalizers. |
| Local/private destinations never become public relay assertions | [Publication privacy](../architecture/derived-read-publication.md), [security](../engineering/security-recovery-and-validation.md) | **Retained.** Exercise loopback, mapped addresses, case/trailing-dot variants and public positive controls. A bad untrusted observation must be rejected/classified without poisoning unrelated valid publication indefinitely. |
| D-F2b: maximally rich but spec-respecting observations; NIP-32 enrichment | [Protocol evolution](../architecture/protocol-evolution.md) | **Changed.** Publish authorized, supported, evidence-backed facts; computability alone does not justify disclosure. Standard wire subset and namespaced opt-in extensions are explicitly different contracts. Unknown measurements are omitted/explained, never fabricated zero/false. |
| O-F1: pin exact changing NIP schemas and test actual tags | [Protocol evolution](../architecture/protocol-evolution.md) | **Retained.** Current pinned normative sources replace historical unsent/open-PR assumptions. NIP-66 timeout prose/example discrepancy remains explicitly documented; proposed upstream text is a draft, not a merged rule. |
| D-F3 / D-F4: genuine event IDs, deterministic retry bytes and acceptance receipts | [Durable publication](../architecture/derived-read-publication.md), [failure validation](../engineering/security-recovery-and-validation.md) | **Changed and strengthened.** Persist intent and exact signed bytes before first send; record per-target ACK afterward. Historical “no state writes before ACK” loses crash-safe recovery and is rejected. |
| D-F5: all facts from the same COMPLETE run | [Generation manifests](../architecture/derived-read-publication.md) | **Changed.** Bind a compatible generation vector with explicit dependency lineage, algorithm/viewpoint and source cut. Independent Monitor/Deriver/Ranker jobs need not share an artificial identical run ID. |
| Stale publication-checkpoint cleanup | [Publication/job retention](../architecture/derived-read-publication.md) | **Changed.** Cleanup is subordinate to retained pins, outstanding delivery and replay safety. Removing a checkpoint cannot make an old accepted request or assertion appear never delivered. |
| Stable timestamps across retry, restart, relay failure and key rotation | [Publication acceptance](../architecture/derived-read-publication.md) | **Retained.** Retry exact signed events across clock changes; a rotated key does not silently re-sign already queued bytes. Relay acceptance is not proof of long-term availability or client receipt. |

## Phase 8: operating, recovering and distributing the complete product

Primary phase source:
`.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-8-ops-packaging/README.md`
(existing snapshot; additional task hashes are in the tracked manifest).

| Original requirement or recovered useful intent | Current destination | Disposition and reasoning |
|---|---|---|
| D-H2 / D-H7: actual CLI composition, all roles reachable, useful finite and resident behavior | [Runtime](../engineering/configuration-and-runtime.md), [Rust/repository](../engineering/rust-and-repository.md), [private administration](../product/private-administration.md) | **Retained.** Run/serve, once, status, configuration, import/export, backup/restore, migrations and schema operations must invoke real domain work. Pure probes do not collect/sign. A module, crate, service role, process and container are different units. |
| D-H1 / O-N1: one binary and numerous named image presets | [Requirements/history](../product/requirements-and-history.md), [deployment/product](../product/dashboard-and-website.md) | **Changed by owner direction.** One BigBrotr image and explicit configuration recipes; no hidden profile behavior. Informal LilBrotr means full archive without provenance, with legacy meaning explained. Social filtering is independent of provenance. |
| D-C7 / D-C14 / R16: import compatibility, caps, verification and no network-cursor advance | [Importer](../architecture/acquisition-services.md#importer-contract) | **Retained with full-fidelity correction.** JSONL, native manifests and pinned pensieve protobuf; bounded joins/scratch, immutable resume identity, current admission policy and explicit imported claims. No verification-disable route into canonical signed storage. |
| Former provenance-required and heavy-to-slim import matrix | [Importer](../architecture/acquisition-services.md#importer-contract), [export modes](../architecture/derived-read-publication.md#exporter-and-integrity-work) | **Changed.** Dataset-only origin is representable without fictional relay evidence. Bodyless analytical projections cannot be restored as original Nostr events. Every canonical archive retains full signed events. |
| D-V6 / R12: reproducible deduplicated corpus and portable provenance | [Exporter](../architecture/derived-read-publication.md#exporter-and-integrity-work), [research journeys](../product/dashboard-and-website.md) | **Retained.** Pin selection, source cut, policy, exact unit, schema, digests and transforms. Deduplicate logical event IDs; keep provenance companions authorized and linked only to selected events. Signed-field fidelity differs from retaining every wire JSON serialization. |
| Incremental backup/export, resume, full versus filtered segment fidelity | [Exporter](../architecture/derived-read-publication.md#exporter-and-integrity-work) | **Changed to new storage.** Catalog-consistent DuckLake backup and immutable object identities replace raw Go segment copying. A filtered corpus materializes selected logical rows; listing whole source files is not filtering. Commit manifests only after durable verified artifacts. |
| D-W4: real isolated restore of archive, control, dedupe/index and acquisition progress | [Recovery validation](../engineering/security-recovery-and-validation.md), [backup contract](../architecture/derived-read-publication.md#exporter-and-integrity-work) | **Retained and expanded.** Include pending authoritative spool and external-effect ledgers, invalidate restored live ownership, resume from proven receipt boundaries. Never infer coverage from maximum event timestamp. Positive test fixtures must restore useful nonempty data; a genuinely empty valid deployment remains a valid backup case. |
| D-W8: checksum/sig scrub, reconciliation and corruption reporting | [Storage services](../architecture/storage-services.md), [integrity work](../architecture/derived-read-publication.md#exporter-and-integrity-work) | **Retained with one owner.** One durable scheduled scrub executor, rotating progress and explicit exhaustive mode; Exporter/admin request that job. Sampled signatures are not exhaustive verification. Intentional exclusions are not accidental corruption. |
| Poisoned records and incompatible formats cannot cause invisible data loss | [Storage services](../architecture/storage-services.md), [private administration](../product/private-administration.md) | **Retained.** Pause affected work with inspectable cause; restore known-good bytes or reprocess with repaired reader. Blind skipping must not manufacture completeness. Irrecoverable loss changes evidence/coverage and requires an explicit recovery decision. |
| D-W1: six original alert classes and an actual route to a receiver | [Shipped operational alerts](../engineering/configuration-and-runtime.md#shipped-operational-alerts) | **Addressed during reread.** Coverage/cursor stall, ingest halt, fsync/disk failure, index backlog, network-class darkness and replica lag are named; conditional applicability, inhibition, local receiver and resolution are tested. |
| D-W2: estimator and explicit authoritative/online/provenance/duplication costs | [Private storage UI](../product/private-administration.md), [data architecture](../architecture/data-and-durability.md), [validation](../engineering/security-recovery-and-validation.md) | **Retained with new accounting.** Include pending spool, archive, CH projections, text/reconciliation indexes, replicas, compaction/rebuild and backup headroom. Dataset distribution and actual throughput matter more than a profile name. |
| D-B2d / D-B13: background compression and release measurement | [Storage architecture](../architecture/data-and-durability.md), [validation](../engineering/security-recovery-and-validation.md) | **Changed.** New Parquet/engine compaction supersedes a mandatory port of Go sealed-segment zstd machinery. Preserve verified replacement and bounded background costs. Reject an unconditional ≥5× compression gate independent of corpus and all indexes/replicas. |
| D-W3: rolling services, versioned storage, schema tracking and downtime | [Rust/repository](../engineering/rust-and-repository.md), [recovery validation](../engineering/security-recovery-and-validation.md) | **Retained with practical migrations.** Explicit reader/writer compatibility window and expand/contract workflow; “additive forever” is not a viable universal migration policy. Downgrade/rollback promises must match actual formats and snapshots. |
| D-W5: audited takedown with derived and exported/backup impact tracking | [Private administration](../product/private-administration.md), [recovery/security](../engineering/security-recovery-and-validation.md), [protocol policy](../architecture/protocol-evolution.md) | **Retained but separated from NIP-09.** Standard deletion changes serving, not automatic physical retention. An explicitly authorized physical exception tracks affected artifacts and unresolved external copies; no promise of controlling arbitrary prior recipients. |
| D-B9 / D-B14 / D-E4: pruning and recomputation | [Full-history requirements](../product/requirements-and-history.md), [storage services](../architecture/storage-services.md) | **Rejected for routine canonical archive pruning.** Low trust or age does not erase signed history. Rebuildable projections may be reclaimed. Recomputing derived tables cannot reconstruct permanently erased source evidence. |
| D-W6: optional API tenancy issue/rotate/quota/revoke | [Private administration](../product/private-administration.md) | **Retained and expanded.** Explicit grants, action authorization, revocation, audit and visible quotas. Public read routes do not become administrative write routes. |
| D-W7: public status distinct from private operational dashboards | [Public dashboard](../product/dashboard-and-website.md), [private administration](../product/private-administration.md) | **Retained.** Expose safe resource health/coverage/age; do not imply an operator SLA or leak Grafana, metrics credentials and private diagnostics. |
| D-S6: multi-platform artifacts, pinned images, SBOM and usable deployment bundle | [Rust/repository](../engineering/rust-and-repository.md), [release validation](../engineering/security-recovery-and-validation.md) | **Retained.** Validate intended architectures and dependency pins, real extracted bundles, checksums/provenance, relative mounts and documentation. A pinned single-platform image is not a portable multi-platform release. |
| Supplemental deployment review: cached `.env`, untracked build material, network mode/private-bind mistakes | [Recovered packaging acceptance](../engineering/security-recovery-and-validation.md#additional-acceptance-recovered-from-historical-task-documents) | **Addressed during reread.** Clean/reused-cache builds and effective bridge/host-network binds are explicit. Extend clean artifact journey to extraction outside the checkout so missing mounted files cannot be hidden by local files. |
| CLI/docs and supervisor behavior must agree | [Lifecycle and once](../engineering/configuration-and-runtime.md#shared-lifecycle-vocabulary-and-once-semantics), [site maintenance](../product/dashboard-and-website.md#content-maintenance-and-historical-migration) | **Retained.** Generate actual command reference; distinguish listener, bounded job and scheduled work-unit semantics. Container termination grace must permit the declared drain; dependency startup health does not magically repair later outages. |

## Phase 9 and the separate website deliverable

Primary phase source:
`.research/source-snapshots/go/docs/superpowers/plans/bigbrotr/phase-9-dashboard/README.md`
(existing snapshot; additional task hashes are in the tracked manifest).

| Original requirement | Current destination | Disposition and reasoning |
|---|---|---|
| D-U1: substantial multipage dashboard, separate frontend repository | [Dashboard/website](../product/dashboard-and-website.md), [repository layout](../engineering/rust-and-repository.md) | **Dashboard retained; repository choice changed.** Rust monorepo with independently deployable frontend artifacts reduces contract/document drift. Static browser code is not part of the backend process and carries no database credentials. |
| D-U2 / D-U5: versioned read-only API, optional direct SQL advanced mode | [Shared API](../architecture/derived-read-publication.md#api), [SDK publication](../product/dashboard-and-website.md#sdk-and-contract-publication) | **HTTP retained; browser SQL rejected.** Private notebooks may separately use an authorized analytical connection. The dashboard consistently uses bounded typed resources and generated contracts. |
| D-U3 / D-U6: instance-aware pages and runtime feature matrix | [Page contracts](../product/dashboard-and-website.md), [requirements recipes](../product/requirements-and-history.md) | **Changed from preset names to real capability and coverage.** Monitor is independently selectable; social data can exist without a SocialBrotr recipe; historical provenance availability is not inferred from today's switch; all canonical events retain bodies but serving may be hot/cold/unavailable. |
| D-U4: public product separated from private Grafana/Prometheus | [Dashboard](../product/dashboard-and-website.md), [private administration](../product/private-administration.md) | **Retained and expanded by owner selection.** A dedicated authenticated administration panel adds configuration, roles, jobs and recovery. Public read pages remain distinct from both that panel and private observability. |
| Relay directory/details, coverage/provenance, event exploration, whole-archive statistics, social and score pages | [Concrete pages and journeys](../product/dashboard-and-website.md) | **Retained.** Resource-specific capability/coverage/staleness conditions replace empty placeholders. Scores expose algorithm/viewpoint/evidence; provenance views distinguish local witness and imported claims. |
| O-V1 / D-V7: search page backed by a real text resource | [Search](../architecture/search-and-reconciliation.md), [dashboard](../product/dashboard-and-website.md) | **Addressed during reread.** Search is a complete-product requirement, with scope/index lifecycle and actual API/NIP-50 acceptance. Existing wording “optional full text” refers to deployment enablement, not optional delivery of the planned feature. |
| Browser quality, responsive UX, no operations leakage | [Browser/accessibility acceptance](../product/dashboard-and-website.md#accessibility-and-browser-acceptance) | **Retained and made concrete.** Real Chromium/Firefox/WebKit and device/assistive checks, deep links, stale responses, permissions, hostile text, graph/table alternatives, downloads and disabled/stale states. Automated accessibility checks do not establish conformance alone. |
| Existing website/docs/blog need their own complete product scope | [Website and maintenance](../product/dashboard-and-website.md), [website findings](../research/product-surfaces-findings.md) | **Added distinct phase, not absorbed by dashboard.** Static docs/blog, service guides, generated API/CLI reference, releases, route migration, English source, future maintained translations and editorial ownership. Preserve historical context rather than presenting old Python/Go tutorials as current Rust guidance. |

## Concrete repairs and discriminating acceptance cases

These are bounded changes to the planning/acceptance contract. They do not demand
porting every historical test, arbitrary review rounds, coverage percentages or
line-count limits. General principles already exist; the cases below recover
specific ways the historical implementation or its guards gave a false answer.
The coordinating review incorporated P69-01–04 into the canonical
[transport/policy regression contract](../architecture/derived-read-publication.md#transport-and-policy-regressions-recovered-from-the-complete-phase-reread).
That section was read after the update on 2026-09-11 and verified against these
cases. P69-05–06 are also incorporated into the canonical relay and packaging
acceptance contracts; their implementation evidence remains due.

| ID | Necessary repair / explicit case | Where it belongs | Why a generic happy-path check is insufficient |
|---|---|---|---|
| P69-01 | Define structured HTTP decoding for unknown names, malformed percent escapes, empty values, repeated singleton fields and tag-key spelling. Reject a malformed narrowing predicate before engine work; never drop it. Keep standard Nostr unknown-extension handling separate. | [API and read acceptance](../architecture/derived-read-publication.md) | `authors=A&since=broken` must not become the broader valid author query. A successful well-formed request cannot detect discarded constraints. |
| P69-02 | Represent policy intersection with no allowed authors/IDs as match-none. Test an author-only blocked request against a corpus containing an allowed author of the same kind, through events/count/search/live/export; also tighten policy on a read-only worker that misses notifications. | [Read policy](../architecture/derived-read-publication.md), [management activation](../product/private-administration.md) | Removing the now-empty `authors` field broadens to every author. An empty corpus or a single blocked-author fixture cannot reveal that leak. |
| P69-03 | Enforce one aggregate DVM job work budget across its constituent filters/queries. Bound serialized complete signed results and rejection feedback, including escaped echoed inputs, before expensive execution. | [DVM admission and acceptance](../architecture/derived-read-publication.md) | Individually valid subqueries can multiply aggregate scan work; a small raw result can become an oversized signed/escaped envelope. Queue-count caps do not catch either. |
| P69-04 | Extend cursor tests with changes to tag key, tag value, search whitespace/escaping, source/policy generation and expiry; accept the next cursor generated by a clamped first page and traverse without duplicate/skip. | [Read/API acceptance](../architecture/derived-read-publication.md), [browser matrix](../product/dashboard-and-website.md) | A digest of only tag values or lossy normalization can accept another query; a cursor test using only unclamped pages cannot expose self-incompatibility. |
| P69-05 | Add a real multi-filter wire transcript: each historical filter's limit, deduplicated union, `limit=0` EOSE followed by old newly committed events, replacement of an existing subscription ID, and COUNT response lifecycle. | [ArchiveRelay acceptance](../architecture/derived-read-publication.md) | A one-filter snapshot fixture does not establish ongoing NIP-01 behavior or distinguish COUNT from a normal subscription. |
| P69-06 | Extract a release bundle into a fresh directory outside the checkout, verify every referenced mount/config/seed asset, then run the documented clean installation and shutdown. | [Packaging validation](../engineering/security-recovery-and-validation.md) | A compose file can work only because undeclared files happen to exist beside the developer checkout. Clean/cache image tests cover a different failure. |

**Reconciled status:** P69-01–06 are addressed in the canonical design; no new
architecture blocker remains from this reread. P69-05–06 strengthen already
selected behavior and remain implementation acceptance obligations. The alert and
private-bind failures initially found in this pass are also addressed in the
canonical documents. None of these planning repairs is a passing runtime test.

Two implementation handoffs should remain explicit without expanding the first
backend arbitrarily: an object-store import source adapter needs an actual named
format/transport contract, whereas local immutable downloads plus the existing
Importer are already a valid route; and any physical-erasure exception must mark
derived/citation lineage whose source evidence is no longer retained. Neither an
unspecified `S3` string nor a “reproducible” label supplies those guarantees.

## Required cross-surface release journeys

The current design already requires real engines, controlled relays and browser
tests. These consolidated journeys tie original intent to observable shipped
behavior. They are future acceptance work, not tests run by this planning review.

1. **Install and use the actual release.** From clean volumes and an extracted
   bundle, configure an instance, bootstrap controlled relays, collect signed
   events, see indexing/derivation progress, read through the actual API and relay,
   inspect the dashboard, export a corpus and shut down durably. Demonstrate
   actionable networkless and missing-dependency states. No conformance-only host
   may substitute for the shipped CLI composition.
2. **One semantic question through every supported adapter.** Independently create
   signed events with overlapping filters, versions, ties, deletions, malformed
   coordinates and a discriminating partial run. Compare real API, relay and
   configured Nostr-query answers at declared compatible cuts. Compare provenance
   only where the transport actually exposes it. Authorized historical reads
   retain evidence; ordinary ID lookup does not bypass visibility.
3. **Reject expensive work and stop abandoned work.** Instrument real engine scan
   work and global allocations. Exercise broad filters, a huge single author,
   mixed-query jobs, count overlap, many clients, slow writes and cancellation.
   Observe pre-scan refusal where possible and bounded runtime abort otherwise.
   Keep the read model responsive while ingestion, compaction and graph work run.
4. **Change policy while work is active.** With ordinary pages, a historical export,
   a live subscription and signed pending deliveries active, tighten policy and
   lose a notification. No actor continues after its bounded authority expires.
   Empty allowed-set intersections stay empty. Derived public aggregates must not
   retain private-input influence merely because their final rows are filtered.
5. **Publish deterministically under failures.** First prove default-disabled startup
   loads no signer and emits nothing; then explicitly enable genuine publication.
   Use distinct algorithm/viewpoint identities and endpoint paths, inspect signed
   payloads with an independent parser, kill before/after target ACK and restart
   after wall-clock changes. Exact pending bytes and per-target receipts survive;
   scope revocation can still forbid delivery of an already signed event.
6. **Backup and restore live work into isolation.** Back up committed archive,
   pending durable work and a coherent control cut while collection continues.
   Restore into fresh real stores; compare complete signed fields, admitted IDs,
   provenance/claim distinctions, current/deletion state and receipt-backed progress.
   Rebuild derived indexes and perform actual reads; resume safely without stale
   ownership or repeated external effects. Include corruption, interrupted copy,
   changed source, a valid empty backup and a required nonempty positive fixture.
7. **Operate from the private panel and route an actual alert.** Enroll an owner,
   test role denial, conflicting config revisions, applied-versus-desired state,
   disconnect/reconnect during a durable job and token revocation. Inject the six
   named operational failure classes where applicable; observe rule evaluation,
   grouping/inhibition, a local receiver and resolution. Public routes disclose
   neither private configuration nor observability credentials.
8. **Search and reconcile what can really be served.** Search multilingual signed
   content through API/dashboard/relay with filters before ranking and stable
   generation pagination. Negotiate differences through a reference NIP-77 client
   with provenance off, policy changes and cold/missing bodies. Fetch actual IDs
   via REQ and distinguish later policy changes from an atomic-export guarantee.
9. **Complete browser and documentation adoption.** Follow first-run commands for
   the exact release, open and reload deep links, investigate a relay/event, inspect
   a score's evidence, export a citable result, manage a job and recover a disabled
   or stale capability. Exercise keyboard, mobile and assistive paths, actual
   downloads, same/cross-origin deployment and private/public separation. Build and
   crawl the separate site, docs/blog routes, redirects, search and release links.

## Review-process lessons retained without inheriting the process machinery

The supplemental sources are unusually useful because they document failures in
the tests and reports themselves. Retain independent oracles and positive/negative
controls where they distinguish a real failure; do not transplant the old process
as a mandatory Rust workflow.

- A recorder that only repeats a hand-set expected label does not measure a
  runtime property. A filter-blind fake store cannot detect discarded filters.
- Counting distinct result IDs cannot prove one delivery when repeated delivery
  uses identical signed bytes. Assert attempts and per-destination outcomes too.
- A fixture with no matching event cannot establish suppression, fidelity or
  successful retrieval. Make the competing wrong result observable.
- Package import reachability, an interface implementation and a listening shipped
  service are different claims. A matching comment, string literal or test-only
  implementer is not production behavior.
- Advertising a limit in NIP-11 does not establish enforcement. Reference relay
  configuration must be pinned and its relevant behavior exercised.
- A grep count is not a semantic inventory. The old report's self-count changed
  when it wrote its own results; this review records exact scoped file sets and
  bytes instead of treating unstable counts as timeless claims.
- A review-round count, mutation percentage, inherited line cap or “two clean
  rounds” cannot replace testing a real user journey. Compiler failures are not
  evidence that a targeted behavioral mutation was detected.
- A source file's instruction to commit, mutate, clear caches or run a Go command
  is historical task data. This review performed none of those actions in the
  legacy repositories.

## Verification of this review's own claims

The scoped Markdown file sets in the legacy and frozen trees were compared after
the recovery. All 21 corresponding files were byte-identical. Every one of the
17 additional-source manifest entries matched its snapshot's SHA-256, bytes and
line count. These checks verify evidence preservation and inventory completeness
within the four named directories. They do not verify the historical Go results,
the larger repository's state or the planned Rust runtime behavior.
