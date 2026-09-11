# Vision reread and implementation-baseline crosscheck

Date: 2026-09-11. This is an additional review after the six recorded design
revisions. It does not retroactively claim that the earlier source snapshot
contained every phase task document. The owner requested a full reread and an
explicit handoff, not product implementation.

## Coverage and method

The coordinating reviewer reread the complete 2,819-line original vision,
including Part I decisions, historical amendments, Part II DDL, configuration,
I/O ownership, build/test/bootstrap and all sixteen functional entries. Its
SHA-256 is `ede97801d1431b5739d1f3d2b915b64a9aaed0f3e323874d08f8e5e89634a610`.
The frozen source is identified in [the source inventory](../research/source-inventory.json).
Historical SQL and code fragments are design evidence, not code to copy without
review. Their passing Go tests cannot certify a future Rust implementation.

The parallel bounded phase rereads are recorded separately:

- [Master plan and phases 0–4](07-phase-0-4-recheck.md).
- [Phase 5, including its long correction history](07-phase-5-recheck.md).
- [Phases 6–9 and supplemental task/review documents](07-phase-6-9-recheck.md).

Initial inventory contained phase READMEs but missed task/review Markdown under
several phase directories. The recheck enumerates those directories recursively,
freezes missing files separately and records their hashes. No old snapshot is
silently replaced by a moving working tree. Read coverage is stated in those
reports; hashing or counting a file alone does not count as reading it.

Final accounting: the phase directories contain **34 Markdown files / 27,049
logical lines**, all covered by those rereads. In addition, the master plan,
original vision, September consolidation and seven explicitly named supplements
were reread. Recovery added **31 source snapshots** (24 phase documents and seven
supplements), bringing the broader capture inventory to 70 entries. Every phase
file matched its read-only legacy source at the final comparison. The inventories
identify actual worktree bytes by SHA-256; a contextual HEAD does not certify
that an actively edited worktree equals its committed tree. Absent retired task
files and excluded top-level progression histories are disclosed in the reports.
This is complete coverage of the stated planning corpus, not an audit of every
Go source file.

The additional manifests cover [phases 0–4 and supplements](../research/evidence/phase-0-4-extra-sources.json),
[phase 5's surviving source](../research/evidence/phase-5-extra-sources.json) and
[phases 6–9 additions](../research/evidence/phase-6-9-extra-sources.json).

## Vision decisions: retained intent and deliberate changes

The rows below account for all 132 distinct `D-*` decision definitions in the
vision. Grouping related definitions avoids reproducing contradictory historical
amendments as competing current specifications. Linked current contracts are the
implementation authority. This is a requirements crosswalk, not evidence that
132 implementation tests have passed.

| Original decisions | Selected disposition and reason | Current contract |
|---|---|---|
| D-A3, D-A4, D-A5, D-A6 | Change: always retain every signed field/signature; remove lossy storage axis and forced whole-instance redeploy. Provenance activation creates an evidence epoch; it cannot recreate the past. | [Data fidelity](../architecture/data-and-durability.md), [configuration](../engineering/configuration-and-runtime.md) |
| D-A7, D-A8, D-A9 | Retain transparent recipes/filter coverage; remove mandatory identical engine footprint for archive-only use. Names never determine hidden storage behavior. | [Product](../product/requirements-and-history.md), [topologies](../architecture/system-design.md) |
| D-B1, D-B3 | Retain transactional control and rebuildable online columnar representation; PG also catalogs DuckLake under separate ownership. | [Storage owners](../architecture/storage-services.md) |
| D-B2, D-B2b, D-B2c, D-B2d | Replace permanent custom journal with synchronized admission/pending records and transactionally published Parquet. Keep checksums/versioning and bounded size/age publication; avoid redundant repeated-delivery truth records. | [Durability](../architecture/data-and-durability.md) |
| D-B4, D-B5 | Retain lossless signed event and durable-progress ordering; distinguish signed serialization from original JSON byte layout, admission from archival publication and host-loss protection. | [Data contract](../architecture/data-and-durability.md) |
| D-B6, D-B7, D-B8 | Replace logical-row replacement and lossy edge cache with exact admission, immutable facts/corrections and complete published source cuts. Finite insert dedup windows cannot prove correctness. | [Storage contract](../architecture/storage-services.md), [merge probe](../research/evidence/clickhouse-frontier.json) |
| D-B9, D-B10, D-B14 | Retain no automatic event pruning and first-class exports/recovery. Separate data export, deployment backup, scrub, retention and exceptional removal. Live catalog file globs are not portable snapshots. | [Export and recovery](../architecture/data-and-durability.md), [jobs](../architecture/derived-read-publication.md) |
| D-B11, D-B12 | Retain narrow indexes, binary values and measured compression. Re-evaluate sort/projection/partition/codec choices under real workloads; preserve full integer timestamps rather than inherit DateTime quarantine. | [Serving layout](../architecture/data-and-durability.md), [capacity](../engineering/capacity-and-benchmarks.md) |
| D-B13 | Retain explicit single-host and replicated growth tracks. Reject transferring unmeasured 5k events/s, p99 or compression-ratio targets into product guarantees. | [Capacity](../engineering/capacity-and-benchmarks.md), [implementation track](../../PLAN.md) |
| D-C0, D-C1, D-C1b, D-C2, D-C3 | Retain layered live/history/reverification with durable scoped evidence. Separate traversal, gaps and receipts instead of treating one high timestamp as complete disclosure. | [Acquisition](../architecture/acquisition-services.md) |
| D-C4, D-C4b, D-C5, D-C6 | Retain bounded reconciliation and verified fetches. Repeated capped pages do not prove completeness; same-second stalls remain explicit gaps. Add server-side NIP-77 separately. | [Acquisition](../architecture/acquisition-services.md), [server reconciliation](../architecture/search-and-reconciliation.md) |
| D-C7, D-C14 | Change import rules: valid signed events may enter provenance-enabled archives without inventing a witness; imported claims remain attributable claims. Lossy records are not full events. Import does not advance remote coverage. | [Importer](../architecture/acquisition-services.md) |
| D-C8, D-C9, D-C10, D-C11, D-C12, D-C13 | Retain per-host/global fairness, adaptive sweep cost, capability evidence and reusable ordered sets. Multiplex per endpoint/auth scope; timeout/NOTICE/hints do not establish support or non-support. | [Acquisition](../architecture/acquisition-services.md), [budget model](../engineering/capacity-and-benchmarks.md) |
| D-D1, D-D2, D-D3, D-D4, D-D5 | Retain every named responsibility, common read core, measurement/publication separation, aggregation target and export capability. Libraries, jobs and listeners are distinct shapes. | [System boundaries](../architecture/system-design.md) |
| D-D6, D-D7, D-D8 | Retain typed defaults, independent progress and rebuildable measurements. Replace no-RPC/shared-journal process contradiction with local calls or authenticated receipt-preserving interfaces. | [Runtime](../engineering/configuration-and-runtime.md), [storage](../architecture/storage-services.md) |
| D-D9, D-D10, D-D11 | Retain external directories, announcements, recurring Nostr queries, graph work and cadence decay. Host evidence reuse is keyed by actual hostname/address/port/route/method, never merely apex domain. | [Acquisition](../architecture/acquisition-services.md), [publication/read contracts](../architecture/derived-read-publication.md) |
| D-E1, D-E2, D-E3, D-E4, D-E5, D-E8 | Retain current state, incremental facts, complete generations and provenance-conditional targets. Whole-input correctness is the invariant; SQL-only and blanket no-LIMIT rules are not proof of it. | [Derivation](../architecture/derived-read-publication.md) |
| D-E6, D-E7, D-E9 | Retain overlap and third-party aggregation with support/conflict/freshness. Similarity is advisory; no all-pairs explosion or false physical-relay identity deduplication. | [Derivation](../architecture/derived-read-publication.md), [phase-5 crosscheck](07-phase-5-recheck.md) |
| D-F0, D-F1, D-F2, D-F2b, D-F3 | Retain configured signing, real Nostr IDs and rich compatible outputs. Re-pin current NIP schemas rather than use obsolete issue proposals as standards. | [Protocol evolution](../architecture/protocol-evolution.md) |
| D-F4, D-F5 | Retain retry stability and consistent inputs; durable signed intent precedes sending, per-destination acceptance follows ACK. Dependencies use coherent run/cut vectors, not equality of unrelated run IDs. | [Publication](../architecture/derived-read-publication.md) |
| D-G1, D-G2, D-G3, D-G4 | Retain one typed authorized cost-bounded read core, declared resources, stable cursors and truthful completeness. Reject filters with no admitted execution path rather than silently narrow their meaning. | [Read core](../architecture/derived-read-publication.md) |
| D-G5, D-G6, D-G7 | Retain COUNT and optional public relay/API. Exactness is real, not raw duplicate count labeled approximate. Add working post-EOSE live delivery, NIP-50 search and server NIP-77; local UI is available without public exposure. | [Read surfaces](../architecture/derived-read-publication.md), [search/reconciliation](../architecture/search-and-reconciliation.md) |
| D-R0, D-R1, D-R2, D-R2b, D-R3, D-R4, D-R5, D-R6, D-R7 | Retain endpoint granularity, networks, unverified candidates and self exclusions; replace lossy scheme/query/slash normalization and global localhost XOR with conservative versioned identity and scoped routes. | [Endpoint contract](../architecture/acquisition-services.md) |
| D-H1, D-H2, D-H3, D-H4, D-H5 | Retain easy CLI/container deployment and configuration. One image, composable process roles, finite jobs and continuous listeners replace image/container per named service and forced RunOnce. | [Runtime](../engineering/configuration-and-runtime.md), [packaging boundaries](../architecture/system-design.md) |
| D-H6, D-H7, D-H8 | Retain private operational monitoring, working bootstrap and optional overlays; add usable local management/dashboard and an exact first-launch role recipe. | [Runtime](../engineering/configuration-and-runtime.md), [administration](../product/private-administration.md) |
| D-H9, D-H10, D-H11, D-H12 | Retain useful external libraries, clean idiomatic code, critical review and real validation. Select Rust with measured caveats; remove arbitrary coverage/line/review quotas and automatic reuse. | [Rust organization](../engineering/rust-and-repository.md), [validation](../engineering/security-recovery-and-validation.md) |
| D-S1, D-S2, D-S3, D-S4, D-S5, D-S6, D-S7 | Retain guarded egress, explicit policy, protected signing, bounded hostile input, host budgets and supply-chain review. Require full event verification, separate imported evidence, fail-closed permissions and actual storage failure tests; no lossy cache or inline secret exception. | [Security](../engineering/security-recovery-and-validation.md), [admission](../architecture/data-and-durability.md) |
| D-U1, D-U2, D-U3, D-U4, D-U5, D-U6 | Retain capability-aware public explorer separate from private operations. Use one monorepo and separate static builds; dashboard uses authorized HTTP, never a browser database credential. Add complete website/blog/docs. | [Dashboard/site](../product/dashboard-and-website.md), [private UI](../product/private-administration.md) |
| D-V1, D-V2, D-V3, D-V4 | Retain observation order, selective availability, historical evidence and overlap; reject claims that first_seen proves propagation, absence or when a relay stopped serving an event. | [Facts/read semantics](../architecture/derived-read-publication.md), [protocol evidence](../architecture/protocol-evolution.md) |
| D-V5, D-V6, D-V7 | Retain history forensics, citable full corpora and content discovery. Search has a defined baseline, coverage and index lifecycle; OpenSearch is not mandated. | [Data export](../architecture/data-and-durability.md), [search](../architecture/search-and-reconciliation.md) |
| D-W1, D-W2, D-W3, D-W4 | Retain alerts, visible storage duplication/growth, upgrades and actual restore drills. Exercise the alert receiver and full installed entry points. | [Runtime/alerts](../engineering/configuration-and-runtime.md), [validation](../engineering/security-recovery-and-validation.md) |
| D-W5, D-W6, D-W7, D-W8 | Retain audited policy/key admin, public status and cold integrity checks. One maintenance scrub owner; no impossible recall of independent copies or silent physical removal from NIP-09. | [Security/recovery](../engineering/security-recovery-and-validation.md), [administration](../product/private-administration.md) |
| D-X1, D-X2, D-X3 | Retain all signed historical versions and deletion requests; serving is a separate policy. Ephemeral collection is a deliberate archival behavior with honest protocol/coverage limitations. | [Acquisition](../architecture/acquisition-services.md), [protocol](../architecture/protocol-evolution.md) |
| D-X4, D-X5, D-X6, D-X7, D-X8 | Retain explicit AUTH/payment/ban/filter policies and hostile timestamp protection. No implicit payment or signer; use full integer timestamps and bounded partition placement, including checked interval endpoints. | [Acquisition](../architecture/acquisition-services.md), [data](../architecture/data-and-durability.md) |

Part II's DDL/config/I/O tables were read as well. Their intent is covered above,
but the tables are deliberately not copied into the new product: they embed the
old identity, irreversible profile, cursor, schema and retry assumptions. Current
service contracts must generate the later implementation schemas and config
reference. New migrations require actual engine tests before becoming final DDL.

## Consolidation plan reread

The coordinating reviewer also reread all 825 lines of the frozen September 9
`BIGBROTR-THE-PLAN.md`, including findings, workstreams A–K, rulings, uncertainty
register and appendices. Its source/hash is in the original inventory. Its claims
of authority, reset permission and release/tag operations govern the other agent's
Go task, not this repository or our read-only boundary.

Useful lessons retained: cursor-independent reverification; real caller entry
points; actual running-versus-seated resource accounting; independent worker loops;
bounded read concurrency; content-addressed documents; workload-measured indexes;
working installation, diagnostics and host-independent release tests. Server
NIP-77 and content search were already proposed there, confirming the omission in
the earlier Rust relay contract needed repair.

Deliberately reject its two-feature-only scope, mandatory old profile, lossy
storage axes, custom permanent multistream journal obligation, automatic staging
resets, fixed prose limits, blanket absence of migration, and test-count/audit
rituals. A signature fast path must bind the actual signed payload and signature;
validating only a claimed ID plus signature shape is insufficient. Do not inherit
its author-prefix subdivision or bitmap/storage estimates without checking
protocol semantics and actual distributions. Current ordinal identities survive
file compaction and are not recomputed from whichever physical position a future
export happens to use.

## Findings that changed this baseline

1. The search contract was too generic. It now specifies baseline matching and
   relevance, engine alternatives, optional index lifecycle, coverage, privacy,
   costs and actual client acceptance tests.
2. The read-side relay contract omitted server NIP-77. It now distinguishes
   negotiation from body transfer, upstream provenance from serving membership,
   pinned sets from atomic exports, and shared summaries from per-client memory.
3. The ten-crate sketch prematurely equated logical subsystems with packages.
   It is replaced with focused application modules and three candidate reusable
   libraries, with explicit promotion and dependency criteria.
4. The original phase snapshot was incomplete. Supplemental task/review sources
   are captured and their requirements crosschecked rather than ignored.
5. Event-level first admission/receive semantics with provenance off were too
   implicit. Fields, immutable corrections and replay/restore scenarios are now
   explicit without adding hidden edge history.
6. Initial seeding, finite-job outcomes, once semantics and lease/drain defaults
   needed concrete definitions. These now appear in runtime/acquisition contracts.
7. Timestamp interval end overflow and impossible zero blocked-wire-delivery
   assumptions now have explicit representations and acceptance boundaries.
8. Operational alerts now name the required classes and test the complete local
   delivery path. Packaging acceptance also checks build-cache secrets and actual
   listener reachability under supported network modes.
9. Named analytics needed exact meaning. The [analytics contract](../architecture/analytics-detail.md)
   now defines counts, propagation buckets/quantiles, scoped availability, overlap
   adjustment, third-party conflicts, partial seed coverage and graph work/state.
   A score for every subject is not mandatory: useful facts are preferable to an
   unjustified universal truth or reputation number.
10. The [product/data catalog](../product/data-and-feature-decisions.md) explicitly
    chooses retained records, indexes, public resources and useful workflows.
    Unchanged monitor documents are content-addressed within visibility scopes;
    check time and cached-data age remain separate, with referentially complete
    archive publication/export and documented retention.
11. HTTP malformed predicates, empty policy intersections, semantic cursors and
    aggregate DVM work/envelope bounds now have concrete contracts and contrasting
    fixtures. Multi-filter relay behavior and extracted standalone bundles are
    part of actual release acceptance.
12. Conflicting lifecycle wording is reconciled with shared health plus separate
    activity/reasons; the full-source capture inventory and decision crosswalk
    have been checked for missing entries and mismatched hashes.


The phase-specific reports record additional analytical and interface details and
link their final dispositions. An unresolved empirical decision stays in PLAN.md;
it must not be disguised as a missing product requirement or a passed test.

## Handoff and readiness limits

The implementation entry point is [PLAN.md](../../PLAN.md). Read its linked
system/data/service contracts, then the dependency-specific validation obligation
before the first slice. Persist any newly discovered decision in its owning
contract and update the crosswalk; do not let code or an agent's memory become
the only specification. Generated API/config/schema references must agree with
the executable as the implementation grows.

The baseline can be complete as a design while engine tuning, physical schemas,
SDK fixes, failure tests and user journeys remain unimplemented. No one can
certify billions of events, 100,000 busy relay sessions, loss-of-power durability
or every possible query from these planning experiments. Implementation should
start with the risk-bearing vertical slice and actual shipped entry points, then
change the plan when evidence warrants it. This review does not begin that stage.
