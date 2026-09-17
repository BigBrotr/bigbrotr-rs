# Specification readiness and implementation gates

Assessment: 2026-09-17. This is the current paper handoff, replacing the open
assessment at `e674d3d`. The former ambiguities have selected contracts, examples,
producer/consumer ownership and discriminating acceptance work. Product code is
still unimplemented. Paper closure is not a claim of global optimality, runtime
safety, supported throughput or completed release qualification.

Start implementation through [the agent guide](../implementation/agent-handoff.md)
only after the owner's separate start instruction. The task checkboxes and
[closure review](../reviews/14-independent-closure-review.md) and
[operational countercheck](../reviews/14-handoff-countercheck.md) record actual work;
historical reviews do not override current specifications.
The [large-storage and final handoff check](../reviews/15-large-storage-and-final-handoff.md)
adds acquisition proof/visibility corrections and actual larger engine evidence.
It does not close the unimplemented product gates below.
The [current full review](../reviews/16-complete-handoff.md) adds complete baseline
read accounting, role/deployment and archive placement contracts, richer scoped
analytics/publication definitions and a durable agent checkpoint. Its real S3 and
CH counterexamples refine implementation requirements, not product readiness.

## Disposition of the eight paper gaps

| Gap | Selected rule and evidence | Implementation owners / remaining runtime proof |
|---|---|---|
| S01 — Same event across scopes | [Identity contract](../specification/identity-and-encoding.md): one shared payload, one first verified signed representative per eligible scope, scoped receive/witness minima. Authorize the source population before winners/deletions; provenance off still admits new eligibility. Arrival/restriction/replay/export truth tables and real signed fixtures distinguish private signatures and influence. | P01-T02; P02-T02–T05; P05-T02; P06-T02; P08-T04. Actual storage, serialized output, grants and restore must match the independent fixture. |
| S02 — Portable identities and exact bytes | The same contract fixes qualified allocation domains, overflow/restore, SHA-256 domains, deterministic CBOR profile, exact core field arrays, schema negotiation and strict request conflicts. [Golden vectors](../../experiments/identity-contract/golden-vectors.json) have independent byte/digest checks. CertifyExisting is a separate nonmutating operation with explicit recovery dependencies. | P01-T02/T07; P02-T02/T03/T04/T05; import/export adapters. Rust codecs and every persisted/transport representation must reproduce vectors and reject incompatible writes. |
| S03 — Effects and recovery | [Operations contract](../specification/operations-contract.md) names authority, atomic write set, ACK, unknown outcome, retry and reclaim at every admission/archive/index/derived/job/outbox boundary. Its independent model covers reordered schedules and wrong-state controls. | P01-T06/T07; P02-T02/T04–T08; P05-T01; P07-T03/T04; P08-T03. Real binding/engine/process tests remain essential; abstract state exploration does not prove fsync or exclusion. |
| S04 — Whole-pipeline admissibility | Operations fixes maximum signed envelope versus wire/fact bounds, parser/encoder expansion, host/process/native grants, transfer order, control/completion lanes and disk debt reserve. Full/archive-only/serving/split arithmetic examples are explicit. | P01-T04/T05; P02; each heavy job. Qualify maximum units, blocked dependencies, OOM/ENOSPC recovery and actual native/cgroup memory. Arithmetic budgets are not measured working sets. |
| S05 — Capability/configuration transitions | One managed or declarative policy authority, locked deployment roots, explicit revision application/mode switch, and role/capability dependency matrices replace hidden override precedence. Provenance/search/target disable/re-enable and retained-history behavior are explicit. | P01-T04/T05; P08-T01–T03; P09 administration. Clean install, concurrent edits, staged drift, restart and restore must demonstrate the same authority. |
| S06 — External operations and useful clients | [Interface catalog](../specification/interface-catalog.md), [read modes](../specification/read-view-contract.md), [DVM](../specification/dvm-profile.md), [MCP/ContextVM](../specification/gateway-contracts.md) and private management own fields, permissions, errors, bounds and workflows. Chronological traversal and strict snapshot are different declared contracts. | P06, including T13/T14; P08/P09/P10. Generate actual schemas/clients, exercise real protocol clients and browser tasks; ContextVM remains experimental until its explicit interoperability gate. |
| S07 — Workloads and independent expectations | [Workload contract](../specification/workload-contract.md) fixes mixed tuples, all-live variants, distributions, aged state, useful provisional objectives, overload and calibration discipline. [Signed fixture](../../experiments/identity-contract/signed-fixture.json), measurement fixture and independent graph/read models provide concrete expected outcomes. | P01-T08 and phase acceptance tasks. Record actual offered/completed/refused work, starvation, maintenance and recovery. No unexecuted 5k/10k/50k/100k mixed run becomes PASS during planning. |
| S08 — Protection, compatibility and recovery | Operations fixes initial local-disk failure class, supported placement, positive fencing, backup closure, isolated restore, upgrade ordering, rollback boundary and interrupted migration. Existing security and release contracts retain secret/platform/dependency obligations. | P02-T08; P08–P10. Clean bundle install, full causal restore, old-version upgrade, both release architectures and license/right-to-distribute checks precede release. |

These are closed **semantic specifications**. Small models, official source
inspection and actual isolated engine experiments support different parts of
them; none substitutes for the future packaged product. The [experiment summary](../research/experiment-summary.md)
and raw evidence state what ran and its limits. A failed practical gate reopens
its affected choice rather than weakening the invariant to preserve the plan.

## Decisions that properly remain empirical

| Decision / selected starting point | Fixed semantics and required comparison | Latest safe point and failure response |
|---|---|---|
| Native Rust/engine release set | Synced Rocks atomic state; bounded killable DuckDB worker; PG-backed archive transactions/files; CH verified serving incarnation. Pin the candidate library/engine set, exercise actual bindings and resource ownership. | P01 risk slices and P02-T02/T04/T05 before dependent adapters spread. Replace binding/worker/engine if required guarantees fail. No ACK downgrade hidden behind a pass. |
| CH physical order and projections | Complete event fields, cut-relative eligibility, no duplicate logical facts. Compare ID-first/chronological layouts and narrow access paths at identical query/filter/preservation scope, including parts, concurrent load, repair and qualified durability. | P02-T09 before release DDL. Projections are rebuildable acceleration, not independent source truth; base fallback/explicit unavailable must remain correct. Verify known fsync/recovery issues on the exact release. |
| Exact inventory and documents | Preserve first times, scopes, signatures and exact membership. Baseline compact ordinals/flat exact keys; compare grouped layouts and wide versus referenced documents at meaningful cardinality/churn, including dictionaries and peak bytes. | P02-T09 before persistent release format. Keep simpler qualifying layout; no custom second LSM or membership-only substitute masquerading as equivalent evidence. |
| Archive batching/inlining | Multi-family atomic receipt, standalone closure, bounded quiet-event latency and safe maintenance. Compare file/row sizes and narrow receipt inlining with explicit engine settings and catalog/backup cost. | P02-T04/T09 before tuning freeze. Smaller files or a refused incompatible configuration are preferable to accepting an event that cannot complete downstream. |
| Local/S3 backend qualification | Local default; configured S3 optional. Preserve semantic receipts and exact closure; qualify actual catalog/object persistence, credentials, request/egress cost, missing-data refusal and isolated restore. Full-file SHA capture is separate from native durability. | P02-T04/T08/T09 and P08 split/restore journeys before provider support. The loopback Garage research proves only its stated interactions; no automatic cloud or multi-host certification. |
| Search backend | Selected exact analyzer/filter/phrase/relevance contract and complete candidate coverage. Start with CH; compare Tantivy only for a demonstrated capability/resource deficit, including operational and index-copy costs. OpenSearch is not mandatory. | Early P06-T06 before search deployment promises. A semantics change needs a new explicit profile; do not call different results an equal-query speedup. |
| Graph/overlap algorithms | Stationary trust target/error contract, independent reference profile, exact logical pair relation and declared approximate alternatives. Measure loading, construction, solver/residual passes, spill, equal-set grouping and output expansion. | Early P05 slices before broad materialization. Refuse incomplete exact work; do not call a spanning forest all pairs or fixed iterations convergence. |
| Sustainable fleet envelope | Mixed workload, fair discovery/revisits, useful reads, monitor/graph/export/maintenance and outage recovery inside declared host limits. | P03–P08 acceptance before capacity advertising. Reduce qualified envelope or change placement; do not hide dropped work, overdue relays or accepted-data loss. |

Private helper names, ownership/borrowing choices and equivalent internal query
construction belong to implementation review. They are not unresolved product
semantics and do not need a second handwritten implementation in Markdown.

## Selected architectural direction

Retain Rust/Tokio, PG control plus separate DuckLake catalog, Rocks admission and
independent derived state, DuckLake-managed Parquet archive, CH online serving,
and DuckDB offline/archive jobs. This intentionally pays for online/archive body
representations to provide independent research retention and rebuild/export use.
CH-only retained authority is a real alternative with different coupling and
portable-export obligations; it is not the selected product and does not remove
receipts, exact admission, control or backup closure. A physical-table benchmark
alone is not an equal-durability comparison of those complete architectures.

Adopt the architecture challenge's scoped nonmutating duplicate certification,
age-banded revisits, ordinary traversal mode, vector-first relay selection,
residual-bounded trust profile, exact-set overlap grouping, one default socket
owner and a single configuration authority. Keep full signed event retention and
optional provenance; no added broker, graph database, generic plugin framework or
public arbitrary RPC. Optional MCP and experimental ContextVM share the read core.
Precompute useful dashboard/client facts and NIP-85 inputs with explicit delta/
retraction state; keep alternate costly analyses as bounded jobs or offline exports.
CH materialized views are qualified accelerators, not a second correctness owner.

The [agent guide](../implementation/agent-handoff.md) requires audits at persistent,
public, derived and distribution boundaries, practical real-system checks and
coherent commits. There is no arbitrary quota of clean reviews or comments.
