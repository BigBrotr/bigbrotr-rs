# Requirement, data and practical-evidence coverage

This ledger connects the selected product to its actual phase tasks. The
[historical crosswalk](../product/requirements-and-history.md),
[feature decisions](../product/data-and-feature-decisions.md) and
[full-source reread](../reviews/07-vision-and-final-recheck.md) retain the original
context and deliberate changes. This table is a coverage argument, not evidence
that future code exists. Follow the task for numbered actions, paths, faults,
configuration, documentation and independent practical oracle.

Coverage is different from specification readiness. The
[readiness register](../planning/specification-readiness.md) maps
S01/S02 to P00-T06, S03/S04 to P00-T07, S05/S06/S08 to P00-T08 and S07/empirical
decision records to P00-T09; P00-T10 reconciles the resulting baseline. The current register records their paper disposition; runtime acceptance remains separate. A row below proves that work has an owner, not that its last
semantic ambiguity or practical evidence has already been resolved.

## Useful data and its complete journey

| Selected information | Producer / authority | Storage and read/export work | Discriminating practical evidence |
|---|---|---|---|
| A01 full signed originals, signatures, ordered tags/content, u64 authored time | P01-T02/T03; P03-T02/T08–T10 | P02-T02–T06; P06-T02/T05/T07; P08-T04 | Recompute ID and signature outside ingestion; preserve empty/repeated tags/extreme times after restart and standalone export |
| First local acquisition/observation, including strictly earlier corrections | P02-T02; P03-T02/T10 | P02-T04–T07; P06-T02; P08-T04 | Old pinned cut remains 100 after later eligible correction to 90; imported claim 50 cannot become local observation |
| Scoped event eligibility/signature and portable event-payload-value/1 | P01-T02; P02-T02/T03; P03-T02/T10 | P02-T04/T05; P05-T02; P06-T02; P08-T04 | Same ID privately then publicly never leaks private signature/time/envelope, including provenance off and standalone export |
| A02 optional first event/relay evidence | P03-T02–T07 | P02-T02/T05/T06; P05-T07; P06-T01/T02; P08-T04 | Duplicate storm preserves one logical edge; provenance-off has no hidden alternate edge table/index/work |
| A03 imported claims and source lineage | P03-T08–T10; P05-T06 | P02-T03/T04; P06-T01; P08-T04 | Out-of-order sidecars, orphan references and disabled families; unsigned claim never becomes local witness |
| A04 endpoint/observer/scope/method dictionaries | P01-T02; P02-T01/T03; P04-T01 | P02-T04/T07/T08; P08-T04/T05 | Independent export reader resolves every ordinal without running PG; restore cannot reuse IDs |
| A05 actual measurements, availability pairs, issuer metadata and uncertain interrupted execution | P04-T02/T06–T10/T13/T14 | P02-T02–T06; P05-T06/T08/T12; P06-T01; P09-T03 | External socket observer distinguishes actual failed probe from unsent intent; no false downtime vote |
| A06 original NIP-11/HTTP/DNS/TLS evidence and parsed views | P04-T07–T09 | P02-T03/T04/T07; P05-T06; P08-T04/T07 | Repeated unchanged body, 304 without body, private namespace and offline reference closure |
| A07 scoped traversal/coverage and unresolved gaps | P03-T01/T05–T07; P04-T01/T11 | P02-T01/T04; P05-T07/T08; P06-T01; P09-T03/T05 | Capped same-second interval and advertised-but-unserved ID stay incomplete after restart |
| A08 current winners, exact facts, graph and score generations | P05-T01–T12 | P02-T05/T07; P06-T01–T06; P07-T05; P08-T04 | Empty new list suppresses old list; delta agrees with full oracle; no private-input influence in public aggregate |
| A09 public scientific signed output and compact lineage | P07-T01–T08 | P02 archive; P07 outbox; P08-T05/T07; P09-T10 | Same bytes after lost ACK; all target obligations preserved; old full graph pins can retire |
| Operational jobs/config/audit/receipts, private DVM payloads | P01-T04–T07; P06-T09–T11; P08-T01–T03 | PG/protected secret refs; P08-T05–T08; private P09-T07–T10 | Expiring job details does not forget an invoice, receipt, cancellation or ownership obligation |

The authoritative field/type/null/origin/retention definitions are
[A01–A09](../specification/data-contracts.md). Measurement attempts, provenance
edges and bodies have different cardinalities and retention; they are not one
generic JSON table. The [import contract](../specification/import-formats.md)
documents each actual format's representable values and ambiguity.

## Every responsibility and consumer

| Product responsibility | Owning task(s) | Consumer and practical completion |
|---|---|---|
| Seeder, bootstrap and operator enrollment | P04-T01/T03 | P04-T06; P01-T09/P08-T09/P10-T03 empty-volume first event |
| Discovery from events, lists, directories and announcements | P04-T04/T05 | Registry/Validator; bounded tag-offset recovery, no omitted late backfill |
| Guarded transport, DNS/TLS/proxies and egress | P04-T02 | P03 collectors, P04 probes, P07 transport; independent destination observer |
| Validator and scoped access eligibility | P04-T06 | Monitor/Synchronizer; actual protocol success before automatic public enrollment |
| Monitor, document capture, geo and optional active probes | P04-T07–T10/T13/T14 | P05-T06/T08/T12; P07-T07; P09-T03; nonmeasurement distinguished from failure |
| Synchronizer live, linear history and NIP-77/NIP-67 paths | P03-T01–T07 | Durable ingest/archive; actual controlled peer, same-second caps, late arrivals and recovery |
| Importer | P03-T08–T10; P08-T04/T06 | Same verifier/admission; actual pinned format fixture and resume behind durable receipt |
| Durable ingest, archive publishing and online indexing | P02-T01–T06 | Every producer/read target; real engine crash/replay/merge and same-cut evidence |
| Deriver and scientific facts | P05-T01–T09/T12 | Read resources/NIP-85; independent full-recompute oracle per target |
| Ranker, named viewpoints and bounded graph artifacts | P05-T10/T11 | P06 scores/P07-T05/P09-T05; actual CSR/skew/resource and convergence evidence |
| Shared read core and counts | P06-T01–T05 | HTTP/relay/DVM/MCP/ContextVM; independent expected membership, exact units and real cursor continuation |
| Optional content search | P06-T06 | API/NIP-50/dashboard; old high-relevance result beyond recent prefix, Unicode/phrase oracle |
| ArchiveRelay and serving-side Negentropy | P06-T07/T08 | Real independent WS/NEG client then full-body retrieval; provenance-independent set |
| Optional MCP / experimental ContextVM | P06-T13/T14; shared T01/T12 | Same read-core resource/mode/grants; actual independent clients, cancellation/request-ID mapping, encryption and bounded oversized refusal |
| Optional query DVM, subscriptions and payments | P06-T09–T11; P07-T01–T04 | Explicit application client/regtest; same request/result identity and unresolved payment ledger |
| NIP-66/NIP-85 publishers and richer compatible proposals | P07-T05–T08 | Actual source-to-wire fixture, local/remote signer, legacy/new context decoders |
| Exporter, standalone analytical projections and explicit artifact publication/withdrawal | P08-T04 | P09-T06/P10-T05; DuckDB reads exported files with all application/catalog services stopped |
| Management, CLI, jobs and access | P08-T01–T03 | P09-T07–T10; real authorization, idempotent lost reply and saved-versus-applied revision |
| Backup/restore, migration, maintenance and diagnostics | P08-T05–T08 | Clean isolated target, old-owner fencing, corruption oracle, actual delivered alerts |
| Packaging, independent roles and release | P08-T09–T12 | Extracted artifact outside checkout, real architecture/engine set, no hidden cached assets |
| Public dashboard and private administration | P09-T01–T11 | Complete actual browser/keyboard/assistive journeys with separately named environments |
| Complete website, reference, blog, releases and adoption | P10-T01–T10 | Built route inventory, executed displayed commands, legacy mapping and artifact verification |

## Feature reconsiderations rather than inherited obligations

| Historical idea or new suggestion | Selected disposition | Reason / work |
|---|---|---|
| Lil/Index profiles omit signed body/tags/signature | Remove storage-loss semantics; one full-event product, optional provenance/filter recipes; analytical projections only in explicitly lossy exports | P01-T04, P08-T01/T04/T06, P10-T02/T08; transformed records cannot masquerade as originals |
| Permanent ingest count, every duplicate delivery, passive last arrival | Exclude from permanent archive; bounded operational telemetry | P02-T02, P03-T11/T12, P08-T07/T08; repeated traffic is not new ecosystem evidence |
| Retain only measurement rollups after one year | Permanent compact actual measurement evidence by default; explicit optional lossy retention | P04-T07–T11, P08-T07; future researchers may ask questions absent from today's aggregates |
| Erase original events on NIP-09, expiry or low rank | Retain accepted originals; ordinary serving follows defined current/deletion/expiry policy; exceptional operator removal is separate | P05-T02, P06-T02/T07, P08-T07 |
| Every relay can be synchronized completely from EOSE or NEG inventory | Qualified scoped coverage and explicit gaps; fetch verification remains required | P03-T05/T06; NIP-77 comparison is not an atomic remote snapshot |
| Every analytic family shares one run and graph dependency | Independent target publication; relay operational quality works without social ranking | P05-T01/T12; failure in one target preserves other complete targets |
| DuckDB or Parquet replaces durable ingress | Synced pending receipt first, transactional immutable archive next; DuckDB for archive/offline work | P02-T02/T04; a portable file format alone is not an admission/recovery protocol |
| OpenSearch is mandatory for content search | CH pretokenized index baseline, exact shared analyzer, optional alternatives measured if they solve demonstrated deficits | P02-T09/P06-T06; no duplicate search cluster by default |
| Rust alone guarantees many more simultaneous relays | Use Rust's ownership/runtime advantages, measure real per-connection buffers/native/TLS and mixed workload | P01-T05/P03-T12/P04-T12/P08-T08; earlier isolated comparison did not prove a fleet capacity |
| Kafka required for multiple machines | Remote owned admission RPC and explicit sharding/fencing first; replicated broker only if deployment requirements justify its operational/replication cost | P01-T07/P02-T08/P08-T10; local pending data is not host-loss replication |
| Universal NIP-90 database queries | Optional explicitly versioned experimental profile; selected exact wire and invoice-only payment contract | P06-T09–T11 and [DVM profile](../specification/dvm-profile.md); no invented standard support |
| Admin UI, useful presets, saved views, explainable scores and citation tools | Add complete backend and frontend flows | P08-T01–T03, P09-T05–T10, P10 tutorials; features have actual authorities and consumers |

## Cross-cutting stress and failure closure

| Concern | Owning work and independent observation |
|---|---|
| 5k/10k/50k/100k endpoints | P03-T07/T12, P04-T12, P08-T08/T12: connection/task/FD/TLS/native bytes, actual serviced intervals, reconnect storms, per-route fairness and deadline misses |
| Billions of bodies and much larger provenance | P02-T02/T05/T09: exact inventory size, two narrow edge orders, compaction/write amplification, cold/warm query cost, retention reserve |
| Constant scientific measurements | P04-T07–T11, P02-T03/T09, P08-T07: actual attempt rate, compact bytes/attempt and source-document version rate; no queue/log amplification |
| CPU validation under duplicate storms | P01-T03/T05, P03-T02/T12: reject forged known-ID input and measure hash/signature/cache work without admitting false witnesses |
| Incremental analytics versus full graph work | P05-T01–T13: affected-key recomputation, retractions/current-state changes, exact oracles, E/V/iteration costs, hubs and external sort/spill |
| Expensive search/count/reconciliation/read fanout | P06-T01–T08/T12: exact scan/candidate/output bound, reusable set/index generations and cancellation reaching actual engine workers |
| Disk/native memory and maintenance overlap | P02-T07–T09, P08-T04–T08/T12: pending+archive+online+indexes+temp+backup overlap, held native permits and finite outage headroom |
| Crash/ENOSPC/ambiguous ACK/restart/restore | P02-T02/T04–T08; P03-T12; P07-T09; P08-T05/T06/T12: external receipt/corpus/signature/target-state oracle |
| Security and privacy through all adapters | P01-T06/T07; P04-T02; P06-T01/T12; P08-T02/T03; P09-T07–T11: policy expiry, restricted-input influence and public-network refusal |
| Distribution/docs that actually work | P08-T09–T12, P09-T11, P10-T03/T05/T10: exact bundle digest, independently executed commands, real UI/API and known missing environments |

The scale matrix is a workload programme, not a statement that one 32-core host
can meet every combination. R, D, U, P, M, C, T, V/E and Q impose independent
costs. Reading/writing N retained facts costs at least their bytes; synchronization
cannot remove the need to interrogate each selected endpoint. Bounds and efficient
algorithms remove avoidable work, not those information and resource requirements.

## Dependency and delivery interpretation

P01-T04 static configuration steps 1–5 feed P01-T05/T06 and P02-T01; its durable
managed/declarative step 6 finishes after P02-T01's PG schema/CAS. This explicit
slice dependency prevents configuration persistence from depending on its own
future control schema through whole-task prerequisites.

P01-T06 authority-core steps and P01-T07 interface-core steps precede P02;
their actual owner/receipt acceptance runs after P02-T01/T02. This is an explicit
step dependency, not permission to mark their full tasks complete early.
P01-T09 is an integration consumer, not a prerequisite of its storage/collector/
read inputs. P04-T02 guarded networking and P07-T01–T04 signer/outbox primitives
are early cross-phase tasks. P06's initial configured read authority does not
wait for P08's browser-managed token lifecycle. P08 supplies that administration
before P09 consumes it. P08-T09 extends P01's initial bundle assembler; it does
not create a second packaging implementation.

M1 demonstrates first durable event/read/restart; its standalone archival export
uses P02's archive reader and the minimal P08-T04 export slice before that
milestone closes. It is acceptable to implement a narrow real slice early and
finish its whole-phase acceptance later, provided the release manifest advertises
only the completed capabilities. A task's input may be a frozen independently
verified fixture for early integration, but whole-product closure must exercise
the real producer. No dependency is satisfied by a stub reporting success.

Mechanical task/reference checks complement this coverage review. They cannot
prove that a paragraph specifies the right behavior; the five substantive
[review rounds](../reviews/09-expanded-plan-rounds.md) describe the actual semantic
and practical corrections made while expanding it.

The [complete-document audit](../reviews/10-whole-repository-audit.md) repairs
availability/issuer producers (P04-T13/T14), research-import versus restore scope,
artifact publication/jobs discovery (P08), producer receipt retirement (P02),
relay NEG output-size eligibility and DVM capability/control gates (P06/P07).
The exact-overlap and common-term search usefulness claims remain assigned
empirical decisions, with the expected difficult workloads written in P05/P06.
