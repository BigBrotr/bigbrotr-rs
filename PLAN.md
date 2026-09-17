# BigBrotr implementation plan

This repository designs a clean Rust implementation of **BigBrotr**. It contains
research, decisions, service contracts, experiments and the implementation order.
It does not yet contain the product. The repository is private and temporarily
named `bigbrotr-rs`; transition to `bigbrotr` is a separate release operation.

The design is one product: full signed-event archive, optional provenance,
configurable acquisition, useful analysis and independently selectable interfaces.
Use one application image/binary with composable process roles. Friendly recipe
names do not encode hidden storage behavior. All services, dashboard, management,
website and documentation carry equal quality obligations.

The [detailed phase/task/step plan](docs/implementation/README.md) is the execution
work breakdown. Start with its [field contracts](docs/specification/data-contracts.md),
[component contracts](docs/specification/component-contracts.md) and
[coverage ledger](docs/implementation/coverage.md). The phase table below is only
an overview; it is not a substitute for task prerequisites and practical evidence.

The current [specification-readiness assessment](docs/planning/specification-readiness.md)
lists remaining paper closure in P00-T06–T10. It separates semantic/format gaps
from justified empirical decisions and private implementation details. The
selected architecture below is not a claim that those remaining contracts are
already closed or that product implementation has begun.

A subsequent [architecture challenge](docs/reviews/13-architecture-alternatives.md)
recommends reconsidering acquisition work, duplicate certification, read modes,
ranking, process placement and selected storage/product alternatives. It is
advisory for discussion; the selected contracts below remain unchanged until
an adopted decision is reconciled across their owning documents.

## Read the design in this order

New to the project: first read [Start here](docs/start-here.md), the
[storage catalog](docs/specification/storage-catalog.md),
[interface catalog](docs/specification/interface-catalog.md) and
[shared naming](docs/specification/naming.md). These explain the terms used below.

1. [Requirements and historical phases](docs/product/requirements-and-history.md)
   and [product/data/feature decisions](docs/product/data-and-feature-decisions.md)
2. [Complete system and alternatives](docs/architecture/system-design.md)
3. [Data, durability and indexing](docs/architecture/data-and-durability.md)
4. [Storage service contracts](docs/architecture/storage-services.md)
5. [Acquisition services](docs/architecture/acquisition-services.md)
6. [Derivation, reads and publication](docs/architecture/derived-read-publication.md)
   and [detailed analytical definitions and costs](docs/architecture/analytics-detail.md)
7. [Content search and server reconciliation](docs/architecture/search-and-reconciliation.md)
   and [Protocol evolution](docs/architecture/protocol-evolution.md)
8. [Dashboard and website](docs/product/dashboard-and-website.md), plus
   [private administration](docs/product/private-administration.md)
9. [Configuration/runtime](docs/engineering/configuration-and-runtime.md),
   [Rust/repository design](docs/engineering/rust-and-repository.md),
   [capacity](docs/engineering/capacity-and-benchmarks.md) and
   [security/recovery/validation](docs/engineering/security-recovery-and-validation.md)

The [questions ledger](docs/planning/questions.md) distinguishes owner context,
reasoned design selections and defaults for unanswered questions. The
[revision log](docs/planning/process.md) records six substantive reconsiderations
and a further [full-source and feature recheck](docs/reviews/07-vision-and-final-recheck.md).
A subsequent [selective full-file Go code walkthrough](docs/reviews/08-go-code-walkthrough.md)
records actual call chains, useful patterns and the resulting contract refinements.
The [five-round expansion review](docs/reviews/09-expanded-plan-rounds.md) records
subsequent planning gaps, concrete corrections and an actual Parquet counterexample.
The [experiment summary](docs/research/experiment-summary.md) states what was
actually run and what remains unverified.

## Selected architecture and why

Rust owns core networking, admission, services and typed contracts. PostgreSQL
owns transactional control and DuckLake catalog state. RocksDB owns synchronized
local admission, exact event/edge identity and pending replay work. DuckLake-managed
Parquet owns the compressed historical archive. ClickHouse owns the complete
online serving/index/analysis representation; DuckDB powers archive jobs and
offline exports. Dashboard/private management and the website have separate static
builds in the same monorepo.

This does duplicate some bodies between archive and online serving. That is an
explicit space/latency tradeoff. Archive-only and tiered-serving recipes can reduce
online storage with declared capability or latency differences. Tantivy/OpenSearch,
LMDB-based relay serving and a replicated broker are alternatives for specifically
different search, serving or availability workloads; they are not mandatory extra
services. Go remains technically capable; the measured language differences do
not justify a claim that Rust alone enables 5,000 relays.

## Rules the implementation must preserve

- A durable receipt never precedes its synchronized data. Archive, serving and
  coverage frontiers are separate and expose their actual scope.
- Every retained event preserves all signed fields and the original signature.
  Projected analytical exports are explicitly incomplete, non-verifiable records.
- Provenance off performs no historical edge persistence/index/derivation. With it
  on, exact compact admission suppresses ordinary repeated edges; observations,
  claims and current availability are distinct evidence.
- NIP-09 requests and targets remain in archival history. Ordinary relay serving
  follows logical deletion/current-state policy; historical resources are explicit.
- Ranking consumes pinned, reproducible inputs and never deletes low-score events.
  Incremental algorithms state their affected set and when global work is needed.
- Raw serving facts retain immutable identities across retries and compaction;
  read snapshots cannot lose older facts due to logical-edge replacement.
- Services use durable contracts, bounded resources, explicit visibility and
  independent lifecycle; a downstream outage cannot be buffered forever.
- SDKs, engine ACKs, leases, protocol hints and passing tests are interpreted only
  as strongly as their actual guarantees support.

## Implementation sequence

The numbering preserves the historical phase map for traceability. Thin vertical
validation starts early; later phases complete the full responsibility rather than
postpone all operational or user-interface thinking until the end. Do not start
product work until the owner begins the implementation stage.

| Phase | Concrete work and artifacts | Practical completion evidence |
|---|---|---|
| 1 — foundation and risk contracts | Rust workspace/application; typed domain/config; lifecycle/budget core; guarded protocol adapter; pinned dependencies; minimal CLI/health; executable source/receipt/filter contracts | Shipped CLI rejects invalid config, bounded loopback relay interaction, cancellation and memory accounting; reproduce/fix admission-cache sequence independently |
| 2 — durable storage and replay | PG typed schemas/migrations; Rocks event/edge admission and ordinal dictionaries; pending log; archive publisher; CH immutable-fact prototype; backup/restore skeleton | Actual Rust binding crash/ENOSPC probes; PG DuckLake receipt/owner tests; pending→archive→replay preserves IDs, bytes and counts; forced CH merge/retry experiment |
| 3 — acquisition | Multiplexed sessions; live/backfill/reverification; NIP-77 and linear fallback; coverage/gaps; imported full events; fair bounded scheduling | Real controlled relays over sockets, caps/same-second storms/auth/late data, disconnect/crash recovery, import idempotency; scoped evidence without false completeness |
| 4 — relay lifecycle | Seeder, all discovery sources, guarded validator, monitor families, registry and evidence lifecycle, optional active probes | Fresh-instance enrollment through CLI/Compose; broken DNS/TLS/overlays and rate limits; all eligible endpoints scheduled fairly; imported claims never become local witnesses |
| 5 — facts and ranking | Current-state reducers, fact/assertion registry, incremental updates, graph artifacts, named viewpoints, relay similarity candidates and exact verification | Delta outputs equal slow full-recompute oracle on changing fixtures; old complete runs survive failures; skewed graph budgets and score reproducibility; no all-pairs relay explosion |
| 6 — read surfaces | Shared typed read core; event/tag/provenance/search/count resources; HTTP API; archive-relay historical + live behavior and server NIP-77; optional DVM adapter | Cross-surface semantic equality, deletion/replacement/tie cases, exact vs estimated counts, stable pagination, slow clients, authorization and stale capability responses; actual NIP-50 and NIP-77 client interoperability |
| 7 — publication and protocol profiles | Signer abstraction, durable signed outbox, per-destination receipts, NIP-66/NIP-85 compatible adapters, optional proposal profiles | Wire validation with reference clients; crash after send/ACK; same-byte retries; algorithm/viewpoint keys; fresh/partial measurement semantics; no implicit publication |
| 8 — operations and distribution | Full CLI, single image/role recipes, versioned config management, import/export formats, scrub/backup/restore/migrate, alerts, diagnostics and sizing | Clean release-bundle installation; real restore during concurrent activity; old-format migration; full/projected standalone DuckDB exports; resource/failure workload matrix |
| 9 — dashboard and private administration | Public data explorer, provenance/relay/social/rank views, private service/config/job/storage/key/access management, shared contracts and accessibility | Browser journeys against real packaged backend, keyboard/mobile, malicious data, stale/empty/disabled states, permission boundaries and concurrent config edits |
| 10 — complete website and adoption | Product pages, architecture explanations, versioned reference/tutorials, blog, release/migration pages, contribution/security docs and examples | Clean-machine tutorial execution, built static site/link checks, exact-version examples, accessibility and download/onboarding journey; legacy redirects and repository transition rehearsal |

Phase 2's serving design experiment precedes committing to final production DDL;
phase 6 completes public adapters. Phase 3's importer supports validation early;
phase 8 completes all operator workflows. Private management command contracts
start with configuration/runtime, then phase 9 completes their browser surface.
The website architecture/content inventory exists now; phase 10 completes and
launches it. This ordering is about dependencies, not relative importance.

## Distributed deployment track

Design remote ingest/read/management contracts in phase 1. Validate split-role
containers during phases 3/6/8. Release single-host and split-role operation only
with the documented owner and shared-storage constraints. Before automatic owner
failover or replicated durability, implement commit-time fencing/replicated-log
semantics and demonstrate old-owner resumption, partitions and recovery. Keep
manual positively fenced failover as an explicit supported operation where
appropriate. Do not advertise zero data loss under host failure from a local WAL.

Admission sharding follows stable event ownership and canonical identity mapping;
provenance facts route with the event. Read frontiers become vectors. Graph/global
queries require cross-shard aggregation and measured data movement. Shared PG is
not a data-plane bus and Docker is not a distributed storage protocol.

## Decisions deliberately requiring implementation evidence

| Decision point | Selected starting design | Evidence that can overturn it |
|---|---|---|
| CH physical sort/projections | Chronological candidate versus ID-ordered baseline | Realistic signed corpus, many parts, concurrent queries/replay/merges and correctness oracle |
| Exact edge inventory layout | Compact ordinals + interned stable identity dimensions | Total P-index/dictionary size, write amplification and lookup rate; not RAM-only cache timings |
| Archive file/row-group sizes | Bounded target sizes, serial publication/parallel preparation | Freshness, compressed bytes, compaction debt, memory and object-store requests |
| Search adapter | CH content token/phrase search with explicit relevance and coverage | Signed multilingual correctness/relevance corpus under mixed load; compare Tantivy/OpenSearch when stronger semantics justify the cost |
| 5k/10k/100k operating envelope | No unmeasured capacity promise | Full workload programme with real signed ingestion and dated real-relay behavior |
| Native vs process DuckDB integration | Isolated archive/job worker using pinned engine/extensions | Packaging, cancellation, RSS/native memory and recovery under actual Rust integration |
| Automatic multi-host failover | Not inferred from leases; explicit fencing contract | Stale writer cannot commit after takeover; loss model and quorum tested |

These are bounded engineering validation tasks with alternatives, not unresolved
product purpose. A plan cannot truthfully make all future performance results
certain. If evidence overturns a selection, update the decision and dependent
contracts before spreading the implementation.

## Delivery and review discipline

Implement coherent vertical chunks, review code/comments/docs together and commit
reviewable outcomes. Run tests proportionate to the risk and execute the actual
product entrypoint. Record failures and limitations as well as passing results.
Avoid tests that only encode implementation accidents or arbitrary process quotas.
No fixed comment length, required coverage percentage or repeated clean-review
count substitutes for correctness and readability.

A release is ready when its documented features work through the delivered
interfaces, recover under the declared failure model, operate within the measured
envelope, and can be installed/upgraded/restored using accurate documentation.
Success also requires useful public datasets/examples, understandable explanations,
responsive maintenance and an accessible interface; GitHub stars are an outcome,
not an engineering proof.

## Complete-document audit and current confidence

The latest [whole-repository review](docs/reviews/11-whole-repository-audit.md)
fully reread its 70-file Markdown baseline, then corrected measurement subjects,
availability confirmation, temporal reads, concrete assertion/ranking definitions,
state/migration ownership and usable management/download routes. The current detailed plan
has 121 tasks/746 steps, including P04-T13 availability observations and P04-T14
issuer metadata. Research import no longer invents a second analytical dataset
product or foreign administrative source-control transfer. See
[decision confidence and alternatives](docs/planning/decision-confidence.md)
for the selected stack's total costs and remaining empirical qualifications.
Complete documentation review is not validation of universal optimality.

The later [focused readiness review](docs/reviews/12-specification-readiness.md)
adds five planning closure tasks. Most existing architecture decisions remain
selected; same-event visibility transitions, portable encoding and cross-contract
composition need further paper work. Empirical choices have explicit decision
points rather than an unbounded instruction to benchmark everything later.
