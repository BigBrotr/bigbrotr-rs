# Decision confidence, alternatives and total cost

Status: selected design after the complete-document audit; no product runtime
has been built. A complete reread is evidence of review scope, not proof that
every decision is correct or optimal. This register separates a reasoned choice,
a narrow experiment and an unperformed qualification. It supplements the
[system design](../architecture/system-design.md), not a competing architecture.

## What the design optimizes

BigBrotr combines a verifiable historical archive, relay observatory, research
analysis and public/private read interfaces. A retained original must survive
reindexing; optional provenance cannot secretly become mandatory; an observation
must state its method and uncertainty. Collection, reads, maintenance and recovery
share finite resources. These are the comparison constraints.

There is no single winner for minimum disk, maximum ingestion, fastest arbitrary
reads, strongest host-loss availability and simplest operation simultaneously.
For example, a second online body representation spends space to avoid repeated
archive lookup; replicas spend more space/network to survive loss. Compare complete
workloads with the same fidelity, failure model, query semantics and host envelope.
Do not invent weighted scores or benchmark winners without measurements.

## Current decisions and what could overturn them

| Decision | Why selected for this product | Cost / strongest relevant alternative | Evidence and confidence |
|---|---|---|---|
| Rust core, TypeScript static interfaces | Owned resources, explicit concurrency boundaries and reusable pure libraries fit the clean redesign | Native/FFI build and cancellation complexity. Go is a credible complete alternative; rewriting solely for socket RAM would be unjustified | Reasoned choice; tuned 5k TLS measurements were close and default buffers reversed the apparent winner. No end-to-end language superiority established |
| Full signed originals, optional provenance | Enables independent verification, rebuild and scientific history without combinatorial lossy storage modes | Original bytes remain a real U-sized cost. Projected exports reduce transfer/analysis cost but cannot replace originals | Strong semantic justification; tiny format probes preserve fields, but full cryptographic/product roundtrip remains P01/P02/P08 |
| PG control and separately owned DuckLake catalog | Transactions support jobs, permissions, ownership and multiple local/remote roles; reuses one deployed server | Operational dependency and central transaction/catalog load. SQLite/local DuckDB would simplify an intentionally single-client product | Reasoned default for selected multi-role product; PG/DuckLake atomicity probe is narrow, not a fleet-scale control benchmark |
| RocksDB exact identity and pending admission | One synchronized batch can bind identity, facts and receipt; persistent exact provenance avoids duplicate-sweep archive growth | U/P/M inventory, write amplification, compaction and Rust/C++ memory. LMDB/other exact stores deserve comparison if they materially improve the actual workload | Python binding process-crash evidence only; actual Rust binding, aged workload and producer retirement/backup closure still require P02 |
| DuckLake-managed Parquet archive | Transactional snapshots, reference-aware maintenance and portable analytical exports without authoring a new permanent file protocol | Native worker, catalog/files coupling, small files, snapshots/compaction. CH as authoritative store plus backups/Parquet exports would simplify an API-first product; Iceberg deserves evaluation for a different multi-engine deployment | Selected conditionally. Existing PG probe demonstrates multitable transaction behavior and failed uniqueness assumption; power-loss path, catalog scale and full restore are unqualified |
| CH full online bodies and narrow query paths | Many concurrent structured reads/analytical jobs justify a dedicated serving representation | Second body representation, tags/tokens, both provenance orders, parts and merge/rebuild reserve. Archive-only DuckDB is cheaper to operate for occasional offline analysis; LMDB serving is relevant to a relay-first workload | Small synthetic comparisons only. Final sort/access path, cold lookup, consistency-cost and mixed workload remain P02/P06; no billion-row proof |
| CH first content-search adapter | Reuses the serving engine and one specified analyzer; avoids another mandatory cluster | Common-token candidate work and positional/token storage can make correct queries unusable. Tantivy or OpenSearch can be better if measured retrieval/relevance needs justify another index and recovery path | Low empirical confidence at target scale. P06 must report common-query refusal rates and equivalent-result comparison; no claim CH is the best search engine |
| Exact provenance and independent analytical targets | Scientifically interpretable edges, current-state corrections and failures isolated by target | P-sized history, exact quantile state, V/E graph passes and snapshot pins. Approximation helps named derived questions, not canonical identity | Semantic contract justified; compact layouts and global graph/cross-relay costs remain measured choices |
| Bounded exact overlap, optional approximate candidate mode | Exact small/scoped comparison has clear meaning; explicit partial/refused work avoids invented cluster claims | Equal-size candidates and complete qualifying-edge output can be quadratic. Group identical sets, improve sound pruning or select explicitly incomplete candidates where valuable | Exact useful fleet envelope not proved. P05 must test mirrors, hubs, window churn and bridge deletion; correct repeated refusal is not useful completion |
| One image, composable roles, few candidate libraries | Installation and shared contracts stay coherent without a service mesh or crate per worker | Role placement still requires dependencies, budgets and writer ownership; remote calls add copies/latency | Reasoned packaging decision; clean-bundle/split-role/browser proof remains P08/P09 |
| Separate public dashboard, private administration and static website | Distinct authority and audiences, with shared contracts and no database credentials in a browser | Multiple builds/versioned assets and frontend maintenance. A single origin can be simpler but does not remove access-control obligations | Justified boundary, unqualified UX/performance until actual browser/tutorial journeys |
| Optional paid DVM capability | A concrete way to fund finite query service, preserving the requested extensible read surface | Financial state, LND, ambiguity/refunds and release maintenance are substantial even when default-off | Fully specified optional scope, no paid-service evidence. Separate free/core and paid release gates; actual regtest mandatory only for advertised paid support |
| No broker/automatic failover in baseline | Owned admission RPC and manual positive fencing meet the selected initial topology without another distributed log | Local disk is not host-loss replication; manual recovery limits availability | Deliberate availability boundary. Replicated log/broker is justified by a specified loss/availability requirement and measured multi-host workload, not merely by Docker deployment |

The official [DuckLake catalog guidance](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
distinguishes local single-client and remote multi-client uses. Our PG selection
is a product inference from those needs, not a claim PG wins every installation.
[Iceberg's reliability design](https://iceberg.apache.org/docs/1.10.2/reliability/)
is a serious alternative; its atomic table snapshot model does not by itself
demonstrate BigBrotr's complete cross-family publication/recovery contract.
[CH object-storage integration](https://clickhouse.com/integrations/amazon_s3)
also means a Parquet archive does not uniquely require DuckDB as a reader.

## Changes that simplify or correct the selected product

* Analytical projections are standalone export/analysis artifacts. There is no
  second unspecified in-product dataset import/catalog/query system.
* Research import does not restore foreign administrative control state or
  pre-scan ordinary JSONL for an undefined source-tombstone schema.
* Local monitor projection and cross-observer claim resolution have distinct
  outputs and reuse matching pure rules, avoiding duplicate authorities.
* Paid DVM verification is its own capability gate; its dependencies do not
  become mandatory for a free deployment. Subscription controls have a bounded
  admission allowance that remains usable under data-query saturation.
* Parquet sizes are engine targets, not hard memory limits. Inlining is explicitly
  disabled in the starting archive connection; low-volume file costs are measured.
* Producer receipt reclamation has a bounded acknowledgement/epoch protocol,
  including incremental progress, admission closure and causal backup requirements.

These changes earn their cost by closing an actual producer, authority or
recovery gap. They do not add a generic plugin framework, arbitrary analyst SQL
server, extra search cluster or distributed broker by default.

## What must be measured before calling the setup suitable

P02-T09 compares whole storage representations and recovery under the same input.
P03-T12/P04-T12 measure actual serviced endpoint intervals under live/backfill/
monitor/import contention. P05-T13 includes graph construction and change handling;
P06-T12 includes common text queries, both provenance directions and active
reconciliation. P08-T12 combines all of them with compaction, backup and faults.
Pins, temporary output, kernel/native memory and offline export copies count too.

Report accepted/refused workload, queue age, freshness, correctness and recovery
alongside throughput and p95/p99. Publish which combinations actually fit Tux;
100k idle sockets cannot certify 100k busy relay synchronizations. Unreachable
or noncooperating relays limit observation independently of local CPU. Results
can overturn a selected engine, index or default before the release advertises it.

There is no remaining permission request hidden in this register. Product
implementation still waits for the owner's separate start instruction. The
current honest conclusion is a reviewed, improved design with explicit empirical
obligations, not a proven global optimum or a finished production system.
