# System design

Status: implementation baseline after six design revisions; product implementation
has not begun. The service
contracts complement this document. A proposal is not a measured capacity claim.
The selected [operational composition](../specification/operations-contract.md)
resolves process placement, recipe transitions, recovery order and resource
reservations; it does not certify the future runtime.

## The selected direction

Use Rust for BigBrotr's core, PostgreSQL for transactional coordination and the
DuckLake catalog, RocksDB for durable admission and replayable pending work,
Parquet managed through DuckLake for the full archive, and ClickHouse for online
analytical and event reads. DuckDB is an embedded analysis/export/archive worker
engine. It is not an additional public database server. Start with one deployment
and independently selectable process roles; distribute roles when the measured
bottleneck or failure requirement justifies it.

Rust is chosen for explicit ownership, predictable resource control and a shared
systems implementation, not a claim that Go cannot hold 5,000 connections. The
prior controlled transport measurements actually found tuned implementations
close enough that buffer settings outweighed the language label. This choice
still requires a careful protocol adapter and idiomatic code; an SDK is not a
complete observatory.

```mermaid
flowchart LR
  R[Eligible Nostr relays] --> C[Session manager and acquisition]
  I[Importer] --> A[Durable ingest owner]
  C --> A
  M[Monitor observations] --> A
  A --> W[(RocksDB WAL and pending records)]
  W --> X[Online indexer]
  W --> P[Archive publisher]
  P --> L[(Parquet archive / DuckLake)]
  L --> X
  X --> H[(ClickHouse serving data)]
  H --> D[Deriver and rank workers]
  D --> H
  H --> Q[Semantic read core]
  Q --> API[HTTP API]
  Q --> AR[Archive relay]
  Q --> DV[DVM adapter]
  Q --> MC[Optional MCP adapter]
  Q --> CV[Experimental ContextVM adapter]
  API --> UI[Dashboard]
  L --> EX[DuckDB export and offline analysis]
  PG[(PostgreSQL control and catalog)] --- A
  PG --- P
  PG --- D
  D --> PU[NIP publishers and signing outbox]
```

The diagram omits ordinary monitoring/health connections. Control tables and the
DuckLake catalog use separate database roles and schemas. Applications do not
edit DuckLake's internal metadata directly.

## Four different frontiers

1. **Admitted:** validated event/observation and its replay record committed in
   an atomic RocksDB batch with WAL synchronization. The process may now return
   a local durable receipt. This survives process restart subject to the engine
   and storage contract; it is not a promise against loss of the host or disk.
2. **Archived:** data and an archive batch receipt committed together through
   DuckLake, with checksummed files and a catalog snapshot. This permits pending
   body reclamation only after the configured backup/replication requirements.
3. **Queryable:** all required serving structures for the published batch or
   generation are visible under the read contract. A partially indexed batch
   cannot be called complete.
4. **Observed coverage:** scoped evidence about a relay/filter/time interval,
   with traversal, gaps, remote claims and verification time. It references a
   durable frontier; it never derives completeness from the largest timestamp.

The online indexer may consume durable pending records immediately. It does not
wait for a large Parquet file to close. It can later replay the same logical
records from the archive. Stable record identities make this switch idempotent.
The archive publisher batches for efficient files independently of online read
latency. Each consumer has a named durable cursor; there is no shared tick that
one slow service can hold indefinitely.

## Service and process boundaries

A service is a responsibility with a lifecycle and owned state. A process is a
resource and failure boundary. A crate is a code boundary. They need not map one
to one. Provide the same binary with `serve` and explicit role selection, plus
single-job commands. Compose can run several roles in one container or separate
containers using the same image. The CLI prints the actual resolved topology.

Use direct typed calls within one process and a versioned authenticated internal
protocol across processes. The same receipt, cancellation and retry semantics
apply to both. Public API endpoints do not double as administrative or ingestion
RPC. Every remote ingest request has a stable request identity and bounded body.

Continuous session loops, finite jobs and listeners have different lifecycle
interfaces. Shared runtime facilities provide cancellation, supervision, budgets,
leases where needed, observability and health. They do not force a live listener
into a synthetic RunOnce polling model. A stopped ranker does not stop collection;
a stopped indexer makes reads stale and grows replay backlog; a stopped archive
publisher eventually fills pending storage and causes visible intake throttling.
No finite machine can buffer an unlimited outage.

### Responsibilities are not container names

| Component | Shape | Required dependency and outage behavior |
|---|---|---|
| URL/filter/event semantics, read core, graph algorithms | Library or internal modules | No independent daemon; callers own their lifetime |
| Seeder, import/export, scrub, backup, migration | Finite or scheduled jobs | Durable job state; stop/resume according to the job contract |
| Discoverer, validator, monitor, synchronizer | Continuous workers/session owners | Control state and guarded transport; admitted data flows through the ingest owner |
| Ingest owner | Exclusive stateful role per shard | Owns RocksDB admission; no durable receipt while its storage is unavailable |
| Archive publication and online indexing | Independent consumers | Each owns its cursor and projection; one can lag without inventing completion for the other |
| Deriver and Ranker | Incremental/scheduled compute jobs | Require their declared complete input generations, not every upstream daemon to be running |
| API, ArchiveRelay, optional MCP and private administration | Listeners with shared libraries | Read/control dependencies and policy leases; do not depend on an active collector |
| DVM, experimental ContextVM and NIP publishers | Network adapters plus durable jobs/outbox | Explicit signing identity, policy and destination receipts; gateway-specific protocol and authority contracts |
| PostgreSQL and ClickHouse | Database processes | Separate persistence, resource limits, lifecycle and recovery |
| DuckDB, RocksDB, Parquet | Embedded engine, embedded store and file format | Not three additional network database servers |

The reference deployment selects one acquisition/socket owner per host and route
budget domain, co-located with its ingest owner. Validator, Monitor, Synchronizer,
directory fetch and publication transport use independently scheduled queues in
that owner. A storage process owns the separate Index Rocks directory, CH projection
and fenced archive coordination; native preparation/export/graph jobs run in
terminable worker processes with whole-job grants. Read and private Control/admin
have explicit process domains; enabled signing has narrow credentials and submits
durable exact bytes to the socket owner. All use the same application image.
Compact mode may co-locate compatible roles and disclose the wider process-failure
scope. Packaging does not require a permanent container for an occasional export.

The [process/dependency matrix](../specification/operations-contract.md#process-placement-and-capabilities)
is the resolved reference. Pausing a projector or algorithm leaves its required
storage owner running; stopping that owner is a separate capability transition.
Use coarse process grants and local byte ownership, with protected archive/control
completion capacity. No per-delivery distributed resource allocator or mandatory
network proxy is part of the full local default.

Logical supervision handles ordinary service errors within a process. It cannot
isolate a process abort, native-library crash or whole-process OOM; workloads
requiring that isolation must use separate processes/cgroups. Across machines,
share durable protocols and accessible archive storage, never an uncoordinated
embedded database directory. Container networking, volumes, secrets and per-host
resource shares remain explicit deployment work, not consequences of a Rust crate.

## What stays optional

Provenance off from initialization means no historical event/relay edge persistence,
edge indexes, edge export or edge-derived facts. Turning it off later stops new
edge evidence and dependent algorithms; already retained edges remain under their
retention/access policy and explicit historical export scope. Re-enabling cannot
reconstruct missed witnesses from retained bodies. The registry and monitoring
measurements still exist. Full signed events are always retained under the archive
policy. Remove the legacy heavy_fields switch and LilBrotr/IndexBrotr profiles; projected exports
satisfy lightweight analysis without weakening canonical event fidelity.

ClickHouse is required by the complete online serving and derivation deployment.
An archive-only deployment may omit it and use DuckDB jobs with different latency
and concurrency expectations. This is a supported topology with explicit missing
online capabilities, not a second silently incompatible storage model. Ranker,
publishers, active probes, public API, DVM and archive-relay are independently
configurable; enabling signing or public exposure requires effective settings.

Content search is included in the complete online product and may be disabled
per deployment. Start with ClickHouse text indexing; evaluate Tantivy for richer
embedded search or OpenSearch for a separately operated search cluster when
query quality, features and measured total cost justify them. Search index
coverage, ranking, rebuild and disablement are explicit in the
[search and reconciliation contract](search-and-reconciliation.md). That contract
also adds NIP-77 serving to ArchiveRelay independently of provenance collection.
LMDB/nostrdb remains a credible alternative for a relay-first serving workload;
it is not an obligatory extra database beside every engine.

## Growth and failure domains

| Topology | Ownership and storage | What it solves |
|---|---|---|
| One host | One durable ingest owner initially; local NVMe; PG + CH; local Parquet plus off-host backup | Simple reference deployment and measured resource accounting |
| Multiple role hosts | Collectors route to owner; archive storage shared through object storage; read/rank workers isolated | Independent CPU, connection and query capacity |
| Sharded admission | Stable event-ID owner routing; durable shard epochs; per-shard frontiers | Write throughput once one owner is measured insufficient |
| Replicated admission | Replicated log with explicit quorum acknowledgements before durable receipt | Continued ingestion and defined RPO under host loss |
| Replicated serving | CH replication/sharding with documented consistency and routing | Query scale and serving availability |

The initial supported placements are one host and fixed split roles with manual
fenced handoff, one ingest shard and object archive access from every split reader.
Sharding, replicated admission/serving and automatic failover in the table are
growth directions requiring their own implemented/qualified capability. Shared
PG, Docker and matching local path names do not enable them.

A shared PostgreSQL connection does not make local RocksDB or a local file path
shared. Never mount the same RocksDB directory in multiple writers. A lease with
an expiry time is not sufficient fencing: a paused old owner may resume. For
single-host operation enforce exclusive owner process and storage locks. For
cross-host failover require positive old-owner exclusion or a replicated storage
service that rejects old epochs at commit. DuckLake's multiwriter support does
not provide application event-ID uniqueness; the isolated probe demonstrates it.

Kafka or a compatible replicated log becomes appropriate when replay fan-out,
write throughput or loss-of-host acknowledgements justify a managed distributed
log. A single Kafka broker does not provide replicated durability. Adding a
broker also does not remove admission verification, archive snapshots, indexes
or application idempotency. Avoid mandatory distributed infrastructure for a
single-host observatory.

## Recipe and capability matrix

| Recipe | Required data infrastructure | Enabled work | Available surfaces |
|---|---|---|---|
| Full local observatory | PG, RocksDB, DuckLake/Parquet, CH | Collection, monitoring, indexing, derivation; rank when input/viewpoint is usable | Local read/dashboard/admin; public/signing roles explicitly configured |
| Full archive without provenance (optional LilBrotr nickname) | Same full topology | Same work except edge persistence/index/derived facts | Event and eligible analysis resources; provenance explicitly unavailable |
| Social-filter example | Same selected topology, explicit social filters and provenance choice | Selected kinds plus documented dependencies of desired facts | Capabilities reflect actual historical coverage, not the recipe name |
| Archive-only | PG, RocksDB, DuckLake/Parquet | Collection/monitoring as selected, archival, import/export/offline DuckDB jobs | Private status/admin and archive exports; online CH read/derivation/rank roles disabled |
| Serving existing data | PG + CH, archive access for rebuild/cold tiers as configured | Read adapters; optional derive/rank | Existing complete generations with freshness/coverage metadata; no collector requirement |
| Custom/split roles | Required durable contracts reachable locally/remotely | Explicit selection | Preflight resolves dependencies; no silently started producer daemons |

A service enabled in a recipe must have its engine/data dependency. Stopping a
service leaves its already retained data readable when the read store remains
available. Archive-only is an intentional capability selection, not a claim that
all CH-backed resources can work without CH.
Feature toggles, missing-dependency behavior, retained generations and exact split
routes/credentials follow the operational matrix; the matrix distinguishes an
enabled role from an actually ready capability. Instance policy has one managed
or declarative authority, and recipes seed initial values rather than overriding
accepted policy on every restart.
