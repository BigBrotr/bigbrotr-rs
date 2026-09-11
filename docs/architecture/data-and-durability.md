# Data, identity, durability and indexing

## Permanent facts and rebuildable representations

| Data | Canonical retained representation | Serving or coordination representation |
|---|---|---|
| Signed Nostr event | Full signed fields and original signature in archive event rows | CH event bodies, narrow author/kind/time/tag access paths |
| Original wire object, if retained | Bounded raw bytes with digest and origin; explicit fidelity mode | Optional retrieval; never confuse with canonical signed serialization |
| Direct provenance | First verified local observation per event/endpoint with observer identity | Both event→relay and relay→event access paths |
| Imported provenance | Separate source assertion, source time and validation metadata | Separate origin-aware query domain; never upgraded to local observation |
| Delivery history | Optional per-delivery series with independent retention | Time-series aggregates, not the first-observation edge table |
| Relay observations | Measurement records, timestamps, method/version, outcome and raw-document digest | CH history; PG latest scheduling state |
| NIP-11 and other documents | Typed envelope plus bounded original bytes/content hash | Parsed versioned fields, never arbitrary JSON as a schema contract |
| Coverage and registry | Transactional PG control state with backup generation | API health/capability views and scheduler queues |
| Derived facts, graph scores | Versioned run output plus inputs/parameters; optionally archived for reproducibility | CH complete generations and graph artifacts |
| Publication intent and receipts | Durable signed payload/outbox and per-target outcome | PG control state; optional long-term archive of publication evidence |
| Policy, migrations and configuration history | PG state and portable backup metadata | Redacted effective configuration and audit events |

An event's ID binds the NIP-01 signed serialization. It does not bind whitespace,
JSON key order or arbitrary top-level extension fields. Keep all signed fields
unchanged and distinguish signed-event fidelity from byte-for-byte wire capture.
An analytical export omitting signed fields cannot be independently reverified,
even if it retains the original ID and signature. A full event export can.

Use binary IDs/pubkeys/signatures rather than hexadecimal strings internally.
Tags preserve native order, duplicate entries, arbitrary strings and nested array
shape allowed by the event contract. Tag indexes store the exact first value of
supported single-letter tags, with deduplication of identical index keys; the full
tag list remains in the body. Do not parse content as JSON unless a kind-specific
deriver asks for it. Signed timestamps remain integer values; partition placement
must not silently alter them or create unbounded adversarial partitions.

Detailed retention/index/exposure rationale and the monitoring document-reference
contract are in [product data decisions](../product/data-and-feature-decisions.md).

## Admission transaction

The owner routes an event to one durable partition, checks policy and full event
validity, and commits `event identity + canonical payload when new + pending
record + producer request receipt + sequence` in one synchronized RocksDB batch.
Same-owner concurrency is serialized for identity admission. Concurrent producers
cannot both perform an unchecked read-then-write into separate owners. A returned
receipt identifies shard, epoch and sequence, not just an in-memory boolean.

Verification caches are bounded accelerators. A received `id` field is untrusted.
Cache hits must bind the exact verified signed payload and signature, or recompute
the canonical ID and use a signature-aware cache. A full ID lookup alone is not
proof that another received JSON object or signature is valid. Bloom filters may
avoid definite misses; a false positive must never drop a new event or provenance
fact. Sampled cryptographic verification is not the default admission policy.

Keep a permanent compact event identity inventory for exact admission and global
reconciliation. With provenance enabled, also keep an exact edge admission
inventory. Use compact event ordinals and interned endpoint/observer/semantic
authorization-scope IDs, with minimum observation time and original receipt.
Observer identity survives restart; auth scope identifies stable authorization
context, not each socket, session or challenge. Dictionary/ordinal mappings are
canonical recoverable metadata, not disposable IDs that can be reassigned on
restore.

A new edge or strictly earlier attributable observation atomically updates this
inventory and appends an immutable fact/correction record. Ordinary repeated
sweeps return existing receipts without creating another permanent edge fact.
This deliberately pays an extra P-sized embedded index and lookup work to avoid
turning first-observation provenance into D-sized delivery history. A bounded
cache sits before exact lookup, not instead of it. An instance created with provenance off allocates neither edge inventory nor
facts. Turning it off later stops new evidence but preserves previously retained
state and its coverage unless an explicit separate removal operation is requested. Read/serving indexes are separately rebuildable.

Reconciliation needs an ordered local-set read interface. It need not add yet
another permanent P-sized RocksDB index: use a projection and bounded committed
tail, or a justified ordered acceleration. A conservative stale set may cause
extra downloads; it cannot authorize false progress. Benchmark the exact edge
inventory with dictionary bytes, compaction/write amplification, backup and
rebuild costs. No storage engine removes the information cost of P.

### First observation without provenance

Every event has immutable `first_admitted_at` (the ingest owner's wall-clock time)
and its original admission receipt. Network acquisition additionally records
`first_received_at`: the earliest attributable receive time reported by an
approved collector in this instance, with observer/clock-quality metadata. It is
not the event's `created_at`, a remote server timestamp or an imported assertion.
An import-only event has no local network-receive time until locally observed.
Provenance off still preserves these event-level fields; it stores no relay edge
or endpoint identity in their place.

An earlier eligible local receive observation updates the compact event metadata
and emits an immutable correction with its own SourceRecordId in the same synced
batch. Ordinary later duplicates return existing receipts. Store the original
admission and correction facts in the archive, preserving the original signed
body. Read-time minimum is evaluated after source-cut filtering, just like edge
corrections, so a late earlier timestamp cannot rewrite an already pinned answer.
Clock regressions and uncertainty are diagnostic metadata, not evidence of
physical network propagation or causal order. Admission receipt order remains
separate from wall-clock order.

A dataset import preserves source metadata in its source namespace and records
local import/admission separately; it cannot backdate this instance's local
network observation. A deployment restore preserves original receipts and these
fields. Test replay, earlier/later duplicates, import followed by network receipt,
clock rollback and restore with provenance disabled; none may invent a new
first-admission time or create hidden edge state.

## Archive publication and reclamation

Parquet is a columnar file format, not an append log with per-event commit. Files
are created in bounded batches, compressed, checksummed and published through a
catalog snapshot. DuckLake manages table membership, snapshots, schema evolution
and compaction. DuckDB executes queries and maintenance. The chosen PostgreSQL
catalog supports independent clients without sharing a writable DuckDB file.
See [Parquet format](https://parquet.apache.org/docs/file-format/),
[DuckLake catalogs](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
and [transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions).

One logical archive publisher owns a catalog publication stream. It may prepare
files in parallel but serializes receipt check and transaction publication.
Events, observations/edges and the batch receipt enter one DuckLake transaction.
A retry after ambiguous acknowledgement first queries that receipt. Two active
publishers can both find no receipt and commit duplicates: this was reproduced
with PostgreSQL in [the probe](../research/evidence/ducklake-postgres.json).
Do not claim a unique constraint that DuckLake does not have, or assume an
unrelated PG advisory lock fences an old publisher at catalog commit.

Prefer table-local partitions based on controlled ingestion ranges and limited
buckets; cluster row groups for expected event/time/relay scans. Event-created
partitions remain a benchmark alternative, not a reason to mutate event time.
Start file-size and row-group targets from bounded experiments; avoid one file
per relay or event. Small batches for latency are merged later. An archive-only
reader sees committed snapshots, while the online serving path can be fresher.

Pending payloads are reclaimed only when archive publication and the configured
safety policy permit it. Identity metadata may remain. Old snapshots and files
are deleted only after active readers, exports, backups and required consumers
release their retention claims. Tombstones, snapshot expiry, physical object
removal and backup expiry are different operations. The selected Q02 interpretation retains NIP-09 requests and their targets in
the historical archive and applies deletion visibility in the read policy.
Operator-directed exceptional physical removal remains a separate explicit
operation with backup implications, never an automatic protocol side effect.

## Serving layout and exact answers

Keep full online bodies for the many-read default and count their disk cost as a
second representation. Narrow access paths contain IDs and filter/sort fields,
not another copy of content for every query family. Candidate layout: chronological
base with ID/author projections, tag postings and two provenance orderings. Compare
against ID-ordered base with narrow chronology/author indexes under realistic
partition and part counts. The prior tiny experiment favored a chronological
layout for tag→100-body retrieval but cannot settle billion-row behavior.

CH insertion retry tokens have bounded deduplication windows. Background replacing
merges do not guarantee duplicate-free reads or atomic publication across tables.
The selected baseline uses immutable source-record identities, deterministic
per-table/per-partition chunk plans, successful data acknowledgements and a
transactionally published contiguous frontier. Raw tables are append-only, or
replace only byte-identical retry copies sharing the full immutable record ID.
Provenance minimum reductions happen after frontier filtering; corrections never
replace an older raw fact. This preserves earlier read snapshots across merges.
Finite engine deduplication is an optimization, not the correctness boundary.

The production implementation must test synchronous single-partition/chunk
insertion, ambiguous acknowledgements, persisted chunk receipts and forced
merges. A PG receipt is not independent proof that CH survived a power loss: use
the selected CH persistence/replication guarantee or invalidate readiness and
revalidate/replay after unclean serving loss. Compacted logical views are newly
published generations, with old versions retained for pinned readers. These are
concrete integration acceptance obligations before production DDL is frozen.

The default read API returns the latest complete available generation with its
age, coverage and policy. Expensive exports pin a snapshot. Keyset pagination
binds filters, generation and stable tie-break order; a changing latest view must
not masquerade as a stable snapshot. Counts say exact or estimated and identify
scope. Unknown provenance is not zero relay presence. First observation order
is not physical propagation and a historical observation is not current availability.

Hot-retention tiers may omit older bodies from CH while keeping them in Parquet.
Cold retrieval then needs an archive lookup directory tied to immutable file and
snapshot identity, bounded fetches and an explicit slower response path. A bare
row offset surviving arbitrary compaction is not a valid locator. Do not promise
identical latency between full-online and tiered deployments.

## What an export or backup contains

Full data export: deduplicated signed events, optional origin-aware provenance,
measurement tables, schema/version/fidelity declaration, exact row counts and
checksums. Analytical export: selected columns and filters, explicitly lossy,
with the same manifest discipline. DuckDB can read a standalone export directly
without the live catalog. A glob of live DuckLake files is not a valid export:
it can include obsolete/deleted versions or omit inlined data.

Deployment backup additionally includes a consistent catalog/control backup,
required WAL/pending state or a quiesced archived frontier, configuration history,
policy, keys references and separately protected key material, consumer cursors,
publication receipts, version information and restoration order. A backup is
accepted only after restoration into an isolated instance and comparison of
logical content, receipts, coverage and usable public/private entry points.

## Serving persistence and incarnation

The starting single-node serving recipe uses synchronous inserts and explicit
`fsync_after_insert=1` / `fsync_part_directory=1` on the relevant tables; measure
that cost. Each table family, including zero-row output, has deterministic chunk
completion before a fenced PG transaction publishes a contiguous source frontier.
A lost ACK retries the identical chunk, producing at most physical copies of the
same immutable projection IDs. Distinct-row/payload verification is an alternative;
raw row count is not sufficient. See [CH transactional insertion](https://clickhouse.com/docs/concepts/features/operations/insert/transactions).

Readiness binds a serving incarnation, table identities and projection generation.
Unclean data loss, corruption, table replacement or restore invalidates it until
revalidated or rebuilt. A surviving PG frontier alone never proves CH content.
Replica routing later certifies the required frontier on each eligible serving
node. Page tokens bind namespace, incarnation, generation, frontier, filter hash,
policy revision, order/key and expiry; all subqueries/body lookups share them.
Security revocation explicitly invalidates old tokens instead of silently changing
what their historical policy meant. Destructive maintenance needs separate
exclusive ownership and retained-reader checks; harmless late immutable inserts
do not justify unfenced DDL or deletion.

## Dictionary and source-encoding portability

Event ordinals, endpoint IDs, stable observers and auth-scope IDs are compact
references, not sufficient portable provenance by themselves. Archive snapshots
include the required immutable dictionary mappings and identity-normalization
versions alongside referring facts. Event rows carry their original ordinal and
Nostr ID. Archive publication includes all newly required dictionary records in
the same logical snapshot; restore never invents different ordinal mappings.
Sensitive auth references export only their permitted opaque scope metadata,
never credentials. Test export to a standalone reader without the live registry.

Pending records use an explicitly versioned schema, with canonical field encoding
and immutable record digest. Remote contracts use a versioned binary schema where
batching benefits it, plus documented JSON inspection/export. Do not serialize
Rust in-memory layout or an unversioned private struct as a permanent protocol.
Original signed content is not mutated when projection or transport schemas evolve.
