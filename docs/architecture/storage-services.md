# Storage service contracts

These are selected semantic contracts. Physical schemas and starting sizes remain
subject to the stated engine integration and workload tests before release.

## Durable ingest owner

Inputs are validated-format admission requests from network collection, monitor
and importer. The owner applies current policy, verifies signed events, assigns
immutable stream/epoch/sequence identities and atomically persists identity,
payload, outbox and request receipt. It is the only writer of its RocksDB directory.
Producer request retries are stable; the same request key with a different digest
is an error. No distributed caller receives a successful durable receipt before
the owner's synchronized write succeeds.

Persistent key spaces: event ID→canonical admission metadata; sequence→immutable
record; request ID→digest/outcome; exact compact edge inventory when provenance is enabled; optional bounded
acceleration caches; owner
metadata/schema version. Canonical event payload is present in pending state until
archive safety permits reclamation. Rebuilding the identity inventory from a
consistent archive must preserve original record identities and first observation
metadata, not manufacture new records with a new ingestion time. Event-level
first-admission and first-receive fields/corrections follow the explicit
[data contract](data-and-durability.md), including when provenance is disabled.

| Key | Starting value | Change behavior |
|---|---|---|
| `ingest.wal_sync` | Required for local durable receipts | Invariant, not a speed switch |
| `ingest.group_commit_max_delay` | 5 ms | Hot, applies to new groups |
| `ingest.group_commit_max_bytes` | 4 MiB | Hot, bounded by global permits |
| `ingest.pending_max_bytes` | Explicit share of available disk | Hot lowering throttles; never erases pending work |
| `ingest.identity_cache_bytes` | 256 MiB generic recipe | Restart/adapter-specific live update; counted in host RAM |
| `ingest.verify_workers` | Resource-derived, explicit override | Drain before reducing |
| `ingest.request_receipt_retention` | At least configured retry/replay horizon | Cannot expire a still-live producer request |

Failure isolates the affected owner partition; producers pause/retry with bounded
queues. Unknown outcome is not a negative receipt. Maintenance must never run
unbounded engine compaction on the async network executor. Measure write stalls,
WAL bytes, admission latency, queue age and physical identity-index growth.

## Archive publisher and maintenance

Input is the durable ordered record feed; output is a DuckLake snapshot containing
full events, origin-aware provenance, measurements/documents and batch receipts.
Owns archive consumption cursor, publication receipts, compaction scheduling,
retention claims and integrity summaries. It reads identity metadata but cannot
advance network coverage by itself. Exporters and indexers may read committed
archive snapshots while this service is stopped.

| Key | Starting value | Change behavior |
|---|---|---|
| `archive.target_file_bytes` | 256 MiB | New files; tune from corpus and row-group memory |
| `archive.max_publish_delay` | 30 s | New batches; bounds archive freshness, not online indexing |
| `archive.row_group_rows` | 64,000 upper starting target | Also constrained by bytes/global memory |
| `archive.compression` | Zstandard level 3 | New files; schema/manifest records actual encoding |
| `archive.prepare_workers` | 2 | Bounded pool; one logical publisher remains |
| `archive.maintenance_budget` | Explicit CPU/I/O/time share | Hot; yields between bounded units |
| `archive.snapshot_retention` | References plus operator recovery window | Never expire pinned readers/backups/exports |
| `archive.scrub_interval` | 7 days, incremental traversal | Hot; persists scan cursor |

Large event rows and wide tags can make a row count alone exceed memory; flush by
bytes as well. Small low-rate files are acceptable initially and compact later;
do not stall durable acquisition to fill a target size. A file digest verifies
bytes, not cross-table completeness or authentic authorship. Scrub compares
manifest membership/digests and logical counts with sampled revalidation; it does
not reverify every signature on every maintenance pass.

On ambiguous commit, inspect the batch receipt in a new transaction. Do not write
a second batch if receipt state cannot be determined. Orphan prepared files are
reclaimed only after an age/reference check, never by globbing all unlisted recent
objects. Backup catalogs and objects together; retain required file generations.
Remote takeover requires fencing beyond a lease in an unrelated transaction.

## Online indexer

Inputs are immutable admitted records from pending storage or archive replay.
Outputs are full event rows, tag access paths, optional edge observation access
paths, monitor history and publication metadata. It owns serving write progress;
API/deriver processes do not mutate these raw tables. Event-body and edge
observation identities survive replay unchanged. A body may already exist while a
new local witness still needs indexing. Exact edge admission suppresses ordinary
repeats; strictly earlier attributable evidence creates an immutable correction.

| Key | Starting value | Change behavior |
|---|---|---|
| `indexer.batch_max_rows` | 20,000 | New batches; also byte bound |
| `indexer.batch_max_bytes` | 16 MiB | New batches; global permits still apply |
| `indexer.flush_interval` | 1 s | New batches; read freshness tradeoff |
| `indexer.insert_workers` | 2 within one logical owner | Independent chunks only; receipt publication ordered |
| `indexer.query_memory` | 256 MiB per indexing operation | Explicit engine setting, host envelope wins |
| `indexer.replay_source` | Pending then archive fallback | Stable logical identities; no new timestamps on replay |
| `indexer.poison_policy` | Pause affected stream and diagnose | No automatic dropping or marking complete |

Split by table and physical partition into deterministic chunks before insertion;
record the schema version, row count and digest. An engine ACK is interpreted
according to the pinned synchronous/async-insert settings. Maintain a complete
published frontier only after every required table's chunks satisfy the contract.
Read views handle physical retry duplicates independently of CH's finite token
retention. Poisoned data has an inspectable location and error; a repaired parser
can replay it without advancing unrelated sources incorrectly.

Selected consistency baseline: immutable source-record identity in serving rows;
append-only MergeTree, or replacing only byte-identical copies of the same full
immutable record identity. Do not replace logical provenance edges in the raw
fact layer: a newly learned earlier observation must not erase an old snapshot's
row. Filter by pinned published frontier before per-edge minimum reduction. A
stable source sequence also belongs to every tag projection row. Compaction to
smaller logical-edge views creates a separately published generation and retains
old generations while readers hold them. The implementation proves this with
forced merges, delayed chunks and replay after dedup-window expiry.

## Control/catalog and schema ownership

PG control tables have domain owners: endpoint registry/admission, configuration
and policy revisions, coverage attempts/gaps, job scheduling, source frontiers,
derivation/rank generations, publication outbox/receipts, read snapshot leases and
backup manifests. Use explicit typed tables and transactions for these contracts,
not arbitrary JSON checkpoint blobs as the only consistency model. Large raw
measurements and event bodies do not belong in PG control rows. Bounded JSON is
appropriate for versioned diagnostic detail, not replacement of keys and states.

A separate migration command with dedicated privileges changes schemas. Normal
service roles cannot execute arbitrary DDL or modify DuckLake internal metadata.
Readers and writers negotiate supported record/config/schema versions at startup.
Unknown versions stop affected work with an actionable error. Upgrade order follows
backward-readable data formats and additive contracts before writer activation;
destructive migration requires a verified recovery path and explicit operation.

## Typed source cuts and projection identity

`SourceRecordId = (archive_namespace, shard_id, ownership_epoch, sequence)`.
`ProjectionRowId = (projection_generation, SourceRecordId, discriminator)`; the
discriminator identifies the exact deduplicated tag key/value or other emitted
subrow. Source facts and payload digests remain immutable. Reprojection changes
the generation, not the original source identity.

`SourceCut` is a validated set of per-stream/epoch inclusive sequence bounds and
its partition-topology revision. A scalar is shorthand only for one stream in one
epoch. Shard transitions record predecessor final cuts and successor ownership;
missing epochs cannot disappear from a supposedly complete prefix. Job runs,
archive batch receipts, graph inputs, serving manifests and page tokens carry the
same typed cut. Comparisons reject incompatible namespaces/topology rather than
using numeric max across unrelated sequences.

Archive maintenance owns one durable scrub ledger and executor. Exporter/CLI/UI
may request a scrub or backup-verification job through that contract; they do not
run an independent periodic scrub schedule. Preserve one cursor, snapshot pin and
resource budget per actual job.

## Replay memory, progress and repair boundaries

The [storage code walkthrough](../research/go-storage-code-walkthrough.md)
records useful prior durable-prefix, in-flight membership and all-target recovery
patterns. The new implementation must include pending/flushing identity state,
positive-cache slabs, decoded rows/tags/documents, probe arrays, partition buffers,
lookahead and native driver buffers in the same budget. A bounded cache or worker
count does not bound those other allocations. Reserve any lookahead separately
before preparation; release it on cancellation and never wait for it while
holding the resources it needs. A large single source object is streamed through
bounded replay units or explicitly refused, not admitted merely because it is
the first item in a count-limited chunk.

Distinguish displayed progress from the actual persisted recovery position. State
which source unit must be reread after an interrupted insert and preserve its
identity across file moves/compaction. A recorded byte offset unused by recovery
is not an advertised seek-resume guarantee. Required family effects and dependency
metadata precede publication of their serving frontier; an optional disabled
projection is explicit scope, not a silently skipped required sink.

Repair and forensic inspection obey memory/I/O/CPU/cancellation budgets too. Never
load an arbitrarily large corrupt tail into RAM or perform unbounded overlapping
checksum scans inside ordinary startup. Use documented engine recovery at its
proven committed boundary, preserve suspicious bytes, and admit expensive forensic
work as an explicit resumable job. Refusal must be diagnostic and must not advance
a complete frontier over unrecovered evidence. Cheap metadata checks remain
distinct from the full-content scrub.
