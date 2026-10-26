# Storage service contracts

These are selected semantic contracts. Physical schemas and starting sizes remain
subject to the stated engine integration and workload tests before release.
The [cross-store effect table](../specification/operations-contract.md#cross-store-effect-and-recovery-table)
consolidates accepted identity, authority, atomic write set, response/unknown
outcome, recovery owner and reclamation at every boundary. These detailed service
rules remain its field/retention authority.

## Durable ingest owner

Inputs are validated-format admission requests from network collection, monitor
and importer. The owner applies current policy, verifies signed events, assigns
immutable stream/epoch/sequence identities and atomically persists identity,
shared payload, scoped eligibility/receive/provenance facts, required dictionaries,
pending records and request receipt. It is the only writer of its RocksDB directory.
Producer request retries are stable; while detailed receipts exist, the same
request key with a different digest is an error. After reclamation every request
in the retired range is refused as RetiredRequest without a digest comparison.
No distributed caller receives a successful durable receipt before
the owner's synchronized write succeeds.

Persistent key spaces: event ID→canonical admission metadata; event/visibility→
first eligibility/receive minima; sequence→immutable record; request ID→digest/outcome;
exact visibility-qualified compact edge inventory when provenance is enabled; optional bounded
acceleration caches; owner
metadata/schema version. Canonical event payload is present in pending state until
archive safety permits reclamation. Rebuilding the identity inventory from a
consistent archive must preserve original record identities and first observation
metadata, not manufacture new records with a new ingestion time. Event-level
first-admission and first-receive fields/corrections follow the explicit
[data contract](data-and-durability.md), including when provenance is disabled.

Pending storage retains each complete admission batch's first/last source identity
and semantic digest separately from request receipts. Producer ACK/receipt GC
cannot erase the boundaries needed for archive consumption. The marker and its
source facts share the admission transaction and pending reclamation conditions;
it is compact technical metadata, not a new scientific fact. The
[pending format](../reference/ingest-pending-format.md) defines the first native
reader and its limits; it does not establish admission authority or archive safety.

The [identity contract](../specification/identity-and-encoding.md) defines full
signed-body factoring, scope-local representatives and fingerprint bytes. A new
eligible scope is NewFacts even for a known event and with provenance disabled;
the original private signature/admission time never becomes public by body reuse.
Admit stays strict: a changed signature, observation or scope under the same
retained request identity is RequestConflict even when both would deduplicate.

`CertifyExisting` is a separate read-only operation with no ProducerRequestId.
After full input/policy validation, one consistent committed inventory read
proves all requested body/eligibility/receive/witness effects already retained,
or returns NeedsAdmission. The operational proof references exact source
dependencies, policy/topology/recovery generation and protection class. It creates
no sequence, request outcome, delivery record or durable certification ledger.
Missing scope, first network receive, earlier correction and new witness require
the mutation path. Proof/checkpoint backup closure follows the
[selected contract](../specification/identity-and-encoding.md#certifyexisting-without-mutation-semantics);
receipt retirement does not remove its retained source basis. Actual adapter
acceptance must demonstrate a write-free known-input path and reject a proof
whose acknowledged progress outlives a missing dependency.

| Key | Starting value | Change behavior |
|---|---|---|
| `ingest.wal_sync` | Required for local durable receipts | Invariant, not a speed switch |
| `ingest.group_commit_max_delay` | 5 ms | Hot, applies to new groups |
| `ingest.group_commit_max_bytes` | 4 MiB | Hot, bounded by global permits |
| `ingest.pending_max_bytes` | Explicit share of available disk | Hot lowering throttles; never erases pending work |
| `ingest.identity_cache_bytes` | 256 MiB generic recipe | Restart/adapter-specific live update; counted in host RAM |
| `ingest.verify_workers` | Resource-derived, explicit override | Drain before reducing |
| `ingest.request_receipt_retention` | Detail retention after producer acknowledgement/retirement | Cannot expire unresolved effects; see the producer-retirement contract below |

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
| `archive.publisher.enabled` | true when archive publication is assigned | Hot pause of new source consumption; retain committed snapshot read/export and resolve accepted publication before stopping |
| `archive.target_file_bytes` | 256 MiB | New files; tune from corpus and row-group memory |
| `archive.max_publish_delay` | 30 s | Batch formation/dispatch timer when authority/resources are available; actual committed archive lag is measured separately |
| `archive.row_group_rows` | 64,000 soft starting target | Engine vector/flush granularity can exceed it; not a memory or row-count cap |
| `archive.compression` | Zstandard level 3 | New files; schema/manifest records actual encoding |
| `archive.prepare_workers` | 2 | Bounded pool; one logical publisher remains |
| `archive.worker_memory_bytes` | 512 MiB per prepare worker | Whole native job grant; its concurrent sum must fit the native domain and must admit one complete atomic unit |
| `archive.maintenance_budget` | Explicit CPU/I/O/time share | Hot; yields between bounded units |
| `archive.snapshot_retention` | References plus operator recovery window | Never expire pinned readers/backups/exports |
| `archive.scrub_interval` | 7 days, incremental traversal | Hot; persists scan cursor |

An enabled publisher requires a declared Ingest source. A retained Archive owner
with `archive.publisher.enabled=false` can serve pinned snapshots without an
active ingest owner. Similarly, an enabled Indexer requires at least one declared
pending or archive replay source; pausing Indexer leaves its independent Index
storage owner available. These static declarations do not prove source identity
or usable committed data; the owning runtime adapters must check those.

Large event rows and wide tags can make a row count alone exceed memory. Bound
application input/decoded staging by bytes and own the native worker's whole
resource grant; engine row/file/byte targets do not enforce that grant.
Small low-rate files are acceptable initially and compact later;
do not stall durable acquisition to fill a target size. A file digest verifies
bytes, not cross-table completeness or authentic authorship. Scrub compares
manifest membership/digests and logical counts with sampled revalidation; it does
not reverify every signature on every maintenance pass.

On ambiguous commit, inspect the batch receipt in a new healthy transaction.
Receipt absence permits retry only after the previous transaction is known to
have terminated/rolled back and cannot commit later. Catalog/credential outage or
an unresolved old transaction is unknown, never a negative receipt. Do not write
a second batch while that state is unknown. Orphan prepared files are reclaimed
only after active worker/transaction exclusion plus an age/reference check, never
by globbing all unlisted recent objects. Backup catalogs and objects together;
retain required file generations.
Remote takeover requires fencing beyond a lease in an unrelated transaction.

Use the native transactional writer and the
[local/S3 placement contract](../specification/archive-storage-contract.md#actual-publication-and-protection-boundaries).
Typed batch preparation is not an application-owned Parquet transaction protocol.
Qualified file/catalog persistence and semantic receipts establish the advertised
protection class; full-file checksum capture/backup/scrub state is separately
recorded. Native INSERT does not imply a precomputed application SHA-256 or a
mandatory extra S3 read of every new file before reclamation.

The selected archive connection explicitly disables DuckLake data inlining
(`DATA_INLINING_ROW_LIMIT 0`, with no persisted table/schema override enabling it).
This matches the existing PG probe and keeps scientific payloads in Parquet.
Verify effective options at startup and inspect the quiet one-event path.
Inlining is an engine capability, not inherently incorrect: a future measured
change may enable it with catalog byte/flush/backup accounting, but cannot silently
change this storage contract. The disabled baseline pays small-file/catalog costs;
P02-T09 must include low traffic across many active tables and compaction debt.

The [real target-size probe](../../experiments/parquet-targets/README.md) wrote
65,536-row groups with a requested 64,000-row target. Actual DuckLake settings,
rounding and native overshoot must be inspected for the pinned release. Application
batch bounds plus worker process/cgroup limits protect the host; an OOM/failure
must preserve pending data and stop archive acknowledgement. It is not success.
Engine settings are documented in [DuckLake configuration](https://ducklake.select/docs/stable/duckdb/usage/configuration)
and [data inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining).

Archive reclamation also requires a verified storage protection class: catalog
commit and matching file membership establish logical publication, but do not
by themselves establish persistence of local file bytes, directory entries or
remote objects under power/host loss. P02-T04/P02-T08 must qualify the actual
filesystem/object-store and catalog path together before that class is advertised.
If the configured receipt class is stronger than the proven archive path, retain
pending recovery data or refuse the configuration; do not silently weaken an
earlier durable acknowledgement when reclaiming its WAL-backed payload.

## Producer acknowledgement and receipt reclamation

The producer registry allocates a monotonically increasing epoch for a stable
logical producer slot (collector/import/measurement worker, not one per relay).
Its process incarnation is bound to that epoch. ProducerRequestId is the registered
slot/epoch plus monotonic request sequence; source-record identity is separate.
An epoch permits a bounded outstanding request window under a shared byte grant:
initially `ingest.producer_outstanding_requests=64`, with pending request/outcome
bytes charged to the existing 32MiB producer allowance and aggregate host grants.
An authenticated producer cannot register arbitrary slots to multiply that grant.
Restart resolves its persisted outstanding requests before opening/retiring an
epoch; a fresh process cannot reset sequence numbers inside an old epoch.

The owner synchronizes request digest/outcome with effects. The producer advances
a contiguous acknowledgement watermark only after storing its downstream durable
incremental outcome/checkpoint. This transfers recoverable responsibility for
those outcomes; it is separate from final page/EOSE coverage. A live stream or a
page longer than the outstanding window must make this incremental handoff and
ACK progress without first claiming complete traversal. Owner-side watermark
persistence precedes receipt-detail GC.
An acknowledged sequence returns `RetiredRequest` after detail reclamation; it
must never be treated as a fresh mutation. Out-of-order completions cannot advance
the watermark over a hole. Time-based display retention cannot close a hole.

To retire an epoch, first install a durable owner-side admission-closing barrier
serialized with the final authority check/WriteBatch. Stop/fence its producer,
drain/resolve requests accepted before that barrier, then resolve or explicitly
abandon remaining workflow obligations and mark retirement complete. A request
already in flight but not committed at closure cannot commit afterwards merely
because its producer stopped before the network request arrived.
Abandonment preserves any already admitted scientific facts and explicit coverage
uncertainty. Keep the current/closed-epoch high watermark per stable producer slot;
older epochs are rejected without retaining one tombstone per request forever.
Removing a slot requires a deployment registry-generation fence, not reuse of
its identity. Backup/restore preserves these watermarks and unresolved receipts
with causal closure: owner ACK(n)/GC requires inclusion of the producer's durable
incremental checkpoint/outcomes that authorized ACK(n). A mixed cut containing
post-GC owner state and pre-checkpoint producer state is refused, not recovered
by treating RetiredRequest as success. Quiesce the handoff or capture a coordinated
barrier with the required ledgers before certifying a backup.
This protocol bounds duplicate-delivery receipt detail by outstanding work rather
than all historical deliveries; permanent event/evidence identity still has its
own U/P/M-sized cost. P02-T02 tests reordered ACKs, missing ACK, crash during
watermark GC, old-epoch resubmission and restore before accepting reclamation.
Include an outstanding window of two with a longer page and continuous live
traffic, a verified RPC paused before commit during retirement, and the mixed-cut
backup counterexample. Charge incremental ledger/group-commit writes as well as
receipt space; bounded retained detail does not eliminate per-Admit delivery work.
CertifyExisting avoids that mutation protocol only for its explicitly proven
already-retained effects. Its bounded producer checkpoint still carries source
dependencies and causal backup obligations; it does not certify relay coverage.
Producer checkpoints accept `EventEffectBasis = Receipt | ExistingProof` and
deduplicate proof dependencies within bounded classified prefixes/pages. A
proof-only prefix allocates no ProducerRequestId and sends no ingest ACK; mixed
prefixes ACK only their contiguous resolved mutation requests. Missing/revoked
proof authority prevents new progress, while restore must validate the retained
basis of already committed checkpoints. Lost proof responses are read again;
an ambiguous Admit remains subject to original-request resolution.

## Online indexer

Inputs are immutable admitted records from pending storage or archive replay.
Outputs are full event rows, tag access paths, optional edge observation access
paths, monitor history and publication metadata. It owns serving write progress;
API/deriver processes do not mutate these raw tables. Event-body and edge
observation identities survive replay unchanged. A body may already exist while a
new local witness still needs indexing. Exact edge admission suppresses ordinary
repeats; strictly earlier attributable evidence creates an immutable correction.

Its Index Rocks storage/RPC owner has a separate lifecycle from source projection
and algorithms. Pausing either leaves typed dirty-only/expiry state commits and
pinned ordered-set reads available when their dependencies/authority remain valid.
Stopping the storage owner exposes the affected unavailable capabilities; it
never authorizes another process to mount the directory writable. The
[operational matrix](../specification/operations-contract.md#process-placement-and-capabilities)
resolves archive-only, serving-only and split placements.

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

The first PG publication adapter fixes ten targets: dictionaries, events,
eligibility, receive, event ID/author/time/tag access paths, and provenance by
event/relay. Schema 1 uses the explicit physical partition `all`; each complete
AdmissionBatch has one deterministic chunk per target, including zero rows.
The accepted provenance-off profile retains both explicit zero witness targets.
Preparation verifies the actual retained pending batch and persists the complete
plan before dispatch. Its only resume unit is that whole batch. A timeout leaves
the plan prepared; it does not certify absence or allow a different plan at that
head. Publication requires the private CH verifier's complete physical proof and
atomically commits all ten chunks, their typed frontier binding and the next
contiguous consumer head. This adapter currently requires the same retained
topology on both cuts; it refuses an unwired topology handoff.

Generation and chunk origins remain immutable across Index restart. A new
session can publish an old prepared plan only after stable local custody excludes
the old owner, the CH verifier revalidates the published prefix and retained plan,
and the actual current Control backend atomically transfers PG authority. The
recovery proof binds the exact published revision/plan and optional single pending
plan. All historical plans are available in bounded pages. Lease age alone never
authorizes transfer. Both actual owner deadlines are checked after blocking locks
and again before COMMIT; scheduling renewal cannot revive either expired owner.
Historical PG reads and lost-COMMIT receipt resolution grant no physical readiness.
The selected source owner must retain required pending batches; preparation alone
does not implement native reclamation or substitute for a retention claim.

The operational `index-plan-metadata/1` fingerprint is SHA-256 over the literal
ASCII domain `bigbrotr/index-plan-metadata/1\0` (one terminal zero byte), followed
by the fields below in order. This is a PG coordination commitment, separate
from scientific fact/admission fingerprints and the CH chunk/row digests. Every
integer here is fixed-width unsigned big-endian; this recipe is not CBOR. Raw
IDs/digests use their bytes, never hex or text. No field is nullable or omitted:

1. Dataset, stable Index resource, projection generation and storage incarnation,
   each 16 bytes; table-identity and custody-binding digests, each 32 bytes.
2. Provenance byte (`1` for `local_first_witness`, `0` for `off`), then expected
   published revision as u64 (`0` means no published prefix).
3. Before cut, then after cut: topology as u64, stream count as u32 (1..1024),
   followed by each strictly `(shard,epoch)`-ordered stream's shard u32, owner epoch
   u64 and complete-through u64. Both cuts belong to the dataset above and share
   the same topology/stream inventory; zero prefixes are explicit.
4. Complete admission-batch semantic digest32, source shard u32, source owner
   epoch u64, first sequence u64 and last sequence u64. The source is nonempty,
   advances exactly one stream from its prior prefix+1 through the entire batch,
   and leaves other streams unchanged.
5. Exactly ten chunks, no count prefix, in target order 0..9: dictionaries, events,
   eligibility facts, receive facts, event ID, author, time, tag, provenance by
   event, provenance by relay. Each chunk encodes target byte, schema u16 (`1`),
   partition byte length u32 (`3`) and UTF-8 bytes (`all`), chunk digest32, row count
   u64 and logical rows digest32. Empty chunks retain all fields and a zero count.

Creating/publishing owner epochs, attempts and wall-clock times are excluded;
recovery cannot change plan identity. The independent literal tests in
`storage/postgres/index_publication/tests.rs` fix both on/off vectors and their
admission-batch input. Another encoding/profile requires a new domain/version and
explicit retained-plan compatibility handling.

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
The [upgrade and rollback procedure](../specification/operations-contract.md#compatibility-migration-and-rollback)
defines adjacent mixed versions, writer-floor activation, the last safe binary
rollback boundary and forward repair or fenced backup restore after it.

## Typed source cuts and projection identity

`SourceRecordId = (dataset_id, shard_id, owner_epoch, sequence)`.
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

Use the [whole-pipeline reservation and disk model](../specification/operations-contract.md#maximum-units-reservations-and-progress):
local queues fit coarse process grants, archive/ACK/recovery retain dedicated
completion capacity, and pending storage can reclaim through verified archive
replay without waiting for CH. Lowering limits never makes accepted work
undrainable; retain its old completion grant or refuse the change.

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
