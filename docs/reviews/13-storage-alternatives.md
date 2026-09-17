# Advisory review — Storage alternatives

Date: 2026-09-17. Inspected baseline: `41cc381`.
Status: independent design challenge; **none of these proposals is adopted**.
This review changes no product contract or implementation task.

The current architecture is defensible, but the evidence does not establish that
its complete storage stack is the best starting product. The most promising
changes concern the amount of work per repeated delivery, the physical cost of
exact provenance, and the number of independently recovered representations.
Replacing Rust or adding a distributed broker is unlikely to address those costs.

## Scope and evidence

Fully read: `AGENTS.md`, `PLAN.md`, `docs/planning/decision-confidence.md`,
`docs/architecture/system-design.md`, `data-and-durability.md`,
`storage-services.md`, `docs/specification/storage-catalog.md`,
`data-contracts.md`, `docs/reviews/12-specification-readiness.md`, and
`docs/planning/specification-readiness.md`. Also read the experiment summary and
selected admission, capacity and P02 implementation sections cited below. This is
not a whole-repository audit or a source inspection of the engine implementations.
The neighboring legacy repository was not inspected or changed.

Current official engine documentation was checked for the specific claims linked
below. Its guarantees must be rechecked against the eventual pinned releases;
current documentation does not upgrade earlier repository experiments. No engine
was started, product code written, benchmark run or crash test repeated. Existing
test results below are reported by the repository. Arithmetic is labeled as such;
performance expectations are inferences and experiments remain proposals.

The comparison preserves full signed events, optional exact provenance, retained
scientific evidence, scope isolation, explicit durability and independently useful
roles. Billions of events and tens or hundreds of billions of provenance facts
are design loads, not workloads demonstrated on the 32-thread/64-GiB host.

## 1. Reopen archive authority as a whole-product choice

**Location.** [Selected architecture](../../PLAN.md#selected-architecture-and-why),
lines 63–76; [decision table](../planning/decision-confidence.md#current-decisions-and-what-could-overturn-them),
lines 30–34; [archive publication](../architecture/data-and-durability.md#archive-publication-and-reclamation),
lines 105–139; [serving persistence](../architecture/data-and-durability.md#serving-persistence-and-incarnation),
lines 198–217.

**Challenge and scenario.** Every full-online installation prepares, compacts,
backs up and recovers an independent DuckLake/Parquet archive while also retaining
full CH event bodies. A large backfill therefore pays two columnar write paths,
two layouts and their maintenance headroom. Following a CH loss, usable public
reads may depend on a lengthy rebuild even while the archive is intact. These
costs can dominate the distinction between language implementations. The
confidence register mentions CH authority, but effectively assigns it to an
API-first product. Keeping verifiable scientific history does not by itself
require Parquet to be the live authoritative engine.

**Concrete alternative.** Compare the selected design with Rocks admission + PG
control + **immutable authoritative CH source tables**, using native backups and
explicit standalone Parquet research exports. The CH source facts, dictionaries
and retained originals are separate from disposable derived generations; ordinary
read-policy deletion never physically removes those originals. Reindexing reads
the retained source tables. DuckDB remains an optional export/analysis tool;
DuckLake publication and its continuously maintained catalog are removed from
this candidate's critical path.

This is three persistence systems, not “one database.” Rocks still provides
atomic exact admission and pending recovery, and PG still owns transactional
control. CH retries still need immutable identities, deterministic chunks,
complete cross-family commit manifests and source cuts. Read pins, authorization,
logical duplicate suppression, synchronized persistence, inventory reconstruction
and a causally closed Rocks/PG/CH backup remain necessary. Pending reclamation
must follow the candidate's proven source-storage protection class. CH documents
bounded insert guarantees and still describes general multi-statement
transactions as experimental; those transactions cannot be assumed to replace
the application protocol. [CH insert guarantees](https://clickhouse.com/docs/concepts/features/operations/insert/transactions).

Native full and incremental backups make the candidate credible, but a CH-native
backup is not an independently queryable Parquet research dataset. Restore
verification remains required. [CH backup and restore](https://clickhouse.com/docs/concepts/features/backup-restore/overview).

**Recommendation.** Make this a serious early competitor, before implementing the
entire dual-source path; do not adopt it merely because its diagram is smaller.
If export portability, archive-only operation and recovery independent of CH are
important enough to justify the second live body store, retain DuckLake and state
that benefit directly. Do not maintain both architectures indefinitely as a new
configuration dimension without evidence that both serve substantial audiences.

**Tradeoff and strongest counterargument.** The selected archive gives engine and
format diversity, independent offline access, and a clean rebuild source after a
serving bug or destructive DDL mistake. CH authority concentrates the retention
and query failure domain, and the archive-only recipe would require redesign or
would still deploy CH. A protected off-host Parquet backup can regain diversity,
but its export and retention cost must be charged; it can erase much of the saving.

**Confidence.** High that the comparison is consequential; medium-low that CH
authority wins this particular research product. No winning engine is established.

**Decisive experiment.** First specify identical failure class, signed-field
fidelity, source cuts and useful query/export outcomes for the two candidates.
Then implement only a small admission→retained-source→read→restore slice for each.
Compare steady and peak bytes, backfill catch-up under reads, time to usable reads
after lost serving storage, independent export and the number of operator recovery
steps. Inject lost acknowledgements and missing source/backup files. Count all
backup/export copies and semantic compromises. Reject a candidate that saves space
by weakening retention or recovery, regardless of ingest speed.

## 2. Give already-retained deliveries a read-only certification path

**Location.** [Admission transaction](../architecture/data-and-durability.md#admission-transaction),
lines 38–68; [producer acknowledgement and receipt reclamation](../architecture/storage-services.md#producer-acknowledgement-and-receipt-reclamation),
lines 104–151; [typed boundaries](../specification/component-contracts.md#typed-boundaries),
lines 79–110.

**Challenge and scenario.** The current design correctly prevents repeated sweeps
from generating permanent provenance facts. However, the admission request still
has a synchronized digest/outcome, producer checkpoint, acknowledgement and later
receipt reclamation. The contract explicitly acknowledges this per-delivery work
at storage-services lines 144–151. A sweep of 5,000 relays returning 1,000 already
known events each can produce five million request outcomes while creating zero
new event or witness facts. Group commit amortizes synchronization but does not
eliminate the outcomes or the cross-owner recovery protocol. This is scenario
arithmetic, not a measured resweep rate.

**Concrete alternative.** Add a separately typed `CertifyExisting` operation. It
is a read-only check with no `ProducerRequestId` mutation or changed-payload
conflict promise. After full input validation, it returns a durable proof of the
already retained effects, or `NeedsAdmission`. It must establish that the body,
required scope eligibility, first-receive metadata and optional witness are
already present and that this observation introduces no earlier correction or
other effect. Verify the exact canonical payload and actual signature; an event
ID hit alone is insufficient. A missing, stale or indeterminate check falls back
to ordinary `Admit`.

The proof identifies durable retained source facts/cut and the effects covered,
not a new mutation receipt. It is only usable under the same owner authority,
retention and causal-backup rules that make existing receipts meaningful. A
producer may durably checkpoint the proof together with bounded page progress;
neither the reply nor an EOSE alone certifies complete relay coverage. Revocation,
private→public eligibility changes, import→network receive metadata, a new edge,
or a strictly earlier observation take the mutation path. S01's visibility
semantics must be closed before the check can be complete.

Keep current request-digest conflict, closure and retirement behavior for
`Admit`/`AdmitEvidence`. A formerly mutating request must never become forgettable
because its first evaluation happened to be a duplicate: that would allow the
same identity to be reused with changed bytes. The proposed operation has a
different contract precisely to avoid that error. Batch certification and exact
lookups; the official RocksDB API offers `MultiGet`, whose documented batching
optimizations are relevant but do not prove workload capacity. [RocksDB MultiGet](https://github.com/facebook/rocksdb/wiki/MultiGet-Performance).

**Recommendation.** Prefer this split if a small state-machine proof and fault
fixture establish a simpler recovery protocol. It directly attacks D-scaled
durable work while preserving U/P/M-scaled facts; it does not reduce network,
parsing or required cryptographic work. Co-located producers can call the same
typed operation directly; remote batching avoids a mandatory extra round trip
for each event, and unknown-heavy workloads can call `Admit` directly.

**Tradeoff and strongest counterargument.** Uniform mutating requests are easier
to specify. A second result class can be misused as permission to skip a new
effect, and producers still need durable progress. If proof lifetime and backup
closure recreate the entire existing receipt ledger, the proposal has failed to
simplify anything. Measure that before adoption.

**Confidence.** High that duplicate-related writes deserve scrutiny; medium that
this exact protocol is the best simplification. This is a semantic proposal,
not a safe implementation shortcut under the current contract.

**Decisive experiment.** Model a window of two requests, a longer page, response
loss, producer restart, owner retirement and mixed backup cuts. Include valid
alternate signatures, invalid signatures, new scopes, new witnesses and earlier
times. A valid proof must permit only already-retained effects and safe progress.
Then compare all-known, mixed and all-new batches using real admission adapters:
WAL/checkpoint bytes, synchronized groups, backlog age and CPU. The all-known path
should perform no new owner mutation for scientific or request-receipt state;
producer coverage checkpoints remain accounted for. Reject it if useful duplicate
throughput does not improve or the mutation semantics become ambiguous.

## 3. Treat provenance inventory as an exact compressed collection, not necessarily one mutable KV per edge

**Location.** [Admission transaction](../architecture/data-and-durability.md#admission-transaction),
lines 52–75; [A02](../specification/data-contracts.md#a02--first-local-provenance-and-corrections),
lines 135–150; [P02-T09](../implementation/02-storage.md#p02-t09--physical-design-and-mixed-workload-decision),
lines 306–308.

**Challenge and scenario.** The target P is large enough that small representation
choices dominate host expansion. At 100 billion edges, every additional byte per
edge costs 100 GB before replication; a hypothetical 24-byte difference is
2.4 TB. This arithmetic is not an estimate of achievable compression. The exact
inventory must retain times, scopes and receipts, while the archive and two
serving orders retain related information again. Random checks against an aged
inventory can consume storage I/O even when the incoming sweep adds nothing.

**Concrete alternative.** Keep one exact admission owner, but compare the flat-key
baseline with bounded grouped base blocks plus a small mutable delta. Within an
event-owner partition, group stable observer/access/endpoint dimensions and a
bounded event-ordinal range. Store exact membership together with aligned minimum
time, receipt/source identity and required metadata; bitmap membership alone is
not an equivalent dataset. New edges and earlier corrections are synchronized in
the delta and immutable source feed. A background job consolidates closed delta
ranges into replacement base blocks, then atomically publishes the base selection
and consumed delta boundary under the same owner. Resolve reads from both until
that switch is durable.

Prefer an initial representation contained within the same RocksDB ownership and
backup domain, rather than immediately creating a new permanent external file
protocol. The additional grouping codec, merge logic and generation metadata are
still real implementation cost. Bound block sizes so one hot relay cannot force
rewriting its entire inventory. Route by stable event ownership; do not introduce
a globally locked relay bitmap as a hidden sharding obstacle. Original qualified
ordinals and source identities survive consolidation unchanged.

**Recommendation.** Promote this already-mentioned alternative from a secondary
format experiment to an early capacity discriminator before permanent inventory
layout is frozen. First compare batched flat lookups with grouped locality; only
add base/delta consolidation if it earns its complexity. Do not infer that the
exactness requirement forces every historical edge to remain an independently
mutable KV forever. Bloom filters may reject definite absence or accelerate
lookup, but never establish exact membership alone.

**Tradeoff and strongest counterargument.** RocksDB already supplies a mature
LSM, compression and atomic batches. A second application-level base/delta design
can duplicate those mechanisms, increase read amplification or make earlier
corrections expensive. Sparse random relations with irregular metadata may
compress poorly; dictionaries and per-edge source receipts can dominate the
membership saving. Flat keys remain the preferred baseline if the grouped design
does not improve total bytes and useful lookup performance after consolidation,
backup and restore costs are included.

**Confidence.** High that the P-sized cost can dominate; medium-low on the proposed
physical winner. No compression ratio or RAM-only cache result is claimed.

**Decisive experiment.** Compare fixed-width exact flat keys + batched lookups,
bounded grouped values, and grouped base/delta with identical full metadata.
Exercise sparse relays, dense mirrors, randomized arrival, older corrections,
multiple scopes and owner partitions. Age the store past its cache budget and
include consolidation interruption, restore and concurrent known-edge sweeps.
Measure physical bytes/edge including dictionaries, logical and device write
amplification, p99 certification latency, rebuild time and peak disk reserve.
Reject any representation that only wins by dropping source-cut or scope meaning.

## 4. Use engine-owned access paths to remove application recovery boundaries where possible

**Location.** [Serving layout](../architecture/data-and-durability.md#serving-layout-and-exact-answers),
lines 143–167; [online indexer](../architecture/storage-services.md#online-indexer),
lines 173–189; [CH relations](../specification/storage-catalog.md#clickhouse-serving-relations),
lines 82–87 and 110–114.

**Challenge and scenario.** Separate event/body/filter and provenance-order
inserts generate separate deterministic chunks, acknowledgement resolution and
frontier conditions. The docs already allow native event projections, but the
design comparison primarily emphasizes sort order and bytes. A reverse provenance
access path with equivalent information can also be an engine-maintained
projection over one immutable witness table. Otherwise an interrupted second
ordering produces another application recovery problem even when all source facts
are already present. At large P, repeating time/scope/receipt fields in two full
orders adds substantial bytes beyond the genuinely necessary alternate key.

**Concrete alternative.** Make native CH projections the first candidate for
simple reorderings, including reverse provenance access. Compare covering and
`_part_offset` projections: the latter retain an alternate sorting key and locate
additional columns in the base data, exchanging lower storage for extra reads.
CH documents both forms and automatic maintenance. It also documents restrictions
on differing TTLs, joins/filters in projection definitions, and some update/delete
operations. [CH projections](https://clickhouse.com/docs/concepts/features/projections/projections).

Treat a base table and its proven engine-owned paths as one application ingestion
unit rather than inventing separate application chunk receipts for each hidden
path. Keep separate family frontiers for genuinely separate data, tag expansion
where required, derived generations and cross-store activation. Projections do
not provide a transaction across unrelated tables and do not solve logical retry
deduplication. Every query still applies the source cut before correction
reduction; optimizer eligibility must be tested with that actual query, not a
simple equality lookup that bypasses the read contract.

**Recommendation.** Prefer the engine-owned path when its supported version and
query plan meet the required behavior. Its benefit is less custom coordination,
not only a faster index. Keep explicit tables when they materially improve
ordered scans, useful mixed-load queries or independent lifecycle. Do not add a
separate body object store or permanent custom pack format merely to save the
second CH body copy: that introduces a locator, fetch, GC and recovery system.
The current full-online body choice remains reasonable while authority is tested
under finding 1; cold-tier latency should remain explicit.

**Tradeoff and strongest counterargument.** A pointer projection can turn an
efficient relay scan into scattered reads of event-ordered base data. An optimizer
may not choose it for source-cut, grouping or pagination queries. Source-table and
projection lifetime coupling can also conflict with tiered bodies. The smaller
layout is not automatically the better product; a covering reverse-order table
can earn its duplication through bounded latency and predictable query plans.

**Confidence.** Medium-high that native paths can remove redundant application
work; medium on their storage/latency benefit for full provenance queries.

**Decisive experiment.** Compare explicit second tables, covering projections and
pointer projections on the same aged immutable corpus. Test event→relay,
relay→event, scoped minimum corrections, counts, keyset pages and batched bodies
under merges/replay. Inspect actual plans. Interrupt inserts and projection
materialization; verify old cuts and restored content. Count application chunks
and recovery states alongside bytes, I/O and tail latency. The winning layout
must preserve every old-cut answer and deliver useful query completion.

## 5. Replace blanket archive inlining prohibition with selective, budgeted use

**Location.** [Archive publisher](../architecture/storage-services.md#archive-publisher-and-maintenance),
lines 54–60 and 78–85; [archive relations](../specification/storage-catalog.md#ducklake-scientific-and-archival-relations),
lines 44–59; [export/backup](../architecture/data-and-durability.md#what-an-export-or-backup-contains),
lines 184–196.

**Challenge and scenario.** `DATA_INLINING_ROW_LIMIT 0` applies even to
`archive.archive_commits`, which is explicitly technical metadata rather than
scientific payload. If a busy publisher commits once every 30 seconds and writes
one new receipt row per transaction, an implementation making a separate Parquet
file for each such insertion creates 2,880 receipt files/day, about 1.05 million
per year before compaction. This is conditional arithmetic, not an observed
engine trace or a promise that the 30-second dispatch timer fixes commit cadence.
Sparse dictionaries and evidence families add similar small-file pressure.

**Concrete alternative.** Keep body-heavy tables non-inlined, but permit bounded
DuckLake-managed inlining for commit receipts and selected narrow sparse
relations. Use explicit table settings, periodic flush thresholds and catalog
byte/age accounting; an engine row threshold does not impose a byte bound. The
current documentation supports persisted table overrides, PG catalogs and
explicit flushing to Parquet. Nested values have catalog-specific handling, so
enable only qualified schemas on the pinned version. These capabilities require
a new probe; the old inlining-disabled PG experiment proves none of them.
[DuckLake data inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining).

Inlining scientific facts is neither inherently incorrect nor automatically
desirable. Authority comes from the transactional retained relation and backup
closure, not whether its current bytes are in PG or Parquet. Start with technical
receipts because their payload is narrow and the existing rationale for keeping
scientific bodies in Parquet does not apply. Leave document bodies and broad
nested scientific schemas out until their byte cost and lossless roundtrip are
qualified.

**Recommendation.** If DuckLake is retained, prefer a measured per-relation
policy over the blanket prohibition. Snapshot reads and exports must query the
catalog-aware relation so that inlined rows are included; standalone export and
causal backup already need more than a glob of Parquet files. The catalog backup
now protects actual retained rows as well as metadata, and its WAL, restore and
flush costs must be part of sizing.

**Tradeoff and strongest counterargument.** Inlining moves load into a PG server
already used by scheduling and control. It adds catalog-sensitive data recovery,
flush behavior and schema compatibility cases. A predictable all-Parquet path
may be preferable on a saturated catalog or very high-volume deployment. Selective
inlining is justified only if total catalog/file/maintenance cost decreases while
control responsiveness and recovery remain acceptable.

**Confidence.** High that the blanket rule discards a useful simplification;
medium-high that inlining narrow receipts helps. Its benefit for arbitrary
scientific families remains unmeasured.

**Decisive experiment.** Use a small PG-backed DuckLake fixture with receipts,
dictionary changes, low-rate evidence and larger body batches. Compare inlining
off with receipt-only and a bounded sparse-table policy. Commit, lose the reply,
flush, pin an old snapshot, back up, restore and export to an independent reader.
Check exact logical content and receipts, file counts, PG WAL/catalog growth,
flush headroom and control-query latency. Do not accept a file-count win that
introduces missing inlined data in export or recovery.

## Choices to retain, and what would overturn this review

Retain full signed originals, immutable correction identities, cut-before-reduce
semantics, distinction between acquisition/imported evidence, and optional
provenance with no hidden edge substitute. The reported CH merge counterexample
supports retaining immutable source facts: these alternatives do not revive
replacement by logical edge key. Preserve exact signature checking, including
valid alternate signatures, rather than treating the Nostr ID as a payload cache
authorization. [Existing experiment summary](../research/experiment-summary.md),
lines 5–9, and [identity refinements](../architecture/data-and-durability.md#identity-and-replay-refinements-from-inspected-code),
lines 238–246.

Retain binary natural identifiers and qualified compact dictionaries. Do not
replace stable exported ordinal mappings or SourceRecordId with a bare physical
row offset. Physical compaction may change placement, never evidence identity.
S01's separately attributable body eligibility remains a sensible direction:
private→public arrival must not expose the private first-receive time or a
private-only signature variant through the single-body record. That is already a
recognized semantic issue, not a new finding invented by this review. Any proposed
certification, projection or compression must respect its eventual resolution.
[Readiness S01](../planning/specification-readiness.md#disposition-of-the-eight-paper-gaps),
lines 62–89.

Retain local synchronized admission, positive owner fencing, separate durability/
archive/query/coverage frontiers and no mandatory replicated broker. Keep PG for
transactional control unless a concrete deployment experiment demonstrates a
useful simpler replacement. Moving the full P inventory into ordinary PG tables
or choosing LMDB by reputation would merely substitute an unmeasured cost. These
findings do not establish that the reference host can support the full target
fleet or its history.

The strongest objection to the review as a whole is that specialized components
can reduce total implementation complexity even while increasing process count.
DuckLake gives archive transactions; Rocks gives exact synchronized state; CH
gives compressed analytical reads; PG gives control transactions. Replacing these
with custom files, hand-built MVCC, complicated hybrid indexes or new recipe
combinations can produce a worse product. For that reason, the recommended order
is to simplify duplicate semantics and engine-owned access paths first, qualify
selective inlining if retaining DuckLake, and test the consequential authority and
provenance-layout alternatives before committing to a large implementation.
If a candidate cannot preserve the existing scientific and recovery contract
with less total operational work, retain the selected architecture.
