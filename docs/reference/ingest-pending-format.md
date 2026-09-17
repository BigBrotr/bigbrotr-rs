# Ingest pending source format

The first internal pending reader consumes complete admission batches through
`IngestStore::read_pending_batch`. It reads actual RocksDB values from one native
snapshot. It is not an event admission command, public API, authority grant,
archive receipt or backup-closure verifier. The producer and archive consumers
remain separate P02 work.

## Persistent bytes

The [key format](ingest-key-format.md) supplies full dataset/shard/owner-epoch/
sequence identities. These initial value-schema-1 records are closed:

| Key domain | Exact value bytes |
|---|---|
| Source stream `01 02` | `01 02` followed by last complete sequence as unsigned big-endian u64; length 10. Zero is a known empty stream. |
| Immutable fact `01 20` | `01 20`, then the full 32-byte fact digest, then its canonical `bigbrotr/fact/1\0` preimage. The source ID inside the fact must equal the complete key. |
| Pending batch `01 21` | `01 21`, last sequence as unsigned big-endian u64, then the full 32-byte admission-batch digest; length 42. The key supplies the first source ID, and last uses the same stream and dataset. |

A batch is nonempty and last is at least first. The last-minus-first count uses
checked arithmetic. `PendingBatchHeader::from_batch` accepts the model's validated
contiguous `AdmissionBatch`; the byte decoder alone establishes only a structural
header. The reader obtains every named fact, checks canonical bytes, digest and
full source identity, reconstructs the exact ordered batch and verifies its
semantic digest. The same snapshot's stream watermark must cover its last record.
Any missing fact or disagreement refuses the entire result; no partial batch or
advanced cursor is returned.
Decoder budget refusals remain `Limit`, allocation failure remains `Allocation`,
and unknown scientific formats remain `IncompatibleSchema`; none is reported as
corruption of an otherwise valid record.

These metadata bytes are operational storage framing, not new fingerprint
domains. Existing scientific preimages stay unchanged. The new key tag is added
before the first product admission writer exists; existing initialized directories
contain only the unchanged manifest. No populated admission format or released
reader is being migrated. A future incompatible value meaning needs explicit
format negotiation/migration; unknown versions never fall back to v1.

## Retention and missing data

An admission writer must place the marker, facts, stream watermark, inventory and
request outcome in the same synchronized batch. Receipt-detail reclamation may
remove the request result after the required durable handoff; it must preserve
the pending marker. Only the established archive protection/replay/backup protocol
may reclaim pending facts and their marker. This reader implements no GC.

`None` means the requested pending marker is absent from this snapshot. It may
already have been archived and reclaimed. It is never evidence that an Admit
did not commit, permission to skip a source range, or permission to create another
request identity. A caller resolves archive availability and its retained cursor
through the owning protocol. A non-first source ID is not rounded to a batch.

## Memory, cancellation and exposure

The caller supplies finite positive record, total encoded-byte and per-fact
decode limits plus an owned data/item permit. Checked requested allocation is
`max_encoded_bytes + max_records*(sizeof(Vec<u8>)+sizeof(FactRef)) +
fact.max_allocation_bytes + sizeof(Fact)`. The complete grant is required before
native reads. Header record count is checked before allocating vectors, and each
pinned value's size is checked before decoding or copying it. The decoder enforces
its own string/tag/allocation bounds. One decoded fact is dropped before copying
its canonical bytes to the returned bounded batch.

The result retains the data permit until its buffers and batch references drop.
Native pins/snapshot remain local to the read call; native block/cache work uses
the store's separate retained allowance. Allocator overhead, native recovery and
OS page cache mean these requested-byte limits are not an RSS ceiling.
Cancellation is sampled before work and between records; it cannot interrupt
native I/O. The owner must retain and join its bounded worker through completion.

Reads enable native checksums and disable cache filling. They write no request,
proof ledger, cursor or source record. Full internal envelopes can include private
admission metadata, so only authorized internal consumers may use the feed.
Structural decoding does not independently verify an event signature, dictionary
closure, source authority or current visibility permission.

## Executed scope

`rocks_pending` uses actual engine files with independent literal framing and
frozen model preimages. It tests reopen without request details, complete original
bytes and batch digest, unchanged native sequence number, permit retention,
partial/corrupt/mis-keyed facts, missing/insufficient watermark, digest mismatch,
oversized counts, resource limits and cancellation before reading. Its structural
fixtures are not a signed-event admission or crash qualification. Concurrent
mutation, mid-read cancellation, power loss and Linux execution belong to their
recorded later acceptance, not an inferred PASS from these cases.
The [Mac slice evidence](../implementation/evidence/p02-ingest-pending-mac-20260918.json)
records exact source/log hashes, the corrected review finding and exclusions.
