# Ingest receipt format

This document fixes version 1 of BigBrotr's persistent operational mutation
receipt encoding. It covers the two receipt values represented by the shared
model: `NewFacts` and `AlreadyDurable`. Invalid, excluded, retryable, conflicting,
incompatible and retired outcomes are separate operational results and do not
gain receipt semantics from this format.

Decoding proves that bytes reproduce a structurally valid `Receipt`. It does not
prove that a write committed, that its producer or policy is current, that its
dependencies remain protected, or that the value can authorize progress. The
ingest storage owner establishes those properties from its fenced store and
retirement state.

## Version 1 bytes

The encoded value starts with the literal ASCII bytes
`bigbrotr/receipt/1\0`. The prefix is an operational format and version marker;
it is not a scientific fingerprint domain and the receipt has no self-digest.
The remainder is deterministic CBOR with this closed array:

```text
[
  result_code:u8,
  [producer_slot_id:bytes16, producer_epoch:u64, request_sequence:u64],
  request_digest:bytes32,
  [dataset_id:bytes16, topology_generation:u64, protection_code:u8,
   recovery_generation:bytes16, policy_epoch:u64],
  batch:null | [FactRef...],
  dependencies:[FactRef...]
]

FactRef = [
  [dataset_id:bytes16, shard_id:u32, owner_epoch:u64, sequence:u64],
  payload_digest:bytes32
]
```

The closed codes are:

| Field | Code | Meaning |
|---|---:|---|
| `result_code` | 1 | `NewFacts`; `batch` is a nonempty `AdmissionBatch` |
| `result_code` | 2 | `AlreadyDurable`; `batch` is exactly CBOR null |
| `protection_code` | 1 | `local-synced` |

All arrays are definite length. Integers and lengths use their shortest CBOR
encoding. Byte strings have their exact declared size. Maps, text, tags, floats,
negative integers, indefinite lengths, undefined values and trailing bytes are
not part of this format. Equivalent wider heads are noncanonical and rejected.
An unknown prefix, result code or protection code is incompatible rather than a
default value.

The `NewFacts` batch retains every exact member and digest. Construction requires
one nonempty contiguous source stream. The dependency array is nonempty and
strictly sorted by full `SourceRecordId`, without duplicates. Every dependency
uses the context dataset. Every batch member occurs in the dependency closure
with the same digest, and no dependency in the batch stream follows the batch
end. Dependencies from other streams may be present. `AlreadyDurable` has no new
batch but retains the same complete, sorted dependency representation.

Dataset IDs, producer slots, producer epochs, request sequences, owner epochs and
source sequences retain their model domains. In particular, allocated identities
and counters reject zero. Topology generation and policy epoch retain their full
unsigned 64-bit domain including zero. Recovery generation retains all 16 bytes,
including all zero; registration and freshness are authority checks outside this
codec.

## API and bounds

`Receipt::encode_persistent(max_encoded_bytes)` counts canonical output before
allocating the exact output buffer. `Receipt::decode_persistent(bytes, limits)`
checks the encoded size before parsing. `ReceiptDecodeLimits` independently caps:

- encoded bytes;
- aggregate requested storage for the batch and dependency `Vec<FactRef>` values;
- batch record count; and
- dependency count.

The initial defaults are 8 MiB encoded, 8 MiB requested vector storage, and
65,536 records for each array. A decoder checks definite counts, remaining
minimum input bytes and checked `count * size_of::<FactRef>()` before calling
`try_reserve_exact`. These are local per-value safeguards. They exclude the
already-held input, allocator overhead, native engine memory and concurrent work,
so the application must retain the corresponding shared byte/item grant through
driver copies and commit handling.

After bounded decoding, the codec calls `AdmissionBatch`, `SourceDependencies`
and `Receipt` constructors. This rejects corrupt datasets, gaps, unordered or
duplicate dependencies, missing or conflicting batch membership and same-stream
future dependencies. It then re-encodes into a comparison writer without a
second output buffer; any different or remaining byte is noncanonical.

RocksDB checksums or an owning envelope detect arbitrary physical byte
corruption. This codec intentionally adds no independent receipt fingerprint. A
different canonical value can still be structurally valid, so callers must bind
the decoded request identity to the lookup key and compare it with the owner
state they intended to resolve.

## Native detail inspection

`IngestStore::read_receipt` reads one exact request key with RocksDB checksums
enabled and cache filling disabled. It checks the complete decoded request ID,
dataset and expected request digest. A digest mismatch is `RequestConflict`;
malformed, incompatible, mis-keyed or foreign data is an error, never absence.
The reader retains the original policy, topology and recovery coordinates rather
than relabeling the result with the store's current manifest. Inspecting an old
context does not authorize its use under current policy.

The caller supplies positive finite codec limits and a data/item permit for
`max_allocation_bytes + sizeof(Receipt)`. The native pin remains local to the
call; decoding checks its encoded length before allocating Rust vectors, and
`StoredReceipt` retains the data permit until its decoded buffers drop. Native
I/O/pin cost uses the store's separate allowance. These requested allocation
bounds exclude allocator overhead and are not an RSS guarantee. Cancellation is
checked before I/O, before decoding and before returning the decoded result;
it cannot interrupt the native read, so the caller must retain/join its worker.

`None` means only that the detail key is absent. The owning producer watermark,
closing barrier and transaction recovery protocol must still distinguish a
retired request, a safely retryable request and an unresolved write. This reader
does not implement those decisions, check retained scientific dependencies, or
write a proof, acknowledgement, cursor or replacement result.

Native tests install unchanged independent receipt literals, close and reopen
the actual engine, compare every returned byte and inspect the engine sequence
number to exclude writes. They cover both result codes, original contexts,
request/dataset binding, strict digest conflict, corruption, unknown version,
bounded decoding, cancellation before I/O and permit retention. They do not
qualify admission writes, ambiguous commit recovery, concurrent retirement,
mid-I/O cancellation, crash durability or dependency protection.

## Ownership and reclamation

The receipt stores exact request identity/digest, protection context, complete
dependency closure and, for `NewFacts`, full batch membership. It contains no
event body bytes. A separate pending-batch marker may bind the batch's last
sequence and `AdmissionBatch::digest()` for publication/recovery; that marker is
not duplicated or certified by this codec.

Receipt-detail reclamation remains governed by the contiguous producer ACK and
closed-epoch protocol. Decoding an old copied receipt neither reopens a retired
request nor substitutes for the owner's retained watermark. Scientific facts
and pending/archive protection follow their own retention rules after request
detail is reclaimed.

The focused model tests contain independently assembled literal vectors for both
result codes, including full-width integers, a batch that is a subset of a larger
dependency closure, and an `AlreadyDurable` null batch. Negative cases cover
truncation, trailing bytes, every equivalent wider primitive head, unsupported
codes, count and allocation limits, scalar domains, namespace corruption,
ordering and exact closure failures.
