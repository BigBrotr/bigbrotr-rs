# Ingest RocksDB key format

Status: product key format version 1 for the first event-admission slice. This
document fixes key bytes only. Value codecs, RocksDB options, authority, atomic
write composition, receipts and recovery remain owned by P02-T02's later steps.

## Column families and prefix

An ingest directory has exactly the `default`, `facts` and `receipts` column
families. Each key starts with two bytes:

```text
[key_format_version: u8 = 0x01, domain_tag: u8]
```

Domain tags are globally unique even though RocksDB also supplies the column
family. A decoder receives the actual column family and rejects a domain assigned
to another one. It also rejects an unsupported version, unknown tag, truncated or
extended key, invalid allocated identity/counter, and an invalid embedded
dictionary namespace. No decoder accepts a proper prefix as a complete key. The
singleton manifest key is exactly its two-byte domain prefix.

`KeyDomain::prefix()` returns the fixed two-byte prefix for caller-bounded engine scans.
`IngestKey::encode()` returns a stack-backed `EncodedKey`; `decode_key()` reads
directly from the supplied slice. Neither operation serializes a value or invokes
RocksDB.

## Primitive fields

Fixed identities and digests are their raw bytes, never hexadecimal text. `u16`,
`u32` and `u64` are fixed-width unsigned big-endian values, so byte ordering
matches unsigned numeric ordering within a fixed prefix. The following composite
identities retain every namespace field:

```text
QualifiedRef = dataset_id:16 || dictionary_kind:2 || allocation_domain:16 || ordinal:8
SourceRecordId = dataset_id:16 || shard_id:4 || owner_epoch:8 || sequence:8
ProducerRequestId = producer_slot_id:16 || producer_epoch:8 || request_sequence:8
```

`DatasetId`, `AllocationDomainId` and `ProducerSlotId` reject all-zero bytes.
Dictionary ordinals, owner epochs, source sequences, producer epochs and request
sequences reject zero. Shards allow zero and the full `u32` range; nonzero
counters allow `u64::MAX`. Dictionary kinds accept only the assigned model values
1 through 9. Event IDs and descriptor digests are complete 32-byte values; a
shortened digest is never a definitive key.

## Closed domains

Lengths include the two-byte version/domain prefix.

| CF | Tag | Domain | Length | Fields after prefix |
|---|---:|---|---:|---|
| `default` | `0x01` | manifest | 2 | none; the value owns dataset/schema/owner identity |
| `default` | `0x02` | source stream | 30 | dataset16, shard BE32, owner epoch BE64 |
| `default` | `0x03` | allocation stream | 36 | dataset16, dictionary kind BE16, allocation-domain16 |
| `default` | `0x04` | dictionary mapping | 44 | full `QualifiedRef` |
| `default` | `0x05` | dictionary reverse candidate | 94 | dataset16, dictionary kind BE16, descriptor digest32, full `QualifiedRef` |
| `default` | `0x06` | event payload inventory | 50 | dataset16, event ID32 |
| `default` | `0x07` | event eligibility inventory | 92 | dataset16, event ID32, full visibility `QualifiedRef` |
| `default` | `0x08` | event receive minimum | 92 | dataset16, event ID32, full visibility `QualifiedRef` |
| `default` | `0x09` | provenance minimum | 218 | dataset16, event ID32, full endpoint, observer, access-scope and visibility `QualifiedRef`s |
| `default` | `0x0a` | immutable owner handoff (manifest key schema 2) | 42 | dataset16, owner-resource16, predecessor owner epoch BE64 |
| `facts` | `0x20` | immutable fact | 38 | full `SourceRecordId` |
| `facts` | `0x21` | pending admission batch | 38 | full first `SourceRecordId` |
| `receipts` | `0x30` | request detail | 34 | full `ProducerRequestId` |
| `receipts` | `0x31` | producer state | 26 | producer-slot16, producer epoch BE64 |

The 218-byte provenance key is the maximum version-1 key. The encoder owns a
fixed buffer of exactly that size and exposes only its initialized prefix.

## Namespace checks

Eligibility and receive keys require a visibility-scope reference from the outer
dataset. Provenance requires endpoint, observer, access-scope and visibility-scope
references, in that order, all from the outer dataset and with their exact
dictionary kinds. A reverse candidate's reference must match both its outer
dataset and dictionary kind. Encoding and decoding perform the same checks, so a
caller cannot create a cross-namespace key and corrupt stored bytes cannot bypass
the model invariant.

Dictionary mappings retain a full `QualifiedRef`. Reverse lookup uses the
descriptor digest only to find candidates: the candidate key also retains the
full reference, and its value retains complete canonical descriptor
bytes for equality. Hash equality alone never selects or merges a mapping. An
ordinal without dataset, kind and allocation domain is never a key. The
[initial bootstrap](ingest-bootstrap.md) fixes descriptor and inventory value
bytes for the first native dictionary write.

Event payload, eligibility, receive and provenance keys implement the exact
logical identities in the identity contract. Provenance disabled means no
`0x09` keys; another domain cannot preserve a hidden event/endpoint edge. The
fact key's big-endian source suffix orders records by dataset, shard, epoch and
sequence. Request detail and producer state remain operational identities, not
scientific fact identities.

The pending batch marker preserves its complete range and semantic digest even
after producer receipt-detail GC. Its [value format and bounded native reader](ingest-pending-format.md)
are independent of the request ledger. It is retained with pending source facts
until their archive protection and replay obligations permit reclamation.

## Compatibility boundary

These bytes are persistent. Changing field order, width, meaning or a domain tag
requires a new key-format version and an explicit compatible migration. Never
reinterpret an existing tag, silently accept trailing extension bytes, or open a
directory whose manifest declares another key format. Unknown future versions and
domains fail closed in this decoder.

The additive `0x0a` domain belongs to manifest key schema 2. Its
[handoff value and atomic migration](ingest-manifest-format.md#initial-owner-handoff-receipt)
retain all existing key bytes and change the manifest compatibility coordinate
before any successor writer can be enabled. An old schema-1-only implementation
must refuse that manifest; it cannot silently ignore the new domain and write.

The selected [repeatable handoff](ingest-manifest-format.md#repeatable-owner-handoff-selected-not-yet-implemented)
keeps this same42-byte key and its predecessor identity/order. It introduces a
closed version2 value at predecessor epochs2 and later, fenced by manifest key
schema3; it adds no key domain and changes no existing key bytes. The predecessor1
record stays byte-exact version1. A complete lineage scan checks the entire`0x0a`
domain, including extra or incompatible values; filtering by value version cannot
establish an exact history. This selection is not implemented or qualified yet.

The singleton manifest key does not prove that its future value is correct, that
the directory is exclusively owned, or that a batch was synchronized. The Rocks
adapter must separately require the exact column-family set and compatible
manifest, while the ingest owner must establish authority and put effects,
counters and request outcome in one WAL-enabled synchronized batch. This format
does not itself define dictionary descriptors, receipt outcomes, ACK/GC,
`CertifyExisting`, engine durability or process fencing.
