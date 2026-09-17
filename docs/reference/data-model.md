# Current portable model values

`bigbrotr-model` contains storage-free values consumed by the protocol verifier.
`EventId` and `PublicKey` are distinct 32-byte types, `Signature` is 64 bytes, and
their JSON spelling is exact lowercase hexadecimal. `NostrTime` carries the full
unsigned 64-bit seconds domain. Full signed JSON uses numeric tokens; clients must
use an integer-preserving parser. A JavaScript `Number` cannot preserve u64 maximum.
Metadata counters, observed UTC microseconds, durations and topology generations
use canonical decimal strings, preserving the full u64/i64 domains. Numeric JSON
tokens, plus signs, leading zeros, negative zero and overflow are refused.

`SignedEvent` preserves all seven fields: ID, public key, authored time, u16 kind,
ordered nested string tags, text content and signature. It is unverified even
after deserialization. Equality compares complete values, including signatures;
event identity/deduplication uses `EventId`, with payload equality and full
verification at admission. Content is never parsed as metadata implicitly.
Unknown fields are transport extensions and do not enter this signed value.

The [protocol reference](protocols.md) documents the first actual consumer and
[event schema](../../contracts/protocol/nip01-event.json). No projected partial
event is accepted as a complete independently verifiable event.

`DatasetId`, `AllocationDomainId`, `ProducerSlotId` and `ProjectionGenerationId`
are distinct nonzero bytes16 values with lowercase hex spelling. They do not
imply a registry allocation or grant authority. Ordinals, owner/producer epochs
and source/request sequences start at one and provide checked advancement.
Topology generation retains u64, including zero, and checked advancement refuses
overflow; the registered topology determines whether a value is usable.

`QualifiedRef` binds dataset, the assigned numeric dictionary kind 1–9, allocation
domain and nonzero ordinal. `SourceRecordId` binds dataset/shard/epoch/sequence,
and its ordering follows that field order. `SourceCut` requires strictly sorted,
unique stream bounds and can check exact membership against a supplied topology.
A zero complete-through bound is a known empty stream; an omitted stream is an
error. This structural value does not certify committed batch boundaries or
handoff closure. `AdmissionBatch` validates nonempty contiguous membership in one
stream, including namespace and overflow checks; it is not a commit receipt.

`ClockQuality` requires an observer clock reference, resolution, explicit nullable
uncertainty and v1 anomaly bits 0–2 only. Missing uncertainty is rejected; null
means unknown and `"0"` is a measured zero uncertainty. No clock or elapsed-time
inference is made from a signed event's authored timestamp.

The first fingerprint recipes stream `bigbrotr/body/1` and
`bigbrotr/admission-batch/1` into SHA-256 using private direct minicbor encoding.
Body identity excludes signature and acquisition envelope; ordered tags, empty
strings, Unicode and every payload field remain significant. The model method
does not verify a signature; the protocol's `VerifiedEvent::body_digest` exposes
it for an already verified invocation. Frozen independent vectors compare exact
preimage bytes as well as hashes. These two recipes are not a complete profile
implementation. Current Serde mappings describe JSON metadata; binary/columnar
adapters must use explicit bytes/integers instead of reusing textual Serde output.

The remaining P01-T02 evidence variants, generated metadata schemas, other CBOR
recipes and bounded typed decoders, durable receipts/proofs and engine mappings
follow the owning
[data](../specification/data-contracts.md) and
[identity](../specification/identity-and-encoding.md) contracts. They are not
claimed implemented by this event slice; no archive schema is activated yet.
