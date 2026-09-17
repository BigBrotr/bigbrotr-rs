# Current lossless event values

`bigbrotr-model` contains storage-free values consumed by the protocol verifier.
`EventId` and `PublicKey` are distinct 32-byte types, `Signature` is 64 bytes, and
their JSON spelling is exact lowercase hexadecimal. `NostrTime` carries the full
unsigned 64-bit seconds domain. Full signed JSON uses numeric tokens; clients must
use an integer-preserving parser. A JavaScript `Number` cannot preserve u64 maximum.
The separate metadata decimal-string mapping is not implemented yet.

`SignedEvent` preserves all seven fields: ID, public key, authored time, u16 kind,
ordered nested string tags, text content and signature. It is unverified even
after deserialization. Equality compares complete values, including signatures;
event identity/deduplication uses `EventId`, with payload equality and full
verification at admission. Content is never parsed as metadata implicitly.
Unknown fields are transport extensions and do not enter this signed value.

The [protocol reference](protocols.md) documents the first actual consumer and
[event schema](../../contracts/protocol/nip01-event.json). No projected partial
event is accepted as a complete independently verifiable event.

The remaining P01-T02 types, metadata schemas, canonical CBOR fingerprints,
qualified identities, receipts/proofs and engine mappings follow the owning
[data](../specification/data-contracts.md) and
[identity](../specification/identity-and-encoding.md) contracts. They are not
claimed implemented by this event slice; no archive schema is activated yet.
