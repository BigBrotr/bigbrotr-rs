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

`EventBody` factors the six shared fields and moves tags/content when split from
or joined to a `SignedEvent`. Joining a signature does not prove its validity or
scope authorization. `Fact` has a private immutable envelope and a closed
`FactPayload` variant for event payload, scoped eligibility, receive, provenance
and dictionary mappings. `event_id()` returns an optional event subject; non-event
dictionaries do not invent an event ID.
Construction checks the dataset and dictionary kind of every reference, including
the clock and import lineage. A correction cannot target itself or a later
sequence in its own stream. Cross-stream causal order, earlier observed time,
target family/key and dictionary allocation still require the storage owner.
`dictionary_refs()` returns field occurrences; dependency owners deduplicate them.

`EventIntent` requires a local observation for network input and a dataset-lineage
reference for import input; import cannot carry a local observation. The current
provenance policy must separately validate presence/absence of witness context.
Its request fingerprint binds the complete supplied signature and intent even
when effects would deduplicate. `ProducerRequestId` cannot contain the zero values
reserved solely for the read-only certification fingerprint. These values grant
no current policy, producer authority, durable outcome or existing-effect proof.

`EvidenceIntent` is the initial dictionary-only `admit-evidence` operation. It
moves a nonempty caller-ordered vector of closed `DictionaryEntry` values, requires
one dataset and a visibility-scope reference, and refuses a repeated QualifiedRef
without sorting or deduplicating the intent. Separate qualified references remain
distinct even when descriptors are equal. Callers supply entry, encoded-intent
and validation-scratch limits; scratch accounting is requested reference-vector
storage rather than allocator overhead or an RSS ceiling. `request_digest` streams
the exact producer-request recipe, while `encode_request_preimage` additionally
checks the caller's full-output limit before allocation. The initial value has no
event/signature/generic payload variant. Prepared allocation references, current
policy, producer authority, shared runtime grants, retained dependencies and the
atomic durable result remain admission-owner checks.

`SourceDependencies` moves a nonempty, strictly source-ID-ordered vector of
`FactRef` values; it refuses duplicates (including conflicting digests) and
foreign datasets without silently sorting or deduplicating. `Receipt` binds the
strict producer request and digest to this closure and a `ProtectionContext`.
Its `NewFacts` constructor requires a contiguous `AdmissionBatch` whose every
member occurs with the same digest in the closure; `AlreadyDurable` has no new
batch. Closure validation traverses the ordered inputs without a second set.
New-fact closure refuses a dependency beyond its batch end in the same stream;
it does not compare sequence numbers across different streams.
`ExistingProof` instead binds a certification intent digest and the same context/
closure, with no request identity. `EventEffectBasis` keeps these two cases
distinct so a proof-only checkpoint has no mutation sequence to acknowledge.

The context carries dataset, topology, `PolicyEpoch`, `RecoveryGenerationId`
and the currently supported `local-synced` protection class. Policy epochs retain
full u64/checked advancement; recovery generations retain all bytes16 values.
Neither a context nor a proof digest verifies registration, authority, sufficient
effects, physical persistence, retention or restore closure. Those remain the
storage/producer owners' responsibility. Receipt/proof/basis values have no generic
Serde deserialization. The [closed operational receipt codec](ingest-receipt-format.md)
serializes the existing receipt with explicit version and resource limits;
storage and transport readers must still validate the owning protocol and authority.

The fingerprint recipes stream `bigbrotr/body/1`, `bigbrotr/fact/1`,
`bigbrotr/producer-request/1`, `bigbrotr/admission-batch/1`,
`bigbrotr/existing-proof/1` and `bigbrotr/dictionary/1` into SHA-256 using
private direct minicbor encoding.
Body identity excludes signature and acquisition envelope; ordered tags, empty
strings, Unicode and every payload field remain significant. The model method
does not verify a signature; the protocol's `VerifiedEvent::body_digest` exposes
it for an already verified invocation. Frozen independent vectors compare exact
preimage bytes as well as hashes, including every implemented fact family,
network/import intents and the reserved certification intent. These recipes are not a complete profile
implementation. Current Serde mappings describe JSON metadata; binary/columnar
adapters must use explicit bytes/integers instead of reusing textual Serde output.

`Fact::encode_preimage` writes the exact profile-1 domain-prefixed fact bytes,
checking the output size before reservation. `decode_preimage` takes those bytes,
an independently supplied expected digest and explicit local limits. It accepts
only the five implemented v1 fact families, including closed dictionaries. It checks
the digest, required array lengths, typed scalar/reference/nullability rules, entire input consumption
and exact canonical re-encoding. Unknown profiles/kinds/versions/enums, indefinite
values, unsupported CBOR types, trailing bytes and non-shortest encodings fail.
The canonical comparison streams against the original input without a second
encoded output buffer. Digest equality does not grant source authority.

Default local limits are 2 MiB encoded input, 8 MiB requested allocations, 65,536
tag arrays/string elements and 256 KiB per retained UTF-8 content/tag string.
Lengths and checked allocation charges precede `try_reserve_exact`; malformed
declared arrays cannot reserve more children than remaining input bytes. These
are codec limits, not runtime grants or hard RSS bounds: input buffers, allocator
overhead, simultaneous work and native staging remain the adapter's responsibility.
The storage owner must bind expected digests to authoritative source identities,
reverify complete events where required and resolve all referenced mappings.
This preimage codec does not activate an archive manifest or storage format.

`DictionaryEntry` binds one full qualified reference to a closed immutable
[descriptor](../specification/dictionary-descriptors.md): event, visibility,
endpoint, observer, access scope, event verification or dataset lineage. Mapping
and fact fingerprints have distinct domains; equal mapping digests are not
authority. `require_same_mapping` compares full values and refuses reference or
value conflicts. Internal method/scope references require the mapping's dataset
and exact kind; foreign source dataset attribution in lineage remains valid.
Route and HTTP-resource descriptor kinds are not implemented and fail closed.

`DictionaryEntry::encode_preimage` and `decode_preimage` use the same bounded
profile with `DictionaryDecodeLimits` (an alias of `FactDecodeLimits`). Labels
are capped at 64 bytes, endpoint text at 8192 bytes, before allocating. Fixed map
keys/enums are borrowed, nullable fields are mandatory and unsupported semantic
versions are refused. All-unknown observer attribution remains explicit nulls.
The model's structural endpoint wrapper does not replace the protocol normalizer
or classify secrets/permissions; those are mandatory admission consumer checks.

The remaining P01-T02 evidence variants, generated metadata schemas, other CBOR
recipes/decoders, actual receipt/proof authority and engine mappings
follow the owning
[data](../specification/data-contracts.md) and
[identity](../specification/identity-and-encoding.md) contracts. They are not
claimed implemented by this event slice; no archive schema is activated yet.
