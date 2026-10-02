# Closed dictionary descriptors v1

This is the field-level A04 contract for the first signed-event admission
consumer, refining [archival data](data-contracts.md#a04--endpoint-observer-scope-and-method-dictionaries)
and [identity/encoding](identity-and-encoding.md#portable-allocation-and-topology).
The model and CBOR codecs implement these closed values. Allocation, atomic
persistence, source/visibility authorization and archive/export closure still
belong to their storage and policy consumers.

## Mapping and compatibility

A `DictionaryEntry` binds the complete `QualifiedRef` to
`[normalization_version, descriptor_schema, descriptor_version, descriptor_value]`.
All schemas selected here use normalization version 1 and descriptor version 1.
Endpoint normalization is the existing `bigbrotr-endpoint/1` rule. For the other
schemas, normalization version 1 preserves the exact typed values without case
folding, trimming or normalization. The mapping fingerprint remains the exact
`bigbrotr/dictionary/1\0` recipe; a containing `dictionary/1` fact separately binds
its source identity, visibility and recorded time under `bigbrotr/fact/1\0`.
Neither digest is an authorization token or proof that a mapping was committed.

The schema and dictionary kind must agree exactly. Unknown kind, schema, enum,
normalization version, descriptor version or required field is refused. Known
portable kind 6 (route) and kind 7 (HTTP resource) have no descriptor codec in this
slice and are refused rather than represented by arbitrary JSON. A later real
producer must define their closed schemas. New semantic fields require a new
version; old bytes/digests remain readable under their original version.

Values are definite CBOR maps with the exact keys below in their listed order,
which is lexicographic order of complete deterministic CBOR key encodings. All
keys occur exactly once; unknown, missing, duplicate and reordered keys fail the
canonical preimage decoder. Every nullable field is present with `null` when
unknown. Empty text is not a replacement for unknown attribution.

A `Label` is 1–64 ASCII bytes, starts with an alphanumeric byte and otherwise
contains alphanumerics, `.`, `_`, `-` or `+`. It is a stable attribution identifier,
not free-form prose, an endpoint, path, token or credential. Operators/consumers
must ensure labels are safe for their selected scope: syntax cannot distinguish
a secret that happens to use these characters. Labels are preserved exactly;
their Debug representation is redacted. No dictionary contains live credentials,
secret references, process IDs, worker IDs or per-delivery identifiers.

## Exact closed values

| Kind / `descriptor_schema` | Fields in canonical map-key order |
|---|---|
| 8 / `event` | `event_id: bytes32` |
| 4 / `visibility` | `class: Label` |
| 1 / `endpoint` | `url: text` |
| 2 / `observer` | `clock: ClockSource|null`, `vantage: Label|null`, `software: Label|null`, `method_id: QualifiedRef|null`, `public_key: bytes32|null`, `route_class: RouteClass|null`, `software_version: Label|null` |
| 3 / `access-scope` | `class: AccessClass`, `label: Label|null`, `visibility_scope_id: QualifiedRef` |
| 5 / `event-verification` | `input: text`, `algorithm: text`, `capability: text`, `output_unit: text`, `failure_taxonomy: text`, `algorithm_version: u16`, `protocol_revision: text`; all fixed values below |
| 9 / `dataset-lineage` | `format: Label`, `format_version: u16`, `source_dataset_id: bytes16|null`, `source_artifact_digest: bytes32` |

`event` binds the exact event ID to its allocated compact reference. The mapping
alone does not verify the event body/signature or grant any scope eligibility.

`visibility.class` is a safe, stable attribution label naming a policy class.
The literal `public` preserves the already frozen v1 vector byte for byte. Other
labels may name restricted classes. Neither this text nor a numeric reference
authorizes access: current policy resolves the qualified scope and grants. Do not
allocate a new scope per relay, connection or delivery, or merge separate scopes
solely because their labels match across namespaces.

`endpoint.url` is at most 8192 UTF-8 bytes and contains the exact normalized URL,
including meaningful scheme, port, path and query distinctions from the single
[URL authority](../architecture/acquisition-services.md#identity-v1).
The model applies only bounded structural ws/wss checks and refuses empty
authority, userinfo, authority percent escapes, fragments, backslashes, whitespace
and controls. The application must parse through `bigbrotr_protocol::Endpoint`
and require byte equality with its normalized result before accepting a mapping.
There is no second normalizer in the model and successful decoding does not
establish canonical URL identity, dialing authority or safe publication. Queries
are preserved, not stripped or re-ordered. Credential-bearing URLs must be refused
by the source/credential policy; private/query-bearing URLs remain restricted
unless explicitly permitted. Debug output redacts the entire endpoint.

`observer` attributes a stable measurement authority/clock without a hidden relay
edge. `ClockSource` is exactly `system-utc` or `external-utc`; null means unknown.
It does not claim synchronization or zero uncertainty: each occurrence retains
its `ClockQuality`. `RouteClass` is exactly `public`, `private`, `tor`, `i2p` or
`lokinet`; null means unknown. It describes vantage, not a dial grant or adapter
availability. `method_id`, when known, must reference kind 5 in this dataset.
`public_key` is configured public attribution, not a signature over the descriptor
or a claim of independent observers. Software/name/version and vantage remain
null if unknown; local producers provide their actual attribution rather than
inventing values for imported evidence. A changed attribution produces a new
immutable mapping; it cannot rewrite earlier observations.

`access-scope.class` is exactly `public` or `protected`. Public access has null
`label`; protected access requires a non-null stable opaque context label. This
avoids equating unspecified protected contexts or retaining token/challenge bytes.
`visibility_scope_id` is kind 4 in this dataset and describes the permitted
historical attribution scope; current policy still governs export and use. The
mapping is independent of a socket, transient authentication challenge or process.
It does not prove that an authentication attempt succeeded.

The initial method descriptor is deliberately one specific implemented method,
not a generic method registration framework. Its exact fields are:

| Field | Fixed value |
|---|---|
| `input` | `nip01-event/1` |
| `algorithm` | `sha256-bip340` |
| `capability` | `event-id-and-signature` |
| `output_unit` | `verification-result` |
| `failure_taxonomy` | `bigbrotr-nip01-verifier/1` |
| `algorithm_version` | `1` |
| `protocol_revision` | `a2494f4f81d46684e5814a9bf35e2b1df978f955` |

This method binds the [pinned NIP-01 semantics](../architecture/protocol-evolution.md#source-baseline-and-reading-coverage):
strict bounded complete event decoding, SHA-256 of the canonical six-element Nostr
preimage, equality with the supplied ID, and BIP340 verification of the actual
supplied signature/public key. It claims neither relay availability nor provenance,
source permission or other NIP conformance. Output is a verification classification,
not latency or a numeric score. Failure taxonomy distinguishes `invalid-parser-limits`,
`wire-limit`, `depth-limit`, `node-limit`, `string-limit`, `parser-arena-limit`,
`event-size-limit`, `invalid-json`, `empty-tag`, `event-id-mismatch`,
`invalid-public-key` and `invalid-signature`; limits are policy/resource exclusions,
not evidence of a bad signature. The success case supplies a verified invocation.
Rust dependency versions are build evidence, not scientific capability identity.
A changed semantic baseline or taxonomy needs a new descriptor version.

`dataset-lineage` attributes the retained input artifact without exposing a local
path, temporary job, transport credential or fabricated local observation. Format
version is in 1–65535. `source_dataset_id` is an actual nonzero dataset ID when
supplied, otherwise null; it is source attribution and can be foreign to the local
dataset. A format label/version is an attributed claim, not a promise that this
binary implements that importer. The owning importer must validate the declared
format and independently compute/verify the digest of the complete retained source
artifact. A raw JSONL artifact normally has no source dataset identity. Importing
an event does not turn the artifact or its unsigned metadata into a signature.

## References, equality and closure

`DictionaryEntry::new` checks the mapping kind and all internal qualified reference
kinds/datasets. A containing fact also requires the mapping's dataset to equal its
source and visibility dataset. A lineage source dataset is not a local qualified
reference and is not incorrectly forced into that namespace.

`Fact::dictionary_refs()` includes envelope visibility and referenced method/scope
fields. It excludes the mapping's defined reference as a dependency on another
mapping. A visibility mapping can legitimately name its own scope in its envelope;
the batch/closure resolver must recognize the mapping provided in the same atomic
batch instead of requiring a previously stored copy or silently dropping scope.
Repeated field references remain repeated until the storage owner deduplicates.

The same qualified reference with different complete mapping bytes is a conflict,
including after restart/import. `require_same_mapping` compares the full immutable
values; a different reference is a distinct namespace even if descriptors are equal.
Hashes are candidate/integrity checks, not definitive equality or allocation.
Existing dictionary/1 fingerprints include the qualified reference and must not be
mistaken for an unqualified descriptor fingerprint. Allocation/restore must retain
mapping and high watermark in the same transaction, refuse exhaustion and never
reuse old identity; this codec does not implement that transaction.

Required dictionary facts precede or share the visible cut of their consumers.
Public export must authorize every selected descriptor; it cannot disclose a
private endpoint/method/observer merely to complete a join. Provenance-off intake
must not allocate an endpoint/access mapping or edge for an event. The observer
and visibility descriptors do not provide a substitute hidden endpoint field.

## Validation and implementation boundary

Independent literal vectors cover each descriptor, known and null observer
attribution, access classes, and native/JSONL source lineage, in both dictionary
and containing-fact domains. Their small fixture assembler uses no product or CBOR
library and checks SHA-256 independently with OpenSSL. The original visibility
vector is retained unchanged. Tests cover full mapping conflicts/namespaces,
canonical maps/nullability, truncation, oversized lengths and allocation limits.
The bounded decoder borrows fixed labels/keys, checks descriptor string limits
before allocation and compares canonical re-encoding against input without another
output buffer. Descriptor nesting is fixed; arbitrary nested extension values
are refused, not traversed recursively.

These are model/codec results. P02-T02 step 2 and P02-T03 step 1 still require the
real atomic allocation/admission consumer, process recovery and independently
resolved archive/export closure before their practical acceptance is complete.

## Post-bootstrap dictionary admission (implementation in qualification)

The initial owner may append fixed-reference `EvidenceIntent` requests after
request1. This uses the same v1 inputs, facts, keys and receipts; applied SQL and
original bootstrap bytes remain unchanged. The next producer request, complete
mapping/visibility values and allocated references must be retained before PG
reservation. Preview validates the complete fixed plan, including the real
recorded timestamp's variable-width encoding, before staging or PG reservation.
It emits no facts and retains the same exclusive native owner across both phases;
dispatch rechecks the plan and current authority. An ambiguous retry cannot rerun
interning to select new identities.
Event mappings remain owned by event admission. Dictionary-only input cannot
allocate them or confer event eligibility, acquisition permission or job progress.

New references advance each current allocation domain contiguously; existing
references require the complete immutable mapping and the same fact visibility.
Mappings, reverse candidates, facts, allocation/source/producer counters, pending
membership and the receipt commit in one synchronized batch. All-existing input
produces AlreadyDurable without another scientific fact. The first append profile
requires every transitive dependency in the exact target envelope. A new visibility
may define itself; other new dependencies must precede their consumers. The two
original installed bootstrap mappings keep their original public envelope. A
usable private observation scope must be explicitly allocated and selected; it
cannot replace an already registered enrollment/scope/job identity silently.

Positive replay validates the original receipt's complete input effects and
closure, retaining its original time, membership and dependencies. Bootstrap
replay must tolerate later allocation advancement without rewriting its receipt;
post-bootstrap evidence needs its own closing-producer recovery path. Dedicated
dictionary scratch and receipt limits cover more than five mappings. Read-only
recovery reserves its closure/receipt scratch separately from write construction;
existing event-only recovery grants remain sufficient for their original profile.
The [append evidence](../implementation/evidence/p02-dictionary-append-native-20261002.json)
records actual reservation/outcome COMMIT loss, cancellation and fresh-Control
historical recovery. Preparation/interning, native interruption of this append
path and the installed observation/archive consumer remain required before this
responsibility or P02-T03 Step1 can be marked complete.

### Initial-owner dictionary preparation

The next installed observation consumer prepares a caller-ordered set under an
explicit, already committed visibility reference. Preparation borrows the same
exclusive native owner, does not write or reserve a producer request, and retains
its bounded memory grant with the sealed `EvidenceIntent`. Sealing fixes values
and references only. The existing pre-staging preview remains authoritative for
the chosen request sequence, window, real recorded time and complete closure.

Reverse hashes only select bounded candidates. Reuse requires full canonical
descriptor equality, a matching primary dictionary fact in the exact target
envelope, and the current allocation namespace. Different envelopes require
distinct mappings. Candidate exhaustion is a resource error, never absence;
malformed matching mappings fail closed. Prepared identical descriptors reuse one
entry; multiple new descriptors of one kind receive distinct consecutive
provisional ordinals. A failed preparation cannot advance a durable counter, and
a failed individual intern cannot consume a provisional ordinal.

This consumer does not intern visibility labels or event IDs. Visibility remains
an explicit identity and cannot silently replace an accepted scope; event mapping
allocation remains atomic with event admission. Owned descriptor capacities,
entry tables, encoding/decoding scratch and bounded candidate work count against
the preparation limits. Cancellation abandons only the read-only plan. Once an
input is staged, retries use that complete original input rather than preparing
different references after an ambiguous outcome.

[Native and connected preparation evidence](../implementation/evidence/p02-dictionary-append-native-20261002.json)
qualifies18ed28a with29 selected native tests andc0fb44e with an actual PostgreSQL/
RocksDB/socket consumer. The latter commits explicit private3, prepares four
attribution mappings without writes, appends/reuses their fixed intent, reopens
under the same owner and acquires a signed event with forged/duplicate controls.
All50 native rows, five original inputs/receipts and exact67-table transitions
match independent expectations. This closes the bounded preparation consumer;
installed finite execution, archive/export closure and successor remain required.
