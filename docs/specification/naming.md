# Shared names and semantic distinctions

Status: selected vocabulary for schemas and public component contracts; no Rust
API has shipped. Use these names when implementing the linked tasks. Private
helper names remain an implementation/review decision: predeclaring every helper
would create an unnecessary second copy of the code.

## Identity and field names

| Canonical name | Exact meaning; do not conflate with |
|---|---|
| `DatasetId` / `dataset_id` | Scientific identity domain. `archive_namespace` in older prose is this value, not an additional source-ID field. |
| `SourceRecordId` | `(dataset_id, shard_id, owner_epoch, sequence)`, one immutable fact. These four serialized field names replace older `ownership_epoch`/stream aliases in formal declarations. |
| `SourceCut` | `dataset_id`, `topology_generation` and complete bounds keyed by `(shard_id, owner_epoch)`. Filter before reducing corrections. A timestamp is not this value. |
| `ProducerRequestId` | `(producer_slot_id, producer_epoch, request_sequence)`: operational idempotency, separate from permanent source facts. |
| `ProcessIncarnationId` | Fresh process nonce. It is neither a persistent producer slot nor the archive owner epoch. |
| `RoleInstanceId` / `role_instance_id` | Configured supervised role instance. A replacement process has a new ProcessIncarnationId while retaining the role identity where the deployment intends continuity. |
| `ProjectionRowId` | `(projection_generation, source_record_id, discriminator)` for a specific projected subrow. |
| `EndpointId` / `endpoint_id` | Versioned Nostr relay identity. An arbitrary HTTP metadata resource is not a relay. |
| `HttpResourceId` / `http_resource_id` | Versioned identity of a guarded HTTP resource descriptor, used by typed non-relay evidence. |
| `ObserverId` / `observer_id` | Stable attributed measurement authority/vantage; not a process, socket or proof of independent ownership. |
| `access_scope_id` | Stable authorization context used to contact a source. No credential bytes and no per-challenge identity. |
| `visibility_scope_id` | Which readers/derivations may use a fact. It differs from the source access context. |
| `acquisition_scope_id` | Typed acquisition descriptor binding the selected endpoint/input filter and coverage context; not a read visibility grant. |
| `measurement_scope_id` | Typed observer/route/access/method-profile context for scheduled measurements; child checks do not create another independent attempt. |
| `analysis_scope_id` | Typed target population/window/profile/viewpoint and visibility context; generation identities bind its particular input/configuration version. |
| `ObjectId` | Qualified visibility reference plus `content_digest`; "document namespace" describes that visibility domain, not another hidden tenant. A compact scope ordinal includes its dataset/dictionary/allocation domain. |
| `payload_digest` | Digest of the versioned typed fact encoding; not the original document's digest. |
| `body_digest` | SHA-256 of the shared event payload under bigbrotr/body/1; excludes private admission envelope, ordinal and signature. |
| `content_digest` | Digest of unchanged document bytes; independent of file position or parsed fields. |
| `created_at` | In signed-event schemas, NostrTime with the protocol's unsigned integer domain. Administrative job/artifact creation uses ObservedTime in its own schema; the shared spelling does not permit casting between the two. |
| `first_admitted_at` | Restricted immutable time this identity domain first durably admitted the event; never substituted for scoped acquisition metadata. |
| `first_eligible_at` | Immutable first admission into a particular visibility scope, attached to that scope's valid signature representative. |
| `received_at` / `first_received_at` | A local attributable observation / its cut-relative earliest authorized value; minima are independently retained per event/visibility scope. |
| `recorded_at` | When an immutable fact was recorded; not a replacement for when an action was measured. |
| `attempt_id` | One actual external request/compound attempt; child evidence and persistence retries do not create independent attempts. |
| `selection_id` | Immutable scientific selection/method descriptor; different from mutable job progress. |
| `generation_id` | Published derived result identity; not interchangeable with an input source cut. |

All compact ordinal references carry a dictionary domain in the enclosing schema
or explicit qualified reference. The portable form identifies dataset, dictionary
kind and original allocation domain as well as the ordinal. A bare integer is
never a cross-dataset or cross-shard identity. Shard migration preserves that
original mapping; it cannot assign a new meaning to an old ordinal.
[Identity and encoding](identity-and-encoding.md#portable-allocation-and-topology)
selects `[dataset_id:bytes16,dictionary_kind:u16,allocation_domain:bytes16,ordinal:u64]`,
checked allocation, topology handoff and restore rules. P01-T02/P02-T03 implement
that contract; they do not choose an incompatible new representation.

Object references follow the same rule: an enclosing dataset/domain may qualify
a local `visibility_scope_id`, but a standalone export/import carries that domain
or a fully qualified reference. Two datasets' equal bare scope ordinals cannot
alias objects or reveal another scope's object existence. Import maps a source
scope through an explicit local visibility policy and preserves source lineage;
it never grants local access solely because an imported scope number matches.

`EventId` is the semantic type. Nostr wire JSON uses the mandated key `id`;
analytical rows use `event_id`. Preserve `pubkey`, `created_at`, `kind`, `tags`,
`content`, `sig` at the signed-event boundary. A "note" is an event of a selected
kind, not another universal archived object type. A public key represents a
cryptographic identity, not proof of one human person.

## Responsibilities and Rust boundaries

Use `Synchronizer` for live/history/reverification acquisition, `Monitor` for
measurements, `Indexer` for source-to-serving projection, `Deriver` for independent
fact/current-state targets, and `Ranker` for viewpoint/profile score jobs.
`ArchivePublisher` writes archival snapshots. `Nip66Publisher`/`Nip85Publisher`
create signed network outputs. `ArchiveRelay` serves the archive through Nostr.
Avoid an ambiguous `Archiver` name for all three responsibilities.

Existing prose capitals name concepts, not function casing. Rust functions and
methods use `snake_case`, types and enum variants use `UpperCamelCase`, and
constants use `SCREAMING_SNAKE_CASE`. Follow the
[Rust API naming guidelines](https://rust-lang.github.io/api-guidelines/naming.html):
conversion names communicate borrowing/copying/ownership, and ordinary field
accessors do not need mechanical `get_` prefixes. Prefer domain verbs such as
`admit`, `resolve_receipt`, `publish_archive`, `query` and `enqueue` at their
appropriate owner; their result types expose ambiguity and durability.

`Receipt` is the shared durable admission receipt, carried by successful durable
variants of `AdmissionResult`; it is not that whole success/failure union. Do not
introduce a competing `DurableReceipt`. Archive commits and projection
acknowledgements have different types because they certify different boundaries. The
[component table](component-contracts.md#typed-boundaries) owns those contracts.
An `AdmitEvidence` operation has the same explicit failure/retired-request
possibilities as event admission, not an unconditional successful receipt.

`ExistingProof` belongs to the distinct nonmutating `CertifyExisting` operation.
It binds retained dependencies and current authority/protection, is operational
and private, and has no ProducerRequestId or changed-request conflict promise.
It is not a Receipt, a new scientific observation or a relay-coverage certificate.

Relation/resource names use descriptive `snake_case`, normally plural for row
collections; established target/access-path names remain explicit in the catalog.
Time/duration units belong in the type/schema, and untyped numeric duration fields
use a unit suffix.
SQL table names are not automatically API resource names. NIP-mandated wire names
and versioned existing profiles remain exact even when internal names differ.

Public configuration exposes useful policy/resource choices, not every internal
constant. A versioned finite-job profile can own numerical limits captured at
submission, while hot host grants constrain execution. Unknown keys and identity
aliases are rejected rather than silently accepted as two equivalent settings.
During implementation, update schema, adapters, examples, fixtures and generated
reference together when a public name changes.
