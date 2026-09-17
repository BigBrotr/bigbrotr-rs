# Event eligibility, portable identity and persistent fingerprints

Status: selected paper contract for S01/S02 and P00-T06; no product adapter or
engine durability qualification. This document owns the choices below. Existing
summaries must use these rules rather than retain the former globally chosen
signature/metadata interpretation. The isolated [probe](../../experiments/identity-contract/README.md)
and [evidence](../research/evidence/identity-contract.json) test the stated model,
not the future Rust implementation.
The separate [real signed corpus](../../experiments/identity-contract/signed-fixture.json)
and its [verification record](../../experiments/identity-contract/signed-fixture-evidence.json)
add actual signature/ID checks and cross-component expected answers; they still
do not execute the future storage/read/export adapters.

## One payload, independently attributable eligibility

The selected representation factors the complete signed event without losing
any signed field:

* `EventPayloadV1`: `event_id: bytes32`, `pubkey: bytes32`, `created_at: u64`,
  `kind: u16`, `tags: array<array<text>>`, `content: text`. Its primary key is
  `(dataset_id,event_id)`. Exactly one logical payload exists in that domain.
* `EventEligibilityV1`: `event_id: bytes32`, `body_digest: bytes32`,
  `sig: bytes64`, `first_eligible_at: i64`, `origin: enum(network,import)`,
  `import_lineage: QualifiedRef|null`, plus the source envelope whose
  `visibility_scope_id` supplies the scope. The logical key is
  `(dataset_id,event_id,visibility_scope_id)`. The signature is the first valid
  representative admitted **in that scope**, not the first signature elsewhere.
  `import_lineage` is required for import and null for network.
* `EventReceiveV1`: `event_id`, `received_at: i64`, `observer_id: QualifiedRef`,
  `clock_quality: ClockQualityV1`, `corrects: SourceRecordId|null`, plus its
  scope-qualified source envelope. Its logical minimum key is
  `(dataset_id,event_id,visibility_scope_id)`; the selected row supplies its
  observer/clock. A first network observation or strictly smaller receive time
  emits a fact. Equal-time later observations do not rewrite the representative.
* `ProvenanceV1`, only when enabled: `event_id`, `endpoint_id: QualifiedRef`,
  `observer_id: QualifiedRef`, `access_scope_id: QualifiedRef`, `received_at`,
  `clock_quality`, `verification_method_id: QualifiedRef`,
  `corrects: SourceRecordId|null`, plus its envelope. Its exact minimum key is
  `(dataset_id,event_id,endpoint_id,observer_id,access_scope_id,visibility_scope_id)`.
  Adding the visibility dimension prevents an inaccessible earlier witness from
  suppressing a permitted first witness. It is not a new endpoint or tenant.

The body digest binds the six `EventPayloadV1` fields below, excluding any local
ordinal or acquisition envelope. `archive.events` stores that shared value and
its internal admission lineage; `archive.event_eligibility_facts` stores the
representatives. `serving.events` and `serving.event_eligibility_facts` retain the
same separation. A full signed-event response/export joins an authorized
eligibility representative to the payload and returns all seven protocol fields.
There is no unsigned-event mode or content omission implied by factoring.

Ingest alone establishes eligibility after full verification, source policy and
scope admission. A producer cannot manufacture a public grant by passing a
dictionary number. The body store is internal; existence, its original envelope,
global admission time and event ordinal are not themselves authorization.
`first_admitted_at` remains the restricted identity-domain first admission time;
ordinary scoped metadata instead exposes `first_eligible_at` and the minimum
authorized `first_received_at`. Never relabel one as the other.

Every required new body, eligibility, receive correction, provenance correction,
dictionary mapping and mutation receipt is one atomic synced admission write set.
Its contiguous source sequence range is complete or absent. Ordinary duplicate
delivery emits none of these facts. Provenance off allocates no endpoint/access
pair, witness inventory or imported relay sidecar; eligibility names visibility,
not a disguised source endpoint. A visibility descriptor must be a stable policy
class, never one newly allocated identifier per delivery/connection/relay.

This choice preserves one copy of large content/tags and adds a small signed
representative only for each distinct permitted scope. Scoped full-body copies
would simplify raw-table isolation but duplicate every body and change the
selected identity contract. A global first signature plus a scope bit would save
64 bytes per extra scope but disclose a signature first seen privately. Neither
alternative is selected. Direct database access remains administrative authority.

### Signatures and effect transitions

Nostr's ID authenticates its canonical JSON preimage, which excludes `sig`.
Validate the actual supplied signature every time, or use a verification cache
bound to exact canonical bytes, pubkey, signature and verification-policy version.
A valid alternate signature is the same event. A new scope retains the alternate
signature actually verified in that scope; a later valid variant in the same
scope creates no new representative. Preserve the first valid signature in its
original eligibility fact permanently. Explicit wire-fidelity retention is the
only default-independent way to retain every later wire variant. An invalid
variant creates neither eligibility nor receive/provenance facts. Distinct
canonical bytes with the same claimed ID fail verification or trigger a halted
collision investigation; never overwrite or alias them silently.

At a fixed source cut, authorization selects scopes **before** body metadata,
minimum corrections, current-coordinate winners or deletion requests are reduced.
For a caller authorized to several scopes, choose the first representative by
`(first_eligible_at, SourceRecordId)` among those scopes; the source-ID tie break
is deterministic, not proof of physical observation order. A caller may request
a particular authorized scope. Source IDs compare as unsigned bytes/integers in
their field order; wall-clock regression never changes an existing representative.

| Arrival or change | Durable effect and visible answer |
|---|---|
| Private E at 10 with sig A, public E at 20 with sig B | One payload, two eligibility facts. Public gets B, first eligibility 20 and public receive minimum 20. Private A/time 10/envelope are excluded. Same with provenance off. |
| Public E at 10 with A, private E at 20 with B | Public stays A/10. Private gets B/20. Neither representative is replaced by the later one. |
| Import E then network E in same scope | Import eligibility/representative remains. First network receive emits a new fact; optional witness is independently admitted. Imported timestamps never become local receives. |
| Private import then public network | New public eligibility, public first receive, optional public witness. No private import lineage/time/signature in the public result. |
| Public network then private import | Public result unchanged; private import may create eligibility with its own lineage, never a new local receive. |
| Known scope, later receive, already known edge | No scientific fact; strict Admit may create its operational request outcome, or CertifyExisting can return the existing dependency proof. |
| Known scope, earlier receive or witness | New immutable correction, NewFacts receipt. Old cut still selects the previous minimum. |
| Known event, new authorized scope or enabled new edge | NewFacts, even if no new payload; provenance off still creates a new eligibility fact. |
| Public grant restricted at a new policy epoch | New ordinary reads, cached pages and publications fail/rebind under that epoch. Historical facts remain retained; authorization does not mutate them. |
| Policy grant later restored | Existing eligible facts may be used again if policy permits. No synthetic new receive or signature. Explicit historical declassification must create an attributed grant/migration, not forge a network observation. |
| Replay/restore | Reproduce the same payload, eligibility signatures, source IDs and cut-relative minima. Replay is not a fresh encounter and cannot reset admission/eligibility times. |

Current-coordinate selection is: source cut → permitted source population →
winner by protocol ordering → authorized deletion/ordinary expiry → ordinary
query predicates. A private newer replaceable event cannot suppress an older
public winner. A private deletion cannot suppress a public event. If the winning
public event is deleted or expired, ordinary current-state reads return no
winner; they must not resurrect an older event by filtering expiry/deletion
before winner selection. History permissions may expose retained originals and
deletion requests with explicitly historical semantics. Body acquisition and
historical eligibility are unaffected by NIP-09 or expiry. Administrative
restrictions remain an independent current authorization gate.

### Export, imports, replay and recovery closure

A full native research export contains the shared payload value and at least one
permitted eligibility representative for each event. It includes only permitted
eligibility/receive/provenance/claim facts and their required safe dictionaries.
The payload's original admission envelope/ordinal is not required for signed
verification and must not enter public closure through a private first encounter.
`body_digest` permits validating the payload value independently of that envelope.
The portable value family is `event-payload-value/1`, whose exact ordered fields
are `[event_id,pubkey,created_at,kind,tags,content,body_digest]`. It has no source
envelope/ordinal and does not pretend to preserve the private internal body fact's
`payload_digest`. The eligibility facts retain their own original source IDs and
digests. The manifest declares `event-payload-value/1` + `event-eligibility/1`; an old reader that
requires the former single-row representation fails preflight. JSONL emits one
complete signed event per ID using the selected permitted representative and
declares that acquisition/provenance metadata is outside that format.

Research import verifies complete events, maps source scopes by explicit local
policy and admits local eligibility. Source eligibility is an imported source
claim, not a local grant or receive fact. Imported dataset/record identity remains
attributed source lineage. A local scope with the same bare ordinal grants no
access. A restore instead preserves the dataset and original identities, all
eligibility facts, compact minima, pending/archive dependencies, dictionaries,
allocation counters, topology, receipts, producer checkpoints and restrictions.
A backup missing any dependency of acknowledged progress is unusable for that
progress; roll the affected producer checkpoint back or restore its dependency.

Public fact lineage may expose its own opaque SourceRecordIds and safe descriptor
identities under the existing export contract. This does not promise hiding
sequence gaps, traffic volume or timing side channels. It forbids disclosing
private event fields, original private signatures/times/envelopes, endpoints,
credentials and private-input effects on public scientific results. Exported
closure must not include an inaccessible dictionary merely to satisfy a join:
refuse the affected optional metadata/export profile, or select a permitted
profile; never silently publish the descriptor or falsify attribution.

## Portable allocation and topology

`DatasetId`, `AllocationDomainId`, `ProducerSlotId` and `ProjectionGenerationId`
are 16 opaque bytes. Use independently generated 128-bit random values, reject
all-zero allocation, check exact uniqueness against the durable namespace, and
retry random generation on collision. Random generation reduces accidental
collision probability; the exact registry check is the identity invariant.
External textual form is exactly 32 lowercase hexadecimal characters, with no
UUID layout/endian interpretation. Imported foreign domains are never allocated
locally and cannot become local authorities by collision.

`QualifiedRef = [dataset_id: bytes16, dictionary_kind: u16,
allocation_domain: bytes16, ordinal: u64]`. Ordinals start at 1; zero is invalid.
Dictionary kinds are: 1 endpoint, 2 observer, 3 access scope, 4 visibility scope,
5 method, 6 route, 7 HTTP resource, 8 event, 9 dataset lineage. Unassigned kinds
are incompatible; they are not cast to the nearest known kind. A dictionary entry
binds that reference to `normalization_version: u16`, `descriptor_schema: text`,
`descriptor_version: u16`, the closed typed descriptor value and its digest.
Existing descriptor field definitions remain in A04; changing their meaning
requires a new descriptor version. Secret bytes are forbidden in all descriptors.

Each allocation domain has exactly one fenced allocating owner recorded in
control authority and its synced local manifest. Allocate an ordinal and its
mapping in the same durable transaction that advances the high watermark.
Existing values are looked up and compared by complete canonical bytes, not only
their hash. The same key/different value is corruption/conflict. Equal semantic
descriptors in different qualified domains may map to one local reference during
import; preserve every source mapping and never change the original identity.
Canonical identities, not bare ordinals, decide semantic equivalence.

Restore carries domains and high watermarks forward without reuse. A restore
that cannot prove the prior allocation watermark must create a fresh domain for
new allocations while preserving every old mapping; it cannot guess the next
ordinal. Topology handoff preserves old domains and mappings. New work may use a
fresh explicitly registered allocation domain; old refs never change. Concurrent
allocation is allowed only in disjoint registered domains, not through separate
writable mounts of one embedded store. This is a controlled handoff, not online
resharding or automatic failover.

`SourceRecordId = [dataset_id: bytes16, shard_id: u32, owner_epoch: u64,
sequence: u64]`; epochs and sequences start at 1. A source-owner epoch has one
positively fenced writer and contiguous atomically committed batches. Shards
have stable configured IDs, not process IDs. New epoch work starts at sequence 1
only after durable closure/exclusion of the prior epoch. A cut is
`[dataset_id, topology_generation: u64,
[[shard_id,owner_epoch,last_complete_sequence], ...]]`; bounds sort by unsigned
`(shard_id,owner_epoch)`, duplicates are invalid, and sequence 0 means that known
stream contributes no committed facts. A topology manifest lists every required
stream and prior handoff boundary; omission is not implicit completion. Preserve
old topology generations and bounds while any cut depends on them.

All u16/u32/u64 fields are checked before encoding, allocation or adding a batch
length. Exhaustion refuses the entire mutation before wraparound. Ordinal
exhaustion requires a new allocation domain; source sequence exhaustion requires
a fenced new epoch; epoch/topology exhaustion requires an explicit format/domain
migration. Producer epochs, request sequences and projection counters obey the
same no-wrap rule. The JSON inspection/API metadata representation of u64/i64
is a decimal string with no plus sign or leading zeroes (`"0"` is allowed);
protocol Nostr JSON preserves its exact numeric tokens. Engine casts may not
narrow these domains. Pins/cursors must bind qualified identity and topology.

## Fingerprint encoding profile 1

Use SHA-256. Except Nostr ID and raw document digest, each preimage is
`ASCII("bigbrotr/") || ASCII(domain) || ASCII("/1") || 0x00 || C(value)`.
The complete literals are fixed below; concatenation adds no newline/BOM.
`C` is RFC 8949 §4.2.1 **core deterministic CBOR**, with this closed profile:

* Unsigned integers use major type 0; negative signed integers use major type 1
  with argument `-1-n`. The schema limits them to u16/u32/u64 or i64 as stated.
  Arguments 0–23 fit in the initial byte; 24–255 use one following byte;
  256–65535 use two; 65536–4294967295 use four; larger values use eight. Multibyte
  arguments are unsigned big-endian and always shortest-width. There is no
  native endian, signed two's-complement serialization or float conversion.
* Bytes use major type 2, text major type 3 with the same shortest definite-length
  argument describing **UTF-8 byte length**. No Unicode normalization, surrogate
  repair, case fold, trimming or BOM insertion. Invalid Unicode is rejected.
* Ordered lists/records use definite arrays, major type 4, length in elements.
  Tags retain order, duplicates, short entries and empty strings. Sets have an
  explicitly prescribed sorting key and no duplicates; they are never inferred
  from an arbitrary list. Empty array, empty bytes and empty text are different.
* Closed typed evidence/descriptors use definite maps, major type 5; keys are
  their exact schema field-name text. Sort keys lexicographically by their
  complete deterministic CBOR encoding (RFC core order, **not** length-first).
  Reject duplicate, missing or unknown keys. Every declared nullable field is
  present: null is `f6`; false/true are `f4`/`f5`. Required absent fields fail;
  null is not empty/zero. Optional RPC transport decoration is not a fact field.
* No floats, indefinite lengths, tags, undefined values, integers outside the
  declared domain, general extension bags or trailing bytes. Measured/derived
  floating values that become persistent inputs use their schema's explicit
  finite binary64 `bytes8` field (IEEE-754 big-endian, negative zero normalized
  to positive zero, NaN/infinity rejected), never CBOR's variable float encoding.

This profile is a standard encoding choice and field-aware fingerprint recipe,
not a new transport or general serialization library. Use a maintained codec
with explicit profile checks in the future product. Parquet/gRPC/JSON adapters
map their typed fields into these same values. Deterministic protobuf output
alone is not a canonical persistent fingerprint, as its
[official guidance](https://protobuf.dev/programming-guides/serialization-not-canonical/)
explains. CBOR's encoding details and core map ordering are specified by
[RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html#section-4.2.1).

### Exact domains and field order

All array positions below are mandatory. `null` appears only at explicitly
nullable positions. A hash/digest is bytes32, never its hex text. Qualified refs,
source IDs and cuts use the arrays above. No self-digest is included in itself.

| Domain / value | Exact contents and interpretation |
|---|---|
| Nostr event ID, no BigBrotr domain prefix | SHA-256 of the protocol's UTF-8 JSON array `[0,pubkey_hex,created_at,kind,tags,content]`. Preserve protocol string escaping, ordered tags and unsigned number tokens; no sig, extra JSON fields or wire whitespace. |
| `content_digest`, no prefix | SHA-256 of unchanged document/wire object bytes. Scoped ObjectId is `[visibility QualifiedRef,content_digest]`; equal hashes do not grant visibility. Length is separately validated. |
| `bigbrotr/body/1\0` | `[event_id,pubkey,created_at,kind,tags,content]`. Semantic shared payload only; never a file or raw transport digest. |
| `bigbrotr/dictionary/1\0` | `[QualifiedRef,normalization_version,descriptor_schema,descriptor_version,descriptor_value]`. Identity mapping digest; its value map is closed by the named descriptor schema. |
| `bigbrotr/fact/1\0` | `[SourceRecordId,record_kind:text,record_schema_version:u16,visibility QualifiedRef,recorded_at:i64,payload]`. Binds complete envelope, including identity/scope/time; `payload_digest` is this digest despite its historical name. Core payload arrays are listed below. Other fact payloads are closed field maps of their named A03–A09 schema, with keys/nullability as declared by that schema; never raw user JSON. |
| `bigbrotr/producer-request/1\0` | `[ProducerRequestId,operation:text,operation_schema_version:u16,intent]`. ProducerRequestId is `[producer_slot_id:bytes16,producer_epoch:u64,request_sequence:u64]`. Event intent array is below. Evidence intent is `[admission_policy_version:u16,visibility QualifiedRef,[[record_kind,record_schema_version,payload],...]]` in caller-declared order; evidence payloads include their full occurrence/clock/source attribution. No owner-assigned fact IDs/time are in intent. |
| `bigbrotr/admission-batch/1\0` | `[dataset_id,shard_id,owner_epoch,first_sequence,last_sequence,[[SourceRecordId,payload_digest],...]]`; records strictly ascending sequence, contiguous, nonempty and exactly covering endpoints. Dependencies added in the same transaction appear as ordinary facts. This binds semantic facts, not Rocks WriteBatch bytes. |
| `bigbrotr/archive-batch/1\0` | `[dataset_id,archive_schema_version:u16,topology_generation:u64,[[shard_id,owner_epoch,first_sequence,last_sequence],...],[[SourceRecordId,payload_digest],...]]`. Ranges sort by `(shard,epoch,first)`, are disjoint and cover exactly records sorted by full source ID. Only complete admission batches; no empty archive batch. |
| `bigbrotr/projection-chunk/1\0` | `[projection_generation:bytes16,target:text,target_schema_version:u16,SourceCut,chunk_ordinal:u64,[[SourceRecordId,child_discriminator,row_value],...]]`. Rows sort by `(SourceRecordId,C(child_discriminator))`; ties forbidden. Child discriminator is `[relation:text,ordinal:u64]` (zero-based tag/child position, 0 for a singleton). Row value is the target's closed typed map/array; includes every materialized field. One engine-owned base/projection family counts as one target only after engine qualification. |
| `bigbrotr/existing-proof/1\0` | `[dataset_id,topology_generation,protection_class:text,recovery_generation:bytes16,policy_epoch:u64,intent_digest,[[SourceRecordId,payload_digest],...]]`. Dependencies sort by full source ID, no duplicates. Operational integrity key, not a signature or independently portable authorization. |

Core fact payload field order:

* `event-payload/1`: `[event_id,pubkey,created_at,kind,tags,content,event_ordinal:QualifiedRef]`.
  Its body digest hashes only the first six values. The internal ordinal and
  envelope are omitted from a public shared-value export.
* `event-eligibility/1`: `[event_id,body_digest,sig,first_eligible_at,origin,import_lineage]`,
  with origin encoded as exact text `"network"` or `"import"`.
* `event-receive/1`: `[event_id,received_at,observer_id,clock_quality,corrects]`.
* `provenance/1`: `[event_id,endpoint_id,observer_id,access_scope_id,received_at,
  clock_quality,verification_method_id,corrects]`.
* `dictionary/1`: `[QualifiedRef,normalization_version,descriptor_schema,descriptor_version,descriptor_value]`.
* `ClockQualityV1`: `[clock_id:QualifiedRef,resolution_us:u64,
  uncertainty_us:u64|null,anomaly_flags:u32]`. Clock ID uses an observer descriptor;
  bit 0 is detected rollback, bit 1 unsynchronized, bit 2 coarse resolution;
  all other bits are reserved and rejected in v1. No detected anomaly is 0;
  unknown uncertainty remains null.

Event request intent is `[admission_policy_version:u16,visibility QualifiedRef,
[event_id,pubkey,created_at,kind,tags,content,sig],origin,import_lineage,
LocalObservation|null]`. `LocalObservation` is
`[received_at,observer_id,clock_quality,WitnessContext|null]`, where
`WitnessContext = [endpoint_id,access_scope_id,verification_method_id]` is required
when network provenance is enabled and null when disabled. Network origin requires
LocalObservation; import origin requires null. Import lineage follows eligibility
rules. Disabled provenance forbids retaining a supplied witness context.

The request binds the actual supplied signature, scope, observation, claimed
source and policy version even if both changed inputs would deduplicate. It does
not bind retry-local deadline, budget grant, trace/socket/process ID, authentication
token, destination owner route/epoch or gRPC serialization. Those are independently
validated current authority and never alter request identity. Same request ID
with different bound intent is `RequestConflict` while detail exists. A retired
request always returns `RetiredRequest`, without reconstructing or promising a
discarded digest. Caller-requested semantics cannot be hidden in transport fields.

Archive and projection digests bind ordered semantic content. File paths,
compression, row groups, Parquet bytes and upload names are excluded. Prepared
artifact/backup files carry conventional SHA-256 checksums in their manifests;
native archive files track uncaptured versus captured checksum state explicitly
under the [storage placement contract](archive-storage-contract.md#actual-publication-and-protection-boundaries).
An engine receipt or ETag is not an application file SHA-256.
Compaction/re-encoding preserves fact digests. Rechunking changes chunk/batch
identity and cannot reuse an old token with different membership. Empty derived
generations use their explicit completion manifest; a zero-row chunk is not
evidence that unspecified input was processed.

### Compatibility before writes

| Encounter | Required behavior |
|---|---|
| Same known semantic version, different transport field order or physical Parquet encoding | Decode, validate and reproduce the identical semantic fingerprint. |
| Unknown Nostr top-level JSON key | Bounded skip; excluded by Nostr canonicalization. Preserve only in separately enabled wire evidence. Required signed keys remain required. |
| Unknown optional RPC tracing/transport field | Bounded skip only when the negotiated schema explicitly marks it nonsemantic; never place it in admitted payloads. |
| Unknown record kind/schema/digest profile/required semantic field or enum | Reject preflight with `IncompatibleSchema`; no mutation, cursor advance, receipt or partial archive commit. Unknown is not null/default. |
| New optional **persistent semantic** field | Requires a new record schema version and negotiated support. An old reader may not drop it and rehash under the old version. |
| Unknown source evidence family in research import | Explicit manifest policy can omit that whole optional family and report loss; `require-all` refuses. No source claims reinterpreted as a known family. Mandatory identity/control/eligibility dependencies cannot be omitted. |
| Old reader/new writer or mixed RPC peers | Handshake advertises digest profile + kind/schema/operation capability sets. Select their exact supported intersection before intake; otherwise route/stream is unavailable. A newer peer may emit an older schema only if the operation is exactly representable. |
| Format upgrade | Drain writes, preserve readable old schemas/identity bytes, migrate under fenced ownership and publish readiness only after closure. No in-place rewrite of old fact fingerprints. Restore checks required versions before advancing progress. |

v1 supports only profile 1 and the explicitly named record/operation versions;
there is no permissive “best effort” v2 interpretation. New adapters must retain
the v1 golden fixtures across upgrades. A signed-event format's own missing/default
rules remain format-specific (including the inspected Pensieve proto3 defaults),
then full ID/signature verification applies. Duplicate keys in native canonical
payload maps are invalid; duplicate JSON signed keys are rejected before ordinary
parser last-key-wins behavior can make them ambiguous.

## CertifyExisting without mutation semantics

Selected operation:
`CertifyExisting(event intent, current authority, bounded dependency lookup) →`
`ExistingProof | NeedsAdmission | Invalid | Excluded | Retryable | IncompatibleSchema`.
It has **no ProducerRequestId** and does not allocate a source sequence, receipt,
proof ledger, permanent delivery record or last-seen value. For its proof's
`intent_digest`, use the producer-request recipe with the reserved proof-only
identity `[zero bytes16,0,0]` and operation `"certify-existing"`; that identity is
never valid for Admit. Repeating this operation with another payload makes a new
read evaluation, not `RequestConflict`.

The owner independently verifies exact event bytes/signature and current admission
policy. In one serialized consistent view of its committed inventory it establishes:
(1) identical complete payload; (2) eligibility in exactly the requested scope;
(3) existing network receive minimum no later than the supplied receive, when
network origin; (4) existing minimum for the requested enabled provenance key no
later than the supplied receive; and (5) all referenced dictionaries, including
the attributed import lineage descriptor, already retained. This operation
accepts an event intent; it does not certify separate imported sidecar claims.
Those use AdmitEvidence and its own progress dependency, even when the event
itself can be certified. New scope, import→network first receive, earlier
observation, new edge or any uncertain/missing effect returns NeedsAdmission;
the caller creates a fresh strict Admit request. An invalid/excluded candidate
must not be certified merely because something with that ID exists. Revoked
eligibility is Excluded, not a successful proof of historical authorization.

The proof binds the above intent, full dependency identities/digests, current
topology, policy epoch, recovery generation and actual protection class. It may
reference a body fact in another scope **internally**; it is a private operational
artifact, never a public response or scientific source fact. Its existence does
not grant read access to that dependency. Lookup at a committed state is enough
only when that state's acknowledged protection/retention contract already covers
every dependency. A RAM cache, retired detailed receipt or bare existence bit is
not such evidence. Pending synced facts can qualify under the selected local-disk
failure class, but not under a claimed off-host replica class they do not meet.

The initial supported protection spelling is `local-synced`, as fixed by the
golden proof vector; it does not claim off-host protection. Other classes require
an explicit qualified recovery contract and compatible readers before use.
The portable recovery-generation field retains all 16 bytes and policy epoch
retains the full u64 domain, including zero. Registration, freshness and current
authority are owner checks, not inferred from either scalar's value. Policy
epoch advancement refuses overflow. Proof dependencies are nonempty, strictly
sorted by full source ID, unique and all within the context's dataset.

A producer durably checkpoints the proof (or its complete deduplicated dependency
set) with the bounded page/import progress it authorizes. This is operational
progress state, retained until superseded progress has at least the same closure;
it is not one archive fact per repeated delivery. Backup closure traverses those
dependencies exactly like ordinary receipt dependencies, including any body fact
in another scope. Retirement of mutation request detail cannot delete this
retained source proof basis. Archive/pending reclamation must preserve a resolvable
authoritative copy; explicit physical loss/removal invalidates dependent progress.
Once a retained complete source-cut manifest demonstrably covers all those
dependencies under the same protection class, a checkpoint may replace the
individual references with that manifest/cut. This bounds cumulative proof state
by owner streams instead of all duplicate deliveries. A maximum sequence alone
without retained complete-range certification does not authorize that compaction.

After restore, a checkpoint is usable only if its source dependencies/digests are
present under the restored recovery manifest. A historical proof need not include
a subsequently recorded earlier correction: its original sufficient minimum
remains a valid basis. A new recovery generation
does not invalidate the fact history, but old proof tokens cannot authorize new
operations without revalidation. A mixed cut containing progress but missing an
eligibility/witness/body dependency is refused or rolls progress back before
resumption. Reads alone cannot reserve an otherwise expiring resource: certification
must return Retryable/NeedsAdmission if existing retention claims are insufficient.

This read-only path proves that this input needs no new retained effects within
its declared policy/protection boundary. It proves neither complete relay coverage,
remote retention, physical propagation nor survival of an unprotected host loss.
It does not weaken Admit: retained request details still bind exact inputs, atomic
write sets still sync, and retired IDs remain refused. The isolated model cannot
prove fsync, positive fencing, crash atomicity, backup capture or zero engine writes;
P02's actual adapter tests must establish those properties before enabling the path.

## Golden vectors and acceptance handoff

[golden-vectors.json](../../experiments/identity-contract/golden-vectors.json)
fixes literal bytes and SHA-256 for every domain above, including a full fact,
request, admission/archive batch, projection chunk and proof. Primitive vectors
cover u64 maximum, i64 minimum, null/empty, UTF-8 and ordered/duplicate tags.
Nostr examples cover canonical bytes separately; synthetic signatures used by
the model are explicitly not a cryptographic verification result.
The additional real signed fixture has 14 distinct event IDs, two valid
representative signatures for the cross-scope event, full provenance-on/off cuts,
authorized replacement/deletion/expiry results, empty-list graph behavior and
permitted history export membership. Its maintained-library and independent
equation checks are separately labeled evidence, with no engine durability claim.

The probe compares an incremental admission/certification model with a separately
written whole-history oracle across arrival orders, cuts, provenance on/off,
import/network, signatures, restrictions, deletion/expiry, replay and restore.
It intentionally evaluates wrong global-signature/time selection, global winner,
private deletion, expiry-before-winner, ID-only validation, omitted proof dependency,
and loose request conflicts. A passing counterexample must reject each named
wrong interpretation. Fixed vectors are independently byte-assembled and checked
with the system OpenSSL SHA-256 as well as the Python implementation; no Rust,
RocksDB, DuckLake or CH behavior is inferred from this research.

P00-T06 steps 1–6 are supplied by this contract and fixture/evidence; step 7 also
requires reconciliation of the existing owning documents and dependent tasks.
P01-T02/T07 generate typed values/negotiation and preserve these vectors. P02-T02
implements atomic scoped eligibility, exact validation, strict conflicts and the
separate certification path; T03 implements qualified allocation/restore;
T04/T05 implement semantic archive/chunk fingerprints and factored projections;
T07 tests causal backup closure; T09 measures costs without weakening semantics.
P03 import tasks map claims/scopes explicitly. P05-T02 and P06-T02 select authorized
populations before winners/deletions. P08-T04 reconstructs full signed exports
without private source-envelope leakage. Their real acceptance must include both
arrival orders and enabled/disabled provenance, and show a useful query/export
through actual adapters; this probe cannot mark those tasks complete.
