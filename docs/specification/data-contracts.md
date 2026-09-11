# Archival data contracts

Status: selected design, before product implementation. This is the field-level
contract used by [the implementation tasks](../implementation/README.md). It
refines the architecture summaries; a physical adapter must preserve these
meanings and pass the specified round trips. Engine DDL, generated JSON schemas
and Rust types are derived from this contract, not independent sources of truth.

## Purpose and boundaries

BigBrotr retains verifiable Nostr history and attributable observations useful to
someone studying the ecosystem later. Operational facts needed to resume a
deployment are durable where necessary, but do not thereby become public research
data. A deployment backup and a portable research dataset are different products.

The baseline permanently retains accepted complete events, first-observation
provenance when enabled, actual executed monitoring measurements and referenced
source documents. There is no permanent per-event delivery count, retry count or
last ordinary duplicate-receive field. Unexecuted scheduling decisions, queue
waits, worker IDs and repeated log messages do not enter the scientific archive.
They have bounded operational state/telemetry. Scientific evidence can still
include observation times, methods, clocks and observers: those establish what
was measured, rather than just how our process ran.

The earlier 365-day raw-monitor default is superseded by permanent compact
measurement evidence. Explicit lossy retention is supported with a coverage and
information-loss manifest; aggregate retention cannot silently stand for raw
history. Acquisition limits/cadence govern collection before spending resources.
Disk exhaustion pauses new admission under the durability contract; it never
silently drops accepted evidence to preserve a throughput number.

## Scalar definitions and encoding

| Type | Meaning and storage | Validation / portable representation |
|---|---|---|
| EventId, PublicKey, Digest256 | 32 raw bytes; Parquet binary with length invariant | Exactly 64 lowercase hexadecimal characters in JSON |
| Signature | 64 raw bytes | Exactly 128 lowercase hexadecimal characters in JSON; verify against actual canonical event ID/pubkey |
| DatasetId, ObserverId, ScopeId, MethodId | Stable identity in a versioned dictionary, not an OS process ID | Export dictionary with every referenced identity; opaque identifiers do not expose credentials |
| Ordinal | Checked unsigned 64-bit local compact reference | Never reuse after restore; portable identity is dictionary key plus canonical value |
| NostrTime | Original unsigned 64-bit seconds | Preserve the exact integer; APIs with unsafe JSON-number clients expose a lossless representation in their typed metadata; full Nostr events keep protocol JSON numbers |
| ObservedTime | Signed 64-bit UTC microseconds plus ClockQuality | Null means absent, zero means the Unix epoch; monotonic elapsed time is a separate duration |
| DurationUs | Checked unsigned 64-bit microseconds | Null if not measured; zero is a measured zero-resolution duration |
| ClockQuality | Observer clock identity, resolution, optional uncertainty, anomaly flags | Unknown uncertainty is null, never an invented zero-error clock |
| Text | Valid Unicode string, preserved exactly | No trim, case-fold or JSON-content parsing at archival admission |
| Tags | Ordered list of ordered lists of Text | Preserve duplicate tags, empty strings and all protocol-valid entries |
| SourceRecordId | DatasetId, shard ID, owner epoch, sequence | Versioned unambiguous binary encoding; one sequence per fact, stable through replay/file relocation/compaction |
| SourceCut | Ordered map of shard/epoch to complete committed sequence plus topology generation | Apply before logical reductions; a wall-clock timestamp is not a cut |

The precise Nostr protocol baseline is pinned in
[protocol evolution](../architecture/protocol-evolution.md). Do not reduce an
allowed Nostr timestamp to a database timestamp type with a smaller domain.
Dates used for archive partition placement are controlled ingest ranges; the
signed timestamp remains untouched. Reject malformed/over-limit input before
expanding large arrays or constructing signed-serialization buffers.

Canonical signed serialization excludes wire JSON formatting whitespace/key order
outside string values and extra top-level fields. Whitespace inside signed
content/tag strings is authenticated and preserved. A full event retains every
signed field and a valid original
signature; exact wire bytes are an independently configured fidelity mode.
Object-like event content remains an uninterpreted string. Typed derived fields
always point to their parser version and original input.

## Immutable source envelope

Every authoritative fact has these non-null fields unless stated otherwise:
`source_record_id`, `record_schema_version`, `record_kind`,
`visibility_scope_id`, `payload_digest`, `recorded_at`. The digest covers a
versioned canonical encoding of the typed fact, not a file path or human-readable
string concatenation. Fact-specific observed time is distinct from recorded time.

Admission assigns a contiguous sequence range to one atomic batch, one sequence
per fact. Its receipt binds first/last sequence and batch digest. Certified cuts
end at complete committed batch boundaries; batches are never partially
certified. Identical retry copies share the complete source identity
and bytes. Corrections are new facts with new identities. Dictionary additions
required by a fact are published in the same or an earlier visible archive cut.

Normalized child rows retain their parent's identity plus a typed child
discriminator; they are not independently admitted facts or independent samples.
Transactional archive batch receipts are technical commit metadata, not newly
admitted scientific facts requiring another archival commit. Canonical identifier
spellings/domains are owned by [naming](naming.md); the exact logical relation
inventory and ownership are in [the storage catalog](storage-catalog.md).

This envelope is technical lineage needed for safe replay, export and reproducible
queries. Public event responses need not display it. It does not justify retaining
request IDs, socket identifiers or every retry as additional permanent facts.
Source receipts and owner routing metadata are included in restore material;
portable exports include the lineage and dictionaries necessary to interpret
selected records, never live credentials or private control tables.

## A01 — Signed events and acquisition metadata

The immutable event row contains `event_id`, `pubkey`, `created_at`, `kind`
(unsigned 16-bit), `tags`, `content`, `sig`, `event_ordinal`,
`first_admitted_at` and its source envelope. One logical row exists per event ID
in the owning archive identity domain. Content and tags are never omitted by a
storage profile. Their indexes are optional representations.

For the same verified canonical payload with another valid signature, retain the
first admitted signature in the canonical event row. This is not a second event.
A newly received invalid signature never gains a witness just because the event
ID is already known. Raw variant preservation, when requested for research, is
bounded explicit wire evidence, not a mutation of the canonical event.

Local receive metadata is a separate sparse fact family:
`event_id`, `received_at`, `observer_id`, `clock_quality`, source envelope.
Emit the first eligible local network observation and strictly earlier
attributable corrections. The source-cut-relative minimum is the event's
`first_received_at`; ties use source identity only for deterministic selection
and do not prove a causal order. It remains null for import-only events.
No later duplicate updates this family. Corrections do not rewrite old snapshots.

With provenance off, these event-level facts carry no endpoint or auth-scope
edge. An observer identifies the measurement authority/clock, not a hidden relay
identity. Source-import metadata remains in its original namespace; local import
cannot backdate this deployment's network observation or overwrite its first
admission. A restore preserves the original deployment identity; importing a
dataset into another deployment creates a local admission with separate lineage.

**Useful queries:** retrieve/reverify the signed event; distinguish authored time,
first acquired time and first local observation; export full or explicitly
projected rows. **Not supported by these facts:** last remote availability,
number of deliveries, true origin time or first appearance on the entire network.

## A02 — First local provenance and corrections

Enabled provenance has logical key
`(event_id, endpoint_id, observer_id, access_scope_id)`. Its immutable first fact
contains `received_at`, `clock_quality`, `verification_method_id` and source
envelope. A correction contains the same key, an earlier attributable observation,
and the prior fact identity it corrects where known. The query reduction chooses
the minimum eligible observed time after source-cut and scope filtering.

The synchronized exact inventory stores the compact key, current minimum and
receipt. It suppresses later repeated deliveries before permanent archive writes.
The inventory is P-sized, even if a cache is small. Both online orderings
event→relay and relay→event contain narrow references; they do not copy bodies.

`access_scope_id` names stable semantics such as public access or a protected
authorization context. It is not a connection, transient challenge, token value
or process lifetime. Protected/public observations are never collapsed into one
unqualified claim. Stable observer identity is not a claim of independence.

A historically witnessed event is not asserted present now. Intentional
event-availability experiments produce distinct scoped measurement facts with
sample-selection and method metadata. They require provenance permission and
enablement; provenance off cannot retain event/endpoint pairs through this
alternative table. Passive later duplicate delivery does not trigger such a fact.

## A03 — Imported claims and dataset lineage

Each imported evidence assertion has `source_dataset_id`,
`source_record_identity`, `source_artifact_digest`, `assertion_kind`,
`subject_id`, typed claimed values/times, `source_observer` when supplied,
`validation_status` and visibility. Status distinguishes verified signed
assertion, internally consistent unsigned claim, unsupported interpretation and
rejected input. Preserve source bytes/references needed to revalidate accepted
claims; an unsigned transport manifest is not a signature.

The source event's signature authenticates its event body, not a third party's
relay or receive-time assertion. Signed NIP-66/NIP-85 events remain A01 originals;
their interpretation is a claim projection with source event ID/parser version.
Importing their claims does not manufacture A02 local witness evidence.

`import.claims=enabled-families` is the default. With provenance off, interpreted
event→relay claims are excluded and declared in the import result/manifest;
non-provenance evidence may still be retained. `require-all` rejects the job at
preflight if any supplied interpreted family is disabled; `none` imports signed
originals without interpreted sidecar claims. Full signed events that themselves
contain relay references are unchanged. No import option silently enables P-sized
state or manufactures a witness.

An import job has a separate bounded operational ledger with input digest,
format/producer version, cursor, outcome and useful counts. It is not copied into
every archive row. A compact dataset-level lineage descriptor can be exported;
local paths, credentials and transient worker IDs cannot.

The optional A03 `IssuerAuthorityBindingV1` claim is specified field by field in
[assertion bindings](assertion-bindings.md#zap-qualification-and-conflict-handling).
It is an operator-attested historical recipient/profile/URL/signer interval with
retained evidence, explicit trusted-source/public-use policy and immutable
correction lineage. Existing import/AdmitEvidence owns it; a self-declared class
or present HTTP fetch supplies no historical authority. Without eligible input,
the strict zap wire bindings stay disabled/withheld. Native exports include its
referenced public evidence and dictionaries, never private control credentials.

## A04 — Endpoint, observer, scope and method dictionaries

Endpoint identity contains normalized URL bytes and normalization version.
Preserve scheme/host/port/path/query semantics specified by the URL contract;
do not merge endpoints by guessed operator, shared IP, redirect or overlapping
events. The discovered spelling belongs in discovery evidence when relevant.
Versioned mappings survive export/restore and allow independent readers to
resolve compact ordinals. A normalization change is an explicit migration.

Observer descriptors contain public attribution when configured, measurement
vantage/route class, software/method versions and clock descriptors. Auth-scope
descriptors contain permitted opaque labels and visibility; credentials remain in
the secret store. Method descriptors define input, output units, failure taxonomy,
algorithm/protocol version and capability claims they can support.

Large mutable health, backoff, queue and eligibility fields are PG registry
state, not rewrites of every historical endpoint dictionary record.

HTTP resources have their own `HttpResourceId` dictionary: normalized guarded
request URL and normalization version, method/visibility attribution and safe
export representation. They do not require a relay enrollment. A redirect or
resolved dial address is request evidence, not a change to relay identity.
Sensitive query credentials are forbidden; an unavailable safely exportable
binding is explicitly restricted, never silently disclosed in a dictionary.

## A05 — Executed measurements

A measured attempt requires permits and evidence that its declared external
action began. A recovered pre-dispatch intent with unknown execution is retained
as a coverage uncertainty record, not counted as an executed measurement. It records `attempt_id`, `subject`,
`observer_id`, `route_id`, `access_scope_id`, `method_id/version`,
`started_at`, `ended_at` when known, `duration_us`, `clock_quality`,
`outcome`, structured error class, measurement values and document references.
Outcomes distinguish success, remote refusal, timeout, local failure and
interrupted/unknown completion. Deferred queue entries are not attempts.

`MeasurementSubject` is a closed tagged union: `RelayEndpoint { endpoint_id }`
or `RecipientMetadataHttp { recipient_pubkey, profile_event_id, http_resource_id }`.
The latter records P04-T14 issuer lookup against an exact signed profile version;
it never creates a fake relay, null endpoint relation or relay availability vote.
Each variant requires its complete references and scope; executed non-relay HTTP
evidence belongs here alongside relay measurements.
Future subjects require a versioned schema and a real producer/consumer contract.

For scheduled probes, retain the effective base cadence and probe profile at the
attempt, including request limits/deadline. Adaptive retry delay is operational
state, not the base cadence used to reconstruct a time-weighted availability
estimate. Unknown or changed cadence/method boundaries limit supported inference.

Use typed child measurements for a compound attempt; optional DNS/TLS/catalog
checks do not acquire a new independent liveness vote accidentally. Child fields
declare check name, value type/unit, observed timestamp and measured/unsupported/
failed/not-executed state. A skipped child is metadata about an executed compound
attempt, not a fresh measurement. Avoid raw arbitrary exception strings.

| Measurement family | Retained values / evidence | Interpretation limits |
|---|---|---|
| DNS | Query name/type, sorted bounded answer set/TTL, resolver/route, response class, duration | Resolver-vantage result; shared IP is not common ownership |
| TCP/TLS/WebSocket | Resolved dial address, SNI/host, transport outcome, phase durations, bounded certificate/handshake metadata | A failure phase is not proven global relay downtime |
| Liveness/read capability | Actual request/probe profile, protocol response/outcome, duration, freshness | NIP-11 declarations and a received EOSE are separate evidence |
| NIP-11/HTTP documents | Status, media type, fetched time, body digest, parser status, conditional-cache provenance | Cache age does not become a new successful body fetch |
| NIP-42/access/paid declarations | Challenge/refusal/declaration kind, public/scoped access context | AUTH support or a paid declaration alone does not prove every public read denied |
| NEG/COUNT/search/filter behavior | Pinned probe input/profile, bounded result/outcome, method/version | Support only for the tested behavior and vantage; malformed/timeout differs from unsupported |
| Active write/retrieve probes | Explicit authorized profile, signed probe reference, destination outcomes, cleanup-attempt result | Requires signing scope; cleanup is best effort, no secret/payment side effect |
| Geo/ASN | Address, family, database edition/version/date, lookup output or missing/unsupported status | Derived location, not physical-location certainty |
| LNURL issuer metadata | Recipient/profile/request binding, URL, actual fetched time, observed key, source document and parser outcome | P04-T14; a current key match cannot backdate issuer authority for historical zap receipts |
| Availability experiments | Eligible sampled event reference, selected endpoint, sampling frame/seed/profile and retrieval result | Provenance-enabled only; sample availability, not an exhaustive census |

Every value has an explicit type; scalar measurements do not become unbounded
JSON labels or Prometheus dimensions. TLS certificate/HTTP/DNS source bytes may
be retained as A06 objects where their method needs them. Scrub/export honor all
references. Raw traffic containing credentials is never archived.

An external action cannot be atomically committed with a local database write.
The initial implementation persists a bounded dispatch intent before the action,
then admits terminal evidence through the shared contract. A crash in between
leaves execution/completion unknown: actual start/end are nullable and a recovered
intent does not prove that a packet was sent. Preserve that uncertainty in scope
coverage, excluding it from measured success/failure denominators. Never invent
a timeout from an absent terminal result. A late attributable terminal record
resolves the same attempt identity rather than creating a second sample. The
additional intent/terminal writes are included in the M-sized monitor budget.

## A06 — Content-addressed source documents

Object identity is `(visibility_scope_id, content_digest)`; the scope is a
qualified dictionary identity, not a bare local integer across imported datasets.
The earlier phrase "document visibility namespace" means this qualified scope.
Bytes are checked before publishing the object. An object row carries original bounded bytes,
length and digest. Each occurrence/reference carries document family, the typed
measurement subject or a typed source-document binding, qualified visibility
scope, fetch or observation time, declared media type/encoding, method and
source envelope. The same bytes can support several observations/types without
duplicating the payload. A different visibility namespace cannot expose another
scope's object existence.

A measurement occurrence references its parent attempt/source record and child
check discriminator. An independently admitted document occurrence has its own
source identity. A directory/import source occurrence uses its attributed source
descriptor, not a fabricated relay measurement. These variants share document
bytes only when the qualified visibility and digest both match. Export/import
must resolve every reference without a live control database.

Parsed views carry object identity, parser/version, outcome and typed fields.
Re-parsing creates a new derived interpretation, not a replacement original.
No default index is created for every JSON field. Maintain reference membership
for archive snapshots, retained measurements, exports and backups; object GC
cannot race a new published reference. Publish object and reference atomically
or ensure the object precedes any visible reference. Standalone exports include
the referenced objects and their mapping; a live PG registry is not required.

Repeated unchanged NIP-11 content costs one logical object plus compact actual
fetch observations. This does not prove lower physical Parquet bytes: dictionary
compression of a wide repeated-body column can outperform a separate tiny object
table. P02-T09 measures both layouts, indexes and standalone export overhead.
The [controlled layout experiment](../../experiments/document-layout/README.md)
found both winners under different churn/file boundaries; it selects no universal
compression ratio. Physical wide rows are allowed if the adapter preserves the
logical object/reference view and its independent reference/retention semantics.
A 304 references the previous body with explicit validator/cache
basis; without that body it is incomplete evidence, not an empty document.
An expired external Geo database does not erase prior version descriptors.

## A07 — Coverage evidence and registry state

Retain the compact evidence necessary to qualify an acquisition result:
endpoint/observer/access/filter revision; requested interval/set digest and
protocol/method; observed start/end; terminal reason; durable source receipt cut;
gaps, loss/exclusion indications and scope. An EOSE marks a response boundary,
not proof of all historical events. Capability fallback and demonstrated support
are separate fields. Detailed need lists/retry maps are operational, bounded and
reclaimable after their safe checkpoint.

Mutable registry rows track canonical endpoint ID, candidate sources, eligibility,
last scheduled/completed attempt references, due time/backoff, quarantine reason,
ownership and configuration epoch. These support scheduling and management;
they do not create an eternal change record on every poll. Meaningful externally
observable discovery/validation evidence can be exported under its scope.
Ban/policy decisions stay private audit records rather than scientific claims.

## A08 — Derived current state, facts, graphs and scores

Current-coordinate headers retain winning event identity/time, parser/version,
interpretation/visibility status and input cut even when there are no child
entries. Derived relation rows reference that header. Old input originals remain
A01. Changed-key state and immutable partition sharing avoid rewriting every
unchanged author at every cycle.

Fact records contain definition/version, subject, typed value/unit, population/
window, input manifest/cut, exactness/method and missing/withheld reason. Counts
are checked integers, not Float64 by default. A graph manifest names vertices,
edge policy, ordinal mapping, partition checksums, input cut and algorithm inputs.
Score rows add viewpoint/seed digest, parameters, raw value, normalization,
published value and component basis. Follow/mute semantics and percentile mapping
are versioned, not implicit global truth. Full definitions remain in
[analytics detail](../architecture/analytics-detail.md).

Keep complete usable/pinned generations and publication lineage. Do not archive
a full graph or score population every scheduler tick forever. Default generation
retention is current plus two preceding complete generations, and any generation
pinned by readers, exports, active jobs, unfinished publication traversal or
explicit research retention. Failed staging artifacts expire only when no recovery references need
them. Published score explanations retain their immutable manifest/digest and
necessary inputs or disclose that exact recomputation inputs later expired.
Completed publication retains compact lineage, algorithm/config/seed descriptors
and the signed originals; it need not pin every full graph/score artifact forever.
Logical source-retention claims preserve required facts across file compaction;
they are distinct from a short-lived reader pin on a physical catalog snapshot.

## A09 — Public outputs versus operational control

An output that BigBrotr signs is itself a full Nostr event and can enter A01 under
an explicit local-origin namespace; it is never a local relay receipt. The signed
outbox stores exact bytes before networking. Per-destination states remain
durable for delivery/retry and retention policy, not permanent claims of remote
availability. Publication lineage retains algorithm/method/input identities and
actual acceptance basis. Private DVM request/result payloads remain operational and retention-bounded by
default; signing a private response does not automatically admit it into the
permanent scientific archive. Public scientific NIP-66/85 outputs retain their
full signed originals and lineage. Independently collected public network events
still follow normal A01 admission; an operational TTL cannot erase those originals.

PG job/config/access/audit tables are deployment state. Initial operational
defaults: redacted logs 14 days, detailed terminal jobs 30 days, completed delivery
details 30 days after all obligations are terminal, private management audit 365
days. Pending/ambiguous obligations, signing-coordinate clocks, import identities,
replay tombstones and required source/deduplication metadata are retained for
their correctness horizon regardless of display retention. A detail TTL cannot
permit a forgotten payment or an already published event to be executed anew.
These are operator-configurable retention classes, not archive-format fields.

## Retention, export and indexing matrix

| Family | Authoritative location | Permanent baseline | Normal online index / export |
|---|---|---|---|
| A01 events + first local receive | Rocks pending → DuckLake Parquet | Yes | CH full body + narrow access; full signed export |
| A02 provenance | Exact Rocks key/min/receipt + immutable Parquet facts | Yes when enabled | Two narrow CH orders; scoped optional export |
| A03 claims/lineage | Source originals + Parquet typed assertions | Accepted evidence | Origin-aware CH interpretation; attributed export |
| A04 dictionaries | Immutable archive mappings; PG mutable registry separate | While referenced | Compact local lookup; required export mappings |
| A05 measurements | Durable dispatch intent + attributable terminal Parquet facts | Yes | CH selected fields/history; scoped measurement export |
| A06 objects | Namespaced deduplicated Parquet object rows | While referenced | Parsed hot fields + lookup; include objects in standalone export |
| A07 coverage | Durable PG workflow + immutable evidence summaries | Useful evidence; work queues bounded | CH scoped coverage; no public control-state dump |
| A08 derived | Published CH/artifact generations, manifest in PG | Pinned/useful generations | Read generation; explicit reproducible research export |
| A09 outputs | Signed outbox; public scientific originals + lineage in archive | Public scientific originals/selected lineage; private DVM payloads operational by default | Per-target delivery state is operational |
| Duplicate ingestion counts | Bounded metrics/log store | No | Operator metrics only, never per-event archive columns |
| Raw hostile frames | Optional isolated bounded diagnostic capture | No | Never valid-event corpus; never default public export |

Research exports declare dataset/fidelity/schema versions, selected source cut,
filters, exposure policy, row units/counts, checksums, dependency dictionaries,
document references, unavailable/lossy families and provenance origin. Full
events, projected events, measurements and deployment backups have distinct
manifest types. A projected event containing ID/signature but missing signed
fields is not independently verifiable.

## Distinguishing examples and obligations

1. Receive E from R1 at 100, again at 120 and 130: one body and one first witness;
   ordinary counters may change, archived event/provenance rows do not.
2. Later receive an attributable observation at 90: new correction, minimum 90
   for a new cut, original minimum 100 for the earlier cut. No mutable merge may
   erase the earlier answer.
3. Import E with claimed R2/time 50, then receive locally at 110: imported claim
   remains attributed; it does not change first local witness to 50.
4. Provenance off: E still has full signed data and first local receive metadata,
   but no hidden event/endpoint facts from delivery or availability probes.
5. Fetch unchanged document D a thousand times: one same-scope object; actual
   fetch times/outcomes remain a thousand compact observations. A thousand queue
   deferrals produce no measurement observations.
6. A newer empty relay list followed by older backfill: current header remains
   the empty winner, with no resurrected entries.
7. Kill a started monitor attempt before terminal evidence: recover start and
   record unknown completion, never classify a remote failure from missing data.
8. Export a document-bearing measurement and detach the running system: resolve
   every document/dictionary, retain empty tag strings and extreme timestamps,
   and identify unavailable/lossy data explicitly.

Each example has a task-level practical scenario. The small isolated
[archive-model experiment](../../experiments/archive-model/README.md) additionally
checks selected columnar representations and counterexamples; it does not prove
the future Rust owner, engine durability or full-system capacity.
