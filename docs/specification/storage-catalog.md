# Storage catalog: retained truth, serving data and control

Status: selected logical relation names and row contracts. These names close the
earlier inventory gap; they are not existing tables or production DDL. The
[A01–A09 field contract](data-contracts.md) owns field meanings and the
[component contract](component-contracts.md) owns receipts and migration authority.
Physical table/projection placement, sort keys and codecs are selected through
P02-T09 with equivalent semantics and actual engine evidence.

`archive.*`, `serving.*`, `control.*` and `derived.*` below identify logical namespaces.
Adapters record the exact database/schema/table mapping in their versioned
manifest. DuckLake's own catalog tables belong to DuckLake under separate PG
ownership; they are not BigBrotr scientific relations or public APIs. Do not
manually duplicate or edit the extension's metadata values/columns. The
[versioned commit-guard exception](archive-storage-contract.md#versioned-catalog-commit-guard)
permits ownership/privilege changes and statement triggers around the28 pinned
native tables. Separate `migrations/catalog/0001_writer_guard.sql` owns
`bigbrotr_archive.installation(singleton)`, immutable `attempts(ordinal)` and
`owner(singleton)` in the catalog database. These bind retained Control intents
to actual native commits; they are not scientific receipts or another admission
authority. No applied Control SQL changes for this separate catalog protocol.

## Common row rules

Archive facts carry the immutable source envelope from A01–A09. A logical key
defines what the fact concerns; it does not replace `source_record_id`, which
distinguishes an original from a later correction. Apply a SourceCut before
selecting a revision/minimum. Identical transport retries can create physical
copies only where the adapter proves duplicate-free logical reads.

Each separately admitted authoritative fact, including a document reference or
later correction, receives its own sequence. Normalizing one fact's ordered tags
or compound child fields into rows instead uses `(source_record_id,
child_discriminator)` with the parent digest; it does not reuse one source ID for
different authoritative facts or add independent measurement votes. Serving
subrows use ProjectionRowId. The schema identifies which form each relation uses.
`archive.archive_commits` is a technical transactional receipt relation, excluded
from this scientific source-envelope rule: recording an archive commit does not
admit another source fact and recursively require a further archive commit.

Fixed-length IDs/signatures are binary, authored time is unsigned 64-bit, and
observed time is signed UTC microseconds with explicit clock quality. Full tags
are ordered nested strings. Structured measurement payloads have closed typed
variants; raw arbitrary JSON is not a substitute for their field schema. Small
nullable variant-specific fields do not mean an unknown value equals zero.

## DuckLake scientific and archival relations

| Selected relation | One logical row / important identity and content | Writer, retention and consumer |
|---|---|---|
| `archive.events` | One EventPayloadV1 per dataset/event ID: ID, pubkey, created time, kind, ordered tags and content; internal event ordinal/admission envelope | ArchivePublisher from ingest; permanent; shared payload value is exported only through permitted eligibility |
| `archive.event_eligibility_facts` | First EventEligibilityV1 per event/visibility scope: body digest, valid original scope representative signature, first eligibility time, network/import origin and nullable import lineage | ArchivePublisher from ingest; permanent; joins payload into a complete signed event; new scope is a new fact even with provenance off |
| `archive.event_receive_facts` | First local network observation or strictly earlier correction per event/visibility scope: receive time, observer/clock; no endpoint when provenance is off | ArchivePublisher; permanent sparse history; cut-relative scoped event metadata |
| `archive.provenance_facts` | New/earlier `(event_id, endpoint_id, observer_id, access_scope_id, visibility_scope_id)` witness with time/method and correction lineage | Only when provenance enabled; permanent; both serving directions |
| `archive.imported_claims` | One attributable typed source assertion with source dataset/record/artifact, subject, validation/trust/time basis | Importer through ingest; accepted evidence; never local witness by implication |
| `archive.dataset_lineage` | Immutable source-dataset/artifact descriptor and format/fidelity interpretation | Import/export through archive publication; while referenced; no local file secrets or per-delivery import logs |
| `archive.dictionaries` | Typed immutable mapping `(dataset_id, dictionary_kind, allocation_domain, ordinal)` to its canonical identity/value and normalization version | Ingest/ArchivePublisher; while referenced; endpoint, observer, access, visibility, method, route and HTTP resource descriptors; export closure |
| `archive.measurement_facts` | Immutable executed-attempt result or attributable resolution: attempt ID, typed subject, method, clock, request/transport context, outcome and bounded typed child checks | Monitor through ingest; permanent; one actual compound attempt is one sampling unit despite child fields |
| `archive.availability_selections` | One immutable finite experiment selection: exact bounded pairs or reproducible descriptor, source cut, method/seed and scope | Explicit availability job through ingest; provenance required; permanent while evidence refers to it |
| `archive.availability_facts` | Executed event/endpoint child result linked to selection and actual request attempt; served, singleton-not-served or unknown with method | Provenance required; permanent scientific evidence; batching/retries do not manufacture negative votes |
| `archive.document_objects` | One logical `ObjectId` with original bounded bytes, length and content digest | ArchivePublisher; while any retained fact/snapshot/export/backup refers to it; no default per-JSON-key indexes |
| `archive.document_references` | One immutable occurrence/reference: source fact or typed source binding, ObjectId, family/media/encoding, actual observed/cache basis | Same or later publication than object; retained with referring evidence; source bytes separated from interpretation |
| `archive.coverage_facts` | Attributable traversal/uncertainty/resolution summary: scope/filter, requested range/set, method, outcome, gap and durable input cut | Acquisition through ingest; useful historical evidence; detailed scheduling queues stay in PG |
| `archive.publication_lineage` | Public scientific signed-output event ID plus method/algorithm/viewpoint/config/input identities and disclosure of expired recomputation inputs | NIP publishers through ingest; compact retained lineage; per-destination retry state stays operational |
| `archive.retention_manifests` | Explicit information-loss/removal policy application with affected logical scope, reason class and retained resolution | Authorized maintenance; retained to explain missing evidence; not an automatic NIP-09 deletion log |
| `archive.wire_objects` | Optional bounded exact original wire variant, event ID/body-signature basis, origin and digest | Explicit fidelity mode only; never an automatic hostile-frame archive; independent declared retention/exposure |
| `archive.archive_commits` | Deterministic batch identity, input sequence ranges, schema/digest and transactional publication receipt; no newly allocated SourceRecordId | Single fenced ArchivePublisher in the archive commit transaction; retained recovery lineage, not a new input fact; excluded from ordinary research exports unless needed for lineage |

The archive has no mandatory table for every graph vector or score at every tick.
Derived generations live in ClickHouse and versioned artifact storage, with PG
manifests. Explicit research retention/export can preserve selected generations
using their A08 schema and dependency manifest. This avoids silently retaining
every full population forever while keeping public scientific output lineage.

Measurement resolutions remain immutable facts. Their method schema defines an
allowed resolution of a named prior unknown fact; it does not overwrite it.
Reduction selects the applicable terminal evidence after the cut and counts the
original attempt once. Conflicting terminal evidence is surfaced as a conflict,
not resolved by whichever duplicate arrived last. P02/P04 fixtures include an
unknown cut, a later resolution and a pinned read of the earlier cut.

## ClickHouse serving relations

These are rebuildable from retained originals/evidence and declared input
manifests. An Indexer writes raw representations; owning derivation jobs publish
separate complete targets. Normal clients use the read core, not arbitrary SQL.

| Selected relation or existing target | Row unit / content and required input | Owner / cost |
|---|---|---|
| `serving.events` | Shared complete EventPayloadV1 plus internal source identity; one logical event, physical retries collapsed correctly | Indexer; a deliberate second payload representation; internal existence is not read authorization |
| `serving.event_eligibility_facts` | Scope-qualified signature, body digest and acquisition attribution, unchanged from archive | Indexer; joined with authorized payload values for full signed responses; required serving family even with provenance off |
| `serving.event_id_index`, `event_author_index`, `event_time_index` | Narrow event/filter/order references; physical projections or tables chosen in P02 | Indexer; no repeated content per index |
| `serving.event_tag_index` | Exact supported tag key/first value + event ID/source subrow; identical posting keys collapse | Indexer; tag fanout is charged |
| `serving.event_receive_facts` | Immutable first/earlier receive evidence per event/visibility scope for cut-relative metadata | Indexer; sparse corrections retained while required cuts remain readable |
| `serving.provenance_by_event`, `provenance_by_relay` | Two narrow orders over the same immutable witness facts | Indexer; optional P-sized cost twice, no body copies |
| `serving.imported_claims`, `dataset_lineage`, `dictionaries` | Source-attributed claims and referenced identity mappings | Indexer; visibility/claim trust preserved |
| `serving.measurement_facts`, `availability_selections`, `availability_facts`, `coverage_facts` | Hot typed evidence and source references corresponding to archive families | Indexer; measurement/provenance costs accounted separately |
| `serving.document_objects`, `document_references`, `document_parsed_views` | Configured hot original objects/references and versioned typed interpretations | Indexer/parser targets; an extra hot byte copy is counted, older object fetches use bounded archive access |
| `serving.current_headers` | One `(generation_id, visibility_scope_id, coordinate)` header: winner ID/time, parser status/version, member count and interpretation/visibility reason, including empty or invalid winners | Deriver P05-T02; independent of child presence; originals unchanged |
| `serving.current_memberships` | Header key plus typed relation/member identity, with signed list order represented separately from distinct logical membership | Deriver P05-T02; children reference the complete header generation; no old members resurrect from an empty winner |
| `current_replaceable_events`, `current_addressable_events`, `current_relay_list_entries` | Existing P05-T02 target/view names over the corresponding headers/memberships | Logical compatibility views/target bindings, not a requirement for another full copy of current state |
| `global_facts`, `social_facts`, `nip85_input_facts` | Versioned typed facts per subject/population/window with exactness and source cut | Independent P05 targets, never a generic unlabeled number |
| `relay_facts`, `relay_state_aggregates`, `relay_coverage` | Attributable observations, resolved evidence, qualified coverage and generation-keyed `relay.composition/1` summaries of a declared witnessed population | P04 local view / P05 resolved targets have separate ownership and input meaning; observed history is not current remote inventory |
| `relay_overlap_groups`, `relay_overlap_members`, `relay_dedup_candidates`, `relay_cluster_edges`, `relay_clusters` | Generation-qualified verified equal-set groups/members, comparable group candidates, verified group relations and connected components; endpoint pairs are the exact logical relation expanded lazily | P05 overlap target; exact/approximate candidate coverage explicit; no relay identity merging |
| `event_propagation_stats`, `relay_selective_availability`, `relay_availability_frontier` | Existing P05 provenance-derived targets with method, denominator, observed scope and unknowns | Optional provenance inputs; first witness is not physical propagation or current presence |
| `serving.current_graph_edges` | Selected current follow/mute relations with vertex/input lineage | Graph builder; partitioned adjacency/CSR artifacts separately account for full V/E work |
| `serving.rank_scores`, `relay_quality_scores` | Subject type/ID, profile/viewpoint, generation, component values and availability/explanation | Ranker / independent relay quality target; bounded useful generations, not permanent full snapshots each tick |
| `serving.search_documents`, `search_postings` | Analyzer-versioned lengths and token/position references; full content remains in events | Optional P06 search projection; measure common-query utility and total postings space |
| `serving.visibility_deadlines` | Optional read projection of authoritative derived expiry state, tied to serving incarnation and input generation | Indexer; due-work scheduling is `derived.window_inputs`, not a competing CH checkpoint authority |

Names without the `serving.` prefix in the middle rows retain existing P05 target
identifiers. They live in the serving namespace; a target may use a versioned
physical relation plus a stable logical view. Its manifest specifies that mapping
and parent/child keys. Raw facts retain source identity; derived rows add target
generation, subject key and a bounded subrow discriminator where applicable.
P05 owns exact per-target dimensions and formulas; a migration filename alone
does not define a table or a score.

The narrow event access paths can share engine projections; their logical names
do not require a physical table each. `search_postings` may map to a CH-managed
text index or an explicit compatible projection: account for its bytes either
way, without requiring a second handwritten inverted index. CH publishes query
results, while the incremental mutation/checkpoint state below has its own owner.

CH is not required for an archive-only deployment. In that topology these online
relations and their associated capabilities are absent, explicitly. Full online
and tiered modes state which body/object ranges are hot and which use a slower
archive fetch. A stale complete target remains dated; a half-written generation
does not become the current result.

## PostgreSQL operational relations

The following names are selected application-owned relations. Related rows in
one table entry are distinct tables, not arbitrary JSON keys. Column/FK/check and
transaction definitions are generated in the listed owning migration task;
keys/recovery constraints below are the implementation contract. Sensitive content
is private; public read models use allowlisted projections, never table dumps.

| Migration owner | Relations and important keys/content | Retention / boundary |
|---|---|---|
| 0001 control | `datasets(dataset_id)`; `topology_generations(dataset_id, generation)`; `role_instances(role_instance_id)`; `owner_epochs(resource_id, epoch)`; `config_revisions(revision)`; `policy_revisions(revision)`; `schema_versions(engine, migration)`; shared `jobs(job_id)` | Dataset/history and currently required authority retained; leases do not fence a still-running writer |
| 0002 ingest | `producer_slots(producer_slot_id)` and epoch/producer-checkpoint metadata; `source_streams(dataset_id, shard_id, owner_epoch)`; `consumer_frontiers(consumer_id, stream)`; `projection_chunks(generation_id, chunk_id)`; `snapshot_leases(lease_id)`; `retention_claims(claim_id)` | Coordinated metadata/causal backup; exact local receipt/pending content and owner ACK/retirement watermarks remain RocksDB-owned, not a second PG acceptance authority |
| 0003 relay | `relay_endpoints(endpoint_id)`; `relay_enrollments(endpoint_id, access_scope_id)`; `network_routes(route_id)` | Mutable enrollment/route policy with explicit reader visibility; immutable archive dictionary separate; no row per event delivery |
| 0004 acquisition | `acquisition_scopes(acquisition_scope_id)`; `acquisition_progress(acquisition_scope_id, layer)`; `acquisition_gaps(gap_id)` | Receipt-backed progress and unresolved needs; retain until safely resolved/checkpointed |
| 0005 import | `import_sources(source_id)`; `import_units(source_id, unit_id)` | Immutable source/artifact identity, unit digest, progress/receipt; display TTL cannot forget an accepted unit |
| 0006 relay lifecycle | `relay_schedule(endpoint_id, measurement_scope_id, method_id)`; `measurement_dispatches(attempt_id)`; `issuer_demands(demand_key)`; `issuer_cooldowns(demand_key)` | Operational scheduling/intents/bounded demand/cooldown; measurement scope binds observer/route/access/profile; unknown execution does not create a measured failure |
| 0007 Control backend | Adds immutable fence protocol/backend attribution to `owner_epochs`, one unreleased Control epoch per dataset and guarded typed Control mutations | Appends after 0802; pre-protocol Control owner history is refused atomically, never inferred to be excluded |
| 0008 job bounds | Authoritative closed-source checks over existing `job_input_bounds` and `source_streams`; indexed reverse lookup, no new authority | Appends after 0007; refuses incompatible existing cuts atomically. Job cuts and source closure use READ COMMITTED and hold conflicting source-row locks through commit; old topology NULL does not remove the authoritative cap. |
| 0009 archive intent issuance | `archive_attempts(attempt_id)` with unique `(dataset_id, ordinal)`, stable catalog, expected predecessor, semantic batch identity and coordinator/worker lifetime | Fenced Control issues immutable intents. Retain for ambiguous publication/credential recovery; not finite jobs, active archive ownership or catalog receipts. Pre-protocol archive owner history is refused atomically. |
| 0010 local coordinator history | `archive_coordinators(dataset_id)` binds one catalog/protocol and the exact local coordinator SHA256; new `archive_attempts.coordinator_binding_sha256` | Explicit fenced empty-history initialization; immutable original attribution. Old unbound rows remain inspectable and cannot be adopted. New issuance requires matching retained local inputs. |
| 0011 producer input reservations | `producer_requests(slot, epoch, sequence)` binds exact retained-input metadata and native manifest; `producer_epochs.reserved_through_request` advances atomically | Fresh-only Ingest history; input bytes remain outside PG. No native acceptance, logical outcome closure, ACK or GC is inferred; all reservations retained and nonempty retirement refused pending closure. |
| 0013 import input bindings | `producer_input_bindings(job, shard, logical_record)` retains complete initial event-only batch membership and original staged request metadata | Bound atomically with every reservation before dispatch; immutable, initially64 retained positions/job. Old unbound reservations cannot be adopted. No logical completion, checkpoint, ACK or GC authority; see [input bindings](../reference/import-input-bindings.md). |
| 0501 analytics/shared artifact inventory | `target_definitions(definition_id, version)`; `target_generations(generation_id)`; `target_inputs(generation_id, input_id)`; `active_targets(target_id, analysis_scope_id)`; `artifacts(artifact_id)`; `artifact_files(artifact_id, file_id)`; `graph_artifacts(artifact_id)` | Current plus two complete generations and required pins; analysis scope includes visibility/window/profile; graph_artifacts is a typed manifest binding to the common artifact inventory, not another file owner |
| 0603 read pins | `read_pins(pin_id)` extending existing retention claims | Query context/cut/generation/expiry and logical source-retention claims; authorization expiry is separate |
| 0609 DVM | `dvm_requests(request_event_id)`; `dvm_results(request_event_id, part_id)` with `intent_id` reference to `signed_outbox` | Request/result-part mapping and unresolved delivery; signed bytes have one outbox authority, not a second result payload copy; private payload display retention bounded |
| 0610 subscriptions | `dvm_subscriptions(subscription_id)`; `dvm_subscription_batches(subscription_id, batch_id)` | Expiry/resume/input cuts, reserved control semantics and pending outcomes |
| 0611 payments | `payment_intents(payment_id)`; `payment_obligations(payment_id, obligation_id)` | Encrypted preimage/reference, hash, amount, settlement/ambiguity/refund state; no forgotten financial obligations |
| 0703 outbox | `signed_outbox(intent_id)`; `signing_coordinates(binding_id, coordinate)` | Exact signed bytes before networking; coordinate clocks survive detailed delivery TTL |
| 0704 delivery | `outbox_destinations(intent_id, endpoint_id)` | Per-destination acceptance/refusal/unknown; one destination's ACK does not complete all others |
| 0705 / 0707 publication | `nip85_progress(binding_id, generation_id)`; `nip66_progress(binding_id, generation_id)` | Resumable subject traversal and compact output lineage; local NIP-66 does not require remote claim resolution |
| 0801 identity | `principals(principal_id)`; `principal_grants(principal_id, grant_id)`; `local_credentials(principal_id)`; `credential_challenges(challenge_id)`; `sessions(session_id)`; `api_tokens(token_id)`; `secret_references(reference_id)`; `management_audit(audit_id)` | Credential hashes/protected handles, no plaintext token archive; disablement preserves actor attribution; default private audit 365 days |
| 0802 management | `management_commands(actor_id, request_key)`; `config_applications(revision, role_instance_id)`; extensions to shared `jobs(job_id)`; `job_dependencies(job_id, dependency_id)` | Atomic command/job/pin acceptance, desired/applied state; terminal job detail normally 30 days, unresolved obligations longer |
| 0803 saved views | `saved_views(view_id)` | Owner, resource/query, label/revision; reauthorize on every reuse |
| 0804 artifact publication | `artifact_publications(artifact_id)` and additive publication/user-catalog fields over existing `artifacts`/`artifact_files` | Immutable manifest/files, storage refs, source/policy basis, revision/state/pin; publication independent of signing; no recreation of the shared file inventory |

Jobs cover exports, backup, restore verification, scrub, repair, migration and
finite acquisition/analysis. Their typed result points to immutable manifests;
there is no extra table for every job kind. Backup files, CSR graph files and
export bundles occupy artifact storage with checksummed manifests and retained
references. Their bytes are included in disk planning. Credential values remain
in the configured protected provider/local provisioning path, not the archive.

`graph_artifacts` adds graph/input/partition interpretation to an `artifacts`
identity and its `artifact_files`; common retention claims protect those files.
An internal graph artifact is not thereby a publishable research export.
Foundational artifact identity/file inventory is supplied in 0501 with its first
graph consumer; P08 adds user catalog/publication operations without imposing a
P05-to-P08 implementation cycle. Public discovery requires its separate verified
export manifest and publication grant/state. Duplicate metadata cannot authorize
one owner to reclaim a file still pinned by another consumer.

The first implemented 0802 consumer appends after 0805 and supplies only
`management_commands` for explicit first security activation. Its bytes16 request
key binds the installation actor, expected configuration/digest and immutable
policy/audit receipt. These bootstrap obligations are retained beyond ordinary
audit TTL. Existing initialization and enrollment rows keep their meanings;
per-role applications, general command kinds and job dependencies remain later
additive migrations under this responsibility, not empty early tables.

Shared finite-job rows are created by P02-T01 in migration 0001 for the first
P03/P04 consumers. The 0802 entry above adds management commands, grants and
dependency/application fields over that existing job authority; it does not
create a second job ledger. Similarly P02-T07 creates common `retention_claims`
and `snapshot_leases` under migration 0002. Migration 0603 adds read-cursor-specific
`read_pins` and extends those existing retention references. Typed reference
variants are validated against their actual owning snapshot/artifact contract;
later graph/cursor consumers do not force early storage tasks to depend on P05/P06.

## RocksDB key spaces

Each writable directory has one owner. The ingest owner's versioned domains are event
identity/admission metadata, exact event/visibility eligibility and receive minima,
event ordinals and dictionaries, optional exact scope-qualified provenance minima,
content-object identity, ordered pending source facts and complete admission-batch
boundaries retained independently of request-detail GC, producer
request digest/outcome and retirement watermarks, and owner/schema metadata.

The exact [identity and encoding contract](identity-and-encoding.md) owns field
order, fingerprint bytes and allocation/restore rules. CertifyExisting performs
a consistent read of these already durable facts and creates no permanent
certification/duplicate-delivery key space. Producer progress may reference its
dependency proof under the same causal backup closure as mutation receipts.
They are not separate daemons. A WAL-enabled synchronized batch binds effects
and receipt. Reclaim pending bodies only after verified archive protection and
retain exact identity/retirement information for its actual recovery obligation.
PG producer registry allocates authority; the Rocks owner checks and persists the
local commit/ACK boundary. No PG row alone proves a Rocks batch committed.
Any PG-observed ACK value is coordination metadata: the producer's durable
incremental checkpoint authorizes its ACK, and the owner-side synchronized
watermark permits receipt GC. Backup must preserve that causal chain; a restored
PG row cannot manufacture an owner ACK or recover an already reclaimed result.

### Shared derived state and reconciliation directory

The selected initial online layout uses a separate **Indexer-owned RocksDB
directory** for mutable incremental state and reusable reconciliation sets.
It never opens the admission directory as another writer. `derived.*` below
names typed key domains in that shared directory, not mandatory SQL tables,
one database per algorithm or new user-visible services. Deriver/Ranker own
their algorithms; the Indexer's storage owner applies their bounded typed
updates under target ownership, expected checkpoint and version checks, locally
or through authenticated RPC. Pausing new source projection need not stop the
storage owner from serving existing state; if the owner is unavailable, affected
jobs wait/refuse while prior complete CH results remain readable under policy.

Target-state domains qualify their keys by dataset, target, analysis scope and
working state generation. A mutable checkpoint cannot rewrite the state pinned
by an older published generation; use retained snapshots or immutable partition
references as required by the target's recovery/read contract. Reconciliation
domains use their separate shape/view generation rather than an analysis target.

| Selected domain | Logical key/state and owning task |
|---|---|
| `derived.checkpoints` | Target/analysis scope, working generation, expected prior checkpoint, input/dependency cuts and committed output-chunk identities; P05-T01 |
| `derived.dirty_keys` | Target/analysis scope/key with reason and maximum relevant cut; coalesced scheduling plus a bounded rebuild marker in the target checkpoint, not a record per duplicate; P05-T01 |
| `derived.current_state` | Typed coordinate header and child memberships, including empty/invalid winners, plus their prior contribution basis; CH current_headers/current_memberships are published read projections; P05-T02 |
| `derived.contributions` | Definition/scope/source event or replacement partition/subject contribution where retraction requires it; reuse committed source-chunk identity for purely additive metrics instead of a permanent receipt per event per metric; P05-T03/T04 and other retractable targets |
| `derived.distinct_memberships` | Definition/scope/subject/actor or exact distinct key and checked reference count; one of two remaining reactions still contributes one distinct actor; exact sets are an equivalent representation; P05-T03/T04 |
| `derived.unresolved_references` | Typed target reference and dependent source/definition, with target-to-dependent access; includes pending matching-author deletion targets until attribution can be resolved; P05-T02/T04 |
| `derived.deletion_effects` | Verified deletion request/author/target or addressable coordinate and horizon, with explicit missing state and input basis; no zero-time sentinel or deletion-request undo; P05-T02 |
| `derived.window_inputs` | Definition/scope/member or exact value/frequency, expiry deadline and inverse removal data needed for rolling windows/quantiles/current visibility; share typed variants, retain required resolution; P05 window/quantile targets and P06 temporal visibility |
| `derived.reconciliation_sets` | Configured shape/view/policy/encoding-limit generation with distinct ordered `(created_at,event_id)` membership and compatible range summaries; P06-T08 |
| `derived.reconciliation_pins` | Owned immutable set snapshot/read lifetime and relevant generation identities, referenced by the common PG read/retention pin contract; bounded client/cache lifetime, not a second authorization lease; P06-T08 |

Relay composition reuses these contribution/distinct domains for scoped
event/endpoint membership, author reference counts and exact concentration state.
It does not create another permanent duplicate-delivery log, raw body copy or
unbounded index for every possible research filter. P05-T07 owns its population
and generation semantics.

Use a WAL-enabled synchronized atomic batch for a target's state changes,
checkpoint and deterministic pending output identity; replay after interruption
cannot count a contribution twice or skip its CH output. CH materialization is
verified before the fenced PG complete-generation pointer changes. A Rocks
checkpoint, an inserted CH row and a published target are distinct boundaries.
Retain enough staged output/state or deterministic replay inputs to resolve a
crash between them; this is not a distributed transaction claim. P05/P06 must
exercise the actual binding, checkpoint/replay and generation retention before
this selected layout is qualified. Immutable partition artifacts remain a
measured alternative for suitable large targets with the same recovery contract.

Reconciliation stores only configured shared shapes durably; arbitrary filters
use the existing bounded temporary/cache allowance. Its keys/pins include source,
current view, visibility, encoding, subscription-ID allowance and serving-limit
revision; expiry/replacement/deletion/policy changes update membership without
new event arrival. Range summaries implement the pinned Negentropy method and
advertise only currently retrievable bodies. Compare persistent Rocks sets with
bounded CH sorted reads for narrow filters; retain extra indexes only where
repeated reconciliation justifies their measured cost. A successful negotiation
does not freeze the policy of later REQ retrieval.

The directory and required checkpoints are backed up coherently with published
manifests or rebuilt from their retained inputs; an incomplete rebuild cannot
replace an old complete generation. Account for its keys, WAL, snapshots, native
memory and compaction reserve separately from ingest Rocks and CH. Archive-only
deployments do not allocate these online domains. Current-state, contribution,
refcount and NEG domains are absent when no enabled target needs them.

## Document layout and archive immutability

The baseline uses logical `document_objects` plus `document_references`, because
original bytes, actual fetches, parser versions and visibility have distinct
lifetimes and identities. It avoids copying an unchanged document into every
scientific source fact. This is a semantic choice, not proof of minimum disk use.

P02-T09 must compare two physical candidates before final DDL activation:

* Separate object bytes plus narrow observation/reference rows: global reuse
  across batches, explicit closure/GC, but extra joins, files and identity state.
* Wide observation rows with repeated original bytes and their content identity:
  simple scans/export and useful compression, but repeated bytes across files or
  row groups, wider writes and no automatic global content deduplication.

The latter can implement the same logical object/reference interface through
versioned views/adapters; repeated physical bytes are not extra logical objects
or measurements. Compare equivalent metadata, visibility, exact bytes, file
counts, dictionary fallback, low/high churn, query cost, compaction and restore.
The [Parquet encoding specification](https://parquet.apache.org/docs/file-format/data-pages/encodings/)
defines dictionary pages per column chunk, with fallback when a dictionary grows.
It provides no global cross-file object store. Small experiments inform this
choice; neither candidate is declared universally smaller or faster.

"Intact archive" means retained original facts and identities remain interpretable
and unchanged across compaction/replay. Physical file names and boundaries may
change transactionally; pinned snapshots and backups retain their referenced
files. It does not mean every old physical file lives forever. A standalone
export writes a manifest and selected table files with all needed objects and
dictionaries. Copying a glob of live DuckLake files is not that export.

The catalog is validated by P01 typed schema fixtures, P02 migrations/roundtrips/
faults and P05/P06/P08 real producer-to-query/export journeys. Compare declared
tables, columns, row units, ownership and dependencies against actual engine
introspection; then query them through independent readers. A name inventory
alone cannot certify the layout or the correctness of its joins.

### Initial installation attribution and application order

The [central migration manifest](../../migrations/manifest.json) records actual
application order separately from reserved numeric IDs. The compiled catalog is
checked against it. Append a migration when its first responsibility exists;
never insert it before already published entries, even if its reserved ID is
lower. Ledger readers validate and order known receipts by catalog position.
Unknown, reordered, missing or changed history is still refused. Old binaries
that do not know 0801 refuse that schema; binary rollback therefore requires an
explicit compatible artifact, not a guessed numeric version comparison.

0801 initially implements only `principals` of kind `installation` and
`management_audit` for accepted `dataset.initialize`. This actor records the
PG session role and local effective UID and is incapable of login or grants.
The initializer atomically binds dataset/config revision 1, actor and audit ID.
The initialization audit is a permanent bootstrap/replay obligation, excluded
from ordinary audit TTL. Existing unattributed config rows remain untouched and
cannot be adopted by bootstrap. P08-T02 extends the schema with real credential,
session, grant and ordinary audit consumers using newly allocated immutable
migrations; its full relation inventory above remains required.

0805 adds the first human consumer: `local_accounts`, `local_credentials`,
`principal_grants` and `owner_challenges`. Principal attribution stays immutable;
installation rows cannot back local accounts. Challenges bind one intended name
and principal, keep terminal history, permit one pending token per dataset and
only one initial owner. Token digests and versioned password hashes are private
credential material, never diagnostic output. Owner transition audit rows extend
0801 without rewriting it. Enrollment receipt and challenge rows remain required
for replay resolution after ordinary audit TTL. This does not implement sessions,
API tokens, account lifecycle or active policy leases. The
[local enrollment contract](../product/private-administration.md#local-first-owner-transition)
owns its command and failure semantics.
