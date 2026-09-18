# Archive row format

Status: implementation in progress under P02-T03/T04. The private `Rows` adapter
converts the five existing immutable fact families to bounded Arrow batches
and explicitly flushes a native appender. It grants no admission, scope, catalog
publication, protection or export authority. Later fact families, dictionary closure,
transaction receipts, production parent supervision and standalone export
manifests are still required. A diagnostic Parquet file is not a published archive.

The owning [data contract](../specification/data-contracts.md) and
[relation catalog](../specification/storage-catalog.md) define scientific meaning.
The physical event column order is:

| Column | Arrow / DuckDB type | Meaning |
|---|---|---|
| `source_record_id` | Struct / STRUCT | `dataset_id` binary16, `shard_id` u32, `owner_epoch` u64, `sequence` u64 |
| `record_schema_version` | UInt16 / USMALLINT | Fact schema, currently 1 |
| `record_kind` | Utf8 / VARCHAR | `event-payload` |
| `visibility_scope_id` | Struct / STRUCT | Complete qualified reference, never a bare ordinal |
| `payload_digest` | FixedSizeBinary(32) / BLOB | Existing canonical fact digest, independent of physical file bytes |
| `recorded_at` | Int64 / BIGINT | Original signed UTC microseconds, including i64 extrema |
| `event_id`, `pubkey` | FixedSizeBinary(32) / BLOB | Original raw bytes |
| `created_at` | UInt64 / UBIGINT | Original unsigned seconds, including u64::MAX; not a calendar timestamp cast |
| `kind` | UInt16 / USMALLINT | Original unsigned kind |
| `tags` | List(List(Utf8)) / VARCHAR[][] | Complete ordered tags, duplicates and empty strings |
| `content` | Utf8 / VARCHAR | Uninterpreted original Unicode, including NUL/control characters |
| `event_ordinal` | Struct / STRUCT | Complete qualified event reference |

All these fields and their children are non-null. A qualified reference has
`dataset_id` binary16, `dictionary_kind` u16, `allocation_domain` binary16 and
`ordinal` u64. DuckDB/Parquet may represent fixed-length bytes as variable BLOB
storage; the typed model and independent readback retain the exact-length
invariant. A shared event row intentionally has no signature: the permitted
eligibility fact owns the scope's original representative signature. Rejoining
an arbitrary global representative would violate that boundary.

All families begin with the same six envelope columns. Their following columns,
in order, are:

| Relation / constructor | Payload columns |
|---|---|
| `archive.events` / `Rows::events` | The event fields above |
| `archive.event_eligibility_facts` / `Rows::eligibility` | `event_id` binary32, `body_digest` binary32, `sig` binary64, `first_eligible_at` i64, `origin` closed text, `import_lineage` qualified reference or null |
| `archive.event_receive_facts` / `Rows::receive` | `event_id` binary32, `received_at` i64, `observer_id` qualified reference, `clock_quality` struct, `corrects` source identity or null |
| `archive.provenance_facts` / `Rows::provenance` | `event_id` binary32, `endpoint_id`, `observer_id`, `access_scope_id` qualified references, `received_at` i64, `clock_quality`, `verification_method_id` qualified reference, `corrects` source identity or null |
| `archive.dictionaries` / `Rows::dictionaries` | `reference` qualified reference, `normalization_version` u16, `descriptor_schema` closed text, `descriptor_version` u16, `descriptor_value` typed struct |

Clock quality contains `clock_id` qualified reference, `resolution_us` u64,
`uncertainty_us` u64 or null and `anomaly_flags` u32. Unknown uncertainty stays null;
measured zero stays zero. Corrections preserve the entire source identity.

`descriptor_value` is a closed wide struct for the seven
[descriptor schemas](../specification/dictionary-descriptors.md). Its fields in
order are `event_id` binary32; `class`, `url`, `clock`, `vantage`, `software` text;
`method_id` qualified reference; `public_key` binary32; `route_class`,
`software_version`, `label` text; `visibility_scope_id` qualified reference;
`input`, `algorithm`, `capability`, `output_unit`, `failure_taxonomy` text;
`algorithm_version` u16; `protocol_revision`, `format` text; `format_version` u16;
`source_dataset_id` binary16; `source_artifact_digest` binary32. These children
are nullable: the schema distinguishes an inactive variant field from unknown
attribution within the selected variant. No arbitrary JSON extension is accepted.
Source lineage can name a foreign dataset; local qualified references cannot.

Nullable references/source identities mask their scalar children as well as their
parent before native conversion. The pinned binding otherwise leaves valid
placeholder values beneath a null struct, which can shift values during Parquet
writing even when ordinary table SQL is correct. The dictionary readback test
reproduced a known method reference becoming zeroed fields; the matching masks
restore it. Child fields remain required when the parent is present. Canonical
fact/dictionary bytes and identities do not change, and no previously qualified
archive files require migration from this never-published failing path.

Conversion first checks the entire input slice without allocating output. Rows
must be nonempty, one family and at most the caller's limit, capped at 1024.
Separate limits count event UTF-8 content/tag bytes or dictionary descriptor text,
and tag containers plus string nodes, including empty ones. Fixed closed enum and
schema text in other columns has a bounded per-row cost. Limits cannot exceed
i32::MAX because Arrow offsets are signed 32-bit. Fixed envelope/column buffers
scale with the bounded row count.
Builders reserve measured string/node capacities and produce fresh complete child
arrays; callers cannot supply sliced Arrow arrays that retain unrelated children.
This avoids the pinned binding's repeated full-child conversion across row slices.

These limits are not an RSS guarantee. The caller retains input facts/decoded
buffers while Arrow and native vectors coexist; the parent grant must also cover
allocator overhead, engine state and signed-extension startup. Native allocation
failure can terminate the worker. `array_memory_bytes()` exposes Arrow's array
accounting only. Appender errors, including explicit flush failure, fail the work;
the transaction owner must abort/resolve its outcome before retry. No pending
reclamation is authorized by a successful appender flush.

Initial Mac arm64 checks use independently signed extreme-time fixtures, real
DuckDB appending and ZSTD3 Parquet reopened by a fresh connection. They compare
every body/envelope field and reverify the reconstructed event using the retained
fixture signature. Additional cases exercise 1024 rows with 24,576 child strings,
a 1 MiB content row with 10,000 empty-valued tags, exact byte/node bounds and
constraint failure during flush. Modified wide inputs test conversion only and
are not claimed signed. Native Linux, production publication and cross-reader
qualification must be recorded separately before those guarantees are claimed.
The initial event-only candidate has
[native Linux adapter evidence](../implementation/evidence/p02-event-rows-native-20260918.json).
The subsequent Mac checks also exercise all ten independent dictionary fixtures,
network/import eligibility, receive/correction/null distinctions and provenance
reference positions. These are field conversion scenarios; conflicting fixture
mappings are not admitted as a real dataset and do not qualify allocation authority.

The five-family adapter at `5c3a8dd` also has
[native Linux evidence](../implementation/evidence/p02-five-family-rows-native-20260918.json),
including the sparse-null counterexample and fixed full-field readback. Later
handoff/staging changes require their own validation.


## Explicit initial scientific schema

The compiled [schema-1 declaration](../../contracts/archive/v1/schema.json) owns
native SQL types for the five existing families and `archive_commits`. Private
staging consumes the same five scientific definitions. The finite private
`initialize-catalog` operation requires an exclusively reserved, empty PostgreSQL
database and an existing canonical empty local data directory. It checks the
catalog before DuckLake ATTACH, disables native automatic migration and persistent
secret storage, and creates all six relations in one native transaction. It does
not adopt or retry an existing catalog. Snapshot author/message plus the declaration
SHA-256 aid inspection; they are not an archive receipt or scientific readiness.

`archive_commits` has no source envelope. Its required columns, in order, are
`dataset_id` (binary16), `schema_version` (u16 semantic batch schema),
`topology_generation` (u64), `batch_digest` (binary32), `record_count` (u64),
`input_ranges` (ordered list of `shard_id` u32, `owner_epoch` u64,
`first_sequence` u64, `last_sequence` u64), `attempt_id` (binary16) and
`publication_ordinal` (u64). The dataset applies to every range. These will bind
the retained complete input and fenced credential lifetime in the publication
transaction. Initialization inserts no receipt and makes no publication/protection
claim. Model validation must enforce lengths, nonzero identities, nested required
children and complete canonical ranges; DuckLake does not enforce primary/foreign
keys or these semantic constraints.

The scientific guard installer validates the complete empty snapshot1 state and
atomically binds the declaration and [native layout](../../contracts/archive/v1/native-layout.json)
through additive migration0002. Applied0001 remains unchanged. `local()` selects
scientific compatibility; the explicitly named native-only foundation path cannot
adopt a scientific installation. Every inspection/activation checks live nested
metadata under the owner lock, independently of PG structural hashes. The
[independent oracle](../../experiments/archive-engine/scientific_layout_oracle.py)
derives all 203 expected native column rows from the closed declaration and can
compare separately collected PG rows.

Installed recovery, publication and the CLI journey remain required. A failed or
lost initialization response must be resolved by fresh privileged inspection;
never delete data or repeat initialization merely because the response was absent.
Only the exact complete empty scientific state can proceed to guard installation;
partial/native-only state is refused.

The [scoped native qualification](../implementation/evidence/p02-scientific-schema-native-20260918.json)
at `0e2ca43` passes twelve real-PG initialization/refusal cases and 39 native Rust
cases. It does not close the scientific guard, publication or recovery gates above.

The [scientific guard qualification](../implementation/evidence/p02-scientific-guard-native-20260918.json)
at `a91ae01` covers 37 actual-PG sequences and two Rust identity cases. It includes
native ALTER/DROP/rollback, all selected schema/NULL/metadata refusals, installation
rollback, lost guard COMMIT and two directly observed SQL lock orders. Those lock
cases use PG statements; no scientific native publisher or batch receipt is
qualified. Two fixture failures and their corrected independent oracles are retained.
