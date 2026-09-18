# Archive row format

Status: implementation in progress under P02-T03/T04. The initial `EventRows`
adapter converts existing immutable EventPayloadV1 facts to bounded Arrow batches
and explicitly flushes a native appender. It grants no admission, scope, catalog
publication, protection or export authority. Other families, dictionary closure,
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

Conversion first checks the entire input slice without allocating output. Rows
must be nonempty, one family and at most the caller's limit, capped at 1024.
Separate limits count UTF-8 content/tag bytes and tag containers plus string
nodes, including empty ones. They cannot exceed i32::MAX because Arrow offsets
are signed 32-bit. Fixed envelope/column buffers scale with the bounded row count.
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
