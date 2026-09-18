# Private pending batch handoff

Status: P02-T04 prerequisite in progress. `bigbrotr-protocol::archive_batch`
transfers one retained complete admission batch from the Rocks owner to a finite
native worker. `PendingBatch::write_archive_batch` retains the pending-data
permit while writing. Transfer and private preparation never acknowledge an
archive receipt, advance a source cursor or reclaim pending facts.

This private framing is not a portable archive/export manifest, an admission
request or a public endpoint. It reuses existing canonical fact preimages and the
`bigbrotr/admission-batch/1` semantic digest. It does not substitute that digest
for the separately defined multi-range archive-batch identity.

## Version 1 bytes

Integers below are unsigned big-endian binary values. The header is 80 bytes:

| Offset | Bytes | Meaning |
|---|---|---|
| 0 | 8 | Literal `42 42 41 50 00 00 00 01` |
| 8 | 16 | Nonzero dataset ID |
| 24 | 4 | Source shard |
| 28 | 8 | Nonzero owner epoch |
| 36 | 8 | Nonzero first sequence |
| 44 | 4 | Nonzero record count |
| 48 | 32 | Complete admission-batch digest |

Exactly `count` frames follow. Each is `payload_digest[32]`, `preimage_length:u32`,
then those exact canonical preimage bytes. Sequence `first + index` must fit u64
and match the decoded fact's complete source identity. No trailing bytes are
permitted. The existing closed decoder verifies canonical representation, version,
structural references and digest; it does not grant policy/allocation authority
or reverify signatures.

The encoder checks the supplied record count, lengths and hashes against the
contiguous membership before writing the header, borrowing the retained
preimages without copying or decoding them. The reader additionally verifies
each decoded source identity. A transport failure can still leave a partial
output. The reader checks header
count/overflow before reserving reference storage and frame size before allocating
or reading its payload. Separate bounds cover record count, aggregate preimage
bytes and existing per-fact decoding limits. Framing overhead is 80 + 36 * count
bytes. The application allocation floor is the bounded reference vector, reusable
maximum preimage buffer and one transient decoded fact/allocation budget; it is
not process RSS, an Arrow grant or native memory.

Any failed read poisons the reader. `next_fact` may yield a prefix for private
staging; only `finish` validates complete membership/digest and EOF. Callers must
drop each fact after staging to preserve the one-fact allocation bound. A missing,
altered, foreign, reordered, truncated or extra frame cannot complete. Resource
limits remain distinguishable from integrity and compatibility failures.
Cancellation is checked between bounded records; synchronous I/O itself can
block. The production parent must own a deadline and positive worker termination.
That supervisor is still required; a dropped future is not termination.

## Private native preparation

`PreparedBatch` consumes an isolated `Engine`. It creates five private staging
tables with explicit typed schemas and feeds each decoded fact through the
[row adapter](archive-format.md), checking native append and flush results.
Only after complete frame validation does it return prepared input. Any failure
drops the engine and partial in-memory staging; job-owned spill cleanup still
belongs to the positively terminated parent. There is no DuckLake attachment,
permanent catalog mutation, publication receipt or source acknowledgement.

The initial path appends one fact at a time. This bounds retained application
input independently of the native aggregate staging but has per-fact appender
overhead. It is not a throughput-qualified batching strategy or a replacement for
P02-T04's file/group/timer/resource acceptance. A future publisher consumes the
staged families under one positively fenced catalog transaction; a diagnostic
count cannot establish full content or reference closure.

The private executable command is:

```text
bigbrotr-archive-worker inspect-batch BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS MAX_RECORDS MAX_ENCODED_BYTES
```

It reads one framed batch from stdin through EOF and emits one bounded diagnostic
result after successful preparation. Per-fact limits currently use
`FactDecodeLimits::default()`: 2 MiB encoded, 8 MiB requested allocation, 65,536
tag nodes and 256 KiB per retained string. Staged counts are ordered dictionary,
event, eligibility, receive, provenance. Source u64 wrappers retain their existing
decimal-string JSON representation. The result is explicitly
`bigbrotr-private-preparation/1`, never an archive receipt. No partial stdout
result is emitted for invalid input. Blocking input/output, pre-exec asset checks
and worker grants still require the production parent.

The independent [fixture author](../../crates/bigbrotr-protocol/tests/fixtures/archive/write_fixture.py)
uses literal canonical CBOR membership and standard SHA-256/struct encoding,
without the Rust codec. Tests retain all truncation positions, digest/source/
version/trailing-byte controls, exact limits and poisoned-reader behavior. The
native staging and executable tests use a synthetic unsigned-extreme model fact;
they do not claim new signed admission or publication evidence. The Rocks test
checks a real synchronized bootstrap/reopen/transfer with unchanged native
sequence. Full signed pending-to-process/catalog readback remains required.
