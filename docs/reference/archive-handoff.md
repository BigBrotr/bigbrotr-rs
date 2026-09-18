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
block. The private application parent described below owns the pipes and positive
worker termination. Whole-role crash/recovery remains required; a dropped future
is not termination.

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
result is emitted for invalid input. The application parent now supervises this
command; full publisher admission, process-crash exclusion and catalog recovery
remain separate required work.

The independent [fixture author](../../crates/bigbrotr-protocol/tests/fixtures/archive/write_fixture.py)
uses literal canonical CBOR membership and standard SHA-256/struct encoding,
without the Rust codec. Tests retain all truncation positions, digest/source/
version/trailing-byte controls, exact limits and poisoned-reader behavior. The
native staging and executable tests use a synthetic unsigned-extreme model fact;
they do not claim new signed admission or publication evidence. The Rocks test
checks a real synchronized bootstrap/reopen/transfer with unchanged native
sequence. Full signed catalog/body/reference readback remains required.

The [native qualification at `0cdd80f`](../implementation/evidence/p02-pending-handoff-native-20260918.json)
records ten scoped Rust cases and two separately executed packaged-binary cases
on Tux Linux amd64. It checks every frozen-wire truncation, typed staging and real
Rocks reopen/transfer. Those adjacent boundaries were exercised separately. The
later [supervision qualification at `c365053`](../implementation/evidence/p02-private-supervision-native-20260918.json)
connects actual signed admission/reopen/pending to the exact packaged worker twice,
with unchanged full Rocks state and bounded failure controls. It does not qualify
publication or full archive content/reference readback.

## Supervised private inspection

`storage::archive::Preparation::inspect_pending` connects a retained Rocks batch
to the actual worker. It admits one complete native-memory/item grant plus a
128 KiB control grant without waiting while holding another scarce grant. The
pending owner retains its existing data credit. The configured engine memory and
explicit extra native reserve fit the admitted total; the extra reserve must
cover decoding, Arrow, extension startup/threads, stacks and allocator cost.
Validation checks the decoder floor, not a measured process RSS ceiling. CPU/PID
composition, filesystem reservations and maximum-unit memory qualification still
gate the deployed worker role. The caller owns its temporary-disk reservation.

Before exec, the parent hashes the expected executable and the compiled target's
library/extensions. The expected executable identity must come from an
authenticated release or a trusted exact-source build; the API never reads its
expected hash from an untrusted adjacent file. Runtime release authentication is
still a P01/P08 gate. The installation must remain immutable to the runtime
identity; hashing cannot exclude a concurrent privileged filesystem writer.
Canonical absolute paths reject symlink substitution. The job root is separate,
empty, owned by the effective user and inaccessible to group/others.

The child receives an absolute executable path and a cleared environment, with
the verified library search path, fixed locale and `BIGBROTR_ARCHIVE_PARENT_PID`
set to the spawning process's positive PID. Before opening the engine, the
worker refuses missing, malformed or foreign parent identity. On Linux it sets
`PR_SET_PDEATHSIG` to SIGKILL and checks parentage again, closing the race where
the parent dies before signal registration. The synchronous spawning thread must
retain ownership through reap: Linux associates this signal with that thread,
not the lifetime of its entire thread group. The worker must not fork, exec or
change credentials after registration. Mac development checks initial parentage
only and does not claim this Linux termination guarantee.

The parent sets its three pipe ends nonblocking, drains at most 4 KiB from each
output, and checks a shared
cancellation flag/deadline between bounded I/O operations. Output floods fail;
complete-looking stdout followed by a failed exit or stderr is not success.
The parent checks returned membership/digest/counts against its retained batch.
An input limit remains a resource refusal, even when the child subsequently
rejects the incomplete input. Diagnostics are private preparation results only;
counts are not a full content-read oracle or durable publication receipt.

On error/drop, the parent sends SIGKILL and polls for reaping for 300 ms. Native
and control grants remain owned until exit is observed. If the OS cannot establish
exit, `TerminationUnknown` takes precedence: preserve the job root and exclude
old activity before retry. Dropping an unresolved handle permanently charges its
capacity in that parent process, preventing timeout from freeing possibly live
native work. This is fail-closed accounting, not a claim that every kernel task
is killable on a wall-clock deadline. File verification and process creation use
trusted local filesystem/OS calls, not an interruptible filesystem sandbox.

This transport is restricted to the trusted worker, which must not fork child
processes. Linux parent-death registration is implemented; native qualification
of the corrected worker remains pending. The retained old-worker negative control
stops the initialized worker before killing its parent; that worker survives
until the harness separately kills and reaps it. A signal does not prove positive
exit or allow immediate cleanup/retry. Host loss, persisted job recovery and
remote PG backend termination still require their own exclusion. In particular,
child process exit alone cannot resolve a catalog commit. The worker has no catalog
credentials in this inspection path and leaves pending untouched. No P02 task or
M1 gate is completed by this slice.
