# Ingest store initialization and reopening

`IngestStore` owns the first private native RocksDB handle for P02-T02. It exposes
explicit `initialize` and `reopen` operations, the bound manifest, read-only native
counters, a [bounded complete pending-batch feed](ingest-pending-format.md), and
[retained request-detail inspection](ingest-receipt-format.md#native-detail-inspection),
and [bounded producer-input recovery](producer-request-inputs.md).
There is no generic put/delete, exported DB/CF handle, admission,
receipt or policy grant. Opening a compatible directory is not proof of current
control authority or permission to accept events.

The stored layout is exactly `default`, `facts` and `receipts`, under the
[ingest key format](ingest-key-format.md), [manifest format](ingest-manifest-format.md)
and [native option profile](ingest-native-profile.md). The selected binding is
`rocksdb 0.25.0`, default features disabled, with `zstd` and `bindgen-runtime`;
`Cargo.lock` pins `librocksdb-sys 0.19.0+11.8.1`. No platform is qualified merely
because this dependency resolves or links.

## Entry conditions and observable effects

Both operations take an absolute directory path, a fully specified expected
`IngestManifest`, checked `RocksNativeLimits` and an owned runtime permit covering
at least `native_allowance_bytes`. The permit is checked before path creation or
native allocation. A control-lane or insufficient native permit is refused and
released. An adequate permit remains owned through every synchronous native call,
manifest check and database close, including error paths.

`initialize` requires an existing parent directory and a target entry that does
not exist, including no dangling symlink. It creates only that target directory,
with private Unix permissions, after reserving its canonical parent/name. Before
native creation, it opens the existing parent as a directory without following a
final symlink and synchronizes that descriptor. RocksDB's synchronization inside
the new DB/WAL directory does not persist the new directory entry in its parent.
An open/sync failure returns `ParentDirectorySync`, retains the partial directory
and performs no native initialization; both reservations remain held through the
call. The filesystem and device must honor the synchronization operation. This
does not qualify power-loss behavior on every filesystem or device.

It does not adopt an empty directory or recursively create missing parents.
Native create and missing-CF creation are enabled only in this path, with
error-if-exists set.
The initial application write is a single batch containing only the manifest at
`01 01`, with WAL enabled, `sync=true` and missing-CF writes forbidden.

A native initialization failure can leave engine files without a manifest. A
manifest write error is `ManifestCommitUnknown`: the write may have committed.
Neither failure deletes files, retries with new identity, or calls repair. Inspect
and resolve the existing state; `initialize` will refuse the now-existing path.
A successful matching reopen can identify the manifest, but does not by itself
supply missing control authority or qualify a failed storage device.

`reopen` requires an existing directory and a regular, bounded `CURRENT` pointer
to a native `MANIFEST-<decimal>` file. It refuses missing, malformed and symlink
CURRENT entries before calling the binding. It then checks the exact CF set,
opens all three explicit descriptors with both creation options disabled, reads
the manifest through a pinned native value and requires exact equality with the
expected manifest. The codec refuses values longer than 266 bytes before field
processing. It makes no additional Rust copy of an oversized native value; native
recovery, block reads and allocations are still subject to engine/OS limits.

The [native profile](ingest-native-profile.md#selected-options) selects absolute
WAL consistency. Detected checksum damage and incomplete records refuse native
open; no automatic point-in-time salvage occurs. Preserve the directory for
explicit recovery. This stricter acceptance can make an interrupted trailing
append unavailable. It neither repairs already lost history nor makes successful
open an authoritative noncommit or backup-validation result.

Unknown versions, malformed or absent manifest, a different dataset, owner
resource, source shard, owner epoch, process incarnation, topology, recovery or
policy coordinate, and any changed current namespace are refused. No mismatch
rewrites the manifest. Native read-write open may perform recovery and update
RocksDB metadata/LOG/OPTIONS before manifest validation; refusal is not a promise
that all filesystem bytes remain unchanged. The application record is preserved.

## Lifetime, exclusion and path boundary

An in-process reservation uses the canonical path before native open. It prevents
ordinary path/symlink aliases from reentering the same native owner. The native
RocksDB `LOCK` supplies the separate operating-system lock between processes.
Neither is a distributed lease or a fence for a host that still owns the storage.
Storage roots and their parents must remain operator-controlled and stable while
open; hostile rename, mount/device aliases, manually replaced LOCK files and
independently writable copies are not qualified by canonical path comparison.

The struct owns the native DB before its retained cache/WBM, permit and path
reservation in field destruction order. The DB destructor closes CFs and joins
its native background work before those reservations are released. The binding's
Drop does not report native close errors and must not be used as a durable ACK.
Initial durability depends on the parent directory synchronization and explicit
synchronized write result.

These operations and destruction are synchronous and can wait on native I/O.
The eventual runtime consumer must use its bounded owner worker, keep the permit
through completion and join it before replacement. Timing out or dropping an
async join handle cannot release these resources or prove native exclusion.
There is no unsafe interrupt or fictitious cancellation guarantee in this API.

A new process obtains a fresh incarnation, so its new expected manifest does not
match an old one automatically. Restart, handoff, restore and policy/topology
changes require their explicit authorized transition and old-owner exclusion;
ordinary reopen must not impersonate the previous process or update the tuple to
make the comparison succeed. Retained source facts, allocation/producer watermarks
and causal backup closure remain obligations of those later consumers.

Native allowance accounting is a reservation floor, not an RSS ceiling. Cache,
WBM, per-family buffers and native background work have overlapping and additional
costs. Use the profile's counters as observations and retain OS process limits.
The current wrapper's CF-list and native manifest replay APIs do not impose a hard
CPU/I/O/allocation bound on a damaged database's physical metadata. Such a refusal
is not evidence that arbitrarily corrupt metadata is cheap to inspect.

## Validation boundary

`rocks_store` exercises actual bindings and local engine files: explicit creation,
external exact manifest/CF inspection, reopen, every identity/namespace mismatch,
missing/malformed CURRENT and manifest, missing/extra CFs, permit lifetime/refusal,
canonical aliases, concurrent initialization and a real second process denied
while the first is open and accepted after it closes. The profile's unit tests
retain the positive shared-WBM
accounting oracle and the CF-only negative control against the same library module.
The Mac-specific case-alias scenario requires an actual case-insensitive temporary
filesystem; absence fails that fixture rather than claiming unexecuted evidence.

Execution identities and results belong to the slice evidence. These tests do not
finish P02-T02: signed-event atomic admission, receipt ambiguity and replay,
producer retirement, crash/ENOSPC/fsync faults, positive stale-owner exclusion,
backup/restore, mixed/aged workloads and Linux target qualification remain separate
practical gates. The explicit first-lifetime CLI below is under qualification;
restart/handoff and initialization recovery are not yet installation commands.

## Explicit initial owning lifetime

`bigbrotr serve --file ROOT --roles control,ingest --initialize-ingest` explicitly
selects fresh-only initialization and retains that original Control identity until
stopped. A separate finite initializer cannot hand this initial writer to a new
Control process. Ordinary Serve still never creates an Ingest directory. Static
composition must declare the Archive counterpart; this process selection does
not start it. Configured archive reader, export destination, cursor keyring or
private asset prerequisites are refused by this narrow path.
Both complete accepted documents must also permit the selected profile.

Accepted administration enables only login, self inspection and logout on the
configured loopback listener. This reuses the ordinary Management dispatcher and
session prerequisites (migration0809 and its SQL privileges). Initial Ingest has
no reauthentication, grant, job, history or asset routes yet. The initialized
record identifies `initial-sessions-only`; session access does not imply any
native admission permission. A disabled managed seed cannot override the two
accepted documents. Changed accepted snapshots stop this initial lifetime.

The worker first refuses existing targets and absent/symlink parents, then claims
Control. A bounded fenced basis read obtains the real accepted snapshot, server
clock/deadline and absent or sealed empty topology. Registration independently
rechecks that basis. Fresh OS identities establish the first shard0/resource,
producer and nine empty allocation namespaces. The producer outstanding limit is
the smaller ceiling of the two accepted documents, never the startup file seed.
The existing native initializer commits its complete manifest; the existing
connected bootstrap then admits exactly the public/private visibility mappings
with original staged input and PG reservation/outcome. No event is collected.

One dedicated blocking worker exclusively owns store and Control. The parent
current-thread runtime drives the actual PG connection, signals and phase
watchdog. Only a closed terminal report leaves the worker. Native store destruction
precedes Control destruction on normal errors and panic; graceful shutdown closes
native before explicitly releasing Control, then joins the driver. Cancellation
does not detach or abort native work. If the worker cannot join within the
configured shutdown bound, the process exits4 without first aborting the driver,
releasing the fence or claiming graceful release. Interrupted/ambiguous state is
preserved. Diagnostics use the existing bounded, nonblocking lifecycle channel.

HTTP runs on the parent runtime; one additional blocking slot holds at most one
password hash. Parent cancellation revokes the socket delivery gate and stops new
connections even while native work is blocked. The worker joins HTTP/password work
before normal native close and explicit Control release. A fixed renewal deadline
wins a ready command queue; processing requests cannot postpone it. Both renewal
responses consume the request-start authority window, and only a timely pair may
renew delivery. Commands and socket writes independently check remaining authority.

Between operations the same worker refreshes Control, then renews initial Ingest.
Every native operation still needs its own fresh authority; scheduling is not a
grant. Expired Ingest cannot be revived. Changed accepted snapshots stop the
initial lifetime. An initialized report explicitly has `ready=false` and
`acquisition=not-started`; maintained dictionaries are not complete Ingest/Sync
readiness. SIGTERM does not close the source stream, acknowledge producers or
qualify successor admission.

The explicit small native profile uses32MiB cache,16MiB WBM,2MiB per-CF buffers,
16MiB WAL target,64 open files and two background jobs, within a128MiB native
reservation. Bootstrap reserves8MiB payload/4MiB native plus24MiB retained inputs;
the shared configured application envelope also covers the existing Control
connection reservation. No second process allowance is borrowed. This ledger is
shared with session request/connection/hash reservations; startup refuses before
binding if one complete login cannot coexist with retained native/input/Control
work. The item ceiling is36 (three retained owners,16 connections,16 requests and
one hash); it is a simultaneous ownership ceiling, not a throughput guarantee.
These are reservations/targets, not measured RSS or capacity. No hot identity cache exists
in this initialization-only path.

Existing/partial native state or a populated source registry is never adopted,
deleted or reinitialized. The in-memory attempt is preserved through the current
worker/driver lifetime, but this slice does not promise crash-resumable setup.
Registry-only interruption can lack the chosen manifest/recovery metadata; retain
it for explicit recovery and do not generate another initial writer. A bounded
loader over existing registry, manifest and staged-input records must precede
restart acceptance. No existing SQL checksum or native format is changed.

The [executed first wave](../implementation/evidence/p01-initial-ingest-native-20260928.json)
qualifies six actual CLI/PostgreSQL/RocksDB cases, including renewal past the
original deadline, graceful release, sealed zero topology and retained-state
refusals. The [native interruption wave](../implementation/evidence/p01-initial-ingest-interruption-native-20260928.json)
holds actual open/close syscalls while observing the retained Control fence,
competing-claim refusal and bounded process death. Close(LOCK) occurs after the
native OS lock is released; this tests an unfinished destructor and driver
custody. The [SQL interruption wave](../implementation/evidence/p01-initial-ingest-commit-native-20260928.json)
executes twelve actual startup COMMIT/cancellation cases with complete SQL/native
and original-input observations. Before/after response loss preserves the exact
committed boundary; no retry regenerates initial native state. Held cancellation
also demonstrates that the deliberate proxy's server transaction can outlive the
terminated CLI, until that upstream connection ends. Client death alone does not
prove server rollback. Other close stages, worker panic, renewal-specific commit
cuts and crash-resumable initialization remain pending; this does not complete the
role or M1.

The [initial private-session first wave](../implementation/evidence/p01-initial-sessions-native-20261001.json)
qualifies actual installed login/logout and incorrect credentials, renewals under
12 concurrent self-inspection clients beyond the original scheduling expiry,
shared-memory refusal before binding, and SIGTERM during observed password work.
Native/input/receipt and64-table comparisons preserve the original owner and
bootstrap. Fixture failures and corrected independent audit expectations remain
recorded. Delayed renewals, disconnected clients and the session listener during
blocked native close remain the next required fault wave; no complete role or
capacity claim follows.

## Exact dependency source basis

The pinned Rust [open implementation](https://github.com/rust-rocksdb/rust-rocksdb/blob/e4aac69f55021e619566b6ca5902665e0ea2af4e/src/db.rs#L630)
creates directories even when native create-if-missing is false, motivating the
application preflight. Its DB destructor closes CF handles before the native DB.
The native [POSIX lock](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/env/fs_posix.cc#L858)
combines a process-local filename registry and `fcntl`; aliases must not bypass
the local reservation. Native [close](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/db_impl/db_impl.cc#L797)
waits for scheduled background jobs. These source guarantees do not substitute
for the process and failure scenarios above.

Native [directory creation](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/env/fs_posix.cc#L756)
does not synchronize its parent. The
[CURRENT installation](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/file/filename.cc#L429)
and [first synchronized WAL](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/db_impl/db_impl_write.cc#L2394)
sync their DB/WAL directories. The wrapper therefore synchronizes the parent of
its newly created target separately, before native initialization.
