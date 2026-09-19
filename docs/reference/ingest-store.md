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
practical gates. This API is not yet exposed as an installation or recovery CLI.

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
