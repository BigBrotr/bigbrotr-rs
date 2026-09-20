# Ingest native RocksDB option profile

Profile version **1** constructs native options for exactly `default`, `facts`
and `receipts`. It supplies checked targets and retained shared native resources
to the private ingest store. It does not open a directory, grant ownership, reserve
runtime resources, initialize a manifest or certify an admission. The store must
record the profile version separately from the persistent
[key format](ingest-key-format.md).

The selected dependency is `rocksdb = 0.25.0`, defaults disabled, features `zstd`
and `bindgen-runtime`; its pinned `librocksdb-sys 0.19.0+11.8.1` supplies RocksDB
11.8.1 at `abeebd9630f11bd08c28b7bd43c7bdfc62050654`. The wrapper enables static
linking transitively. See the [dependency record](../research/dependency-selections.md)
for the selected boundary and separate native-binding evidence.

## Checked inputs and ownership

`RocksNativeLimits::new(RocksNativeSettings)` validates explicit cache, write
buffer manager (WBM), per-CF write buffer, total WAL target, open-file target,
background-job target and native allowance. There is no implicit unlimited or
machine-sized default. The validated fields are private and immutable.

* All targets and the allowance are nonzero; byte values crossing `size_t` and
  C integer counts are checked without narrowing.
* Per-CF buffers are 64 KiB through `min(size_t::MAX, 64 GiB)`. The native engine
  clamps values outside this range; refusing them avoids silently increasing a
  requested small target. Open-file targets start at 20 for the same reason.
  An OS limit may further reduce the effective file target.
* Background jobs start at two: the native scheduler permits at least one flush
  and one compaction even when supplied a value of one. Counts cannot exceed
  `i32::MAX`; the runtime must also admit their real CPU/thread cost.
* WBM covers at least one per-CF buffer target. Cache capacity must exceed WBM
  by at least one 16 KiB block. This leaves a declared block-cache share; it is
  not a promise that pinned entries or cache sharding can never exceed capacity.
* WBM's `size_t * 7 / 8` threshold calculation must not overflow. All accounting
  sums are checked before constructing native resources.

These native range and arithmetic details come from the pinned engine's
[`SanitizeCfOptions`](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/column_family.cc#L230),
[`SanitizeOptions`](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/db_impl/db_impl_open.cc#L46),
[`GetBGJobLimits`](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/db_impl/db_impl_compaction_flush.cc#L3461)
and [WBM constructor](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/memtable/write_buffer_manager.cc#L23).

`RocksNativeProfile::build(limits)` returns DB options, three explicit CF
descriptors, and `RocksNativeResources`. The store retains those resources beside
its database and live runtime reservation. Cache/WBM handles remain private;
`usage()` exposes independently sampled cache, pinned-cache and memtable counters
without setters. WBM usage overlaps cache accounting, and pinned cache is part of
cache usage: summing these counters double-counts memory. The counters are not a
coherent process-memory snapshot.

## Selected options

Every CF, including `default`, uses the same LRU block cache, 16 KiB blocks,
cached index/filter blocks, two write buffers, the supplied per-CF buffer target,
Zstandard compression at ordinary and bottommost levels, and no retained flushed
memtable history. One stalling WBM charges memtable arenas to that same cache.

The WBM is installed on **DB options**. Assigning it only to CF options does not
work in this binding: native open converts each descriptor to `ColumnFamilyOptions`
and takes `DBOptions` solely from the top-level argument. The C setter writes
`Options::write_buffer_manager`, which belongs to DBOptions. This is visible in
the exact [open implementation](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/c.cc#L2041)
and [setter](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/c.cc#L6180).
The older isolated binding experiment put WBM only in CF options; its successful
cache, compression and crash cases did not establish WBM attachment.

DB options explicitly select `use_fsync=true`, atomic flush, paranoid checks,
`wal_recovery_mode=kAbsoluteConsistency`,
automatic WAL flushing, supplied WAL/open-file/background targets, one file-opening
thread and one subcompaction. mmap reads/writes remain disabled. Diagnostic LOGs
rotate at a 1 MiB target with four retained files, avoiding the default single
unbounded LOG. WAL and LOG sizes are rotation/flush targets, not strict disk quotas.

Ordinary reopening must refuse detected WAL damage. The engine's default
`kPointInTimeRecovery` can successfully reopen before a corrupted record, losing
even a previously synchronized application batch. `paranoid_checks=true` alone
does not prevent that fallback. The pinned [recovery modes](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/include/rocksdb/options.h#L411)
and [error handling](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/db_impl/db_impl_open.cc#L1715)
define this distinction. Absolute consistency also refuses an incomplete trailing
record, including one caused by an interrupted append. Preserve the suspect
directory for explicit recovery; never retry automatically with a permissive mode.

This corrects an omitted option in profile1 without rewriting manifests, keys,
receipts or reservation digests. It tightens open acceptance, not the persistent
format. Earlier executables remain permissive; rolling back to one reintroduces
the risk. A manifest version bump alone would not fence their native recovery,
which occurs before the application manifest check. No previous permissive
recovery is retrospectively qualified by this fix.

Even strict recovery cannot detect every loss: a WAL truncated exactly between
complete records can still parse successfully. Nor does successful open establish
causal backup closure or prove that a whole WAL was never removed. Consequently,
it is not a native noncommit witness. The initial producer resolver remains
positive-only; authoritatively resolving reserved-but-uncommitted work requires
additional provenance and exclusion evidence before checkpoint/retirement use.

The builder starts with creation and missing-CF creation disabled. The store may
enable them only after its explicit fresh-initialization checks; reopen separately
requires the exact CF set, existing healthy metadata and compatible manifest.
The builder does not change the default bytewise comparator or configure TTL.
Background scheduling uses RocksDB's shared default environment. `Env::new()` in
this wrapper also returns that shared environment; it does not create private
thread pools. Per-DB job targets are not total process thread limits.

## Accounting and remaining bounds

The checked allowance must cover this deliberately conservative reservation floor:

```text
accounted_target_bytes = cache + WBM + 3 CFs × 2 buffers × per-CF buffer
native_allowance_bytes >= accounted_target_bytes
```

The small profile used by the native-binding experiment is 32 MiB cache, 16 MiB
WBM, 2 MiB buffers, 16 MiB WAL, 64 open files and two background jobs. It accounts
for **60 MiB** by this formula; the focused product-profile tests supply a 128 MiB
allowance. The difference is uncommitted headroom, not a measured overhead bound.
Real deployments select targets within their admitted native reservation.

WBM normally charges memtables against cache capacity, so the formula intentionally
counts overlapping targets separately. It still cannot bound RSS: pinned blocks,
arena slack, allocator fragmentation, table-reader metadata, concurrent batches,
compression/compaction scratch, thread stacks and OS page cache have other costs.
The native [WBM cache reservation implementation](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/memtable/write_buffer_manager.cc#L77)
even absorbs cache-charge failures; strict cache admission would not turn it into
a global allocator limit. The owner needs mixed/aged-workload measurements,
additional headroom, admission backpressure and an OS process/container limit.

`max_total_wal_size` initiates flushes of CFs retaining old WALs; it is not a maximum
size of a write, a hard WAL-byte cap or an ENOSPC guarantee. Cache and WBM thresholds
can be exceeded by in-flight work. The store must bound batches, readers, iterators
and snapshots and retain their grants until native work actually ends. Closing a
Rust task or dropping its join handle does not stop a native operation.

Every durable store write still requires **`WriteOptions::set_sync(true)` and
`disable_wal(false)`**, with missing-CF writes rejected. Neither DB `use_fsync` nor
atomic flush supplies that per-call guarantee. Synchronization errors and lost
responses remain unknown until the owner resolves durable state.

## Focused validation

Unit tests in `apps/bigbrotr/src/storage/rocks/options.rs` check integer/coupling/accounting refusal,
closed CF names and absence of implicit creation. Actual RocksDB tests write
512 KiB to each CF and require WBM usage plus cache/pinned charges to increase,
then inspect native persisted OPTIONS for expected DB/CF values. A negative
control attaches the manager only to CF descriptors: actual memtable properties
increase but the configured manager stays unused. That assertion was first
observed failing when tested as if the old placement worked.

These are local option-attachment/configuration checks. They do not qualify the
complete product store, hard RSS limits, load/stall behavior, ENOSPC/fsync errors,
crash recovery, backup closure or fencing. Native target support and release
acceptance remain owned by the integrated store's exact-revision tests.

`wal_recovery_tests` adds actual synced bootstrap, child SIGKILL/reap, complete
frozen native-row comparison, one-byte WAL-checksum corruption and a truncated
record body. Each damaged case includes a permissive-mode negative control that
loses the entire batch; ordinary store and recovery opens must both refuse and
preserve the damaged WAL through retry. An intact crash reopens with every original
row. A separate record-boundary truncation control deliberately demonstrates the
remaining limitation above. These bounded cases do not test power loss, mid-FFI
kill, missing whole WALs, filesystem failure or historical lossy recovery adoption.

The [ec386ac qualification](../implementation/evidence/p02-wal-recovery-native-20260920.json)
records the Mac semantic failure before correction and actual Linux53 native plus22
integration tests, including the packaged worker, followed by Clippy. Exact source,
artifacts, failed collection attempts and terminal resource audit are retained.
The complete-record-loss control is an observed limitation, not recovery authority.
