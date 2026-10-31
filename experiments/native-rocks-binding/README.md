# Native Rust RocksDB binding qualification

This historical isolated research experiment exercises the selected Rust binding
against its bundled native engine at the recorded source revisions. It is an
unpublished standalone Cargo package and does not share the product workspace or
claim storage-service authority.

The original published tuple is retained as complete local packages. The current
standalone lock selects path patches, so registry checksums below identify the
original archives, not patched bytes. See the
[vendored-source contract](../../vendor/README.md#rocksdb-owned-background-error-guard)
and per-package provenance for current source identity. The exact versions remain:

* `rocksdb 0.25.0`, crate checksum
  `d8d90add70d1d420ee487bce4a1449880a8d147451c6051b2ee5f8354553dcbf`;
* `librocksdb-sys 0.19.0+11.8.1`, crate checksum
  `4f45e86edad8e88efe97dbf384b4e48e1ff0f111eabf154c7b09d7a1e5fb573c`;
* bundled RocksDB 11.8.1 at upstream commit
  `abeebd9630f11bd08c28b7bd43c7bdfc62050654`;
* default features disabled, with only `zstd` and `bindgen-runtime` selected by
  the wrapper (`static` is enabled transitively by `rocksdb`).

The current source resolves the added guard API, but this historical experiment
adapter does not install it or inject background NoSpace. Its unchanged cases do
not qualify the new fail-stop behavior, current product profile or numerical
bounds. The separate guard controls below establish scoped healthy API/lifetime
and direct callback behavior. Running-DB NoSpace, concurrency and new platform
qualification remain NOT_RUN.

The source audit reused by this experiment is inventoried at
`.research/checkouts/rocks-dependency-audit-20260917/inventory.json` in the main
checkout. It records the published source archives and the exact wrapper/native
source hashes. In particular, `rocksdb` 0.25.0's `db.rs`, `db_options.rs`,
`write_batch.rs`, `column_family.rs`, and `snapshot.rs` and the bundled RocksDB
headers/lock implementation were inspected before this run.

## What the experiment exercises

`src/main.rs` contains a private `OwnerDb` adapter. Callers cannot supply or mix
column-family handles. It separates initialization of a fresh path from reopen,
which requires an existing `CURRENT` and the exact CF set. It explicitly opens
`default`, `facts`, and `receipts` with the same shared 32 MiB block cache,
an intended shared 16 MiB stalling write-buffer manager (the wiring defect below
means this claim was not established), 2 MiB per-CF write buffers, two write buffers
per CF, 16 MiB total WAL trigger, 64 open files, two background jobs, and Zstandard compression. The DB uses
`use_fsync=true` and atomic flush. Every admission-shaped batch uses
`WriteOptions.sync=true` and `disable_wal=false` and writes one literal value to
all three CFs.

`driver.py` is the independent process oracle. It creates a fresh directory for
each case and checks:

1. kill before calling `DB::write_opt`, then reopen and require all three values
   absent;
2. let the synchronized call return OK, classify the lost reply as `UNKNOWN`,
   kill the process, reopen, and require all three literal values present;
3. kill five writers while they continuously issue distinct synchronized batches,
   then reopen each database and reject every union key whose three-CF presence
   mask is not `111`;
4. read an old snapshot before and after a later write, then drop it, reopen, and
   require both committed batches in the ordinary view;
5. hold one writer open and require another process to fail on both the real path
   and a symlink alias; the private adapter also canonicalizes paths and refuses a
   same-process alias before exposing another owner;
6. refuse existing databases with either a missing required CF or an unknown CF,
   and refuse a store whose `CURRENT` was removed without recreating it;
7. force an atomic two-CF flush and inspect every live SST for RocksDB 11.8.1's
   exact compression metadata marker `BuiltinV2;07;`. In the selected engine,
   compression type `0x07` is ZSTD. The oracle also requires the native RocksDB
   LOG to report version 11.8.1, the configured DB limits, and one shared block
   cache identity plus ZSTD settings across all three explicit descriptors.

The loop cannot prove that `SIGKILL` landed inside one particular native
instruction. It deliberately withholds per-call completion from the controller,
so each affected token remains unknown until reopen resolves the union of all
three CFs. A passing run establishes only the observed all-or-none results for
these process-death schedules on the tested filesystem and host.

## macOS run

Run from a clean exact Git tree with isolated Cargo state and a fresh output
directory. Both checked-in vendor packages must be present at the repository
paths; the standalone workspace patches `../../vendor/rocksdb` and
`../../vendor/librocksdb-sys`, independently of the root workspace. Its own lock
must remain unchanged during the build:

```sh
mkdir -p .research/cargo-home .research/target .research/rawdata
CARGO_HOME="$PWD/.research/cargo-home" \
CARGO_TARGET_DIR="$PWD/.research/target" \
LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib \
  cargo build --locked --manifest-path experiments/native-rocks-binding/Cargo.toml

LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib \
python3 experiments/native-rocks-binding/driver.py \
  --binary .research/target/debug/native-rocks-binding \
  --cargo-lock experiments/native-rocks-binding/Cargo.lock \
  --lab-root .research/rawdata/native-rocks-mac/lab \
  --output .research/rawdata/native-rocks-mac/result.json
```

The observed 2026-09-17 final Mac run used Rust 1.98.1, Apple clang/c++ 21.0.0,
`LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib`, and
`aarch64-apple-darwin`. Its five ambiguous trials resolved 1, 54, 252, 769, and
2,228 contiguous complete batches with the expected literal values and no partial
batch. All other cases above passed. The ignored raw result is
`.research/rawdata/mac-final-v2/result.json`, SHA-256
`051d27b2944ae53f68948c7de8a93a12426cae7352b9e703d2647c2f383f837d`.
The executed debug binary SHA-256 is
`def9cb89a3fa5884b939bb3688f9fc14714a2d926a9b5d77997972666b28f8d0`;
the committed `Cargo.lock` SHA-256 is
`7d05f7fbb69433767f4cbba32552aebb8cbb847c2cc5a3a18d81c5f304dc1f31`.

## Fresh Linux container requirement

The Dockerfile uses the foundation's pinned Rust 1.98.1 Bookworm manifest and
adds `libclang-dev` explicitly; the inherited build toolchain already supplies
GCC/G++ and make. Export this experiment and both complete vendor packages plus
provenance from one clean committed revision, preserving their relative paths.
Use that bounded export as context; the Dockerfile copies only those inputs.
Run as the actual Tux uid/gid and mount only a fresh owned output directory:

```sh
test -z "$(git status --porcelain --untracked-files=all)"
mkdir -p .research/native-rocks-context
git archive HEAD experiments/native-rocks-binding vendor/rocksdb vendor/librocksdb-sys \
  vendor/rocksdb.provenance.json vendor/librocksdb-sys.provenance.json \
  | tar -x -C .research/native-rocks-context
docker build --pull=false -t bigbrotr-native-rocks-binding:local \
  -f .research/native-rocks-context/experiments/native-rocks-binding/Dockerfile \
  .research/native-rocks-context
mkdir -p .research/native-rocks-linux
docker run --rm --user "$(id -u):$(id -g)" \
  --cpus 2 --memory 2g --memory-swap 2g --pids-limit 128 --network none \
  --mount type=bind,src="$PWD/.research/native-rocks-linux",dst=/out \
  bigbrotr-native-rocks-binding:local \
  --lab-root /out/lab --output /out/result.json
```

The image also fixes `CARGO_BUILD_JOBS=2`. The run publishes no ports and the
mounted fresh output directory is its only host write boundary.

The integrating owner schedules and inventories the Linux run. This branch does
not mutate Tux or claim amd64/arm64 results before that exact revision executes.

The public CI workflow has an explicit `native_rocks_binding=true`
manual selection for the isolated native Linux arm64 case. It runs only this
research job, using a clean Git archive, the same pinned container recipe and
finite build/run limits; it does not promote an image or certify the product.
The job retains source, binary, lock, image and actual case identities in publicly
accessible Actions artifacts for 14 days. Download concise evidence before
expiry. A queued or successful build without the driver's nonempty case report is not acceptance.

## Explicit limits

System power loss, firmware cache loss, dedicated-filesystem ENOSPC/fsync failure
injection, multi-host fencing, and same-process bind-mount or device/inode alias
enforcement are `NOT_RUN`. The canonical registry case covers a symlink alias.
This experiment does not create P02 key formats, admission outcomes, source
allocation, receipts,
`CertifyExisting`, resource grants, cancellation, backup closure, or runtime
ownership authority. A write error or lost response is never labeled rollback or
success before a fresh open checks its exact keys. Cache and WBM settings are
soft native controls, not a process RSS guarantee.

## Subsequent WBM wiring finding

The original source assigns `set_write_buffer_manager` only to column-family
Options. In the selected native C API, `rocksdb_open_column_families` converts
those into `ColumnFamilyOptions` and constructs `DBOptions` only from the separate
database options. WBM belongs to `DBOptions`, so the configured shared manager
was not attached. The old driver's non-null LOG pointer check does not establish
the selected budget. Historical reports remain reproducible, but their shared
WBM claim is withdrawn in the
[recorded evidence](../../docs/implementation/evidence/p02-native-rocks-binding-20260917.json).
Other observed persistence, snapshot, locking, CF and ZSTD results retain their
narrow scope. Do not use this old WBM wiring or its pointer-only oracle in the
product adapter: attach the manager to DB options and observe its actual memory
accounting after writes, with a disconnected-manager negative control.


## Planned guard qualification

Source `f8c534f7e5abbeba16a7b26e64a6d623a3261665` passed all five public-API
binding smoke controls on native Linux AMD64, with zero ignored cases and one
retained library ELF. The whole existing C API test also passed, with 96 recorded
phases, the guard-refusal phase exactly once, enabled assertions and final `PASS`.
These actual results used the pinned Rust 1.98.1 and native Clang/LLVM 14 image
under two CPUs, 6 GiB memory, no network and a read-only root. The
[maintained binding report](../../docs/implementation/evidence/p02-native-rocks-binding-20260917.json)
owns exact identities and the distinction from historical unpatched results.

The first run's overall exit 1 is preserved: its five Rust controls passed before
unfiltered offline Cargo metadata failed with exit 101 on an unused Android-only dependency.
A separate 1.533-second context run selected the actual Linux platform; metadata
and same-profile no-run JSON passed, all 271 compiler-artifact entries reported
`fresh=true`, and no tests or engine compilation were repeated. The later whole C qualification took
6.592 seconds and used only the captured archives, without a Cargo cache mount.
The five canonical controls are in
[`background_guard_tests.rs`](../../apps/bigbrotr/src/storage/rocks/options/background_guard_tests.rs),
a `cfg(test)` child of the existing application options module. They use the
root workspace's existing exact `rocksdb` and `tempfile` dependencies and lock.
There is no second test workspace, new dependency or qualification lock. The
vendored upstream wrapper tests remain original. The two thin read-only native
getters retain the resume-count and purge-flag observations; they do not infer
listener attachment or purge completion from a Rust mirror.

From the exact clean source, use the maintained isolated native environment,
compiler pins, source manifest, Cargo configuration checks and owned output
lifecycle. Restrict offline metadata to the actual native target before compiling;
the command below is the recorded Linux selection, not an all-platform cache
claim. Compile only the application library in the release profile, then list
and execute the exact module:

```sh
cargo metadata --locked --offline --format-version=1 \
  --filter-platform x86_64-unknown-linux-gnu
cargo test --locked --offline --release -p bigbrotr --lib \
  --no-run --message-format=json
cargo test --locked --offline --release -p bigbrotr --lib \
  storage::rocks::options::background_guard_tests:: -- --list --format pretty
cargo test --locked --offline --release -p bigbrotr --lib \
  storage::rocks::options::background_guard_tests:: -- --test-threads=1
```

Retain the compile JSON and one library test ELF. Require exactly the five
source-declared names to be listed and executed, zero ignored, and unchanged
source/tool/ELF hashes before and after. Reuse the existing nonempty
`listed_tests`/`completed_tests` controls in
`experiments/network-transport/initial_index.py` and `tools.ci.execute`; a filtered
zero-test success fails qualification. These commands compile no product binary,
worker or PostgreSQL journey. The existing workspace `rust-tests` CI command also
runs this module on AMD64, ARM64 and macOS in its declared workspace profile;
that does not replace this first exact-source release-profile result. The smoke
module installs the guard only in its synthetic databases. The application
factory, resource/grant and four-write-site/downstream ACK wiring remain absent.

### Existing native C test with the same archives

The sixth addition is the competing-listener refusal in the existing
`vendor/librocksdb-sys/rocksdb/db/c_test.c` phase
`table_filter_listener_metadata`. It is not a sixth Rust test. Keep the whole C
test and all its existing assertions. Its `main` ignores arguments; do not invent
a phase filter or extract a mirrored assertion. It now refuses to compile when
`NDEBUG` is defined, so disabled C assertions cannot silently produce PASS.

For the Linux AMD64 C qualification, obtain the exact sys and Zstandard package
IDs from the same locked, actual-platform Cargo metadata above. Require the sys
manifest to be this source's `vendor/librocksdb-sys/Cargo.toml`, version0.19.0+11.8.1, and the
selected `zstd-sys` version2.1.2+zstd.1.5.7. Match those IDs to unique
`build-script-executed` messages in the retained compile JSON. Their `out_dir`
values must lie under the admitted `CARGO_TARGET_DIR/release/build`, from this
same target/profile/configuration. Refuse absent or ambiguous matches. Select
only the regular `librocksdb.a` and `libzstd.a` files directly in those exact
`out_dir` directories, record sizes/SHA-256 and retain their source/tool/config
binding. Never guess an archive by a wildcard, neighboring cache path or system library.

The root-owned existing native controller supplies `SOURCE`, `QUALIFICATION`,
`SYS_OUT`, `ZSTD_OUT`, pinned `CC` and `CXX` as explicit verified paths. It creates
one fresh private `QUALIFICATION/tmp`, then runs these commands through the
existing finite command/log lifecycle:

```sh
"$CC" -std=c11 -UNDEBUG -D_POSIX_C_SOURCE=200809L \
  -I "$SOURCE/vendor/librocksdb-sys/rocksdb/include" \
  -I "$SOURCE/vendor/librocksdb-sys/rocksdb" \
  -c "$SOURCE/vendor/librocksdb-sys/rocksdb/db/c_test.c" \
  -o "$QUALIFICATION/c_test.o"
"$CXX" -std=c++20 "$QUALIFICATION/c_test.o" \
  "$SYS_OUT/librocksdb.a" "$ZSTD_OUT/libzstd.a" \
  -pthread -ldl -lrt -lm -o "$QUALIFICATION/c_test"
TEST_TMPDIR="$QUALIFICATION/tmp" "$QUALIFICATION/c_test"
```

The engine-root include resolves the existing generated C roundtrip-test
fragment. Direct pinned compiler invocation inherits no ambient CFLAGS, CXXFLAGS,
LDFLAGS or prebuilt RocksDB override. The C test is a separate regular retained
ELF with its own hash. Require exit0, the actual named phase and final `PASS`,
and unchanged source/compiler/archive/test-ELF hashes afterward. Preserve any
compile/link/runtime failure. Do not invoke `make c_test`: it may build another
engine/test-harness configuration. Linking this existing C test uses the same
Cargo-built native archives; only its C front end and executable are additional
compilation. This exact Linux compile/link/run route passed at the recorded f8
source, preserving all eight admitted input hashes and all 4,162 source files.
The container stopped with exit 0, PID 0 and no OOM. This is a scoped healthy C ABI
and competing-listener result; it is not Mac/ARM qualification. The ordinary Rust
workspace CI does not run this separate C test automatically.

The earlier five Rust and C results do not establish the direct callback result
recorded below. Actual concurrent callback/query/close and real NoSpace injection
remain separate. Include both single-hook and retry-count-only negative
controls, healthy automatic maintenance, explicitly
funded reopen and positive purge/file-pin reconciliation. Current app wiring,
CW-N1/P1/P2, physical output/reserve bounds, absolute64 enablement, full
workspace/Clippy, affected AMD64/ARM64/macOS qualification, complete
dependency/license and release gates remain OPEN.


### Direct callback component controls

Source `d3dc8d5e781fee3298a0cbbcea5f4a2e2061c6dd` passed all five exact Rust
controls, the whole C API program with 96 phases and all 14 direct callback cases
on native Linux AMD64. The run took 363.615 seconds including native compilation,
using the same pinned Rust 1.98.1/Clang 14 image, two CPUs, 6 GiB memory, no
network and a read-only root. The
[maintained binding report](../../docs/implementation/evidence/p02-native-rocks-binding-20260917.json)
owns the actual commands, source/context/archive/three-ELF identities and root
terminal receipt. All 4,164 source files and retained inputs remained unchanged;
the container stopped with exit 0, PID 0 and no OOM. The captured RocksDB archive
matched f8 byte-for-byte; the d3 archive context, compiled C++ frontend and
executed callback proof have their own d3 bindings. The original f8 setup failure
and later f8 scoped successes above remain unchanged.

The source increment moves only the existing native C Options definition
into one owning `db/c_options_internal.h`, included by `c.cc` and its generator
template. The public C header stays opaque and the listener stays in `c.cc`.
The new [`background_guard_test.cc`](background_guard_test.cc) uses that same
internal type, the real C installer and the installed listener's public virtual
interface. It does not include `c.cc`, copy a listener implementation, fabricate
an opaque layout or add a production testing API. Application Rust is unchanged.
The earlier f8 results remain tied to f8. The d3 result uses its own frozen
source, archive context and frontend identities.

The explicit `guard-callback-controls` profile in the existing driver has 14
fixed named cases: five NoSpace severities, OK, a different error code, genuine
IOStatus flags across both boolean values and all three scopes, recovery veto,
sticky/Options-copy/close lifetime, guard-first destruction, successful exact
promotion allocation, no-copy paths and isolated allocation-failure termination.
Checks compare code, subcode, message/nullness and severity individually; they do
not use Status equality, which checks code only. IOStatus uses a normal public
base reference and its genuine flag accessors. No database is opened, and these
are direct component calls, not running-DB delivery or concurrent-close evidence.

Only the control executable replaces ordinary array allocation. An inactive
thread-local gate counts the actual pointer-taking callback's message copy.
The positive case must intercept exactly one allocation of the independent
literal message size, including NUL; null-message and already fatal/unrecoverable
paths must intercept none. The replacement preserves ordinary zero-size,
alignment, new-handler and matching ordinary/sized/nothrow deallocation semantics;
over-aligned overload pairs remain the standard library's responsibility. The
bookkeeping and termination handler allocate no memory themselves.

The termination case prepares options, status, query handle, reporting and a
terminate handler before arming one size-checked failure. It disarms before
throwing `std::bad_alloc`. The handler accepts exactly one expected injection and
a latched public guard; wrong size, later unarmed array allocation or wrong latch
uses a different exit. The driver requires the prearmed marker, the exact
injected-and-latched marker and reserved exit86. A returned callback, escaping
exception, arbitrary signal/death or zero cases fails. The positive copy
interception and isolated prearmed/injected-and-latched exit86 were observed
at d3.
The injected allocation is only the `Status::CopyState` array copy during the
pointer-taking background-error severity promotion. It does not qualify global
OOM, allocations before entry to a by-value callback, other parameter-copy
failures or a real NoSpace condition.

For each exact source, first capture its selected archives with the same
locked actual-platform metadata/package-ID/out-dir controls above. The frontend
must use the same pinned compiler, standard library, target and native ABI. For
the first Linux AMD64 profile, keep the selected C++20, default namespace,
`NDEBUG=1`, aligned-new and POSIX definitions; do not enable
`ROCKSDB_ASSERT_STATUS_CHECKED`, a lite/debug engine, SyncPoints or a different
native archive. Unlike the whole C test, this frontend requires NDEBUG and uses
always-on checks. The positive interception control must work on the actual
compiler/archive; otherwise qualification fails without changing engine settings.

```sh
"$CXX" -std=c++20 -DNDEBUG=1 -DHAVE_ALIGNED_NEW \
  -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -include cstdint \
  -I "$SOURCE/vendor/librocksdb-sys/rocksdb/include" \
  -I "$SOURCE/vendor/librocksdb-sys/rocksdb" \
  "$SOURCE/experiments/native-rocks-binding/background_guard_test.cc" \
  "$SYS_OUT/librocksdb.a" "$ZSTD_OUT/libzstd.a" \
  -pthread -ldl -lrt -lm -o "$QUALIFICATION/background_guard_test"
python3 -B "$SOURCE/experiments/native-rocks-binding/driver.py" \
  --profile guard-callback-controls --binary "$QUALIFICATION/background_guard_test" \
  --source-root "$SOURCE" --source-manifest "$SOURCE_MANIFEST" \
  --native-context "$NATIVE_CONTEXT" --cargo-lock "$SOURCE/Cargo.lock" \
  --rocksdb-archive "$SYS_OUT/librocksdb.a" --zstd-archive "$ZSTD_OUT/libzstd.a" \
  --lab-root "$QUALIFICATION/callback-cases" --output "$QUALIFICATION/callback-result.json"
```

The native context is the existing `bigbrotr-native-c-api-archive-context/1`
receipt for this exact new source/profile, not an f8 receipt relabeled afterward.
The driver verifies the new header/frontend/driver and relevant native source
hashes against the source manifest, archive bytes/features against that context,
exact nonempty inventory, expected exits/output and unchanged inputs after every
case. It retains partial failures. Root still owns trusted compilation/link
custody and compiler/config matching; unsigned hashes do not authenticate an
untrusted producer. No new workspace, lock, dependency or generic fault service
is introduced. The historical default driver profile remains unchanged, and its
existing ARM job does not automatically execute these new component controls.

The 14 direct controls qualify the recorded d3 component tuple only.
Real default-POSIX ENOSPC, SFM/Resume suppression with retry-count-only and
single-hook negatives, concurrent fault/query/drop/close, already-started work,
joined stop, failed-unlink/unknown debt, explicitly funded reopen and positive
purge remain separate. App wiring, CW-N1/P1/P2, request65, physical bounds and
release remain open. This component increment is not an M1 prerequisite and must
not delay unrelated installation reconciliation.
