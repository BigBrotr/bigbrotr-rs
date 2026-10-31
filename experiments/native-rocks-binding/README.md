# Native Rust RocksDB binding qualification

This isolated research experiment exercises the selected Rust binding against its
bundled native engine before BigBrotr implements P02 admission. It is an
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
bounds. Fresh callback/lifetime/fault and cross-platform qualification is NOT_RUN.

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

The existing private CI workflow has an explicit `native_rocks_binding=true`
manual selection for the isolated native Linux arm64 case. It runs only this
research job, using a clean Git archive, the same pinned container recipe and
finite build/run limits; it does not promote an image or certify the product.
The job retains source, binary, lock, image and actual case identities in private
artifacts for 14 days. Download concise evidence before expiry. A queued or
successful build without the driver's nonempty case report is not acceptance.

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

All new guard tests are NOT_RUN. The root owner first reviews the local source
patch and both resolved path graphs, then uses the maintained clean-source native
builder and environment (pinned compiler/libclang, no ambient prebuilt RocksDB).
The wrapper's existing test targets contain `background_error_guard_` lifetime,
failed-open and healthy-write controls; its library tests check retry-count
installation/refusal and the purge setter. The engine's existing `c_test` adds
competing-listener refusal while preserving its prior callback counters.

For the wrapper's standalone upstream test workspace, use a fresh source export,
retain its recorded original lock separately, resolve a qualification lock with
the explicit sys path patch, then execute the same commands with `--locked`:

```sh
cargo generate-lockfile --offline --manifest-path vendor/rocksdb/Cargo.toml \
  --config 'patch.crates-io.librocksdb-sys.path="vendor/librocksdb-sys"'
cargo test --locked --offline --manifest-path vendor/rocksdb/Cargo.toml \
  --config 'patch.crates-io.librocksdb-sys.path="vendor/librocksdb-sys"' \
  --no-default-features --features zstd,bindgen-runtime --lib bigbrotr_guard_options_tests
cargo test --locked --offline --manifest-path vendor/rocksdb/Cargo.toml \
  --config 'patch.crates-io.librocksdb-sys.path="vendor/librocksdb-sys"' \
  --no-default-features --features zstd,bindgen-runtime --test test_db background_error_guard_
```

These planned commands can require explicit preparation of the locked upstream
test dependencies; no private cache is a product-build prerequisite. They do not
replace the actual native callback identity/severity/exception tests or real
NoSpace recovery-stop proof. Include both single-hook and retry-count-only
negative controls, healthy automatic maintenance, explicitly funded reopen and
positive purge/file-pin reconciliation. Current BigBrotr guard installation and
all four write-site/downstream ACK checks remain separate application work.
