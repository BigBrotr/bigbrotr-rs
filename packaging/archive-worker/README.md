# Private native archive worker

P02-T04's first native boundary is `bigbrotr-archive-worker`, a separate unpublished
Cargo application package. Only this executable links DuckDB; the ordinary CLI
and Control/Ingest library do not depend on DuckDB. This is compile/link and
process isolation inside the same product, not a permanent extra service or a
published plugin API.

The current executable implements the finite `inspect-engine` diagnostic. It
loads the pinned offline bundle and reports actual engine/source identity,
extension versions and effective settings. Publication, typed scientific rows,
parent supervision/grants, receipt recovery, export and storage protection remain
P02-T03/T04 work. Do not use this diagnostic as an archive commit or a readiness
certificate. No step or release target is qualified by an asset download.

## Build inputs and installation shape

[The native registry](../../contracts/release/archive-engine.json) fixes engine,
header and compressed/uncompressed extension sizes and SHA-256 for the native
build matrix. Asset availability for Linux arm64 and Mac x86_64 is not executed
qualification. Linux amd64 results do not qualify arm64.

`tools/build.py --output <new-directory>` prepares these assets at build time,
links against its explicit `DUCKDB_LIB_DIR`, and produces:

```text
<new-directory>/bigbrotr
<new-directory>/archive-worker/bigbrotr-archive-worker
<new-directory>/archive-worker/lib/{libduckdb.so OR libduckdb.dylib,duckdb.h}
<new-directory>/archive-worker/extensions/*.duckdb_extension
<new-directory>/build-result.json
```

The worker has an executable-relative library search path. The result records
worker hash, native asset identities and actual library build environment. The
existing complete source and compiler receipts remain required. An explicit
`--archive-bundle <directory>` copies and rechecks an existing matching bundle;
it does not accept a different system engine. The worker's build script also
refuses mismatched library/header inputs and implicit download/static overrides.

For development, prepare a fresh bundle with
`python3 tools/archive_bundle.py --output .research/archive-bundle --cache .research/archive-downloads --download`.
Omit `--download` to require the checksum-addressed offline cache. Set
`DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` to its absolute `lib` path;
`DYLD_LIBRARY_PATH` on Mac or `LD_LIBRARY_PATH` on Linux selects that same path
for development test executables. Set `BIGBROTR_ARCHIVE_TEST_BUNDLE` to the
absolute bundle root before `cargo test -p bigbrotr-archive-worker`. Missing
assets fail the required test instead of silently skipping it. The shared CI
entrypoint prepares these inputs in its separately owned `<output>-native-inputs`
directory and passes the same bundle to its clean build.

## Runtime boundary and remaining gates

The private invocation is:

```text
bigbrotr-archive-worker inspect-engine BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS
```

Paths must be absolute and the temporary root empty/canonical. The engine uses
that private root for home, temporary data and its secret directory. Autoload,
autoinstall, unsigned/community extensions, persistent secret creation and the
progress bar are disabled. All bundle hashes are checked before loading any
extension; DuckDB additionally verifies signed extension metadata. Failed native
operations produce redacted error categories. The worker accepts no arbitrary SQL.

The parent must verify the executable and library **before exec**, control the
loader environment and keep the bundle immutable. The worker's own library hash
check occurs after the dynamic loader ran and is not protection against a replaced
library. This diagnostic does not yet implement that parent. Native memory limits
do not bound total RSS or Arrow/extension allocations. Parent resource ownership,
blocked-output termination, native cancellation/kill and process/backend exclusion
must be implemented and tested before a publication worker is admitted.

The new binding's Cargo graph introduces Zlib-licensed `foldhash`/`zlib-rs` and
CDLA-Permissive-2.0 `webpki-roots` (the latter through its build-only downloader).
Their exact-version license allowances follow inspected source notices retained
in `notices/`. The selected build does not invoke that upstream downloader.
This narrow notice set is not the complete native engine/extension release SBOM;
P01/P08 still require all native and transitive distribution notices and release
authentication. No public distribution is authorized by this prerequisite.
