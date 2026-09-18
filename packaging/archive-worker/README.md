# Private native archive worker

P02-T04's first native boundary is `bigbrotr-archive-worker`, a separate unpublished
Cargo application package. Only this executable links DuckDB; the ordinary CLI
and Control/Ingest library do not depend on DuckDB. This is compile/link and
process isolation inside the same product, not a permanent extra service or a
published plugin API.

The current executable implements the finite `inspect-engine` diagnostic. It
loads the pinned offline bundle and reports actual engine/source identity,
extension versions and effective settings. The new private `inspect-batch`
[handoff diagnostic](../../docs/reference/archive-handoff.md) additionally checks
complete pending membership and stages the five implemented typed fact families.
The application now has a bounded parent for that private inspection, including
pre-exec verification, sanitized loader environment and retained memory/control
grants. Finite scientific initialization, publication and fresh receipt observation
are now private consumers under the same handoff contract. Installed lifecycle
recovery, whole-role resource/crash supervision, export and storage protection
remain P02-T03/T04 work. Do not use a diagnostic as an archive commit or a readiness
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

The process creating any private command must set
`BIGBROTR_ARCHIVE_PARENT_PID` to its own positive PID. The product parent does
this in its cleared child environment. Missing or different parentage fails
before engine/job-root effects. Linux binds SIGKILL to the lifetime of the
spawning thread and rechecks parentage after registration. A caller must retain
that thread through worker reap. Mac development validates initial parentage
without Linux's parent-death signal; it does not qualify production crash recovery.
This private invocation change requires a matching parent and worker build, not
a persistent-format migration. Historical experiment scripts remain bound to
their recorded revisions and are not launchers for the new worker.
The [Linux amd64 parent-binding report](../../docs/implementation/evidence/p02-parent-binding-native-20260918.json)
records positive exit/reap of the stopped packaged worker after parent SIGKILL,
with the old-worker survival control retained. This does not certify persisted
job recovery or exclusion of a remote catalog transaction.

The parent must verify the executable and library **before exec**, control the
loader environment and keep the bundle immutable. The worker's own library hash
check occurs after the dynamic loader ran and is not protection against a replaced
library. The private inspection parent implements these checks under an immutable
installation precondition, with the expected executable identity supplied by the
trusted builder/release authority. Native memory limits do not bound total RSS or
Arrow/extension allocations. The [handoff contract](../../docs/reference/archive-handoff.md#supervised-private-inspection)
owns bounded pipe supervision and its remaining parent-crash, disk/CPU/PID and
backend exclusion gates before a publication worker is admitted.

The [Linux amd64 prerequisite qualification](../../docs/implementation/evidence/p02-archive-engine-native-20260918.json)
exercises the real packaged engine, offline signed load, refusal cases and a
blocked output pipe. Its external harness kills the blocked worker; this does
not implement a production supervisor. DuckDB's query `threads` setting also
does not bound signature-verification threads: the three pinned amd64 extensions
created 35 + 40 + 21 temporary hash threads across sequential loads. With a
container PID cap of 8 the worker aborted, emitted no result and exited. Parent
admission must budget this startup allowance and keep the reservation until
termination; an abort never becomes a publication receipt. Native peak RSS/stack
usage, production recovery and Linux arm64 execution remain unqualified.

The new binding's Cargo graph introduces Zlib-licensed `foldhash`/`zlib-rs` and
CDLA-Permissive-2.0 `webpki-roots` (the latter through its build-only downloader).
Their exact-version license allowances follow inspected source notices retained
in `notices/`. The selected build does not invoke that upstream downloader.
This narrow notice set is not the complete native engine/extension release SBOM;
P01/P08 still require all native and transitive distribution notices and release
authentication. No public distribution is authorized by this prerequisite.

## Private native catalog bootstrap prerequisite

`bootstrap-catalog BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS`
uses the same expected-parent binding and verified native bundle. Bounded stdin
JSON contains only `dsn` and canonical local `data_root`; no secret goes on argv
or either output pipe. The parent must validate the trusted route/TLS policy and
supply a dedicated restricted bootstrap login, never the catalog manager. The
worker creates only native DuckLake metadata through ordinary attach, using an
explicit temporary memory secret, inlining0 and automatic migration disabled.
It detaches/closes the native engine before returning a fixed diagnostic.

Positive exit/reap is required before the separate privileged PG guard installer
transfers ownership/revokes bootstrap privileges and checks the pristine catalog.
Bootstrap success alone cannot adopt an existing catalog or grant publication.
The installed initialization command, credential provisioning and persistent
recovery remain P02/P08 work; this finite private consumer is not a new public
operator command or support claim. The
[native guard prerequisite report](../../docs/implementation/evidence/p02-catalog-product-native-20260918.json)
qualifies this finite packaged consumer on Linux amd64 at `944f3e6`; installed
initialization and persistent recovery remain unfinished.

The scientific consumers `initialize-catalog`, `publish-batch` and
`inspect-receipt` use the same supervised finite process. Their exact input,
limits and authority are documented in the
[private handoff contract](../../docs/reference/archive-handoff.md#private-scientific-publication-candidate).
The new [receipt reader](../../docs/reference/archive-handoff.md#fresh-receipt-observation-candidate)
uses a separate SELECT-only credential and fresh readonly catalog/data access.
[Native receipt qualification](../../docs/implementation/evidence/p02-archive-receipts-native-20260919.json)
exercises six signed recovery schedules and eighteen isolated reader cases;
neither Present nor Absent is a protection or replay grant. These remain private
application interfaces, not an installed Archive role or new public command.
