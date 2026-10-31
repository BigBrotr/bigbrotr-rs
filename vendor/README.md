# Temporary dependency patches

## ClickHouse metadata bounds

`clickhouse/` and `clickhouse-types/` retain the complete published 0.15.2 and
0.1.4 packages under their MIT OR Apache-2.0 licenses. BigBrotr selects the MIT
option for distribution; both upstream licenses remain alongside each source.
The types package omits license files, so its copies come from its exact upstream
commit. The [client provenance](clickhouse.provenance.json) and
[types provenance](clickhouse-types.provenance.json) record original archive/member
hashes, changed files and added license sources. Upstream manifests, package locks,
features, tests and examples remain available; the root lock controls builds.

The upstream validated-row header parser allocates from an untrusted declared
column count before consuming names/types. Its recursive type parser also uses
unchecked byte slices on malformed strings. A transport byte cap cannot prevent
those allocations or panics. The local client checks the count against the
actual Rust row kind/count before allocation, then accepts at most 64 columns,
128-byte nonempty UTF-8 names and 256-byte nonempty type declarations with at
most 8 unquoted parenthesis levels. Checked wrapper framing preserves ordinary
type validation. INSERT's DESCRIBE metadata uses the same bounded type parser,
checks names and refuses a 65th row. DESCRIBE row strings are copied only after
the bytes exist; the selected transport also bounds total response bytes.

These are BigBrotr's closed initial Index metadata limits. All ten selected
schemas have explicit controls against the exported bounds, including the 35-column
largest row, signature FixedString(64), nullable scalars and nested arrays/tuples.
Compression remains disabled and validation remains enabled. The unselected Native reader is retained without the added metadata limits
and is not qualified by these checks. Legacy standalone parser APIs are retained; BigBrotr's selected client
paths use the bounded entry points.

As of 2026-10-26 the latest client 0.15.3 still calls the same unbounded header
parser and the latest types package remains 0.1.4. Remove both patches when a
supported upstream version provides equivalent preallocation, framing and
metadata bounds, after running these retained regressions and actual engine
qualification. Component tests do not establish TLS, server cancellation,
durability, full application or release fitness.

## DuckDB ZIP build dependency

`libduckdb-sys/` retains the complete published MIT-licensed 1.10505.0 package
from [crates.io](https://crates.io/crates/libduckdb-sys/1.10505.0), with one change:
its normalized `Cargo.toml` selects ZIP `=9.0.5` instead of `6` for the build
dependency. The original manifest, license, build scripts, generated bindings,
bundled sources and upstream package lock remain unchanged. The workspace
`Cargo.lock` controls BigBrotr builds; the retained upstream lock is provenance.

The original `.crate` archive has SHA-256
`6cb514dab5e271e849235c1cb98bd65a2ae107fbd619a6740219319c54a71d95` and identifies
upstream commit `88f673c5cdf09e287c010a910db53b97e523a9fa`. The
[provenance manifest](libduckdb-sys.provenance.json) records all original member
hashes and the one patched member. No file from an unverified cache is required
to build a clean checkout.

ZIP 6 is affected by RustSec
[0367](https://rustsec.org/advisories/RUSTSEC-2026-0367),
[0368](https://rustsec.org/advisories/RUSTSEC-2026-0368) and
[0369](https://rustsec.org/advisories/RUSTSEC-2026-0369).
The selected 9.0 maintenance patch also includes extraction fixes shipped after
9.0.0. As of 2026-10-26, no published 1.10505.x binding patch fixes this dependency;
the next binding line encodes DuckDB 1.5.6. This local manifest patch preserves
the selected DuckDB 1.5.5 engine, C API, Arrow 58 family and exact native assets.

BigBrotr selects the linked-library build with explicit `DUCKDB_LIB_DIR` and
`DUCKDB_INCLUDE_DIR`, using its own checksum-pinned asset preparation. The
upstream downloader is not selected, but its build dependencies must still pass
the ordinary dependency policy. The full 7.1 MB package includes a 6.1 MB bundled
source archive: retaining it avoids silently breaking upstream features or
maintaining a larger source fork just to remove an inactive archive. The package
is excluded from workspace membership; no additional product feature is enabled.

Replace this patch with a supported upstream release when it supplies the fixed
dependency for the same native tuple, or as part of a separately qualified
native-engine update. Then remove the local patch and vendor sources together,
rerun dependency policy and affected build/runtime checks, and update the release
inventory and notices. Advisory suppression is not a replacement condition.

## RocksDB owned background-error guard

`rocksdb/` and `librocksdb-sys/` retain the complete published 0.25.0 and
0.19.0+11.8.1 packages, including their original manifests, package locks,
engine sources and notices. The [wrapper provenance](rocksdb.provenance.json)
and [sys provenance](librocksdb-sys.provenance.json) bind every original member,
the local patch and added notice. The root lock controls product builds; the
upstream package locks remain unchanged provenance. The standalone
[native experiment](../experiments/native-rocks-binding/README.md) explicitly
patches both packages and exports the same checked-in sources.

The sys patch adds three C functions with the `rocksdb_bigbrotr_` prefix in the
existing owning C binding. Installation rejects any existing listener, including
a second installation or a clone of guarded Options. It allocates the shared
atomic state, dedicated native EventListener, query handle and replacement
one-element vector before committing with a no-throw swap. Success also fixes
`max_bgerror_resume_count=0`; failure returns no guard and leaves Options
unchanged. No Rust callback, foreign-listener cast or private-layout copy is used.
The same additive changes are retained in the upstream C API source templates
so regeneration keeps the extension. The existing cc/bindgen build is unchanged.

On a non-OK background error, the listener latches failure and raises severity
to at least fatal using the public Status severity-copy constructor. Original
code, subcode, message, retryable, data-loss and scope survive; unrecoverable
severity is never lowered. The copy can allocate: its failure terminates rather
than unwinding or continuing with a soft error. The recovery-begin callback
latches failure and vetoes automatic recovery. Both hooks are needed on the
selected default local POSIX path, in addition to disabling the separate Resume
retry count. Healthy automatic flush/compaction remains enabled. Custom
file-scoped retryable filesystem plugins are outside this contract.

The safe wrapper returns `BackgroundErrorGuard` from
`Options::install_background_error_guard`. Its borrowed `failed()` query exposes
only sticky refusal, with no diagnostic tuple or borrowed Status. A concurrent
true query is conservative refusal; the synchronous callback returns the complete
promoted native status or terminates. The handle and listener independently own
native shared state. Dropping either Options or the handle first, copying
Options, failing open or querying after DB close cannot borrow freed Rust state.
Options clones share the same failure group, and there is no reset API. Final
Options must not be parsed, replaced or modified after installation. Persisted
OPTIONS does not recreate the live listener. Every newly funded recovery attempt
needs fresh final Options/guard and external preservation of prior unknown debt.

`WaitForCompactOptions::set_wait_for_purge` exposes an existing C setter.
Read-only native-backed getters expose its flag and the configured background
Resume retry count; these option observations alone prove neither attachment nor
purge. The setter does not prove failed unlink, snapshot or descriptor credit
release. Callers must separately bound waiting and positively reconcile actual allocated files/pins.

This patch is source-only and **NOT_RUN**. It installs no guard in BigBrotr's
application factory and changes no resource grant, ACK, persistent format,
absolute64 guard or numerical capacity bound. Earlier unpatched engine/CI results
do not qualify it. The five canonical public-API controls now live in the
application's `cfg(test)` options child module, using existing root dependencies
and lock; no duplicate tests or private qualification lock remain. The original
upstream wrapper tests are retained. These ownership/refusal/healthy-I/O tests
and the existing C competing-listener control require actual execution; see the
[maintained commands](../experiments/native-rocks-binding/README.md#planned-guard-qualification).
The C test refuses a build with assertions disabled. Native callback
identity/severity/sticky/recovery and allocation-failure controls, real NoSpace
injection with both single-hook negative controls, joined stop/funded reopen,
positive purge reconciliation and selected AMD64/ARM64/macOS builds remain gates.
The existing dependency/license policy must pass without widened exceptions.

The wrapper and binding additions use Apache-2.0, preserving upstream headers.
Retain the engine Apache notice, LevelDB BSD-3-Clause notice, and xxHash
BSD-2-Clause notice; the added xxHash notice is the exact comment from the
retained original header. The maintained candidate assembler includes these
applicable notices. Complete distribution-license qualification remains open.
Remove both patches only when a supported official tuple provides equivalent
owned hooks and purge wait with the same refusal/lifetime semantics, followed by
the retained source, native and target qualification.
