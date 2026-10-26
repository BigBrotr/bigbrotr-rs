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
