# Temporary dependency patch

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
