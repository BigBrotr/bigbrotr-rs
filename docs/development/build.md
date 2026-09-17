# Building the current implementation

The current foundation binary provides `--help`, `--version` and `build-info`.
It does not yet collect, persist or serve events. Available product capabilities
are explicitly empty in its build information; selected future adapters are not
compiled just to make a manifest look complete.

Install Rust through rustup, Python 3.9 or later and Git. The repository pins
Rust 1.98.1 with Rustfmt and Clippy; this is also the initially tested minimum.
The compiler selection incorporates upstream fixes documented in
[Rust 1.96.1](https://blog.rust-lang.org/2026/06/30/Rust-1.96.1/) and the
[1.98.1 release notes](https://doc.rust-lang.org/releases.html#version-1981-2026-09-03).
Do not substitute the older research solver's toolchain for a qualified build.

From the repository root:

```sh
cargo build --locked
cargo run --locked -- --help
cargo run --locked -- --version
cargo run --locked -- build-info
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo doc --locked --workspace --no-deps
python3 -m unittest discover -s tests/acceptance -p 'test_*.py'
```

`--help`/`--version` and requested JSON go to stdout; usage errors go to stderr
with exit 2. Output I/O failure exits 1. These paths use no runtime configuration,
Git executable, files or network. No subcommand initializes a database implicitly;
unimplemented commands are rejected by the parser.

## Exact-source builds

Commit a coherent candidate, then run:

```sh
python3 tools/build.py --output .research/build-candidate
.research/build-candidate/bigbrotr build-info
```

Choose a fresh output directory for each run; existing directories are refused.
The helper rejects dirty source checkouts, extracts `git archive HEAD`, inventories
the extracted files, uses an isolated Cargo home/target directory, and checks the
source inventory again after building. Ignored developer files cannot supply
workspace source. `--revision COMMIT` selects another committed revision explicitly.
The executing helper must match the selected revision's helper byte for byte;
otherwise use a checkout of that revision. Build tools may need
network access to download locked dependencies/toolchains; the runtime diagnostics
do not. Artifacts are native to the executing build host.

`source-manifest.json` records Git SHA-1 commit/tree identities, the archive SHA-256
and SHA-256 for every extracted source file. `build-result.json` binds the executable
SHA-256, source manifest, executing helper and embedded build information. The binary contains actual
`rustc -vV`, Cargo target, optimization/debug settings, and lock/toolchain/dependency
inventory hashes. It also binds the source-manifest SHA-256 and the builder's
clean-source/profile assertions. It does not contain hostnames, build time or checkout paths.

Ordinary Cargo builds report source `unrecorded`, with unknown (`null`) dirty state
and profile assertion. The clean helper supplies `BIGBROTR_SOURCE_REVISION`,
`BIGBROTR_SOURCE_TREE`, `BIGBROTR_SOURCE_MANIFEST_SHA256` and
`BIGBROTR_BUILD_PROFILE`; build metadata
labels them `builder-supplied`. They are assertions by the builder, not proof of
authenticity. The eventual signed release manifest and independent acceptance own
that stronger guarantee. Never use a copied version string as release evidence.

## Native targets and limitations

Mac development uses the actual local architecture. CI defines native Linux amd64
(`ubuntu-24.04`), Linux arm64 (`ubuntu-24.04-arm`) and macOS arm64 (`macos-15`)
checks. A configured runner is not an executed qualification; the
[checkpoint](../implementation/progress.md) and evidence record actual outcomes.
Native storage, packaged installation and workload qualification remain separate
gates before any platform is advertised as a supported product deployment.

Follow [development and delivery](../engineering/development-and-delivery.md)
for Tux: inventory resources, transfer a candidate through a Git bundle, compare
commit/tree and clean state, then build in a fresh owned lab. No mutable directory
sync or release credentials are needed. Tux Linux amd64 never qualifies arm64.

The optional `tests/acceptance/linux/Dockerfile` is a development lab image, not
the product image. Pin its built image digest in the run record. Run it with
`--user` set to the lab owner's numeric UID/GID, `--init`, finite CPU/memory/PID
limits, no published ports, the exact source checkout mounted read-only, and only
the inventoried output directory writable. Its root default is for image building;
root execution cannot pass the read-only-directory acceptance check.

This foundation requires no native storage bindings. Their selected versions,
C/C++/libclang/protoc requirements, ring TLS provider and offline DuckDB/extensions
remain explicit unexecuted gates in the
[dependency selection](../research/dependency-selections.md). Add each dependency
with its first real adapter and update
[the release inventory](../../contracts/release/dependencies.json), lock and tests.

All packages are unpublished. Project code is MIT; dependency rights and notices
remain separate. With cargo-deny 0.20.2 installed, run:

```sh
cargo deny check licenses sources advisories
```

The explicit allowlist covers the current dependency expressions. New licenses
require inspection before changing it. Record the actual advisory database commit
with evidence; a clean advisory scan is not a full security audit. CI is isolated,
read-only and credential-free after checkout; it does not deploy or publish images.
