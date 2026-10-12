# Building the current implementation

The current binary provides `--help`, `--version`, `build-info` and
[`event verify`](../reference/protocols.md), offline
[configuration commands](../reference/configuration.md) and explicit initial
[control-schema migrations](../operations/schema-upgrades.md). Event verification
and the installed [archive worker verifier](../../packaging/archive-worker/README.md)
have distinct scopes. Event verification
does not collect, persist or serve events. Build information names the currently
advertised bounded capabilities and their pinned protocol/parser versions;
experimental command availability is not complete product qualification.

Install Rust through rustup, a native C/C++20 compiler and libclang, Python 3.9 or later, OpenSSL
(for ephemeral TLS test certificates) and Git. The repository pins
Rust 1.98.1 with Rustfmt and Clippy; this is also the initially tested minimum.
The compiler selection incorporates upstream fixes documented in
[Rust 1.96.1](https://blog.rust-lang.org/2026/06/30/Rust-1.96.1/) and the
[1.98.1 release notes](https://doc.rust-lang.org/releases.html#version-1981-2026-09-03).
Do not substitute the older research solver's toolchain for a qualified build.

On Debian/Ubuntu, install `clang llvm libclang-dev g++`; the clean builder uses
the single LLVM installation selected by `llvm-config` on PATH, including its
`clang`, `clang++`, `llvm-ar` and `libclang.so`. On macOS, install Apple's Command
Line Tools or Xcode and select it with `xcode-select`. The builder uses `xcrun`
to find that installation's compilers, archiver, SDK and libclang; Homebrew LLVM
and inherited `LIBCLANG_PATH` are not required. Missing tools fail explicitly.

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

Ordinary Cargo output lives under ignored `.research/targets/product`, as set by
`.cargo/config.toml`; exact-source builds override it with their owned run directory.

`--help`/`--version` and requested JSON go to stdout; usage errors go to stderr
with exit 2. Output I/O failure exits 1. These paths use no runtime configuration,
Git executable, files or network. No subcommand initializes a database implicitly;
unimplemented commands are rejected by the parser.

## Exact-source builds

Commit a coherent candidate, then run:

```sh
python3 tools/build.py --output .research/build-candidate
.research/build-candidate/bigbrotr build-info
.research/build-candidate/bigbrotr archive verify-worker
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

Native asset downloads use a checksum-addressed cache. Each attempt writes inside
its own private temporary directory, checks pinned length and SHA-256, then
atomically links the verified file into the cache without replacing another
attempt's result. The cache filesystem must support hard links. Every reuse is
verified; this cache does not promise power-loss durability. Ordinary errors clean
only the current attempt. A killed process may leave its unique temporary root;
it cannot block a later attempt and is removed only after ownership/lifecycle
inspection. Historical deterministic `.partial` files are likewise not reused
or deleted opportunistically. Bundle output roots still require a fresh path.

Native compilation defaults to two Cargo jobs, including the `cc` crate's shared
jobserver. This is the initial setting for the isolated 2-CPU/6-GiB builder,
not a memory guarantee. `--jobs N` selects another positive job count explicitly;
the caller must provide a suitable resource envelope. Ambient job counts,
compiler wrappers/flags, target-specific compiler overrides, `ROCKSDB_LIB_DIR`,
header overrides and loader search overrides are excluded from the child
environment. Vendored RocksDB is compiled from its locked crate sources; the
helper never copies a prebuilt RocksDB archive into its fresh target directory.

`source-manifest.json` records Git SHA-1 commit/tree identities, the archive SHA-256
and SHA-256 for every extracted source file. `build-result.json` binds the executable
SHA-256, source manifest, executing helper and embedded build information. The binary contains actual
`rustc -vV`, Cargo target, optimization/debug settings, and lock/toolchain/dependency
inventory hashes. It also binds the source-manifest SHA-256 and the builder's
clean-source/profile assertions. It does not contain hostnames, build time or checkout paths.
The builder compiles the worker before the main executable and embeds the actual
worker length/hash in `archive_worker`. It verifies the installed bundle through
the actual CLI before writing its receipt. Ordinary unbound Cargo builds expose
`archive_worker: null` and refuse installed worker selection; runtime environment
variables and adjacent receipts cannot replace the embedded anchor. The build
receipt also records the worker length, without introducing a self-hash cycle.

The receipt's additive `native_toolchain` field records the exact `CC`, `CXX`,
`AR`, `CLANG_PATH`, `LIBCLANG_PATH`, SDK and job settings passed to Cargo.
It includes compiler/discovery/library versions, invocation and resolved paths,
tool/library SHA-256 values, and macOS SDK version/metadata hash. The `clang++`
invocation name is retained even when it points to the same binary as `clang`:
that name selects C++ driver behavior. Apple `ar` has no version-query flag, so
its version is null and its executable hash supplies the identity. The selected
tools and SDK metadata are checked again after compilation. This receipt remains
builder-supplied evidence; it does not inventory every system header/library or
establish a reproducible, authenticated release toolchain by itself.

Ordinary Cargo builds report source `unrecorded`, with unknown (`null`) dirty state
and profile assertion. The clean helper supplies `BIGBROTR_SOURCE_REVISION`,
`BIGBROTR_SOURCE_TREE`, `BIGBROTR_SOURCE_MANIFEST_SHA256` and
`BIGBROTR_BUILD_PROFILE`; build metadata
labels them `builder-supplied`. They are assertions by the builder, not proof of
authenticity. The eventual signed release manifest and independent acceptance own
that stronger guarantee. Never use a copied version string as release evidence.

## Private browser consumer

Rust compilation does not run pnpm or generate frontend assets. The initial
private SvelteKit panel and shared session client are separate private workspace
packages. Use Node26.4.0 and pnpm9.15.9, then run from the repository root:

```sh
pnpm install --frozen-lockfile --ignore-scripts --strict-peer-dependencies
pnpm contracts:check
pnpm check:client
pnpm test:client
pnpm --filter @bigbrotr/admin check
pnpm --filter @bigbrotr/admin test:unit
pnpm --filter @bigbrotr/admin build
```

The last command emits `apps/admin/build` and a closed version1 asset manifest.
`BIGBROTR_ADMIN_BASE=/control` builds the selected subpath variant; omitted builds
`/`. `BIGBROTR_BUILD_REVISION` accepts the exact40-character lowercase Git commit
for build attribution; omitted uses the explicit development marker, never a
clock-derived ID. Attribution and content hashes do not authenticate a release.
The packager refuses symlinks, unknown file types and limits before publishing
the manifest. The [Control runtime](../user/control-runtime.md) verifies and serves
it from `administration.assets.directory`; no Node production server is needed.

The export browser laboratory can reuse a qualified Rust parent/worker pair with
an explicitly enumerated set of separate frontend inputs. The job-page consumer
includes its OpenAPI/generator/client/model/component/tests; the earlier label
regression included only `JobStatus.svelte` and `style.css`. `runtime-reuse.json`
records both versions' hashes and requires equality of every other product/build
input. Fresh current-Git frontend builds and native checks own the new bundle;
the ELF retains its original source identity. This is no Rust qualification or
release promotion. [Earlier label evidence](../implementation/evidence/p09-skip-button-native-20260924.json)
retains its narrower scope.

These executor laboratories retain the product's64GiB completion reserve. Their
preflight adds setup headroom for copied native/frontend/compiler inputs, and the
execution fixture records installed `config show` plus actual container filesystem
capacity after setup. [An original insufficient-headroom run](../implementation/evidence/p09-job-pages-capacity-incomplete-20260928.json)
stopped before browser qualification; its database/result remain unchanged.
Retire verified obsolete caches rather than lowering the product reserve to pass.

The OpenAPI3.1 [session subset](../../contracts/management/v1/openapi.yaml) owns
wire types and runtime schemas. After an intentional contract edit, run
`pnpm contracts:generate` and review the generated client diff. Standalone Ajv
validators ship without a runtime compiler or eval. Relative URLs, same-origin
credentials, streaming response limits and explicit timeout/cancellation remain
in the handwritten client transport. Session models are not action capabilities.
Build and upgrade the backend and private bundle together. Session observations
now require `config_revision` alongside `policy_revision`, so commands can use
the heads actually observed during authentication. Older closed validators reject
the added field; the current client rejects its absence. This pre-release wire
change requires no database migration and does not permit automatic command retry
with newer revisions.

The initial export-resume controls keep one bounded recovery request per account
in IndexedDB at the private origin. They wait for native transaction completion
before sending, retain the original key/revisions and restore without automatic
submission. Sign-out clears the view while preserving this non-secret recovery
metadata. The [private administration contract](../product/private-administration.md)
owns its limits and failure behavior. [Scoped native evidence](../implementation/evidence/p09-resume-browser-native-20260924.json)
covers actual store operations on all three automated engines and the installed
Chromium root resume/reload/restart journey. The [remaining five pairs](../implementation/evidence/p09-resume-browser-matrix-native-20260924.json)
now have the same installed recovery evidence on native Linux amd64, with new
bundles and exact reused Rust artifacts. Each executes its selected engine's
native store cases and the real HTTP/PostgreSQL journey. This is scoped recovery
coverage, not full accessibility, session-expiry or released-installation acceptance.

These checks establish local build/client behavior. Real production CSP,
Chromium/Firefox/WebKit, Rust/PostgreSQL and installed root/subpath journeys still
require their own recorded execution. Native browser and complete frontend CI/
release assembly are not implied by a local build.

## Native targets and limitations

Current authoring and tests run on Tux Linux amd64 under Q34. Historical Mac
development evidence retains its actual architecture. CI defines native Linux amd64
(`ubuntu-24.04`), Linux arm64 (`ubuntu-24.04-arm`) and macOS arm64 (`macos-15`)
checks. A configured runner is not an executed qualification; the
[checkpoint](../implementation/progress.md) and evidence record actual outcomes.
Native storage, packaged installation and workload qualification remain separate
gates before any platform is advertised as a supported product deployment.

Follow [development and delivery](../engineering/development-and-delivery.md)
for Tux: inventory resources, isolate a candidate through local Git/archive or a
bundle, compare commit/tree and clean state, then build in a fresh owned lab. No mutable directory
sync or release credentials are needed. Tux Linux amd64 never qualifies arm64.

The optional `tests/acceptance/linux/Dockerfile` is a development lab image, not
the product image. Pin its built image digest in the run record. Run it with
`--user` set to the lab owner's numeric UID/GID, `--init`, finite CPU/memory/PID
limits, no published ports, the exact source checkout mounted read-only, and only
the inventoried output directory writable. Its root default is for image building;
root execution cannot pass the read-only-directory acceptance check.

Event verification links `secp256k1` 0.33.1 and vendored `secp256k1-sys` 0.14.1
through locked `cc` 1.4.6. Only `std`/`alloc` features are active; no signing RNG,
recovery or SDK pool is selected. The protocol crate refuses the dependency's
intentionally broken `secp256k1_fuzz` compilation mode. The clean builder also
strips ambient compiler flags. Upstream wrapper/native licenses remain distinct
and are recorded in the dependency inventory.

The portable model uses `minicbor` 2.3.0 without optional features for its first
closed fingerprint recipes. It streams typed fields into SHA-256 without a second
encoded buffer. Its [BlueOak license notice](https://blueoakcouncil.org/license/1.0.0)
applies separately from project MIT code; the complete release must retain
dependency notices. This is not yet a complete persistent CBOR reader/writer.

The initial PostgreSQL consumer uses tokio-postgres 0.7.18,
tokio-postgres-rustls 0.14.0 and rustls 0.23.45 with the explicit ring provider,
configured CA roots and expected server identity. No implicit system/web CA store
or connection pool is enabled. The selected RocksDB adapter uses `rocksdb` 0.25.0
with default features disabled and only `zstd`/`bindgen-runtime`, backed by
`librocksdb-sys` 0.19.0+11.8.1. Its native build requires C++20 and a runtime-loadable
libclang. The inspected `clang-sys` 1.9.1
[search implementation](https://docs.rs/crate/clang-sys/1.9.1/source/build/common.rs)
accepts an exact `LIBCLANG_PATH` file, preventing a later search from selecting
another installed library. Bindgen 0.72.1 uses that library and `CLANG_PATH` for
include discovery; the locked `cc` 1.4.6 build honors the explicit compiler and
archiver paths and Cargo jobserver. These are build inputs, not storage durability
qualification. Remaining native bindings, protoc and offline DuckDB/extensions remain gates in the
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
