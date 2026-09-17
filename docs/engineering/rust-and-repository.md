# Rust implementation and repository organization

This is the planned layout, not a request to create empty product crates now.
Keep independently useful boundaries; split further only when ownership,
compile-time isolation or a public contract justifies it.

```text
apps/
  bigbrotr/                private Rust application package: CLI and role wiring
    src/
      config/              typed configuration and resolved topology
      runtime/             budgets, supervision, cancellation and instrumentation
      ingest/              durable admission, receipts, owner routing and replay
      storage/             focused RocksDB, PG, archive and CH adapters
      acquisition/         seed, discovery, validation, monitoring and sync
      analytics/           job orchestration and publication of derived generations
      read/                typed resources, plans and shared serving semantics
      network/             guarded DNS/TLS/WebSocket and configured overlay routes
      publication/         signed outbox, NIP-66/85 and query-result delivery
      interfaces/          HTTP, ArchiveRelay, DVM, MCP/ContextVM, internal RPC and management adapters
  dashboard/               SvelteKit/TypeScript static client
  admin/                   separate private SvelteKit/TypeScript static bundle
  site/                    Astro/Starlight product pages, documentation and blog
crates/
  bigbrotr-model/           storage-free event/evidence/coverage/policy types
  bigbrotr-protocol/        information-preserving wire/filter/URL/probe primitives
  bigbrotr-analytics/       pure reducers and graph algorithms over supplied inputs
contracts/                 versioned external schemas and generated-client inputs
config/                    minimal overrides and supported deployment recipes
migrations/                PG/CH changes and archive schema transitions
packaging/                 Compose, images, native service units and release bundles
experiments/               reproducible bounded research probes, not shipped code
docs/                      design, operations and contribution sources
```

This replaces the earlier ten-library sketch. Runtime, configuration, ingestion,
reads and database adapters initially remain focused modules in the application,
not separately published packages. The three candidate libraries have useful
storage-independent boundaries. They are introduced when their first real use
exists, not scaffolded empty to satisfy this drawing. Split further when build
isolation, independent reuse or deployment requires it; merge a proposed library
back into the application if it has no coherent external contract.

Avoid a generic `utils` crate, one crate per service/struct, public traits for every
function, a universal key/value checkpoint API for unrelated state machines, and
plugin frameworks before a real extension boundary exists. Services own modules
and state transitions; database adapters do not call business services. Dependency
direction is interfaces/wiring → application responsibilities → model/protocol/
algorithm libraries; infrastructure implements focused storage/transport contracts.
The pure model/analytics libraries do not depend on Tokio, ClickHouse or RocksDB.
Protocol I/O is an explicit optional dependency where actually useful. Keep
application-specific configuration and secrets out of reusable public types.

## Rust terminology and external reuse

| Term | Meaning here | Does it require a process or container? |
|---|---|---|
| Module | Namespace and visibility boundary inside Rust source, often a file/directory | No |
| Library crate | A compiled Rust library with public APIs another Rust program can use | No |
| Binary crate | A compiled executable, such as the BigBrotr CLI/server | Running it creates a process |
| Cargo package | A manifest and versioned source distribution; may contain a library and binary targets | No |
| Cargo workspace | Related packages built/tested together with shared dependency coordination | No |
| Cargo feature | Compile-time selection of code/dependencies | No; it differs from a runtime service toggle |
| Release packaging | Binary/image/bundle installation and distribution | Depends on the selected deployment |
| Extension/plugin | An extension mechanism belonging to a particular host application | No universal meaning or automatic Rust plugin ABI |

These distinctions follow the [Cargo glossary](https://doc.rust-lang.org/cargo/appendix/glossary.html).
A DuckDB extension, an application service and a Rust crate are different things.
Turning monitoring off in TOML does not require recompiling a Cargo feature.
Container users obtain one tested image and select roles/configuration at runtime.

Other Rust projects should be able to depend on a useful BigBrotr library through
Cargo, either from a published registry package or a Git revision. A private Git
source still requires repository access; this stage publishes no package. Go's
externally importable packages are a useful analogy, but Rust package/crate/module
are not interchangeable terms. HTTP/Nostr contracts let Go, Python and other
languages use a running BigBrotr without linking Rust. Add an FFI or language
binding only for a concrete consumer that needs embedded execution.

Keep application packages `publish = false`. Candidate libraries also stay
unpublished until their API is useful and reviewed: documented invariants/errors,
small runnable examples, doctests, dependency footprint, MSRV, compatibility and
semantic versioning. Avoid exporting engine clients, internal job schemas, or an
entire application framework as the price of reusing one filter or graph routine.
Choose and document reuse-compatible licensing before public release; repository
visibility alone grants no license. Existing code/dependency license obligations
must be checked before copying or distributing it. Follow the
[Cargo publishing guide](https://doc.rust-lang.org/cargo/reference/publishing.html)
for package metadata and publication mechanics, not as a reason to publish now.

Initial extensibility uses ordinary Rust interfaces compiled with the application,
versioned resource/measurement registries, and authenticated external protocols.
No dynamic native-plugin loader, custom runtime ABI, WASM host or polyglot service
mesh is justified yet. Rust core plus TypeScript frontends already crosses the
necessary language boundary through explicit schemas. A future specialized engine
or independently maintained worker can join through a durable/versioned contract.

## Idiomatic implementation

Prefer owned domain values at durable boundaries, borrowing within synchronous
parse/transform stages, and reference-counted immutable byte buffers only where
ownership is actually shared. Profile before introducing unsafe parsing, custom
allocators, unsafe Send/Sync, manual arenas or zero-copy lifetimes spanning I/O.
Use enums for states/outcomes, typed errors with retry classification, Result for
expected failure and contextual errors at process edges. Panics indicate bugs,
not hostile-input control flow. Avoid locks across awaits and unbounded task
spawning. Cancellation is part of every long-running API. Document why an
invariant exists, not a prose translation of the code.

## Dependencies and boundaries

| Area | Direction | Review/validation requirement |
|---|---|---|
| Async networking | Tokio, rustls, `tokio-tungstenite` 0.29 family | Supplied guarded streams preserve DNS/IP pinning and Host/SNI; explicit frame/message/buffer limits, proxy behavior and cancellation |
| HTTP and private RPC | axum 0.8 / Hyper 1 / Tower 0.5; tonic + tonic-prost 0.14 / prost 0.14 | One runtime/middleware family; hard body/queue bounds, both RPC codec limits, propagated deadlines and operation authorization; no public generic RPC |
| Nostr semantics/crypto | `nostr` types and maintained secp256k1 implementation | Independent admission verification; wire extensions retained; no blind SDK pool defaults |
| Durable embedded state | rust-rocksdb `rocksdb` 0.25 family | Actual binding's synced WAL/atomic batch, exclusive owner, native build/cache/compaction bounds; native 11.8.1 candidate needs fresh qualification |
| PostgreSQL | `tokio-postgres` 0.7 + `deadpool-postgres` 0.14 + rustls connector; explicit migrations | Direct PG transaction ownership and bounded pool; parameters, lost COMMIT resolution and connection-wide cancellation races; SQLx/ORM not selected |
| Parquet/Arrow | Apache Arrow/Parquet 58 family, aligned to duckdb-rs | Nested tags/raw bytes/extreme integers round trip, bounded row groups; ordinary artifacts only, no independent DuckLake file mutations |
| DuckLake | Official `duckdb` 1.10505 family (DuckDB 1.5.5) + signed compatible extensions in isolated Rust worker | Exact offline native assets, PG catalog/receipt recovery, bounded output and interrupt/kill/panic behavior; Arrow iterator alone is not a memory bound |
| ClickHouse | Official `clickhouse` 0.15 family with validated typed rows and server-side value parameters | Real engine version matrix, binary values, time ranges, explicit insert completion, server cancellation and retry/visibility semantics |
| Agent gateways | Official rmcp 3.4.0 with explicit MCP 2026-07-28; bounded ContextVM framing over Nostr primitives | Optional/off; ContextVM SDK 0.2.2 is an interoperability oracle for its legacy binding, not a mandatory server worker; see [gateway contract](../specification/gateway-contracts.md) |
| CLI/config/logs | clap, serde/TOML, tracing | Versioned CLI behavior, actionable errors, redaction and generated reference |
| Frontends | SvelteKit static dashboard; Astro/Starlight site | Rust process serves static assets; no Node process required in operator deployment |

Versions are pinned as a tested release set with Cargo.lock, toolchain, native
libraries and container digests. `latest` is not a deployment contract. Do not
select a release solely from stale search snippets: the Nostr docs lookup returned
different versions, so the probe records the exact source commit. Dependency
updates include protocol conformance, storage/recovery and reproducibility checks.
Vendoring/offline bundles are distribution decisions, not substitutes for review.

The [dependency selection record](../research/dependency-selections.md) fixes exact
source-inspected candidates, alternatives, feature intent and early P01/P02 gates.
These families are selected for implementation; the actual release tuple remains
unqualified until Rust bindings, engines and Linux targets pass those gates.
The isolated Cargo resolution result is not a build or durability test.

The SDK source probe reproduced a valid-ID-cache bypass in its private receive
handler at the recorded commit. Its NIP-67 parser also discards hints. Therefore
BigBrotr owns admission and the information-preserving wire envelope; an SDK
client can still be used behind a tested adapter for suitable publishing jobs.
The core should not inherit client-oriented expiry/deletion rejection when
collecting a historical archive. See [probe evidence](../research/evidence/nostr-boundary.json).

## Maintenance and release

Use rustfmt and applicable Clippy diagnostics, review public APIs and docs, and
check unsafe/native dependencies. No arbitrary line limits or coverage quotas.
Pin the minimum supported Rust version deliberately; compile it in CI. Support
Linux amd64 and arm64 as an explicit release matrix, with macOS development
support. A multi-platform container reference must identify the image index when
one manifest is expected to support both architectures.

CI validates fast pure tests, protocol fixtures, engine integration, document
links/examples and frontend builds. Scheduled/release jobs add large workloads,
restore/fault drills and browser journeys. Publish release provenance, checksums,
SBOM, migration notes, compatibility matrix and rollback limitations. Separate
trusted release workflows from untrusted pull-request execution. Keep security
reporting private and document supported versions.

The repository stays private and named `bigbrotr-rs` now. Before the final rename
to `bigbrotr`, inventory legacy remotes, package/image names, GitHub Pages paths,
OAuth callbacks, webhooks, CI badges, links and redirects; back up and tag the old
project; rehearse migration and publish a version map. Do not rename or repurpose
the current repository while the other agent works there. Branding throughout is
BigBrotr; the suffix is temporary infrastructure naming.
