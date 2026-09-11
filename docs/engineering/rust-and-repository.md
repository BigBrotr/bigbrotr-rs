# Rust implementation and repository organization

This is the planned layout, not a request to create empty product crates now.
Keep independently useful boundaries; split further only when ownership,
compile-time isolation or a public contract justifies it.

```text
apps/
  bigbrotr/                CLI, wiring and process roles
  dashboard/              SvelteKit/TypeScript, independently built static client
  site/                   Astro/Starlight docs, product pages and blog
crates/
  domain/                 event/evidence/coverage/policy types and pure rules
  config/                 typed config, validation, resolved topology
  protocol/               Nostr adapters, guarded transport, bounded wire envelopes
  ingest/                 durable receipts, owner routing, admission and replay
  storage/                RocksDB, PG, archive and CH adapters behind focused traits
  acquisition/            six acquisition service modules
  analytics/              derivation, graph snapshots, algorithms and run publication
  read/                   resource registry, typed query plans and answer semantics
  publication/            signer policies, durable jobs, NIP-66/85 and DVM adapters
  runtime/                supervision, budgets, cancellation, instrumentation
contracts/                versioned external schemas and generated-client inputs
config/                   minimal overrides and supported deployment recipes
migrations/               PG and CH changes, archive schema transitions
packaging/                Compose, images, native service units, release bundles
experiments/              reproducible bounded research probes, not shipped code
docs/                     design, operations and contribution sources
```

Avoid a generic `utils` crate, one crate per tiny struct, public traits for every
function, a universal key/value checkpoint API for unrelated state machines, and
plugin frameworks before a real extension boundary exists. Services own modules
and state transitions; database adapters do not call business services. Dependency
direction is application → services → domain/contracts; infrastructure implements
focused storage/transport interfaces. Keep protocol values out of unrelated
storage engine abstractions.

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
| Async networking | Tokio, rustls, bounded websocket transport | Explicit buffer limits, DNS/IP pinning, proxy behavior, cancellation and multiplexing |
| Nostr semantics/crypto | `nostr` types and maintained secp256k1 implementation | Independent admission verification; wire extensions retained; no blind SDK pool defaults |
| Durable embedded state | Maintained RocksDB Rust binding | WAL sync/atomic batch checked through actual binding; native library builds and cache budgets |
| PostgreSQL | Async typed SQL adapter, explicit migrations | Transaction ownership, parameterized statements, pooled connection budgets |
| Parquet/Arrow | Apache Arrow/Parquet Rust libraries where useful | Nested tags/raw bytes round trip, bounded row groups, schema compatibility |
| DuckLake | Pinned DuckDB engine + signed compatible extensions in archive worker | PG catalog probe, snapshot backup, extension installation unavailable at runtime still works |
| ClickHouse | Maintained Rust client plus parameterized typed row model | Real engine version matrix, binary values, time ranges, retry/visibility semantics |
| CLI/config/logs | clap, serde/TOML, tracing | Versioned CLI behavior, actionable errors, redaction and generated reference |
| Frontends | SvelteKit static dashboard; Astro/Starlight site | Rust process serves static assets; no Node process required in operator deployment |

Versions are pinned as a tested release set with Cargo.lock, toolchain, native
libraries and container digests. `latest` is not a deployment contract. Do not
select a release solely from stale search snippets: the Nostr docs lookup returned
different versions, so the probe records the exact source commit. Dependency
updates include protocol conformance, storage/recovery and reproducibility checks.
Vendoring/offline bundles are distribution decisions, not substitutes for review.

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
