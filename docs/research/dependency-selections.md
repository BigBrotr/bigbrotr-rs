# Selected Rust adapter families

Selected 2026-09-17 for the implementation handoff. These are default dependency
families and exact **qualification candidates**, not a qualified production lock.
P01-T01 freezes the actual toolchain, features, transitive lock, native libraries
and extension assets after the gates below pass. The initial research implemented
no product adapter; implemented subsets are recorded below. Existing engine probes do not qualify Rust
bindings or their cancellation, serialization and connection-pool behavior.

This replaces the unnamed SQL/HTTP/RocksDB/ClickHouse adapter choices in the
[Rust organization](../engineering/rust-and-repository.md#dependencies-and-boundaries).
Record/storage semantics remain owned by the component, identity, operations and
storage contracts; library defaults cannot override them.

## Selected set and alternatives

| Responsibility | Selected candidate | Reason and alternative disposition |
|---|---|---|
| PostgreSQL control transactions | `tokio-postgres` 0.7.18 + `deadpool-postgres` 0.14.2; `tokio-postgres-rustls` 0.14.0 | Direct PG transaction, parameter and cancellation APIs fit the explicit control ledger. Deadpool supplies a bounded pool without an ORM. SQLx 0.9.0 remains a credible alternative, but its additional database abstraction and optional compile-time SQL metadata workflow are unnecessary for the selected PG-only control adapter. No second PG driver is selected. |
| HTTP API, management, static assets, ArchiveRelay upgrade and native MCP mount | `axum` 0.8.9 over Hyper 1/Tower 0.5, with `tower-http` 0.6 for appropriate middleware | Shared Tokio/Tower foundation with tonic and rmcp; route boundaries preserve distinct listener/auth/budget policies. Raw Hyper-only routing would add application plumbing; Actix/Rocket would introduce another framework with no demonstrated requirement. This is a fit decision, not a benchmark claim. |
| Guarded outbound Nostr WebSocket | `tokio-tungstenite` 0.29.0, aligned with axum's WS dependency | Supply the already guarded TCP/TLS stream, then perform the handshake; preserve Host/SNI. Version 0.30.0 was observed but adds a second major-minor line without a required feature. No client-pool SDK defaults define acquisition behavior. |
| Private typed RPC | `tonic` + `tonic-prost` + build-only `tonic-prost-build` 0.14.6; `prost` 0.14.4 | Generated protobuf service/message adapters on the same runtime. In-process calls remain direct typed calls. No public generic RPC, gRPC-Web or reflection endpoint follows from selecting tonic. |
| Ingest and Index embedded stores | `rocksdb` 0.25.0 from rust-rocksdb; candidate `librocksdb-sys` 0.19.0+11.8.1 | Established Rust access to the already selected RocksDB engine, including atomic batches, sync options, snapshots and explicit resource controls. No raw C wrapper or different KV engine is justified before this adapter's gates fail. |
| ClickHouse projection and query transport | Official `clickhouse` 0.15.2 from ClickHouse/clickhouse-rs | Typed Serde rows, checked row metadata, streaming HTTP responses, explicit insert completion and server-side value parameters. No older similarly named native-protocol driver or generic SQL facade is selected. |
| DuckLake archive/offline worker | `duckdb` and `libduckdb-sys` 1.10505.0, encoding DuckDB 1.5.5 | Official Rust bindings inside the already specified isolated native worker. One Rust application/release, no Python production bridge or permanent extra database service. Package an exact native library and matching signed DuckLake/Postgres/object-storage extension assets. |
| Columnar interchange and ordinary Parquet artifacts | Apache `arrow-array`/`arrow-schema` and `parquet` 58.4.0 | Align with duckdb-rs's Arrow 58 dependency. Current Arrow/Parquet 60.0.0 was inspected but is not selected merely for recency. Avoid duplicate Arrow types or conversion copies across two majors; Polars and DataFusion are not additional engines. |

Version-qualified published sources and archive hashes are recorded in
[the source inventory](evidence/dependency-selections.json). Source inspection is
targeted at the adapter boundary, not an audit of every dependency. SQLx's
[query macro documentation](https://docs.rs/sqlx/0.9.0/sqlx/macro.query.html)
describes both online schema checking and checked-in offline metadata; SQLx can
also operate without those macros. Its rejection here does not claim otherwise.

## Implemented event-verification dependency slice

P01-T02/T03 uses `secp256k1` 0.33.1 with only `std` and locked
`secp256k1-sys` 0.14.1, plus Serde 1.0.229/serde_json 1.0.151 and SHA-2 0.11.0.
The narrow API verifies the actual signature against the recomputed 32-byte digest.
The model and parser are BigBrotr-owned: source inspection of `nostr` 0.45.5 found
its EventId/PublicKey deserializers also accept NIP-19/NIP-21 spellings, and it
would add a separate secp256k1 0.30 line. Its timestamp/kind types themselves
retain u64/u16; no narrowing failure is alleged. Its full verifier recomputes IDs,
but signature-only verification and higher-level cache behavior cannot define
BigBrotr's admission boundary. This is a focused departure from the earlier
`nostr`-types candidate, preserving the selected protocol/data semantics.

The [release inventory](../../contracts/release/dependencies.json) pins actual
dependencies; [protocol reference](../reference/protocols.md) records the
implemented boundaries and independent fixtures. Exact published-source archives
and inspection identities are inventoried under ignored
`.research/checkouts/nostr-dependency-audit-20260917/`. The maintained wrapper is
CC0-1.0, its vendored native code retains MIT notices, and a native C compiler is
required. Source comparison found sys 0.14.1 is a metadata-only patch over 0.14.0;
the actual lock and native tests, not that observation alone, qualify its behavior.

Sources: [secp256k1 0.33.1 verification](https://docs.rs/secp256k1/0.33.1/secp256k1/schnorr/fn.verify.html),
[nostr 0.45.5 Event](https://docs.rs/nostr/0.45.5/nostr/struct.Event.html), and
[pinned NIP-01](https://github.com/nostr-protocol/nips/blob/a2494f4f81d46684e5814a9bf35e2b1df978f955/01.md).

## Implemented fingerprint codec slice

P01-T02 selects `minicbor` 2.3.0 with default features disabled and no optional
features/transitive dependencies. Exact published source at upstream commit
`67b22f849a0ff12b669a0c304236ffe9744f9a79` uses shortest big-endian integers and
definite arrays/bytes/UTF-8 through a streaming writer. The model privately uses
direct typed body/admission, core event fact, event request and existing-proof
recipes; it does not expose a general CBOR value API or infer canonical maps from
Rust map ordering.
Frozen independent preimages test those actual recipes, not every future
fact/proof format.

The upstream decoder accepts non-shortest encodings and its generic decode helper
does not require EOF. The implemented core fact decoder caps input, retained
strings, tag nodes and requested allocation before reservation, checks closed
schemas/arity/required fields and EOF, and compares streaming canonical
re-encoding against the original bytes without a second output buffer. Tests
refuse 626 equal-value noncanonical encodings; see the
[codec evidence](../implementation/evidence/p01-fact-codec-20260917.json).
These are codec-local bounds, not allocator/RSS or runtime grant guarantees.
Other persistent families and proof/request decoders remain unimplemented and
must meet the same requirements; generic map/derive decoding or unknown-field
skipping cannot enforce the profile. No engine persistence follows from decoding.

The exact archive hash/source inspection and isolated Rust probe are inventoried
in `.research/checkouts/cbor-dependency-audit-20260917/inventory.json`. The
BlueOak-1.0.0 notice was inspected and explicitly allowed; distributed copies must
include its [license text or link](https://blueoakcouncil.org/license/1.0.0), also
recorded in the release dependency inventory. Full packaged license/SBOM delivery
remains P08's responsibility.

Sources: [encoder](https://github.com/twittner/minicbor/blob/67b22f849a0ff12b669a0c304236ffe9744f9a79/minicbor/src/encode/encoder.rs),
[decoder](https://github.com/twittner/minicbor/blob/67b22f849a0ff12b669a0c304236ffe9744f9a79/minicbor/src/decode/decoder.rs),
[streaming writer](https://docs.rs/minicbor/2.3.0/minicbor/encode/write/trait.Write.html).

## PostgreSQL ownership and cancellation

Use typed domain-to-row mappings and `$n` parameters; checked conversions retain
the identity contract's full integer ranges. A PostgreSQL `BIGINT` cannot silently
narrow an unsigned u64. Migration SQL remains explicit and runs through the
existing privileged migration command, separate from ordinary application roles.

Each active transaction/cancellable query exclusively owns one checked-out
connection. Bound acquisition wait, connection count, connect time and query work
through the existing service budget. Choose Deadpool `RecyclingMethod::Verified`
initially; use transaction-local settings and prohibit session state leaking
between borrowers. Session advisory locks/listeners, where required, use separately
owned connections. `Clean` is not a substitute for transaction ownership, and
`Fast` is not the selected initial health check. These modes are explicit in
[Deadpool 0.14.2's configuration source](https://github.com/deadpool-rs/deadpool/blob/bba260f93ef4f7a63c82799563f92518b9e5d42a/crates/deadpool-postgres/src/config.rs).

Set server statement/lock deadlines as well as caller deadlines. A PG cancellation
token addresses the connection's current query, is racy and does not confirm
successful cancellation. Retain/quarantine that checkout until the original
operation reaches a known terminal protocol state, or discard its connection;
do not return it for unrelated work while a late cancellation could reach it.
Resolve an uncertain COMMIT using the control ledger's fixed operation identity.
Neither future drop nor a pool health query proves no durable effect.
[tokio-postgres 0.7.18 cancellation source](https://github.com/rust-postgres/rust-postgres/blob/f1cb6ec0d5766b136cbd68f3010d64142a5daa66/tokio-postgres/src/cancel_token.rs)
states the limitation; its [transaction source](https://github.com/rust-postgres/rust-postgres/blob/f1cb6ec0d5766b136cbd68f3010d64142a5daa66/tokio-postgres/src/transaction.rs)
queues rollback on drop rather than producing a caller-observed rollback receipt.

Use the rustls connector with explicit CA/server-name policy and the same selected
crypto provider as the other clients. The candidate supports rustls 0.23 and
TLS channel binding; its [published connector source](https://github.com/jbg/tokio-postgres-rustls/blob/0dd60da1804e6146633e5b87199a00db890b7ef5/src/lib.rs)
does not replace negative certificate/hostname and pool-recovery tests.

## HTTP, WebSocket and RPC limits

Apply hard request-body accounting at the transport/body layer, including custom
extractors and decompressed bodies. Axum's extractor-local default limit does not
cover every body consumer. Explicitly cap WebSocket messages, frames, read/write
buffers and queued sends; the inspected WS defaults include a 64 MiB message
limit and an unlimited maximum write buffer. These are source facts, not suitable
BigBrotr defaults. Use the existing operation-specific limits and bounded control
lane. [Axum body limits](https://docs.rs/axum/0.8.9/axum/extract/struct.DefaultBodyLimit.html)
and [0.8.9 WS source](https://github.com/tokio-rs/axum/blob/c59208c86fded335cd85e388030ad59347b0e5ae/axum/src/extract/ws.rs)
define the relevant APIs. A Tower timeout alone does not terminate an engine task.

Tonic's `Endpoint::timeout` does not set `grpc-timeout`; set the request deadline
metadata too, propagate the shrinking budget and handle server cancellation.
Override both client/server encode **and** decode limits to the operations
contract's 8 MiB, with 4 MiB application batches and complete atomic records.
The inspected codec defaults are 4 MiB decoding and effectively unlimited encoding;
the endpoint buffer defaults to 1,024 requests. Bound that queue, stream count,
flow-control windows and metadata within the application grant; no hidden second
queue can bypass admission. Disable RPC compression initially, enabling it only
with independent compressed/decompressed limits. See
[tonic endpoint source](https://github.com/hyperium/tonic/blob/6cb6056b5a748bc5a29bd48f4602dbc4e552bb7d/tonic/src/transport/channel/endpoint.rs)
and [codec source](https://github.com/hyperium/tonic/blob/6cb6056b5a748bc5a29bd48f4602dbc4e552bb7d/tonic/src/codec/mod.rs).

Protobuf owns transport values, not semantic fingerprints. Reject unsupported
semantic schemas during handshake; preserve the identity contract's canonical
fingerprints across JSON/protobuf/columnar mappings. Pin the protoc/build generator
with P01's release tooling. TLS peer identity still requires operation authorization.

## RocksDB and ClickHouse adapter boundaries

Use RocksDB's synchronized `WriteBatch` across the owner's column families for
the declared atomic unit, with WAL enabled. Explicitly set sync true: it defaults
false. Do not enable ignoring missing column families, which could accept only
part of the intended batch. Charge native caches, memtables, WAL growth, pinned
iterators/snapshots and compaction debt to the owner grant. Run synchronous FFI
work through the owner's bounded workers; dropping `spawn_blocking` handles does
not stop native writes. The [0.25.0 option source](https://github.com/rust-rocksdb/rust-rocksdb/blob/e4aac69f55021e619566b6ca5902665e0ea2af4e/src/db_options.rs)
supplies these APIs. Its published lock selects native RocksDB 11.8.1; earlier
engine experiments do not establish that new tuple's recovery behavior.

Use ClickHouse's validated row mode initially, explicit `Insert::end`, named
query IDs, bounded row streaming and per-operation server work limits. Keep
deterministic BigBrotr chunk identities separate from query IDs. Late stream
errors, insert completion timeout or process loss retain the existing unknown
outcome/replay/frontier rules. A dropped HTTP future is not evidence that server
work stopped; cancellation uses the dedicated control lane and remains unresolved
until the target query is known stopped. Mocks force validation off in this client,
so actual engine tests must cover schema/type mismatches.
[Client validation source](https://github.com/ClickHouse/clickhouse-rs/blob/fcb8b02537072d75d157361c59269b5d2e4102ce/src/lib.rs)
and [insert timeout source](https://github.com/ClickHouse/clickhouse-rs/blob/fcb8b02537072d75d157361c59269b5d2e4102ce/src/insert.rs)
establish the relevant behavior; both insert timeout defaults are disabled.

Use `Query::param(name,value)` for server-side typed value parameters and
`with_setting` for approved engine limits/query identity. SQL structure comes
from the registered planner, never arbitrary request fragments. The inspected
[0.15.2 query source](https://github.com/ClickHouse/clickhouse-rs/blob/fcb8b02537072d75d157361c59269b5d2e4102ce/src/query.rs)
supports this even though the current website still contains an older
no-server-parameters limitation and 0.12.2 examples. Follow exact-version source
and engine fixtures when they disagree. The automatic `Inserter` batching feature
is not selected for deterministic projection chunks; client buffering does not
provide multi-family transactions or logical retry deduplication.

## DuckDB worker and Arrow/Parquet responsibilities

The first qualification path links the `duckdb` crate with default features off
to an explicitly packaged DuckDB 1.5.5 native library, using a declared build-time
library path. Do not silently discover a workstation library or download one at
runtime. Produce that native asset in the release build and record its hash/ABI;
verify required built-ins and signed extensions offline on both Linux targets.
The upstream bundled C++ build is a supported fallback **build method**, not a
different runtime architecture; qualify and record it before substitution.
[The pinned binding manifest](https://github.com/duckdb/duckdb-rs/blob/88f673c5cdf09e287c010a910db53b97e523a9fa/crates/duckdb/Cargo.toml)
and [release](https://github.com/duckdb/duckdb-rs/releases/tag/v1.10505.0)
identify the binding/engine relationship and Arrow dependency.

Run finite archive/offline work in the existing isolated job role, with native
memory/thread/temp limits before execution and a separately owned interrupt
handle. `query_arrow` materializes the complete result. `stream_arrow` fetches
chunks lazily but some statements still materialize inside DuckDB; its iterator
panics on later fetch/conversion failure. Use bounded batches for selected
streamable statements, never collect an entire corpus. A worker panic or forced
termination fails the output and triggers receipt resolution before retry; no
success manifest can be inferred from EOF after process loss. The worker retains
its grant until joined/positively terminated. These limitations are explicit in
[statement.rs](https://github.com/duckdb/duckdb-rs/blob/88f673c5cdf09e287c010a910db53b97e523a9fa/crates/duckdb/src/statement.rs)
and [arrow_batch.rs](https://github.com/duckdb/duckdb-rs/blob/88f673c5cdf09e287c010a910db53b97e523a9fa/crates/duckdb/src/arrow_batch.rs).

Arrow is the in-process columnar interchange representation; Rust Parquet handles
ordinary research/import/export artifacts, independent fidelity checks and bounded
file readers/writers. DuckLake/DuckDB owns its transactional archive catalog and
referenced Parquet files. Rust Parquet must not independently add/rewrite/delete
those files behind DuckLake. No Arrow Flight server is added. Bound row groups,
batch bytes, nested elements and outstanding FFI buffers; preserve ordered tags,
null/empty distinctions, raw bytes and unsigned event times. File bytes and Arrow
layout remain distinct from canonical semantic hashes. Arrow 58.4.0 is an explicit
compatibility selection: the pinned duckdb-rs manifest requires major 58 and its
published lock resolves 58.4.0; latest observed Apache 60 is not evidence that
those Rust types interoperate.

## Qualification handoff

Use the existing tasks rather than another implementation phase. A failed gate
stops promotion of that tuple; fix the adapter or document a specific replacement
with the same fixtures before changing families.

| Owner | Required actual-adapter evidence before enablement |
|---|---|
| P01-T01 | Resolve/build the selected feature set; inspect duplicate runtime/TLS/Arrow versions; pin Rust/MSRV, protoc, C/C++/libclang and native assets; offline clean Linux amd64/arm64 builds and macOS development load; licenses/SBOM. Registry MSRVs are lower bounds, not a tested workspace MSRV. |
| P01-T02/T03/T07 | Signed fixture and u64/i64 extremes through real PG/protobuf/CH/Arrow/Parquet mappings; duplicate JSON fields remain rejected; HTTP custom-extractor/body/WS/RPC oversized and compressed payloads, shrinking deadlines, mTLS/auth and bounded queue behavior. |
| P02-T01 + P01-T05 | PG lost COMMIT reply, pool exhaustion, statement timeout/cancel racing completion and unrelated next borrower, TLS failure, stale prepared metadata after migration. A fresh connection/ledger lookup resolves effects; query drop alone fails the oracle. |
| P02-T02 | Rust binding synced atomic batch, process-kill/reopen, second writable owner refusal, maximum accepted unit, full disk/fsync failure, unknown result resolution and actual CertifyExisting no-write path. Preserve CF options/format identity for recovery. |
| P02-T05/T06 | Real CH insert/schema/stream failures, lost completion, replay beyond dedup TTL, cancellation observed at server, stopped query before releasing grants, and frontiers only after all required family chunks. |
| P02-T03/T04/P08 backup/export tasks | Exact native/extension load with networking disabled; DuckLake PG transaction/receipt resolution after worker termination; streaming/panic/interrupt/large nested row under worker RSS/temp bounds; cross-reader Parquet fidelity and causal snapshot restore. |

The isolated resolution manifest/lock under `.research/dependency-selections/`
resolved successfully: 351 dependencies, one Arrow 58.4 line, one rustls 0.23 line
and one tokio-tungstenite 0.29 line. Its explicit feature plan uses ring-backed
rustls, RocksDB Zstandard plus build-time bindgen, ClickHouse LZ4/validation,
axum HTTP/JSON/WS and tonic transport/codegen. DuckDB uses the packaged native
library path; its Arrow dependency is already present. Add no unused Polars,
DataFusion, RPC compression or automatic ClickHouse inserter feature. Full-release
qualification must also include the separately selected optional gateway features.

This is a dependency solver check only. It compiles no adapter, starts no engine and
proves no performance, durability, license completeness or platform compatibility.
The evidence inventory records its outcome separately from published-source
inspection. Downloaded upstream source stays under ignored
`.research/checkouts/dependency-selections/`; it is not shipped product source.

## Implemented static configuration and endpoint identity slice

P01-T04 selects `toml` 1.1.6+spec-1.1.0 with only `std`, `parse`, `display`
and `serde`. The actual parser uses the borrowed/spanned `DeTable` tree: the
ordinary `toml::Value` integer is i64 and cannot preserve a configured full-u64
owner epoch. The selected tree keeps the integer spelling until checked i64/u64
conversion, rejects duplicate definitions, and enforces its finite nesting bound.
BigBrotr rejects unknown keys and tables and caps input at 10MiB. Raw endpoint
array spans are capped before canonicalization/deduplication; measuring only the
canonical output accepted an independently reproduced encoded list larger than 8MiB.
Parser diagnostics never expose source excerpts or supplied values.

`url` 2.5.8 supplies maintained IDNA, IPv6 and URL parsing beneath the pure
`bigbrotr-endpoint/1` wrapper. Inspected `url/src/host.rs` intentionally accepts
legacy numeric IPv4 spellings. The wrapper rejects those spellings and browser
repairs before normalization, retains query bytes/order and redacts path/query
in Debug output. It has no DNS, route authorization, HTTP redirect or dial
behavior; those P01-T03 gates remain separate.

`getrandom` 0.4.3 supplies OS randomness for nonzero initialization candidate IDs.
`rustix` 1.1.5 (`fs`) supplies safe `O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC` opening
followed by a regular-file descriptor check. A symlink, device or FIFO cannot
become an unbounded configuration read. Parent-directory alias checks are not
claimed. `config init` uses exclusive creation and never replaces existing work;
it does not establish a durable deployment ownership receipt.

Exact source archives, Cargo checksums, upstream commits, inspected license
choices and focused source locations are recorded in
[the static dependency evidence](evidence/static-configuration-dependencies.json).
The four published crates offer MIT; the `url` IDNA/ICU transitive set remains in
Cargo.lock and the existing Unicode license policy. This is focused parser/file/
normalization inspection and local CLI evidence, not full SBOM, platform or
runtime qualification. No alternative TOML or URL parser is carried alongside it.

Sources: [toml DeTable](https://docs.rs/toml/1.1.6+spec-1.1.0/toml/de/struct.DeTable.html),
[url host parser](https://github.com/servo/rust-url/blob/d6ea13c5f8e7e6e627f6390161b3e185bda5e5ce/url/src/host.rs),
[getrandom fill](https://docs.rs/getrandom/0.4.3/getrandom/fn.fill.html), and
[rustix open](https://docs.rs/rustix/1.1.5/rustix/fs/fn.open.html).
