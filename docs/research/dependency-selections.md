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
| Local password credentials | `argon2` 0.6.0, default features off, `zeroize`; direct `zeroize` 1.9.0 with alloc and `subtle` 2.6.1 | The closed 64MiB/3/1 Argon2id format uses explicit caller-owned fallible block memory and the existing OS entropy provider. No PHC parser or hidden hashing pool is needed for this first enrollment consumer. Native listener/concurrency qualification remains required. |
| PostgreSQL control transactions | `tokio-postgres` 0.7.18 + `tokio-postgres-rustls` 0.14.0; `deadpool-postgres` 0.14.2 remains the selected later pool candidate | Direct PG transaction, parameter and cancellation APIs fit the explicit control ledger. The first migration/inspection owner uses one unpooled connection, so Deadpool is deliberately deferred until a real bounded pooled consumer exists. SQLx 0.9.0 remains a credible alternative, but its additional database abstraction and optional compile-time SQL metadata workflow are unnecessary for the selected PG-only control adapter. No second PG driver is selected. |
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

## Exact persisted-policy fractions

The same pinned serde_json 1.0.151 now enables `float_roundtrip` in addition to
`std`. The actual default decoder changed the legal configuration fraction
`2.0030397744267762e-253`, reproduced as a stored-policy digest mismatch. The
upstream regression is in `tests/test.rs::test_roundtrip_f64`; `src/de.rs` selects
the lexical exact parser under this feature instead of arithmetic scaling of a
rounded significand. Published source commit:
`de8500740cdcabffb9734f503e4889def823cf10`.
The [feature inventory](https://docs.rs/crate/serde_json/1.0.151/features)
adds no dependency or persistent-format change. The locked version and serializer
stay unchanged. Stored decoder regression checks exact f64 bits and policy digest;
the native fixture also reads PostgreSQL's expanded numeric JSONB representation.
This is a concrete decoding correction, not a new JSON document authority.

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

The Control owner uses a dedicated unpooled connection and database-local
session lock, retaining the same lock at transaction scope through each guarded
commit. PostgreSQL 16's [advisory-lock semantics](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS)
and exact [REL_16_15 lock functions](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/utils/adt/lockfuncs.c)
establish shared exclusion between session/transaction holders and transaction
retention after session unlock. The [pg_locks representation](https://www.postgresql.org/docs/16/view-pg-locks.html)
uses database identity, two int keys and objsubid 2. SQL guards check that tuple
and the actual backend attribution; PID or a stored epoch alone is insufficient.
The opaque Rust owner has no automatic reconnect or raw-client escape. Native
kill/commit/old-connection acceptance is a separate gate; this does not fence
another engine. Lock-view scans occur on Control operations, not per event, and
must be included in later mixed-workload measurements.

Use the rustls connector with explicit CA/server-name policy and the same selected
crypto provider as the other clients. The candidate supports rustls 0.23 and
TLS channel binding; its [published connector source](https://github.com/jbg/tokio-postgres-rustls/blob/0dd60da1804e6146633e5b87199a00db890b7ef5/src/lib.rs)
does not replace negative certificate/hostname and pool-recovery tests.

The implemented connection-preparation slice pins `tokio-postgres` 0.7.18 with
only `runtime`, `tokio-postgres-rustls` 0.14.0 with only `ring`, rustls 0.23.45
with `std`, `ring` and `tls12`, and Tokio 1.53.1 with `rt`, `net`, `time` and
`sync` requested directly; `tokio-postgres` also enables Tokio's required
`io-util`/`bytes` support. It installs no process-global crypto default: each TLS configuration uses
an explicit ring provider and only the configured CA file. A narrow
`MakeTlsConnect` wrapper substitutes the separately configured expected server
name while preserving the upstream connector and channel-binding implementation.
The preparer opens public CA files through no-follow descriptors, and loads the
password plus optional client certificate/key through the protected-file reader.
The current 64 KiB password, 1 MiB CA/certificate and 256 KiB key caps are
bounded application choices, not measured PostgreSQL protocol maxima. The
reported input byte sum and cap-plus-one reservation exclude rustls/driver copies
and allocator overhead. Local-plaintext preparation checks only the configured
hostname shape and retains its loopback/private-bridge boundary for the connected
owner; that owner must resolve, vet and pin an in-bound address before connecting.
It performs no DNS, TLS handshake or PostgreSQL I/O;
wrong-name/CA, authentication, timeout, task ownership and lost-COMMIT cases stay
in the actual adapter acceptance gate. The later migration consumer has now executed
the scoped native Linux amd64 COMMIT/TLS scenarios recorded in
[its evidence](../implementation/evidence/p02-control-faults-tls-20260917.json);
this does not qualify the future pooled service or other native targets.

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

The [native binding experiment](../../experiments/native-rocks-binding/README.md)
now exercises that exact tuple on Mac arm64 and Tux Linux amd64. Its
[recorded evidence](../implementation/evidence/p02-native-rocks-binding-20260917.json)
covers nine actual binding cases: synchronized multi-family write/reopen,
selected SIGKILL schedules, snapshot isolation, writer/alias refusal, exact
column-family compatibility, missing-CURRENT refusal and observed Zstandard
SSTs/shared native resource options. This isolated program is not the product
admission owner. Disk-full/fsync failures, power loss, device/inode aliases,
native Linux arm64 and the complete P02 receipt/authority scenarios remain open.
Keep handles private to their database: the binding does not prevent a column
family handle from another database being passed to a batch. Always describe
the default family explicitly with the same resource/compression policy.

The first private product `IngestStore` candidate now consumes `rocksdb =0.25.0`
with default features disabled and only `zstd` and `bindgen-runtime`. Its initial
responsibility is explicit initialization and exact manifest/column-family
reopening, retaining native resources through close; this is not event admission.
The product lock selects `librocksdb-sys 0.19.0+11.8.1` and
`zstd-sys 2.1.0+zstd.1.5.7`. The active sys features are `static`,
`bindgen-runtime`, `zstd` and its optional dependency; Zstandard enables only
`legacy` and `zdict_builder`. `cargo tree --locked --offline` for Mac arm64 and
Linux amd64/arm64 confirms those graphs. The `bzip2-sys` and `libz-sys` lock
entries are inactive: the sys crate's `static` feature forwards their static
options only if the dependencies are otherwise enabled. Snappy, LZ4, zlib,
bzip2, jemalloc and io-uring are not selected. Graph resolution does not execute
or qualify any target.

The exact [sys build source](https://docs.rs/crate/librocksdb-sys/0.19.0+11.8.1/source/build.rs)
compiles bundled C++20 and runs `bindgen 0.72.1` with `runtime`, using
`clang-sys 1.9.1`; `cc 1.4.6` enables bounded Cargo jobserver parallelism.
Runtime here means loading libclang in the build process. This feature does not
add an application runtime libclang requirement. The [clean builder](../development/build.md)
selects and records the actual C/C++ compiler, archiver, libclang and applicable
SDK, defaults to two jobs, and excludes ambient native overrides and prebuilt
engine paths. Zstandard uses its bundled C build; the clean environment excludes
`ZSTD_SYS_USE_PKG_CONFIG` as well. Native product Linux builds, crash/resource
faults, admission/replay and release qualification remain separate evidence gates.

The [release dependency inventory](../../contracts/release/dependencies.json)
records exact version-qualified license paths. The wrapper and inspected sys
headers use Apache-2.0; select the Apache option offered by RocksDB and retain
its LevelDB BSD-3-Clause and xxHash BSD-2-Clause notices. Select Zstandard's BSD
option and retain its wrapper and generated-binding notices. Build dependencies
retain bindgen's BSD-3-Clause, clang-sys's Apache-2.0 and cc's MIT/Apache notices.
This focused inventory does not replace the complete packaged SBOM and upstream
notice collection, including platform runtime libraries and toolchain assets.

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

Query `threads` does not limit signed-extension startup. DuckDB 1.5.5's
[`ComputeHashesOnSegments`](https://github.com/duckdb/duckdb/blob/d8cdaa33fda8df955cc76ef58a280f68f4cd43fa/src/main/extension/extension_load.cpp)
creates one native thread per 1 MiB signature-hash segment and joins them before
loading the extension. The pinned Linux amd64 assets require 35, 40 and 21 hash
threads across three sequential loads. Native tracing observed 96 successful
clones with query `threads=1`; this is not 96 simultaneous or persistent workers.
An isolated PID cap of 8 produced `clone` EAGAIN, then worker SIGABRT, no successful
result and no remaining worker process. Keep signed verification enabled. Include
startup thread/stack allowance separately in parent admission and OS limits;
retain the native grant until positive process termination. Actual peak RSS,
thread overlap and production parent recovery remain acceptance gates. See the
[native prerequisite qualification](../implementation/evidence/p02-archive-engine-native-20260918.json).

The private worker also uses the already pinned rustix 1.1.5 safe process API
(`src/process/prctl.rs` and `src/process/id.rs`). Linux
[`PR_SET_PDEATHSIG`](https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html)
can deliver SIGKILL even to a stopped worker, but registration is not retroactive.
Check the expected spawning PID before and after registration. Its lifetime is
the creating thread's; no later fork/exec/credential transition is permitted to
clear the setting. `Pid::from_raw` does not validate negative numbers in release
builds and asserts in debug, so the private PID parser checks positivity before
calling it. The old actual-worker stopped-orphan control reproduced survival;
the [corrected Linux amd64 package](../implementation/evidence/p02-parent-binding-native-20260918.json)
exits and is positively reaped after parent SIGKILL while stopped. This is neither
a PostgreSQL backend fence nor evidence that uninterruptible kernel work has ceased.

The pinned PostgreSQL extension has a concrete credential-lookup edge: with
`allow_persistent_secrets=false`, an absent default secret makes `GetSecret` in
[`postgres_catalog.cpp`](https://github.com/duckdb/duckdb-postgres/blob/41223e51559cd581f1c06e170b71c71df25bbaac/src/storage/postgres_catalog.cpp)
fall back to unavailable `local_file` storage. Real Rust catalog attach failed.
Provide an explicit TEMPORARY PostgreSQL memory secret and pass its name through
DuckLake `META_SECRET`; the actual probe verifies the secret is nonpersistent and
in memory. Keep `META_SECRET_STORAGE_TABLE=''` as a separate exclusion of optional
PG secret-table registration. This preserves the credential boundary without
enabling persistent secrets. The [native four-case investigation](../implementation/evidence/p02-catalog-fence-native-20260918.json)
records the failed attach and successful corrected tuple, unfenced duplicates,
live sessions after NOLOGIN, terminated old backends and lost COMMIT response.
It does not select a production fence: PostgreSQL16.15 checks login permission
before publishing backend activity. A [real PG16.15 startup counterexample](../implementation/evidence/p02-postgres-startup-native-20260918.json)
then committed from an initially invisible client after NOLOGIN and an empty
activity observation. Login revocation plus enumeration alone is insufficient;
actual client/process exclusion and startup ordering remain recovery gates.
This research adds no archive schema or migration.

The same selected libpq18.3 source makes authentication policy explicit.
`fe-connect.c`'s `pqConnectOptions2` maps `require_auth=scram-sha-256` to required
authentication and only the SCRAM SASL mechanism. `fe-auth.c`'s
`check_expected_areq` rejects AuthenticationOk until `client_finished_auth`;
`fe-auth-scram.c` sets it only after checking the server-final signature, returning
failure for a mismatch. The [official connection contract](https://www.postgresql.org/docs/18/libpq-connect.html#LIBPQ-CONNECT-REQUIRE-AUTH)
also requires completed authentication. The shared native route selects this
policy plus disabled GSS transport/ambient client certificates. That client
requirement does not audit all server HBA routes or authorize arbitrary clients.
Exact source hashes belong to the existing archive-worker inventory. The
[native old/new qualification](../implementation/evidence/p02-native-catalog-auth-20260919.json)
executes 16 actual PG/native cases at e98f484. The unchanged 0ca4fff client accepts
trust and an omitted server-final; the new client refuses, sending zero SQL
messages in the incomplete-exchange case. Verify-full TLS positive/refusal cases
also pass. Native mutual TLS, GSS and MD5 challenges were not executed.

Scientific schema initialization also uses an explicit memory secret on its
separate READ_ONLY PostgreSQL preflight attachment, with `SECRET_STORAGE_TABLE=''`.
Pinned `PostgresCatalog::Initialize` registers secret storage regardless of read-only
access; excluding it on DuckLake's later attachment alone is insufficient.
`postgres_query` uses its transaction connection by default; READ_ONLY is an actual
PG transaction access mode. The selected DuckLake revision
`d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4` writes every nested field to
`ducklake_column`, with parent field IDs and native scalar type names. Schema
metadata is row data, so a checksum of PG table definitions cannot validate the
scientific schema. The initializer uses native CREATE TABLE statements in one
transaction and annotates the initialization snapshot using
`ducklake_set_commit_message`; it never writes native metadata itself.
The [versioned row declaration](../reference/archive-format.md#explicit-initial-scientific-schema)
records the current implementation scope. The schema1 inspection fixture derives
all 203 columns independently from that declaration, then compares actual native
PG metadata. Pinned `ConvertColumn` and `ColumnToSQLRecursive` retain empty
`default_value_type` for STRUCT/LIST and `literal` for scalars; child nullability
is native schema representation, not permission to lose required model children.
`LoadDuckLake` rejects unknown setting scopes; logical inspection must reject
malformed/duplicate/unknown persisted settings before granting a scientific lifetime.
The [scientific guard qualification](../implementation/evidence/p02-scientific-guard-native-20260918.json)
records 37 actual-PG cases at `a91ae01`, including that malformed-scope control.
File/name mappings are a separate data-file interpretation boundary and allocate
from next_file_id, not next_catalog_id. They remain publication/readback work.
Actual PG ambiguous initialization and installed recovery remain qualification gates.

The [actual native COMMIT-loss case](../implementation/evidence/p02-native-commit-file-loss-20260918.json)
at `c9a29e0` invalidates ordinary INSERT cleanup for ambiguous commits. PG committed
all three synthetic file references, then the proxy withheld COMMIT completion;
DuckLake removed all body/witness/receipt files. Fresh counts still returned1,
while an actual body read failed. Pinned `DuckLakeTransactionState::Commit` catches
the native commit error and invokes `CleanupFiles`; the PG extension marks its
transaction finished before sending COMMIT. Returning Rust CommitUnknown happens
after native cleanup and cannot prevent it. The previous lost-ack experiment
killed after native COMMIT returned, a different boundary.

No cleanup-disable setting was found in the inspected pinned option paths.
Supported `ducklake_add_data_files` marks registered files `created_by_ducklake=false`,
which skips that unlink branch. The [registration experiment](../implementation/evidence/p02-native-registration-20260918.json)
at `ea270ac` confirms retained files/full bodies after the actual lost native COMMIT
response, including receipt, and retained unregistered files after pre-forward
rollback. This is scoped synthetic evidence, not signed scientific publication or
a reason to weaken ACK.
The source revisions and hashes are retained in the report. No native dependency
version or scientific schema has changed from this finding.

Pinned registration calls `parquet_full_metadata` with globs even for singleton
lists; DuckDB `FileSystem::HasGlob` recognizes `*`, `?`, `[`. An actual embedded
counterexample registers sibling `/x/file.parquet` instead of the existing literal
`/[x]/file.parquet`. Local root validation therefore refuses these characters.
Explicit `hive_partitioning=false` prevents a path such as `kind=7` from replacing
the file's actual kind. Native `GetRelativePath` is lexical: canonical checked
schema/table parents and exclusive attempt directories own filesystem placement.
Registration preserves nested field mappings recursively and commits them with
the file rows; full scientific PG readback and relocation remain acceptance work.
The narrow independent AI source review inspected these paths at the pinned
DuckLake revision and bundled DuckDB1.5.5 sources; it performed no remote test.

The same pinned binding rejects nested List/Array/Struct/Map/Union values through
`ToSql`; its `Value` enum is not evidence that those parameters can be bound.
Ordered nested tags therefore need the already selected Arrow path rather than
scalar parameter conversion. The `appender-arrow` feature accepts a RecordBatch,
slices it to the native vector size and converts one DataChunk per slice. Include
both Arrow buffers and the native converted chunk in the grant. Its list
conversion passes the entire `values()` child array to the native child vector;
an Arrow slice can retain child values outside its visible rows. Do not infer a
small byte bound from the sliced parent row count, and avoid repeatedly converting
a large shared child buffer across small parent slices. Qualify bounded record
batches and actual nested allocation/amplification at this boundary. Explicitly flush
and check every appender before commit: Drop discards flush errors. These are
scoped source observations from `src/types/to_sql.rs`, `src/appender/arrow.rs`,
`src/appender/mod.rs` and `src/arrow_interop/to_duckdb.rs` at the pinned revision,
plus checksum-verified arrow-array 58.4.0 `GenericListArray::slice`; they are not
executed adapter qualification.
Nested conversion, huge rows, allocation/flush errors and readback remain P02-T04
acceptance work.

The first typed dictionary test found a second native compatibility requirement:
mask scalar children when a containing qualified-reference/source-ID struct is
null. Arrow permits valid placeholder children beneath a null struct, but the
pinned binding writes children and then marks only the parent invalid
(`struct_array_to_vector`, `StructVector::set_null`). DuckDB's Parquet definition
levels account for parent nulls while `StandardColumnWriter::WriteVector` filters
values using the child validity mask. A sparse nullable method reference was
correct in Arrow and native table SQL, yet returned zeroed fields after Parquet.
A SQL-created null-struct control passed. The adapter now supplies matching
parent/child validity; the same complete dictionary oracle passes. A minimal
paired negative control preserves the unmasked loss (73 becomes 0) and masked
readback (73 stays 73). Mac arm64 and the exact native Linux amd64 candidate
`5c3a8dd` reproduce both cases; [the native report](../implementation/evidence/p02-five-family-rows-native-20260918.json)
records all 18 executed adapter/engine cases. This does not qualify the future
DuckLake publisher or another Parquet implementation.

The scoped native sources are
[`StructColumnWriter`](https://github.com/duckdb/duckdb/blob/d8cdaa33fda8df955cc76ef58a280f68f4cd43fa/extension/parquet/writer/struct_column_writer.cpp),
[`HandleDefineLevels`](https://github.com/duckdb/duckdb/blob/d8cdaa33fda8df955cc76ef58a280f68f4cd43fa/extension/parquet/column_writer.cpp)
and [`StandardColumnWriter`](https://github.com/duckdb/duckdb/blob/d8cdaa33fda8df955cc76ef58a280f68f4cd43fa/extension/parquet/include/writer/templated_column_writer.hpp).
Their retained hashes and failed/intermediate/fixed observations are inventoried.
This normalization preserves logical nullability and canonical fact identities;
it adds no nullable identity fields to a present reference and requires no
migration of qualified archive data. The failing dictionary path was never
qualified or used for publication. Do not replace the failed oracle with a count
or accept metadata-only SQL checks as Parquet fidelity evidence.

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


## Catalog writer credential material

P02-T04 adds direct edges to already locked `ring` 0.17.14 and `base64` 0.22.1;
`serde` is also used directly for the borrowed private bootstrap wire. There are
no resolved package/version changes. Ring and base64 disable default features and
select `alloc`. Fixed 256-bit random ASCII-hex passwords avoid normalization ambiguity.
The implementation uses ring PBKDF2-HMAC-SHA256 (4096 iterations), HMAC Client/Server
Key, existing sha2 SHA256 StoredKey, and standard padded base64 in PG's verifier
format. This is infrastructure credential provisioning, not a general SCRAM client
or human-password policy. Owned secret buffers use existing zeroize; compiler,
cryptographic and downstream driver copies prevent a process-wide erasure claim.

[PG16 CREATE ROLE](https://www.postgresql.org/docs/16/sql-createrole.html) accepts a
pre-encrypted SCRAM verifier as-is and warns that plaintext SQL passwords can reach
logs. The verifier itself is sensitive and must not be logged as evidence.
[PG16 role membership](https://www.postgresql.org/docs/16/role-membership.html)
distinguishes ADMIN, INHERIT and SET. Exact REL_16_15 source inspection of
`src/backend/commands/user.c` lines532–590 and765–815 establishes implicit creator
ADMIN-only membership and ALTER ROLE requirements. Explicitly clearing the local
createrole_self_grant GUC prevents additional inherited/SET authority. The default
connection limit is an approximate guard, not a hard native concurrency bound.

Exact source SHA256: user.c
`082713e0ba52a4a6896e9be4beba0d7258030b2b02291d3e8551619509845574`;
complete `src/common/scram-common.c`
`804383b3882f5574b81b23c03933ea9e4557fa25ea0038d02a6f33addf43baee`;
complete `src/include/common/scram-common.h`
`c97092c9bb334fcbbf7ff1199c6e0f0c43a0428d008c38a03f7f64129d965e98`.
Sources/URLs remain under the inventoried ignored credential-work root. Scoped
ring source inspection covers pbkdf2.rs90–255 and hmac.rs150–300,385–400; the
[versioned derive API](https://docs.rs/ring/0.17.14/ring/pbkdf2/fn.derive.html)
owns output/iteration semantics. An independent Python hashlib/hmac vector and
three Rust material cases pass locally. The [native credential report](../implementation/evidence/p02-catalog-credentials-native-20260918.json)
at `d2b2609` qualifies actual PG16.15 SCRAM login, wrong-password refusal,
non-superuser provisioning/membership, sealing, commit ambiguity and finite native
handoff. Persistent service recovery and scientific publication remain separate.


### Local archive custody descriptor candidate

P02-T04 adds `command-fds`0.3.3 without default/optional features for one concrete
private-worker descriptor. Inspected upstream commit
`750b7be437f3063ed9e080bffd9050e293031e82`, `src/lib.rs`:
`preserved_fds` owns its input until Command destruction and clears CLOEXEC only
in the forked child through a nonallocating pre-exec callback. The parent keeps
its ordinary CLOEXEC descriptor. The implementation uses preservation, not fixed
FD remapping, avoiding collisions with standard streams or the exec-error pipe.
No application unsafe code or child-side unsafe inherited-FD initializer is used.
The lockfile descriptor is process-owned until exit and must never be explicitly
unlocked, closed or reused by worker code. This is a trusted native worker
contract, not containment against malicious child code.

The locked additions are nix0.31.3 (fs; safe fcntl wrapper), cfg_aliases0.2.2 and
thiserror/thiserror-impl2.0.20. Their MIT/Apache-2.0 licenses and the direct
Apache-2.0 license use the existing license policy. No engine version changes.
Source inspection of rustix1.1.5 confirms direct Linux flock and the Darwin libc
flock wrapper. [Linux flock semantics](https://man7.org/linux/man-pages/man2/flock.2.html)
retain a lock across dup/fork/exec until the final descriptor closes; explicit
unlock would instead release every duplicate. A separately opened descriptor
contends even in the same process. Network filesystems have different semantics
and are excluded from this initial Linux ext4 candidate. Mac development tests
cannot qualify Linux or host recovery.

The permanent journal inode is never replaced or deleted by retry. Lock acquisition
precedes complete manifest publication, and recovery locks before comparing the
exact retained identity. A dedicated catalog connection must also retain a clone
through transport destruction: tokio-postgres0.7.18 marks COMMIT done before
awaiting its reply and its connection task can continue draining queued work
after the operation future is dropped. Joining the operation alone is insufficient.
[Native Linux qualification at 5ad77af](../implementation/evidence/p02-local-custody-native-20260919.json)
now exercises the actual inherited worker descriptor, cancelled queued COMMIT,
transport destruction, manager/parent crash and fresh local/catalog observations.
It qualifies this finite custody prerequisite; installed recovery, missing-import
resolution and resource/protection obligations remain separate.

The shared `storage::postgres::transport` adapter returns the actual unpolled
connection future after authentication. Control's wrapper starts/joins it;
catalog custody is attached before task creation. Exact locked
`tokio-postgres`0.7.18 `src/connect_raw.rs` performs startup/authentication and
`read_info` before constructing Client/Connection. Driver destruction is still
not remote rollback; DNS may retain runtime blocking work after timeout. No new
pool, reconnect/fallback or resource-accounting guarantee is introduced. The
publication fixture now consumes the same protected preparer and transport.
[Native qualification at `6606103`](../implementation/evidence/p02-shared-postgres-transport-native-20260919.json)
executes the actual CLI TLS good/wrong-CA/wrong-name, signals, blocked query
cancellation/restart and signed catalog consumers. A single pinned query stays
pending until custody attachment/driver polling. This confirms the finite adapter
boundary; no shared process-budget or installed Archive readiness is claimed.
