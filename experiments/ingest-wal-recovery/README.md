# Strict native WAL reopening

P02-T02 step4 prerequisite discovered while designing reserved-but-uncommitted
resolution. The actual product had inherited RocksDB11.8.1 point-in-time recovery.
A synchronized batch could disappear after detected WAL corruption while native
open still succeeded. The product now explicitly selects AbsoluteConsistency;
the [owning profile](../../docs/reference/ingest-native-profile.md#selected-options)
records compatibility, availability cost and remaining limits.

The real native tests in
`apps/bigbrotr/src/storage/rocks/wal_recovery_tests.rs` start a child product store,
synchronize the frozen bootstrap, observe its completion marker, SIGKILL/reap the
actual process, then reopen the same directory. They compare every original key
and value against `tests/fixtures/ingest/staged-bootstrap-crash.json`.

One case flips one CRC byte in the bootstrap's complete WAL record; another removes
its final payload byte. A separate permissive-mode control for each loses the
whole batch. Ordinary and recovery opens must refuse both cases and retain the
damaged WAL through identical retry. The intact crash preserves every row.
A further control removes the entire final physical record: even strict open
accepts this complete shorter WAL. This is an executed limitation, never success
evidence for noncommit resolution. No negative resolver is introduced.

Local focused commands (with the repository's selected native compiler environment):

```sh
cargo test --locked -p bigbrotr --lib wal_recovery_tests -- --nocapture
cargo test --locked -p bigbrotr --lib storage::rocks::options::tests -- --nocapture
```

`run_native.py` is an isolated Linux qualification runner, not an installation
command. Export a clean exact Git revision to `/out/build/source`, with its normal
`source-manifest.json`, isolated Cargo home/target and a checksum-verified offline
archive asset bundle at `/assets`. Use an inventoried nonroot container with two
CPUs,6GiB RAM,512PIDs,512MiB tmpfs,read-only source/root, no network, host ports,
Docker socket, PostgreSQL or credentials. The runner uses the shared CI executor
and exact builder, constructs the actual same-source worker before the application,
then tests native stores, formats and Clippy. Existing output is refused. A trusted
copied compiler/dependency cache is recorded as such, never source authority.

Source/artifact manifests and command receipts belong to the owned ignored lab;
the final evidence report must distinguish executed native targets and preserved
REDs from pending qualification. These tests do not exercise real power failure,
mid-FFI kill, whole-WAL removal, historical salvage adoption, causal backup,
producer checkpoint/ACK/GC or successor activation.
