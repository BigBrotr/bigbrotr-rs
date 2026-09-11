# Experiment summary and limitations

| Evidence | Actual observation | Design consequence |
|---|---|---|
| [DuckLake / PG](evidence/ducklake-postgres.json) | Atomic multitable visibility after process kill; two concurrent identical logical batches both committed | Exclusive publication; no invented uniqueness guarantee |
| [Nostr boundary](evidence/nostr-boundary.json) | Valid ID cache allowed a changed-content object through actual receive handler; 16 parser tests passed while hints were discarded | Independent admission verification and information-preserving wire adapter |
| [RocksDB admission](evidence/rocks-admission.json) | Five-key synchronized batch recovered together after process kill; second directory owner refused | Atomic local admission is plausible; final Rust binding and hardware failures still need testing |
| [CH frontier](evidence/clickhouse-frontier.json) | Forced merge destroys old snapshot with logical-edge replacement; immutable-record keys preserve it across retries and delayed family writes | Use immutable projection IDs and complete publication frontier |
| [Archive model](evidence/archive-model.json) | Real DuckDB/Parquet preserved extreme integers/tags, cut-relative corrections and reference closure; tiny object/reference layout was larger than compressed wide rows | Keep logical evidence identity, measure physical deduplication; six negative controls, no product durability/crypto claim |
| [Prior components](evidence/prior-components/manifest.json) | Controlled Go/Rust transport, graph and storage/query comparisons | Buffer/layout/workload effects outweigh technology slogans |

The PostgreSQL container and both new Tux process groups were dedicated to this
research. The container was stopped afterwards. The Go repository, its runtime
containers and the other agent's workload were not changed. Test sources are
under `experiments/`; generated data, dependencies and the patched upstream
checkout are ignored laboratory material.

No claim here establishes product-level correctness, a 5k-real-relay ingestion
SLO, billion-row query capacity, power-loss durability, split-brain fencing,
actual website usability or complete production audit. Those claims require the
specified implementation and practical acceptance programme. A research patch
asserting a reproduced bug is explicitly not a regression test claiming desired
behavior. The reproduction has not been posted upstream.

[Laboratory inventory](laboratory-inventory.json) records retained sources/caches,
exact upstream revisions and final stopped state of the owned test resources.

The archive-model run used only a local isolated directory and completed without
background services. Its event signatures are synthetic format fixtures; its
admission portion is a Python reference model, not the Rust/Rocks implementation.
The [Pensieve source inspection](../specification/import-formats.md) is additional
read-only source evidence, not an executed interoperability or performance test.

A subsequent [Parquet target probe](../../experiments/parquet-targets/README.md)
used local DuckDB 1.4.5, one thread and a 128MB engine setting. A requested
64,000-row group produced 65,536-row groups, with exact arithmetic readback.
This corrects a hard-cap assumption; it is not a native-memory, DuckLake,
durability or capacity result. [Evidence](evidence/parquet-targets.json).
