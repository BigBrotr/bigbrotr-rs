# Experiment summary and limitations

| Evidence | Actual observation | Design consequence |
|---|---|---|
| [DuckLake / PG](evidence/ducklake-postgres.json) | Atomic multitable visibility after process kill; two concurrent identical logical batches both committed | Exclusive publication; no invented uniqueness guarantee |
| [Nostr boundary](evidence/nostr-boundary.json) | Valid ID cache allowed a changed-content object through actual receive handler; 16 parser tests passed while hints were discarded | Independent admission verification and information-preserving wire adapter |
| [RocksDB admission](evidence/rocks-admission.json) | Five-key synchronized batch recovered together after process kill; second directory owner refused | Atomic local admission is plausible; final Rust binding and hardware failures still need testing |
| [CH frontier](evidence/clickhouse-frontier.json) | Forced merge destroys old snapshot with logical-edge replacement; immutable-record keys preserve it across retries and delayed family writes | Use immutable projection IDs and complete publication frontier |
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
