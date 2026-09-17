# Experiment summary and limitations

| Evidence | Actual observation | Design consequence |
|---|---|---|
| [DuckLake / PG](evidence/ducklake-postgres.json) | Atomic multitable visibility after process kill; two concurrent identical logical batches both committed | Exclusive publication; no invented uniqueness guarantee |
| [Nostr boundary](evidence/nostr-boundary.json) | Valid ID cache allowed a changed-content object through actual receive handler; 16 parser tests passed while hints were discarded | Independent admission verification and information-preserving wire adapter |
| [RocksDB admission](evidence/rocks-admission.json) | Five-key synchronized batch recovered together after process kill; second directory owner refused | Atomic local admission is plausible; final Rust binding and hardware failures still need testing |
| [CH frontier](evidence/clickhouse-frontier.json) | Forced merge destroys old snapshot with logical-edge replacement; immutable-record keys preserve it across retries and delayed family writes | Use immutable projection IDs and complete publication frontier |
| [Archive model](evidence/archive-model.json) | Real DuckDB/Parquet preserved extreme integers/tags, cut-relative corrections and reference closure; tiny object/reference layout was larger than compressed wide rows | Keep logical evidence identity, measure physical deduplication; six negative controls, no product durability/crypto claim |
| [Prior components](evidence/prior-components/manifest.json) | Controlled Go/Rust transport, graph and storage/query comparisons | Buffer/layout/workload effects outweigh technology slogans |

The earlier PostgreSQL container and both Tux process groups were dedicated to
that research. The container was stopped afterwards. The Go repository, its runtime
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

## Current contract and adapter research

| Evidence | Executed scope | Consequence and boundary |
|---|---|---|
| [Identity and portable encoding](evidence/identity-contract.json) | 14 frozen vectors, Python/Node encoders, three hash implementations, 52 histories/2,018 oracle comparisons, plus 44 acquisition inventory comparisons and six wrong-path controls | Exact digest/scope/replay rules; proof-only pages create no mutation receipts and private witnesses cannot suppress public acquisition. Model signatures are synthetic; no engine behavior is tested |
| [Real signed fixture](../../experiments/identity-contract/signed-fixture-evidence.json) | 15 valid signature variants/14 IDs, independent BIP340/Node checks, 30 named public read/graph cases, four reverified exports | Shared payload with scope-specific signatures and authorized source reduction; no product storage adapter |
| [Operational composition](evidence/operations-contract.json) | 120 admission orderings, recovery/activation/configuration scenarios, seven wrong-state controls | Explicit authority, unknown outcomes, resource composition and backup closure; in-memory model, not fsync |
| [Workload obligations](evidence/workload-contract.json) | Literal delivery quotas, bounded virtual scheduler, four negative controls | Includes overdue/refused work and detects monitor starvation despite fast control; no real fleet SLO |
| [Read/rank contract](evidence/read-rank-contract.json) | Three tiny exact-rational stationary systems and traversal counterexamples | Error-bounded target and candidate/current-eligibility distinction; no large-graph speed claim |
| [MCP/ContextVM](evidence/gateway-research.json) | 13 schema cases, 20 upstream in-memory/wire tests and three negative cases | Different protocol versions/transport contracts; no live relay, OAuth or BigBrotr acceptance |
| [Rust dependency selections](dependency-selections.md) | Published source inspection plus [Cargo resolution](evidence/dependency-selections.json): 351 dependencies | Concrete adapter families selected; no native adapter build or release qualification |
| [Handoff validation](evidence/handoff-validation.json) | Canonical GitHub fragments, task references/counts, bad-anchor/unknown-task/skipped-step controls | Detects structural mistakes; prose dependency cycles still require review and tests do not prove semantic completeness |

The [independent closure review](../reviews/14-independent-closure-review.md)
and [operational countercheck](../reviews/14-handoff-countercheck.md) record actual
findings, repairs and review scope. Current choices live in the owning specs,
not in superseded recommendations. Large storage/recovery research has its own
physical workload and cannot upgrade these models to product acceptance.

## Large signed-body storage and recovery

The [September 17 Tux report](tux-storage-qualification-20260917.md) records a fresh
20-million-event corpus, four actual engine representations, independent signed
sample/population checks and archive restore. The source Parquet occupies
79.168 GB; the explicit DuckLake layout uses 79.222 GB of event files and CH's two
parent-part totals about 81 GB each including their projections, before other
deployment overhead. Default DuckLake options were substantially larger in this fixture;
the report distinguishes combined layout tuning from an isolated codec test.

Full signed sample reads support CH for the selected frequent-read product, but
do not prove a universal engine ratio. Failed/corrected runs and scope limits
remain documented. Real transaction/replay/mixed and restart results are recorded
separately from the baseline insert-call timings. None certifies the future Rust
adapters or the 5k/10k/50k/100k busy-relay workload.

**Remote retirement, 2026-09-17:** the owner subsequently authorized deletion of
all pre-existing BigBrotr resources on Tux. The
[retirement record](tux-retirement-20260917.md) supersedes the old labs' stopped/
reusable status. Tracked experiment sources and selected result files remain
local; new runs must recreate their environments. The prior component bundle's
recorded local `/private/tmp` path was also absent on recheck, so its recorded hash
does not establish current availability of the full original bundle.

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

## Review 11: controlled document placement

The [document-layout experiment](../../experiments/document-layout/README.md)
compares wide observations with separate objects/references across churn, entropy
and file boundaries using local DuckDB 1.4.5. Eight corrected cases each reconstruct
16,384 exact observations. Split objects save 20.04–60.10% in the two repeated-body
16-file fixtures; wide rows win both one-file repetition cases and all high-churn
cases. Both executed orderings, source/output hashes, 348 Parquet files and their
metadata were retained/rechecked. The first object's namespace/digest ordering
confound is disclosed. These are compression/reconstruction measurements, not
throughput, memory, recovery or production NIP-11 distribution results.

The separate [relay-profile arithmetic probe](../../experiments/relay-profile/README.md)
independently enumerates the documented minute-aligned timeline and confirms its
S/F/U, availability, coverage, lower-median latency and composite calculations.
Incorrect probe-count, middle-average and unknown-carry alternatives differ.
It establishes neither production window/clock correctness nor optimal ranking
weights; those remain practical implementation/utility qualification work.

## Review 16: deployment, object placement and derived-state semantics

The [integrated review](../reviews/16-complete-handoff.md) records a complete
106-Markdown baseline read and the following separately scoped experiments.

| Evidence | Actual result | Selected consequence |
|---|---|---|
| [Deployment](evidence/deployment-contract.json) | 26 model fixtures, nine wrong-rule controls and two daemon-free Compose parser cases | Resolve capabilities from selected roles, placement and real authorities; no implicit collector or mandatory object server |
| [Relay composition](evidence/protocol-composition-research.json) | Nine real signed events, 22 independent SQLite comparisons, replay/expiry checks and 13 wrong-rule controls | Materialize useful exact population statistics with source cuts, retractions and clock invalidation; observed history is not remote inventory |
| [Local/S3 archive](tux-object-storage-20260917.md) | 1,048,576 rows in both layouts, 132 complete signed samples, separate reader, precommit kill, missing-object refusal and same-host 20-file restore | Optional qualified S3 placement; catalog and all family dependencies define membership, not a directory glob |
| [Stronger S3/PG boundaries](evidence/object-storage-20260917.json) | Position-dependent multipart/range bytes, exact source/restore closure, denied writer operations and role DML/DDL | Retain narrow claims and corrected oracles; lab trust authentication does not qualify production auth |
| [Actual CH views](../../experiments/relay-composition/README.md#clickhouse-materialized-view-counterexamples) | Three counterexamples on CH 26.5.1.882 with 67 checked HTTP commands | Insert-time views do not repair replay overcounts, right-side join changes or expiry without new input |

All five newly owned Tux containers stopped after these experiments. The prior
qualification corpus was read only. No result adds a Rust product, WAN, host-loss,
cloud-price or 5k–100k busy-relay capacity claim. Earlier weak experimental oracles
and provisioning failures remain disclosed rather than rewritten as successful
runs of later source.
