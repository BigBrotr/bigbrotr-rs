# Tux: large signed-body storage and recovery slices

Date: 2026-09-17. These are isolated **engine experiments**, not the Rust product
or a fleet acceptance result. The reproducible
[research programs](../../experiments/storage-qualification/README.md) describe
the fixture, setup, corrected harness, exact scopes and known confounds. The
legacy local checkout and unrelated host applications were not modified.

The [condensed engine evidence](evidence/storage-qualification-20260917.json)
records exact values, raw-report hashes, retained failures, versions and owned
resource inventories. The [recovery evidence](evidence/storage-recovery-qualification.json)
separately describes the actual transaction/mixed cases and corrected harness.

## Question and actual workload

Does a useful Parquet archive necessarily waste more space than CH, and can
archive-only reads reasonably substitute for indexed online full-body reads?
Can the selected engines preserve complete signed fields and recover selected
transaction/backup boundaries? These are narrower questions than the whole-stack
comparison in P02-T09/P08-T12.

Tux supplies a Ryzen 7950X, 16 physical cores/32 threads, about 61.9 GiB usable RAM
and an ext4 NVMe filesystem. The Python worker has 8 CPUs/20 GiB; CH 8 CPUs/12 GiB;
PG 1 CPU/1 GiB. Individual baseline queries use 4 threads/7 GB engine memory. Limits
include different things: the query ceiling is not total process/cgroup memory.
Other host workloads and page cache remain uncontrolled; no global cache flush
or host restart was performed.

The main corpus contains **20,000,000 unique signed events**, with at least 8192
content bytes, ordered nested tags, Unicode and deterministic public test keys.
It occupies 79,168,070,464 bytes in 308 source Parquet files, generated in 537.92 s.
There is also a 100,000-event calibration corpus. The hash-rich wide content is a
stress distribution, not a measured Nostr content/compression distribution. The
input exceeds host RAM, but that alone does not establish cold-cache queries.
The path named `aged` denotes resident cardinality; the store has not experienced
weeks of random updates, deletes, compactions, provenance churn or mixed traffic.

## Completed baseline results

Decimal GB below means 10^9 bytes. Ingest time sums the actual batch insert/commit
calls; it excludes some preparation/reporting, later merges and other lifecycle
work. Main CH tables use default fsync settings, so **these are not comparable
durable-ACK throughput measurements**. There were 77 batches per variant.

| Representation | Event-file / active parent-part bytes | Summed load calls | Recent 100 median, 3 runs | ID content-length median, 24 runs |
|---|---:|---:|---:|---:|
| DuckLake engine-default file options | 135.569 GB; 150 files | 359.914 s | 118.9 ms | 96.1 ms |
| DuckLake explicit ZSTD3/8192-row-group/256MB target; narrow receipt inlining | 79.222 GB; 326 files | 313.407 s | 69.3 ms | 93.4 ms |
| CH ID-first plus chronological `_part_offset` projection | 81.009 GB; 29 active parts | 774.084 s | 126.9 ms | 2.46 ms |
| CH chronological-first plus ID `_part_offset` projection | 81.033 GB; 36 active parts | 789.690 s | 21.1 ms | 3.30 ms |

CH's parent-part totals **already include** their projection bytes. The separately
reported projection component occupied approximately 0.817 GB and 0.737 GB
respectively; do not add it again. This interpretation was checked against the
exact release's [part accounting](https://github.com/ClickHouse/ClickHouse/blob/v26.5.1.882-stable/src/Storages/MergeTree/MergedBlockOutputStream.cpp#L221-L249)
and [checksum sum](https://github.com/ClickHouse/ClickHouse/blob/v26.5.1.882-stable/src/Storages/MergeTree/MergeTreeDataPartChecksum.cpp#L99-L104).
These are capture-time component metrics, not complete or
peak deployment bytes: obsolete parts, receipt/catalog/WAL data, temporary
files, caches, replay reserve and backups have separate costs. Archive and online
representations intentionally duplicate body values; a compression result does
not eliminate that cost.

DuckLake's documented default is Snappy; the explicit layout avoids attributing
default-codec cost to an unavoidable archive architecture penalty. That candidate
changes several options together, so it does not isolate codec versus row-group
effects. BigBrotr already selects explicit Zstandard; exact row/file/inlining
tuning still belongs to its low-traffic/maximum-row/recovery gates.
[DuckLake configuration](https://ducklake.select/docs/stable/duckdb/usage/configuration).

Post-load footer inspection confirms SNAPPY versus ZSTD on actual event files;
the sampled row groups contain 65,536 versus 8,192 rows. The default receipt table
has 77 Parquet files/59,748 bytes; the explicit receipt-inlining table has 77 rows
and zero Parquet files. The catalog backup must therefore carry those inlined
receipts. CH `EXPLAIN indexes=1,projections=1` reports use of the ID projection
for a point lookup on the chronological table. These are post-load observations:
merges had already reduced active parts, so their plans do not reconstruct every
earlier timed execution.

The ID content-length query does not fetch the content. A stronger readback
regenerates **1,743 complete signed events across 988 authors**, includes every
source-file boundary and random ordinals, and compares all signed fields/tags.
All four variants produce the same complete-result SHA256:
`b5f349b5031930a57a7990922474f9507e357afcfe3934fad64c5855d8cdd50e`.
Every sampled row passes independent canonical ID/Schnorr verification; a
same-length corruption fails. Full population checks cover row/unique-ID counts,
the complete ordinal domain and kind counts. A modular-time oracle independently
checks the newest 100 including timestamp ties. This is not cryptographic
verification of every one of the 20 million rows.

| Full signed sample fetch, one measured batch | Time |
|---|---:|
| DuckLake default layout | 238.18 s |
| DuckLake explicit layout | 193.30 s |
| CH ID-first | 3.84 s |
| CH chronological-first | 3.73 s |

The distributed sample forces broad archive-file access. This result supports
retaining CH for this product's frequent online reads; it does not establish a
universal ratio, single-event tail latency, real concurrent QPS or a winner for
every query. An occasional offline scan/export product could still prefer only
DuckLake/DuckDB. Chronological-first is a promising CH candidate, not frozen
production DDL from three queries and one bulk-load order.

## Recovery evidence and its limits

Recovery reports are kept separately from load timings. The archive drill copies
all data referenced by the quiescent lab catalog, including both alternative
archive layouts, then restores a fresh PG database and reads only remapped backup
paths. Its scope is catalog/file restore on the same device, not off-host safety
or BigBrotr's future cross-store causal recovery. The first attempt failed before
any copying because `docker top -eo args` omits a PID column required by Docker;
the corrected fresh run uses `pid,args`. Failure evidence is retained.

The completed `archive02` run copies and verifies **557 files / 215,866,532,029
bytes**, with equal source/destination SHA256 for every file. Copy plus both hash
passes take 538.07 s. The catalog is restored into a fresh PG database; the reader
requires all event-file paths to point inside the backup root. All four restored
tables pass population checks and 261 regenerated full signed samples each;
matching layouts produce equal sample hashes. Narrow inlined receipts survive
through the catalog rather than a separately copied receipt Parquet file.

The smaller transaction drill independently verifies every signed event in one
source file, kills actual callers before/after archive commit, resolves their PG
sessions, checks complete body/tag/receipt families, forces physical CH replay
and resolves a lost frontier-commit reply. Its optional workload has one writer
and two readers for 600 s per engine. It is not simultaneous ingestion/graph/search/
provenance/backup fleet qualification; completion and missed work remain reported
instead of being converted into an SLO claim.

Completed transaction cases retain the following exact observations:

| Injected boundary | Actual fresh-reader result |
|---|---|
| Kill caller before DuckLake COMMIT | Zero bodies, tags and receipts; old identified PG sessions gone |
| Kill after COMMIT before caller receives outcome | 25,000 complete bodies, 75,000 ordered tag rows and one content-bound receipt; old PG sessions gone |
| CH body family present, tag family absent | Incompleteness detected; active PG frontier stays zero |
| Complete CH families, replay with insert deduplication disabled | 50,000 physical bodies/150,000 tags, but 25,000/75,000 exact logical records |
| Kill caller after PG frontier commit before response | New connection resolves the committed generation and source digest |

The first composite run `recmixed01` remains **FAILED**: its later CH mixed
reader query shadowed stored binary `id` with a hex-output alias. Both readers
failed their first full-body assertion. The completed DuckLake mixed case has
600 writer and 1,200 reader operations over 600 s, zero missed slots/errors, and
25,000 logical/49,600 physical bodies after 24,600 deliberately replayed rows.
The CH writer was interrupted after 176 completed operations rather than waste
the rest of the invalid test. Its old raw `not_started=0` is another harness bug:
600 scheduled slots minus 176 started leaves 424 never started. Failed readers
each started one of 600 scheduled slots and completed none. These are interrupted
schedule counts, not 600 actually issued requests per worker.

A real-engine countercheck reproduces the wrong query's missing body and verifies
the corrected qualified-column query against complete signed values. The fixed
worker recomputes interrupted accounting in `finally`; an independent injected
interruption distinguishes the old and fixed behavior. The separately named
CH-only repeat preserves the original failure and does not rerun or relabel the
already completed DuckLake/crash cases.

That corrected `recmixed02ch` run completes **600 writer operations and 1,200
full-body reads over 600 s**, with zero missed slots/errors. Its final oracle finds
25,000 logical/49,600 physical bodies, including 24,600 added replay rows, matching
the intended offered work. Together with the separate DuckLake case this exercises
the stated fixed low offered rate; it does not locate saturation, establish maximum
throughput or supply a whole-stack simultaneous workload result.

Native CH backup/restore of the separate 100,000-row calibration table also
passes its population oracle and 259 regenerated signed samples in both source
and fresh restored tables. The graceful CH stop/start then rechecks both original
20M-row tables: population and all 1,743 signed samples match the initial hashes.
The corrected restart reaches health in 2.69 s; stop/start plus verification takes
14.02 s in this run. These timings do not measure disaster recovery or a copied
20M-row CH restore. The first restart attempt stopped/started cleanly but its
harness compared Docker's unordered mount list as an ordered array, so it stopped
before readback; that FAILED record is retained. Fresh run `02` compares the exact
mount tuples independent of order and completes the data checks.

Process restart cannot establish power-loss behavior. An upstream confirmed
[projection fsync issue](https://github.com/ClickHouse/ClickHouse/issues/111318)
and its still-open [proposed fix](https://github.com/ClickHouse/ClickHouse/pull/111392)
were inspected during planning. This lab has not reproduced power loss or
established the issue's exact applicability to 26.5.1.882. P02-T05 explicitly
qualifies projection health/rebuild and correct fallback/refusal on its release;
an active PG frontier cannot certify an unavailable or damaged projection.

## Interpretation for the implementation agent

Retain Rust/Tokio, synchronized exact admission, PG control/catalog ownership,
DuckLake archive and CH online bodies. The result narrows storage uncertainty;
it does not prove Rust beats Go, finalize all DDL, qualify the selected Rust
bindings, or remove the need for a complete signed event→archive→read→export→
restart product slice. Do not reuse these simplified tables as production schema.

Both worker and CH cgroups reached their configured memory ceilings through
resident/cache accounting during this series, with recorded pressure events and
no recorded OOM kill at the live inventory capture. Reporting only 7 GB query memory
would understate the exercised envelope. Captures are cumulative observations,
not a continuous isolated per-operation profile. The backup copier is a separate
host process, with one stream and bounded application buffers; its OS cache/I/O
is not charged to the worker cgroup.

At final inspection all three owned containers are stopped, with eight unrelated
containers still running. PG/CH exited normally; Docker terminated the idle
worker's PID1 `sleep infinity` after its shutdown timeout (exit 137, OOMKilled
false), after all experiment children had exited. This final idle-container stop
is not a failed workload or an injected persistence test. The source data, engine
directories, backups, failed reports and frozen sources remain under the owned
lab path; the filesystem has 405,335,126,016 free bytes at that capture. Stopping
the engines releases runtime use, not retained disk space.

P02/P03/P05/P06/P08 still owe actual adapters, aged exact provenance inventory,
mixed useful fleet work, search/relevance, graph construction/convergence and
maintenance/recovery debt under the fixed workload contract. No 5k, 10k, 50k or 100k
busy-relay target is certified by these storage slices. Failed qualifying gates
must change the affected choice or supported envelope, not the truth of the
test or its required output.
