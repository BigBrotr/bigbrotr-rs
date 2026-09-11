# Capacity, efficiency and evidence

## What has actually been measured

The September 10 independent lab exercised components on Tux: AMD Ryzen 9 7950X,
16 physical cores / 32 hardware threads, about 64 GiB RAM, local NVMe and a 1 Gbit
NIC. WAN capacity was not measured. These are reference resources, not a host
reservation: other applications run there. The September 11 tests used separate
bounded process groups and a disposable PostgreSQL container. No legacy project
or its containers were changed.

| Experiment | Observation | Practical limit |
|---|---|---|
| 5,000 loopback TLS websocket sessions, tuned Go | About 166.02 MiB client RSS | Synthetic ~2 KB message/s/session; no event parsing, signatures or storage |
| Same fixture, tuned Rust | About 145.73 MiB client RSS | Same limited scope; not a 5,000-live-relay certificate |
| Default buffers at 5,000 sessions | Go 214.75 MiB, Rust 751.06 MiB | Configuration dominated the naive language comparison |
| Graph kernel, 500k nodes / 4.545M edges | Go 71.84 ms, Rust 68.87 ms median | Same CSR synthetic graph, 18 iterations; excludes loading, graph construction and I/O |
| 200k events / 1M provenance edges | Compressed journal 132.97 MB; Parquet 107.78 MB | Synthetic distribution and fake signatures; decimal MB, not universal compression ratio |
| CH base and narrow indexes | 169.78 MB | Logical body plus author/tags/provenance structures; not archive replacement |
| CH added full text | 288.36 MB total | High-uniqueness synthetic text; index cost depends strongly on corpus |
| Alternative CH chronological layout | 111.04 MB; tag→100 bodies ~30.47 ms | Versus ~106.47 ms earlier layout; tiny warm dataset, not final DDL selection |
| LMDB / RocksDB fixture | 614.71 / 258.92 MB allocated | Generic layout, Python adapter; not optimized nostrdb/strfry or native engine ranking |
| Embedded DuckDB / CH HTTP reads | Different query families favor different paths | Transport, concurrency and engine paths differ; do not compare as equivalent API benchmarks |

Selected result files and the original bundle hash are in
[evidence](../research/evidence/prior-components/manifest.json). The full original
laboratory bundle remains separately retained; its path is local evidence, not a
publicly downloadable artifact. New reproducible probes live under `experiments/`.

Five thousand connections are plausible in both Go and Rust. Whether BigBrotr can
continuously synchronize those relays on Tux depends primarily on received bytes,
duplicate factor, signature work, relay behavior, historical backlog, indexes,
query load and maintenance. Ten thousand quiet sessions can cost less than a
hundred busy backfills. A hundred thousand endpoints require explicit scheduling
and likely multiple collectors; no extrapolation from a 5k loopback run certifies
that topology. A remote relay may also be unreachable, rate-limited or unwilling
to disclose history; extra local CPU cannot repair those conditions.

## A workload model instead of a relay-count slogan

Let D/s be delivered events, U/s unique events, P/s new provenance facts, B average
wire bytes, V verification CPU seconds/event, and Q query work. Network input is
approximately D × B plus protocol/TLS overhead; verification work is D × V before
safe cache reductions. Archive growth tracks U and P, not only D. Repeated body
deliveries still consume network and parsing even when storage deduplicates them.

At an illustrative 5,000 unique events/s, 432 million new events arrive per day.
Even 500 compressed bytes/event would mean 216 GB/day before provenance, indexes,
replicas, backups, WAL and maintenance overhead. An 8 TB disk would not provide
unlimited retention. This is arithmetic, not a measured Nostr rate. Measure the
actual unique-event rate before estimating months of retention.

Model total disk as canonical event archive + provenance/documents + online
bodies + indexes + identity inventory + pending log + graph artifacts + temporary
merge/compaction space + backups retained on the same device. A copy on the same
disk is not off-host protection. Model peak space, not just steady-state compressed
files. Expansion to 4/8 TB postpones exhaustion; it does not remove the need for
forecasts, retention/placement decisions and explicit capacity warnings.

## Efficiency choices that matter

Multiplex subscriptions on one endpoint/auth connection; reuse verified TLS and
DNS state within bounded validity; bound every buffer by bytes and count. Use
binary identifiers, interned registry IDs on internal edges, batched reads/writes
and compact columnar representations. Parse once at admission, share immutable
buffers where useful, and avoid repeating cryptography for exactly verified
payloads. Never optimize by treating an untrusted claimed ID as verification.

Discovery consumes new admitted records, not repeated full archive scans. Current
replaceable state changes only affected subjects. Facts update from deduplicated
deltas with periodic reconciliation. Rankers consume a pinned graph generation
and reuse warm-start vectors only when mathematically valid; periodically compare
to full recomputation. Approximate distinct counts, sketches and sampled ranking
have explicit error semantics and must not replace exact archival truth.

Do not admit unbounded all-pairs relay comparisons: R=100,000 implies almost five
billion pairs. The exact baseline uses comparable eligible scopes and sound size
pruning, then refuses work beyond its candidate/scan budget. A separately named
approximate mode may use sketches/buckets to generate candidates before exact
pair verification; it cannot claim the exact baseline's exhaustive candidate
coverage. Equal-size fleets expose the worst case rather than making it disappear. Clusters
are advisory; near-identical content does not mean two relay identities or their
future behavior are interchangeable. Global PageRank passes remain O(E); caching
one personalized vector per possible user can itself become infeasible. Cache
requested viewpoints with bounded lifetime and publish versioned inputs.

## Required workload programme before capacity claims

| Dimension | Required variation |
|---|---|
| Connections | 1k, 5k, 10k, 50k and 100k; larger counts include a separately justified distributed simulation |
| Activity | Idle, low-rate, skewed firehose, reconnect storms, slow peers |
| Duplication | Global duplicate fraction and event/relay fan-out varied independently |
| Event shape | Real signed samples plus adversarial large content/tags/timestamps |
| Acquisition | Live + backfill + safe resweeps + imports together; heterogeneous NIP support |
| Data age | Fresh and aged stores, many parts/partitions, maintenance debt, cold/warm caches |
| Readers | By ID, author, kind, tags, search, both provenance directions, counts, exports |
| Compute | Incremental derivation, full reconcile, graph construction and ranking |
| Failure | Kill, ambiguous ACK, unavailable PG/CH/archive, full disk, throttling and recovery |
| Topology | One host, split roles, network latency and owner loss |

Record versions, configuration, fixed fixture seed/manifest, host background load,
wall/CPU time, cgroup/RSS/PSS, socket/kernel memory, bytes, IOPS, queue age,
p50/p95/p99, correctness digest, warmup/repetitions and confidence/variance. Compare
Go and Rust with the same semantics, buffers, crypto/library path, durability,
batch size and resource limits. Component speed does not replace end-to-end tests.

Provisional engineering targets must be labeled as such: bounded memory under
sustained overload, no lost acknowledged data under the defined crash model,
no starvation of eligible endpoints, declared read-latency/query budgets and
replay capacity above steady admitted rate with headroom. Publish the measured
operating envelope per release; do not turn an unmeasured 50 ms or 5k-relay number
into a marketing guarantee.

## Billion-scale representation challenge

Design evaluation must include U in billions and P in tens/hundreds of billions.
Do not extrapolate allocated storage from a key's nominal byte width: RocksDB
prefix compression, value encoding, block indexes, bloom filters, compaction,
write amplification and dictionary state all matter. Neither assume compression
will rescue an oversized schema nor treat raw key width as an information-theory
lower bound.

Compare exact provenance admission layouts before freezing them: one compact key
per edge; event-grouped sorted endpoint IDs with aligned first-observation values;
compressed bitmap membership plus the required timestamp/receipt representation;
and bounded group/bucket layouts. Membership alone does not answer earliest
observation or preserve immutable correction receipts. Grouped values can reduce
bytes but rewrite too much as new relays arrive; merge operators and chunking need
idempotency/crash tests. Do not introduce a custom compressed database merely to
win a synthetic bytes-per-edge chart.

Use realistic sparse/dense and skewed event/relay fan-out, stable auth/observer
scopes, monotonically growing IDs, random late arrivals, correction rates and
resweeps. Report bytes per retained edge including all dictionaries and indexes,
RAM, lookup/read amplification, write amplification, compaction CPU/I/O and restore
time. The selected semantic contract is exact admission; its compact physical
layout remains evidence-driven. The same care applies to CH reverse provenance
orders, tag fan-out and graph adjacency encodings.

At extreme P, a single machine may be unable to provide full online indexes plus
all required archives and recovery reserve. The honest options are tiered serving,
more storage, sharding or more hosts—not undocumented dropped edges. Public query
budgets and asynchronous export jobs must prevent an unrestricted full-archive
query from consuming the entire shared machine. Maximum throughput is measured
inside an explicit fairness and latency envelope, not by starving every other role.

## Concrete network stress scenarios

For illustration only, assume every active relay delivers one 2,000-byte event
per second before protocol/TLS overhead. This is not a measured public-relay rate.

| Active relays | Deliveries/s | Payload ingress | Implication for one 1 Gbit/s NIC |
|---|---:|---:|---|
| 5,000 | 5,000 | 10 MB/s | Network arithmetic permits it; CPU/storage/queries still require tests |
| 10,000 | 10,000 | 20 MB/s | Same qualification; more active work and kernel state |
| 50,000 | 50,000 | 100 MB/s | Near the 125 MB/s theoretical line rate before overhead/other traffic |
| 100,000 | 100,000 | 200 MB/s | Exceeds that line rate regardless of Go/Rust or storage choice |

A single relay can exceed the assumed rate by orders of magnitude. Conversely,
quiet monitored endpoints need not deliver one event/s. The scheduler must expose
active sessions, monitoring cadence, delivery rates and historical backlog
separately. More CPU cores cannot make a 1 Gbit/s link carry 200 MB/s of payload.
Use faster networking or multiple collectors/links when that is the measured
limit, and route/shard admission so the network improvement does not merely move
the bottleneck into one owner.

Illustratively, 100 billion retained edges at 10 compressed bytes each would be
1 TB for one representation alone; at 30 bytes, 3 TB. These are sensitivity inputs,
not achieved encodings. Add both serving directions, exact admission inventory,
dictionaries, correction history, archive and recovery reserve before claiming
that an 8 TB host fits the workload. Publish measured bytes/edge for actual
fan-out distributions and scopes.

Representation research references: [Roaring format and implementations](https://roaringbitmap.org/)
and [Rust implementation](https://github.com/RoaringBitmap/roaring-rs). These identify
candidates, not a measured membership/metadata layout for BigBrotr. Explicit bounded
CPU pools can use [Rayon thread configuration](https://docs.rs/rayon/latest/rayon/struct.ThreadPoolBuilder.html);
pool size must fit the shared host envelope rather than defaulting each service
to all logical processors.

## Optional read indexes under the same host budget

Content search and server-side reconciliation are exercised together with normal
reads, not allocated another copy of the host's free memory. Record text postings/
positions, shared set summaries, ad-hoc filtered sets, active snapshot retention
and body-lookup cost separately. Do not benchmark a per-client full-set rebuild
and call its memory inherent to the Negentropy protocol. Conversely, count the
initial shared-index build and ongoing membership/expiry maintenance; cached
fingerprints are not free to produce. See the
[search/reconciliation scenarios](../architecture/search-and-reconciliation.md).

Monitor volume has its own attempt/check/document-change dimensions. Compare
compact attempt rows plus content-addressed document references against repeated
wide document payloads. Vary unchanged catalog periods, churn, retries, route
count and raw-retention/aggregate policies. Retained metadata cost and reference
pins belong in the same disk forecast as event/provenance growth.

Keep provenance cardinality `P` distinct from social graph vertices `V`, current
edges `E`, historical input tags `H` and concurrent viewpoints `K`. The
[graph capacity contract](../architecture/analytics-detail.md#graph-state-is-different-from-provenance-state)
accounts for adjacency, vectors, rebuilds and global iterations independently.
Benchmarks must cover full graph construction, loading, normalization and artifact
publication under competing work, not just a warm numerical kernel.

## Permanent measurement sensitivity

The selected compact raw-measurement archive has no automatic age expiry. With
all endpoints actually measured every five minutes, R endpoints produce 288R
attempts/day, before independent catalog/probe families. At a purely illustrative
300 compressed bytes/attempt, 5,000 endpoints produce 432 MB/day; 100,000 produce
8.64 GB/day, approximately 3.15 TB/year for this one representation. This is
arithmetic, not measured row size, achieved cadence or a recommendation to change
retention. Add documents, online indexes, pending intent/terminal writes, copies
and maintenance. Unexecuted queue entries do not count as measured attempts.

P02-T09 and P04-T12 measure actual bytes/attempt and achieved service intervals.
The real tiny [Parquet probe](../../experiments/archive-model/README.md) found
that split objects/references can be larger than already compressed wide rows;
representation choices must include file/dictionary overhead and wider workloads.
On finite resources, adjust explicitly configured acquisition cadence/placement,
add capacity or select an explicitly lossy research policy; do not hide deletion
or missed measurements behind a successful monitor status.
