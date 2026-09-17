# Tux: object-storage placement and incremental-view boundaries

Date: 2026-09-17. Isolated research on actual engines, not product implementation.
The [programs](../../experiments/object-storage/README.md) and
[evidence](evidence/object-storage-20260917.json) preserve versions, inputs,
source/report hashes, counterchecks and limits. The legacy repository is read only.

## Question and placement decision

Can the selected PG-catalog/DuckLake archive place its files on a real S3-compatible
server and be read without sharing local archive files? What breaks when credentials,
objects or a writing process fail? This tests an optional storage route, not the
case for adding an object server to every installation.

Select local files for a simple single-host deployment. An external S3 endpoint
can offload archive capacity or support archive-byte consumers on different hosts.
A collector/gateway split alone does not require S3: the local archive and its
workers can remain on the data host. Docker packaging does not establish shared
data authority, failover or an independent failure domain.

The lab uses Garage 2.3.0 as an independently maintained S3 compatibility candidate,
not a mandatory distribution dependency. Its single node has no redundant data
protection. The production contract requires actual provider/version qualification.
The [official Garage quick start](https://garagehq.deuxfleurs.fr/documentation/quick-start/)
documents this single-node limit. MinIO Community's
[official repository](https://github.com/minio/minio) is archived as of 2026-04-25;
familiarity is not a reason to introduce an unmaintained default dependency.

## Actual signed-body slice

The input consists of the first 16 source files from the prior signed corpus:
**1,048,576 unique events**, each with at least 8,192 content bytes, Unicode and
ordered tags. This is deterministic synthetic wide content, not a measured Nostr
distribution. Input file hashes are recorded. The source fixture directory is
mounted read only; previous engine data and backups are untouched.

DuckDB 1.5.5 loaded local and S3 layouts sequentially with explicit ZSTD3,
8,192-row groups, 256 MB target files and data inlining disabled. Each of four
transactions inserts 262,144 events plus one receipt into the same DuckLake
catalog transaction. Installed extensions identify DuckLake `d8a1881e`, HTTPFS
`827222f` and Postgres scanner `41223e5`.

| Capture | Local files | S3 / Garage |
|---|---:|---:|
| Event rows and distinct IDs | 1,048,576 | 1,048,576 |
| Event files | 17 | 16 |
| Event file bytes | 4,153,651,472 | 4,153,651,207 |
| Summed insert/commit calls | 13.979 s | 24.755 s |
| Whole-population and 132 signed-sample verification | 1.270 s | 4.225 s |

Population oracles include exact count, distinct IDs/ordinals, sum and bounds.
All 132 regenerated complete sampled rows match every signed field and pass
canonical-ID/Schnorr verification. Both layouts return sample SHA-256
`2ab39efad5c9d706314883caf1d51d5d6fe9cdfc0740b4c08b1b5c0195ac4239`.
The S3 closure contains 20 objects/4,153,652,511 bytes including four receipts.

These are single ordered runs with uncontrolled shared-host caches. They do not
establish a latency ratio, equal hardware durability, cold-cache behavior, WAN
throughput, cloud cost or a production row/file-size choice. Native memory, metadata,
WAL, Garage blocks and backups are additional physical costs.

A second container has no local archive, input corpus or Garage data mount. It
reads the remote catalog/object route with the restricted reader role/key and
passes the same population and signed-sample checks, plus receipt count/total.
Its object PUT attempt returns 403 AccessDenied. The captured mounts substantiate
that placement. PostgreSQL trust authentication is restricted to loopback in this
lab: this is **not** proof of authenticated PG isolation, TLS or another-host reachability.

## Failures and same-host restore

| Executed case | Result and exact scope |
|---|---|
| Invalid S3 signing secret | GET denied with 403; no empty-success interpretation |
| SIGKILL after event/receipt INSERT, before COMMIT | Child exited; its PG sessions disappeared before resolution; no added visible body or receipt |
| Remove one committed event object | A fresh content scan fails instead of reporting a complete empty/missing result; restore the saved object and signed readback passes |
| Catalog backup into another PG database plus object closure at another prefix | 20 files copied; source/destination SHA-256 equal, 4,153,652,511 bytes, 15.649 s including copy/hash and readback; population, signatures and receipt totals match |

The copy is on the same host/device and retains access to the source bucket.
The initial restore checks destination event paths and actual receipts; the
supplemental boundary probe strengthens all-family path/closure comparisons.
Quiescence was established by the coordinator's sequential lab execution, not by
a reusable distributed backup lock in `copy_catalog.py`. The precommit kill is
not an interrupted-upload or ambiguous-COMMIT test. Those broader cases remain
P02/P08 acceptance, alongside source-inaccessible restore and independent-host loss.

An independent code review caught weak experimental oracles: the first multipart
payload repeated every 256 bytes, LIST membership was recorded without comparison,
and first scripts did not consistently guard/rewrite failure reports. The initial
results retain their exact scope. A separately identified boundary run uses
position-dependent bytes, full/cross-part/end readback, explicit membership and
permission checks; its report is separately included in the evidence. No corrected
source is retroactively attributed to an earlier execution.

The initial reader provisioning command also failed because a CLI output parser
expected exactly one space after a label. Its correction accepts whitespace and
explicitly resolves the already-created key. No credential values are tracked.
This was a harness failure, not a measured database fault or an implicit second key.

## Publication design correction

The native engine writes files and commits its own catalog membership. Source
inspection of the installed DuckLake revision confirms path/row/file-size/footer
metadata without an application SHA field in the examined path. The
[archive contract](../specification/archive-storage-contract.md) now states this
explicitly instead of implying precomputed checksums around native INSERT.

An initially proposed mandatory whole-file S3 readback was rejected after independent
review: computing the first SHA after writing cannot by itself establish persistence,
and adds GET/egress. Reclamation requires the resolved semantic receipt, complete
dependencies and a qualified native persistence path for the promised failure
class. File-hash capture, semantic readback and verified backup/scrub remain explicit
separate evidence. Unknown persistence retains pending data; a hash does not upgrade it.

## Incremental ClickHouse views

The separate [real-engine probe](../../experiments/relay-composition/ch_materialized.py)
tests three small counterexamples on a new bounded CH container: replay despite
source replacement, right-side JOIN changes and clock-only expiry. It does not
benchmark query speed or reject materialized views generally. Exact outcomes are
recorded in the evidence and linked analytical contract: one logical source row
versus naive count two after replay, a stale pre-change join label, and count one
versus current eligible count zero after expiry. Native views can accelerate
qualified additive paths; they do not replace replay identity, retraction state,
temporal invalidation or coherent target publication.

## Resource and release boundary

The owned root is `/home/tux/bigbrotr-object-storage-20260917`. Main quotas are
worker 4 CPUs/10 GiB, Garage 2 CPUs/4 GiB and PG 1 CPU/1 GiB. The separate reader
has 2 CPUs/8 GiB and runs after loading; the extra CH counterexample has 2 CPUs/
3 GiB and is created after that reader exits. Idle worker/container overhead is
still counted. Shared host I/O/cache and background applications are uncontrolled.
Generated files use the persistent data filesystem, not `/tmp` tmpfs.

All five owned containers are stopped. Garage, PG, reader and CH exited zero;
the already-idle worker's sleep PID1 required the Docker stop timeout (exit137,
OOMKilled=false), after experiment children had completed. The worker cgroup peak
reached 10,738,454,528 bytes with 2,274 memory-limit events and no OOM kill. This
includes file cache and documents pressure against the 10 GiB cap; it is not peak
process RSS or evidence of spare capacity. Docker's recorded default swap
allowances also prevent a RAM-only performance interpretation.

Final resource states, retained paths, package/image/extension digests and
396,239,507,456 bytes of available disk are in the evidence/lab inventory. No global cache flush, unrelated
container restart or host reboot was used. The results qualify these narrow
engine interactions only. They do not qualify Rust adapters, the complete
multi-family schema, sustained 5k–100k busy-relay work, S3 pricing, a distributed
Garage cluster, a real cloud provider or power-loss durability.
