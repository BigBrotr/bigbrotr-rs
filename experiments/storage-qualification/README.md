# Large storage and recovery qualification slices

This directory contains **research programs**, not BigBrotr services, migrations,
adapters or a second implementation. They compare bounded storage representations
and exercise selected real-engine recovery boundaries. They never contact public
relays. Their complete signed rows are deliberately simpler than the production
payload/eligibility/provenance schema; production interface parity is not claimed.

The owned Tux root is `/home/tux/bigbrotr-qualification-20260917`, mounted as
`/lab` in an isolated worker. Generated data, engine volumes, logs, downloads and
backups stay there. Nothing writes the neighboring legacy checkout. Never point
these scripts at an existing application database or reuse an evidence run name.

## Environment and reproducible sequence

The recorded run uses Rust 1.96.1 to build the small fixture generator with its
tracked Cargo.lock, Python 3.11 and `requirements.txt`, DuckDB 1.5.5 with separately
hashed DuckLake/Postgres extensions, PostgreSQL 16 and ClickHouse 26.5.1.882.
`inventory.py` records exact container image digests, extension/source identities,
host capacity and actual cgroup counters. Extension installation in `engines.py`
is research-only; the future product must use its verified offline bundle.

The owned containers and starting grants are:

| Name suffix after `bigbrotr-qualification-` | Image digest | Grant and local port |
|---|---|---|
| `worker-20260917` | `python@sha256:1042b61448fef4ba92d16a8c7eb4996d027568ce64792a7877fd88511e0af7c6` | 8 CPUs, 20 GiB including cache, no extra swap, host network, UID1000, lab mounted `/lab` |
| `pg-20260917` | `postgres@sha256:f1c3376c26f2609ab9f29f71f824103fe2fcd8ee0346485cb6122a4f93df6f94` | 1 CPU, 1 GiB, shared_buffers=128MB; host 127.0.0.1:48632, fresh DB `qualification` |
| `ch-20260917` | `clickhouse/clickhouse-server@sha256:07afc18d8a9706eb9d85c5c5d2752e5270f91bbc2894caeaecb73e4d0f603bf5` | 8 CPUs, 12 GiB/no extra swap, UID1000, nofile=131072; host 127.0.0.1:48623 |

PG trust and CH's skipped user setup are restricted to this loopback-only lab;
they are not deployment recipes. CH mounts `ch-data`, `ch-logs`, `backups` and
read-only `input` at `/var/lib/clickhouse`, `/var/log/clickhouse-server`, `/backups`
and `/var/lib/clickhouse/user_files/input`. Its tested extra configuration is:

```xml
<clickhouse>
  <max_server_memory_usage>10000000000</max_server_memory_usage>
  <max_thread_pool_size>1024</max_thread_pool_size>
  <background_pool_size>16</background_pool_size>
  <background_schedule_pool_size>4</background_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <backups><allowed_path>/backups/</allowed_path></backups>
  <logger><level>warning</level></logger>
</clickhouse>
```

Create the worker's venv **inside its image**, not with the host's different
Python. Build the locked generator for that environment, then from `/lab` run:

```sh
venv/bin/python src/storage-qualification/generate.py --binary src/storage-qualification/fixture/target/release/bigbrotr-research-fixture --directory input/calibration --rows 100000
venv/bin/python src/storage-qualification/generate.py --binary src/storage-qualification/fixture/target/release/bigbrotr-research-fixture --directory input/aged --rows 20000000
venv/bin/python src/storage-qualification/engines.py --corpus aged --engine lake
venv/bin/python src/storage-qualification/engines.py --corpus aged --engine id
venv/bin/python src/storage-qualification/engines.py --corpus aged --engine time
venv/bin/python src/storage-qualification/engines.py --corpus aged --engine lake_zstd
venv/bin/python src/storage-qualification/verify.py --corpus aged --include-zstd --hash-inputs
```

Run all four engine variants on calibration first. The actual run generated the
aged corpus while calibration was checked, then ran aged variants sequentially.
The shared host's other workloads/cache were not controlled. Do not run another
engine workload concurrently and then present its latency as the sequential case.
The table and evidence files refuse silent reuse. `--queries-only` explicitly
archives earlier evidence and starts a new status before queries.

The generator uses public fixture private keys 1–4096, a modular 90-day timestamp
distribution, Unicode/escaping and ordered tags. Content is at least 8192 bytes of
hash-rich material. This is a wide, high-entropy storage stress fixture, not a
sample of actual social content, network arrival, deletion/churn or search quality.
`aged` means a large resident corpus here; it does **not** mean weeks of aged
compaction/update debt. Do not generalize its compression or service rates to
billions of real events.

## What each program proves

* `generate.py`/`fixture`: deterministic event construction and bounded Parquet
  writing. Generation verifies sampled cryptography, not every generated event.
* `engines.py`: actual load, row count, receipt totals and bounded query timings.
  `id_content_length` reads byte length, **not the full content**. Early reports
  called that case `id_body`; their label does not change the exercised query.
* `verify.py`: full population count, unique ID and exact ordinal-domain checks,
  kind counts, independently derived chronological order, and regenerated signed
  full-field samples spread across file boundaries and authors. Same-length
  content corruption is rejected. This is not full-corpus crypto validation.
* `backup.py`/`restored_verify.py`: quiescent copy/hash of all lab archive files,
  consistent PG dump, fresh catalog restore, remapped backup-file paths and actual
  reads of all four event tables plus their receipts. There is no deletion or
  in-place overwrite. The caller must establish quiescence; the baseline marker
  alone is not a general distributed fencing mechanism.
* [`recovery.py`](README-recovery.md): killed callers before/after archive commit,
  PG session termination checks, complete body/tag/receipt oracles, partial CH
  families, physical replay and ambiguous frontier commit. Optional mixed work
  measures offered/missed operations. Its smaller file is fully crypto verified.
* `inventory.py`: nonsensitive owned-resource identities/counters at the named
  capture time; post-load capture is not reconstructed pre-run telemetry. Stopped
  resources are inventoried without restarting them; package versions then refer
  to the preceding live capture.
* `layout_inspection.py`: actual Parquet codec/row groups, narrow receipt storage
  and CH plans with both index and projection diagnostics.
* `ch_backup.py`: native backup/restore of the 100k-row calibration table under
  a fresh name, population checks and regenerated signed samples.
* `restart.py`: host-side graceful stop/start of only the owned CH container after
  all workloads finish, followed by both 20M-row CH population/sample checks. It
  checks container/image/mount identity and equality to the initial signed result.
  This is not a host crash, power failure or disk-loss test.
* `summarize.py`: condenses raw reports while requiring their exact tested scope;
  retains failed runs separately and rejects missing-engine/changed-result reports.

Backup runs on the host after successful baselines and verification, with no
archive writers or maintenance running. Create the marker only after inspecting
their successful exit/status at
`results/baselines-and-verification-complete`; then run:

```sh
python3 src/storage-qualification/backup.py --root /home/tux/bigbrotr-qualification-20260917
```

It reserves 120 GiB beyond the copy size, refuses an existing destination/database,
and records failure before returning a failed exit. It copies the whole lab
catalog's data directory, including more than one alternative representation.
This is not a production incremental/online backup, a host-loss drill or a test
of BigBrotr's multi-store producer-ledger causal closure. Files and the backup
are on the same device, so the retained copy is not off-host protection.

After the recovery/mixed work has fully exited, run `ch_backup.py` in the worker,
capture `inventory.py --root /home/tux/bigbrotr-qualification-20260917 --label before-stop`
on the host, then run host-side `restart.py` with that same root. The restart
program checks the recorded actual run identities (`recmixed02ch` and the native
CH backup); a reproduction in another fresh environment must deliberately name
its own evidence rather than fabricate these markers. Stop the three owned
containers only after final verification, and capture inventory with label `final`.
Never use blanket Docker stop/prune or shared-host reboot commands.

Collect reports under ignored `.research/storage-qualification-20260917/results/`.
The concise tracked report is produced by:

```sh
python3 experiments/storage-qualification/summarize.py --input .research/storage-qualification-20260917/results --output docs/research/evidence/storage-qualification-20260917.json
```

This collector records this particular series, including its failed first mixed
run and corrected CH-only follow-up. It is not a general benchmark suite that
silently accepts different run identities or smaller qualification scopes.
The actual corrected restart uses `--run-id 02`; its first record remains FAILED
because the original harness treated Docker mount-array order as meaningful.
Mount tuples are now sorted for semantic comparison, with exact source/path/RW
identity still required. Final engine readback and result hashes are in the
[completed research report](../../docs/research/tux-storage-qualification-20260917.md).

## Confounds and corrections retained in the evidence

The first DuckLake path deliberately used engine-default file options and wrote
Snappy. The explicit candidate changes codec to ZSTD3, row groups to 8192 and target
files to 256 MB, and inlines only narrow receipt rows. It is a combined layout
candidate, **not** an isolated codec benchmark. CH bodies use ZSTD3 with two base
orders and `_part_offset` projections. Active parts/files are different physical
metrics. CH `system.parts.bytes_on_disk` includes the child projection bytes;
`system.projection_parts` reports that component again, not an additional cost.
Count obsolete parts, receipts/catalog/WAL/temp/cache and backup space before
claiming total deployment cost. The report links the exact release's accounting.

Initial `engines.py` success checked counts/content lengths; use the later
preservation report for its stronger, still explicitly sampled fidelity scope.
An initial Unicode character/byte-length assertion and an incorrect DuckLake
file-size column failed calibration and were corrected. Initial sampling every
4096 rows aliased the 4096-key cycle; the retained final verifier uses independently
chosen ordinals plus every source-file boundary. New generation samples every 4093 rows.
Initial receipt digest hashed file **names**, not contents; it is now honestly
named `file_set_digest`. Only the separate recovery slice binds source-file bytes.

Generation source/binary hashes were captured before adding list-mode validation;
initial input manifests recorded sizes, with content hashes captured after loading.
The final checked-in harness includes these corrections, so do not attribute every
early timing to its exact current source bytes. Early logs and previous evidence
are retained. Initial setup also exposed a read-only mount chown failure and
an undersized CH global thread pool; final nonroot mounts/pool are stated above.

Main CH load timings use default fsync behavior. They are not equal-durable-ACK
comparisons with DuckLake and are not production throughput. Process restart or
caller kill is not power loss. Known upstream projection persistence concerns
remain an exact-release qualification gate. No test here qualifies Rust adapters,
fair 5k–100k busy relay fleets, full provenance/search/graph workload or the complete
BigBrotr deployment. Those remain explicit early implementation/release gates.
