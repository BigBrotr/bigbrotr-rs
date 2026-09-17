# Bounded recovery slice

`recovery.py` is an isolated experiment, not a BigBrotr adapter. It ran on the
owned Tux qualification engines on 2026-09-17 after the aged baselines and a
successful quiescent archive backup/restore. The
[recorded evidence](../../docs/research/evidence/storage-recovery-qualification.json)
combines named successful checks while preserving the failed first attempt.
Its `--self-test` alone exercises only the row oracle and resource arithmetic.

Prerequisites: the pinned dependencies in `requirements.txt`, including
`psycopg2-binary==2.9.10`; existing DuckDB/DuckLake/Postgres extensions under
`/lab/extensions`; the qualification PG/CH endpoints; a manifest-listed Parquet
file with at most 65,536 rows. No extension installation/download occurs here.
Use the already recorded source/engine/container identities and grants. Do not
run concurrent with the aged baselines if interpreting latency or load costs.

```sh
python3 recovery.py --self-test
python3 recovery.py --root /lab --corpus calibration --run-id recovery01
python3 recovery.py --root /lab --corpus calibration --run-id mixed01 --mixed
python3 recovery.py --root /lab --corpus calibration --run-id mixed10m --mixed --mixed-duration 600
python3 recovery.py --root /lab --corpus calibration --run-id ch10m --ch-mixed-only --mixed-duration 600
```

Each invocation needs a fresh run ID. `--file` selects one existing manifest
basename; otherwise the first listed file is used. Defaults match the existing
qualification ports 48632/48623. `--pg-dsn` and `--ch-url` may explicitly select
the owned fixture engines. Do not target another workload's database. Reports
avoid storing the DSN. Source files and existing 20M-row tables are untouched.

All writes use `qualification_recovery_<run-id>`: a DuckLake schema, ClickHouse
database and PG control schema, plus job-local DuckDB temporary directories and
the matching report under `/lab/results/`. Namespace reuse refuses instead of
silently resuming a different experiment. There is no DROP/DELETE, automatic
cleanup, engine-server restart, whole-host action or failure injection into the
aged tables. An interrupted run leaves resources for inspection; a new run uses
a new namespace. Root may separately stage a backup and restart its owned CH
server after all aged tests; that is not an action or proof of this script.
`--ch-mixed-only` creates only a fresh CH database/table and executes that selected
mixed slice; it does not repeat the archive, crash or control-frontier cases.

The independent source pass reads one file in 256-row batches, hashes its bytes,
verifies **every row in that file** with canonical Nostr SHA-256 and libsecp256k1,
and records exact full-field and ordered-tag digests. It retains digest maps and
16 small sampled bodies, not the entire wide corpus in RAM. This stronger oracle
is bounded to the selected file; it does not upgrade the 20M corpus to full
signature verification. Full engine outputs stream through that source oracle.
Physical identical CH copies are counted separately from logical identities.

Required cuts:

1. A child inserts complete bodies, ordered tag family and a receipt in one real
   DuckLake transaction, signals **before COMMIT**, then is killed by its parent.
   The parent proves that exact child exited and its identified PG sessions ended;
   only then does a fresh connection require all families and receipt absent.
2. The same child operation signals **after COMMIT but before caller receipt**,
   then is killed. A fresh connection requires exact bodies/tags and exactly one
   matching content-bound receipt. No duplicate retry is performed after finding
   that receipt. Unknown lingering transaction/session state fails the run without
   authorizing another attempt.
3. CH bodies arrive while tags are absent. Full family verification must fail and
   PG active frontier remains zero. Tags then complete. Identical body/tag replay
   with `insert_deduplicate=0` must produce two physical copies per source record
   while the logical exact result remains unchanged. This tests replay independent
   of token TTL, not time-travel through a real deduplication window.
4. Only after complete family verification does a child CAS/commit the dedicated
   PG frontier to one. It is killed before delivering the parent receipt; a new
   PG connection resolves the exact source digest and generation.
5. Same-length content corruption must fail the full-field oracle. Missing and
   duplicate rows are also negative controls in `--self-test`.

The CH test uses plain raw families with explicit fsync settings; it does **not**
use projections or qualify the upstream projection-fsync issue. HTTP reads parse
every JSON row; commands require a fully empty success body and check exception
headers. The script never treats timeout/HTTP 200 alone as authoritative success
or automatically retries an ambiguous write. Child boundary acknowledgements are
test synchronization, not simulated database commits.

Optional mixed work runs DuckLake and CH sequentially. Per engine, one writer has
3,000,000,000 bytes/2 threads and two readers have 1,500,000,000 bytes/1 thread each:
6,000,000,000 bytes aggregate configured query/engine memory, below 7 GB. Parent
Python buffers, engine baseline memory, OS cache and native overshoot still count
against the recorded worker/server cgroup grants; this is no assertion that
native memory equals these limits. Each reader offers one full-body lookup/sec;
the writer offers one bounded source subset/sec. Queries target a new mixed table
inside the owned namespace. The writer adds at most one file's rows total,
independent of duration; no unlimited replay disk growth occurs. Final verification
checks every distinct body and exact physical count against completed inserts.

The default 60 s is calibration; `--mixed-duration 600` selects the ten-minute
slice. Reports retain offered/started/completed/not-started operations, queue and
service times, errors and written rows, including missed scheduled work. Successful
completion of the correctness slice is not an SLO or capacity pass. No synthetic
tick is converted into a throughput claim; cache condition remains uncontrolled.
Filesystem use includes source bytes, new body/tag/mixed copies, native spill and
engine/WAL overhead. Provision a declared grant and retain the engine's recovery
reserve; never fill the shared root filesystem for this experiment.

Observed runs used one 25,000-row calibration file (98,994,244 Parquet bytes).
Every row in that file passed canonical-ID/signature verification; complete bodies
and 75,000 ordered tags were compared against source digests. DuckDB was 1.5.5,
DuckLake extension `d8a1881e`, PostgreSQL scanner extension `41223e5`, and CH
26.5.1.882. Exact source hashes, raw reports, gates and frozen scripts are retained
under `.research/storage-recovery-qualification/` and the owned Tux lab's `results/`.

| Run / selected phase | Observed result |
|---|---|
| `recmixed01` recovery cuts | Pre-commit worker kill: zero bodies/tags/receipts. Post-commit kill: 25,000 exact bodies, 75,000 exact ordered tags, one matching receipt. Old PG sessions absent before resolution. CH missing-family negative control rejected; dedup-disabled replay retained 25,000/75,000 logical records with 50,000/150,000 physical copies. Committed PG activation resolved after caller-response loss. |
| `recmixed01` DuckLake mixed | 600 seconds; each of three roles completed all 600 scheduled operations, zero not-started slots and errors. Writer added 24,600 rows; full oracle found 25,000 logical / 49,600 physical bodies. |
| `recmixed01` CH mixed | **FAILED**, retained unchanged. Each reader failed its first lookup because an unqualified predicate referred to the hexadecimal output alias. Writer was interrupted before the deadline after 176 completed inserts; this composite run is not reported as PASS. |
| `recmixed02ch` CH mixed | Corrected predicate uses `stored.id`; fresh namespace and source hash. 600 seconds; each of three roles completed all 600 scheduled operations, zero not-started slots and errors. Writer added 24,600 rows; full oracle found 25,000 logical / 49,600 physical bodies. Process exit 0; no recovery workers remained. |

The raw field `offered=600` means 600 **scheduled** slots per role. In the failed
CH phase, both readers have started=1, completed=0 and not_started=599. The writer
has started=completed=176; 424 scheduled slots were never reached because it was
interrupted before the deadline. Its old raw not_started=0 is an accounting bug
preserved in the evidence, with the arithmetic correction labeled separately.
The fixed worker sets this counter unconditionally in its cleanup path. An
injected interruption before the first operation reproduced old=0 versus fixed=600
without engine calls. A separate real-CH countercheck rejected the original query
and accepted the exact full signed body with the corrected source-column predicate.

Each successful mixed slice offered one 41-row insert/sec and two total
lookups/sec over 16 known bodies. Observed per-role service-time p95 values were
55.85/164.60/163.51 ms for DuckLake writer/reader-a/reader-b and
66.05/22.47/22.87 ms for CH, using nearest-rank percentiles of 600 completions each.
These are descriptive observations of fresh tables with uncontrolled cache, on
the recorded worker 20 GiB and CH 12 GiB cgroups. They do not qualify the service
SLOs, full `mixed-v1` workload, 5k–100k socket fleet, aged-table concurrency,
production visibility, graph/jobs, or the reference host's total resource budget.
No recovery test restarted a server; all owned namespaces remain available.

Reports start RUNNING, become FAILED on an exception, and become
`PASS_bounded_engine_recovery_slice` only after every selected case finishes.
The CH-only mode reports `PASS_bounded_ch_mixed_slice` for that narrower scope.
No product producer retirement, positive host fencing, power loss, hardware
persistence, multi-family production schema, production visibility or restore
closure is claimed. Those have their own implementing acceptance boundaries.
