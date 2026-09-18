# Authoritative job cut regression (MON-009)

`job_bounds_cases.py` uses an inventoried running `lab.py` PostgreSQL16.15
laboratory and its actual BigBrotr executable. Synthetic IDs, digests and cuts
exercise relational constraints only; no source receipt or admission is certified.
Run from the clean selected Git checkout, with a canonical UID-owned0700 root:

```sh
python3 experiments/control-migrations/job_bounds_cases.py --root /owned/runtime --run-name negative --mode negative
python3 experiments/control-migrations/job_bounds_cases.py --root /owned/runtime --run-name corrected --mode qualify
```

The negative mode applies original migrations1/2 through the executable, checks
their exact hashes, then independently observes both invalid states: closure5
before job6, and job6 before closure5, with old topology handoffs still NULL.
`COUNTEREXAMPLES_REPRODUCED` confirms the defect, not safe application behavior.

Qualification requires a binary containing migration8. Every case creates a
fresh database; resources or results are never reset. It exercises eligible
populated upgrade/reapply, whole-schema/data rollback of incompatible upgrade,
closed5 with job0/5/6, job5 with closure4/5/6, both lock acquisition orders with
commit and rollback and both valid/invalid limits, two-stream completeness and
zero closure, whole-transaction rollback, and explicit unsupported-isolation
refusals. Literal expected source/job/bound/topology rows are read independently.
All-row/schema pg_dump hashes protect negative cases; random psql restrict tokens
are the only dump bytes excluded. Migration receipts use independent file hashes.

The existing bounded psql Session helper observes exact SQLSTATEs. Concurrency
requires pg_locks/pg_blocking_pids and both backend incarnations; time alone cannot
pass. Each case positively cleans its owned sessions. All reports and databases
remain for diagnosis; lab.stop is separate and checks ownership. No trigger is
disabled, no incorrect row repaired, and no expectation changes after execution.
An unfinished report or unknown cleanup cannot qualify the migration.

Native correction execution is pending until linked implementation evidence
records exact source/binary/engine and results. This is not full job execution,
physical durability, production load or M1 qualification.
