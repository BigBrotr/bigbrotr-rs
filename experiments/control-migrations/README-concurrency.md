# Actual PostgreSQL control transaction races

Status: **NOT_RUN** on PostgreSQL. Local preparation checks Python syntax, the
bounded subprocess response reader and independent row-oracle negative controls.
These checks do not execute a PostgreSQL transaction or qualify a race. Record
native execution separately with its exact harness, application and engine.

`concurrency_cases.py` imports the adjacent `lab.py` and requires an existing,
running owned PostgreSQL 16.15 laboratory. It creates one new database through the
observer, applies `0001` using the actual BigBrotr executable, and opens dedicated
`bigbrotr_migrator` psql sessions on that container's explicit Unix socket. This
uses the laboratory's isolated socket trust rule; it does not test authentication
or propose a production connection policy. The final actual CLI inspection uses
its configured SCRAM route. Run this alone while other lab actions are stopped.

```sh
python3 experiments/control-migrations/concurrency_cases.py \
  --root /absolute/owned/laboratory-root --run-name races_first
```

The canonical root must be owned by the executing UID with mode 0700. The name
contains 1–20 lowercase letters/underscores. The source checkout must be clean.
The imported helper must belong to that same checkout. Container labels, exact
container IDs/running state, mounted binary SHA-256, PostgreSQL version and
`fsync`/`full_page_writes` are checked. The report records source revisions,
source hashes, live image/container identities and live server identity. Existing
databases, configuration files and run directories are refused, never reset.
No container, network, port, HBA, server setting or volume is created or altered.

## Independent cases and observations

The fixture uses READ COMMITTED and synchronous commit. Two contending sessions
are open at a time. A third independent observer reads `pg_stat_activity`,
`pg_locks` and `pg_blocking_pids()`. Each required lock barrier must show the
recorded waiter incarnation actually waiting on a lock with the exact holder PID
and backend start time. Elapsed time or a slow command alone cannot pass.

| Case | Ordering and independent durable expectation |
|---|---|
| `revision_cas` | Both transactions first read head 1. A performs expected-head CAS to 2 and inserts revision 2. B's same CAS waits for A, then affects exactly zero rows after A commits; B rolls back. Exactly history `[1,2]` and head 2 remain. |
| `owner_acquisition` | Two transactions insert epoch 2 with distinct process incarnations after the same released epoch 1. A holds the predecessor lock; B demonstrably waits. A commits; B receives exactly SQLSTATE `23505` and rolls back. Only A's active epoch 2 survives. |
| `topology_uncommitted_parent` | A creates generation 1, adds shard 0 with explicit closed bound 0, and seals it without committing. The observer sees no generation 1. B's append receives exactly `P0002` because the parent is not visible, then rolls back. A commits the exact complete one-member topology. |
| `topology_late_append` | A locks that now committed sealed parent. B's attempted new child demonstrably waits for A's lock; after A commits it receives exactly `23514` and rolls back. Every row remains unchanged. |
| `job_before_release` | A queues accepted job 40 with the active exact owner tuple. B's owner release waits on the trigger's shared owner lock. A commits, then B commits the release. The job is queued at revision 2. A subsequent independent job update receives exactly `23514`, without any row mutation. |
| `release_before_job` | A releases a different owner. B's first job update waits on that owner row. After A commits, B receives exactly `23514` and rolls back; job 41 stays accepted at revision 1. Another independent update is also refused. |
| `final_inspection` | The actual CLI inspects version 1. All rows in all ten control relations match independent expected fixtures, and the original migration receipt is unchanged. |

The CAS order uses the schema's deferred head foreign key: insert the new revision
only after winning the head update, in the same transaction. The loser never
creates a revision and explicitly rolls back. This tests the database constraint
and transaction boundary, not a shipped policy activation adapter; its authority,
source digest verification and full managed-write journey remain separate work.

The topology cases deliberately respect MVCC. An unsealed parent cannot commit
because of `topology_complete`; consequently another transaction cannot start
from a valid, visible unsealed parent. The fixture does not disable triggers to
manufacture that state. It separately exercises invisibility during construction
and parent-lock serialization of late insertion after the seal. It does not
claim that the second transaction helps construct an uncommitted topology.

Fixture timestamps, identities, request bytes, job states and exclusion digests
are literal synthetic data. The expired owner lease is deliberately irrelevant:
only an explicit synthetic release permits the successor. No real native writer
exists in this fixture, so its `joined` record is not evidence of native worker
exclusion. No TTL takeover, job execution or source frontier is qualified.

The initial expected rows are constructed independently from literal fixture
values. After every case an observer reads every column of every row in the
explicit ten-relation list, and compares it with the expected delta. A unique
violation or other negative code alone cannot pass: the exact winner and all
unchanged rows must also match. Expected results do not call the production
planner, trigger functions or reducers. The runtime-generated receipt is checked
against the independently hashed migration and retained unchanged for comparisons.

The PostgreSQL 16 [isolation contract](https://www.postgresql.org/docs/16/transaction-iso.html)
describes READ COMMITTED rechecking of a row after a concurrent updater commits.
Its [lock observation contract](https://www.postgresql.org/docs/16/view-pg-locks.html)
explains transaction-ID waits for row locks and recommends `pg_blocking_pids()`
for blocker attribution. The [psql interface](https://www.postgresql.org/docs/16/app-psql.html)
supplies the observed `SQLSTATE` and `ROW_COUNT` values. SQLSTATE expectations are
fixed by the fixture before execution; an unexpected actual code is a failure,
not an acceptable alternative selected after the run. The exact PostgreSQL 16.15
[psql main loop](https://github.com/postgres/postgres/blob/REL_16_15/src/bin/psql/mainloop.c)
was inspected for flushing stdout before reading the next piped command.

## Bounds, outcomes and cleanup

Each command is at most 4096 input bytes; each psql session admits at most 64 KiB
of captured output. The connect timeout is 5 seconds, statement timeout 30 seconds,
lock timeout 20 seconds and idle transaction timeout 60 seconds. A response waits
at most 12 seconds, and a required lock observation at most 10 seconds. The helper's
finite observer-command deadline still applies. No automatic retry is used to
hide a missed barrier, deadlock, failed transaction or unknown commit response.

Only bounded SQLSTATE/row counts, backend identities, lock observations and
synthetic rows are reported. Raw psql errors are not copied into reports, and no
password or secret-file contents enter command arguments or fixture output.
Each subprocess response uses a distinct marker. EOF, timeout, malformed output,
a changed process identity or an unavailable observer yields `UNKNOWN`; known
contradictions yield `FAIL`. Unreached cases stay `NOT_RUN`. A zero process exit or
a missing error is never a transaction commit oracle.

Cleanup checks the same owned container identities before targeting a session by
its complete recorded `(PID, backend_start, database, user, application_name)`.
It then observes that exact backend's absence independently. It closes and joins
the local Docker wrapper as well, but wrapper termination is not treated as proof
of database rollback. Unavailable backend identity or incomplete termination
leaves `cleanup=UNKNOWN` and prevents overall PASS. Sessions are cleaned between
cases and again in `finally`; no unrelated backend is signalled. Session identities
are retained in the report for explicit recovery after SIGKILL or host loss, when
Python cannot execute `finally`.

The script-specific lock serializes only this harness. The integrating owner must
serialize it against other laboratory tools. The database `bb_<run-name>_concurrency`,
its configuration, the lock file and `<root>/<run-name>-concurrency/` are retained
for inspection and explicitly owned cleanup. Successful runs write `result.json`,
`expected-rows.json` and `observed-rows.json` there. A complete result requires all
seven cases PASS and `cleanup=PASS`; an unfinished report is not release evidence.
This fixture does not qualify P02-T01 in full, persistent runtime adapters,
frontier/CAS consumers, process or storage fencing, or power-loss recovery.
