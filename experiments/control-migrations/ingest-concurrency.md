# Actual PostgreSQL ingest-control races

Status: **NOT_RUN** on PostgreSQL. Local checks cover Python syntax, the fixed
case/table inventory and bounded fixture command sizes. They do not execute a
transaction or qualify migration 0002. Native results must identify the exact
application revision, binary, harness, migration files, containers and server.

`ingest_concurrency_cases.py` imports the bounded psql `Session`, exact backend
cleanup and `pg_blocking_pids()` observer from `concurrency_cases.py`. It requires
an existing running owned PostgreSQL 16.15 laboratory created from the same clean
source checkout and binary revision. It creates a new database, invokes the real
CLI with explicit `--target 2`, and retains the database and report directory for
inspection. Run it alone while other lab mutation tools are stopped:

```sh
python3 experiments/control-migrations/ingest_concurrency_cases.py \
  --root /absolute/owned/laboratory-root --run-name ingest_races_first
```

The canonical root must be owned by the executing UID with mode `0700`. The run
name contains 1–20 lowercase letters or underscores. Existing configuration,
database and output names are refused rather than reused. The harness does not
create, remove or reconfigure containers, networks, ports, volumes or TLS state.

## Fixture boundary

The one-dataset fixture registers four source streams in a complete sealed
topology and the exact Ingest, Archive and Index owner tuples used by the cases.
It also creates two consumers, three projection generations, two prepared
chunks, and two source-cut snapshot leases. Every frontier and chunk cut contains
all four topology streams. A zero bound is represented without a fabricated
receipt.

Archive-batch, projection-engine, closure and exclusion digests are deterministic
synthetic fixture bytes. They establish relational lock and transition behavior
only. No adapter resolves them against RocksDB, archive files or ClickHouse, and
the synthetic `joined` exclusion is not evidence that a native writer stopped.
The report records `fixture_attestations = synthetic-unvalidated`.

The fixture does not disable triggers or constraints. Four schedules issue
`SET CONSTRAINTS ALL IMMEDIATE` after making a complete candidate transition.
This forces the normal deferred checks while the transaction remains open so an
observer can see their source-row locks. It neither suppresses a check nor changes
the durable result that commit would require.

## Independent schedules

Every required wait must show a PostgreSQL lock wait, the exact holder PID in
`pg_blocking_pids()`, and unchanged waiter/holder backend start times. Elapsed
time alone cannot pass.

| Case | Schedule and durable oracle |
|---|---|
| `frontier_seal_before_lower_close` | A constructs and validates complete Archive frontier 2 with shard 1 at sequence 5, retaining the source locks. B's attempt to close shard 1 at 4 waits, receives exactly `23514` after A commits, and rolls back. The sealed frontier remains; the source remains open. |
| `lower_close_before_chunk_commit` | A closes shard 2 at 4 and retains its row lock. B tries to commit a prepared chunk whose complete cut reaches 5. Its deferred validation waits, then receives exactly `23514` after A commits. The source is durably closed at 4 and the chunk remains prepared. |
| `close_before_mismatched_handoff` | A closes shard 3 at 5. B constructs a sealed successor topology that claims handoff 4 for that source. Its source-membership validation waits, then receives exactly `23514` and rolls back. No generation-1 row survives. |
| `drain_before_new_chunk` | A changes one Index owner from active to draining. B's new chunk insertion waits on that exact owner and then receives exactly `23514`. The owner is draining and the candidate chunk is absent. |
| `chunk_complete_before_release` | An existing prepared chunk belongs to another draining Index owner. A validates and commits the chunk, holding the owner lock. B's release waits, then succeeds after A commits because no prepared dependent remains. The committed chunk and released owner both remain. |
| `frontier_publish_cas` | Two Archive transactions read current frontier 1 and issue the same expected-head update to sealed revision 2. B waits for A. A commits; B's READ COMMITTED recheck affects zero rows and rolls back. Exactly one head publication remains. |
| `claim_before_lease_release` | A inserts a valid source-range claim and holds the lease row. B's explicit lease release waits, then commits. The lease is released and the claim remains independently active. |
| `lease_release_before_claim` | A releases another lease and retains its row lock. B's claim insertion waits, then receives exactly `23514` after seeing the released state and rolls back. The lease release remains and no new claim exists. |

After every case, the observer reads every column of every row in all 23 control
relations. The expected result starts from the prior full snapshot and applies
only the literal row insertion or field transition named by the case. Exact
normalized equality detects partial writes, failed rollback and mutation in any
unrelated relation. Separate literal fixture counts and states prevent an empty
or incomplete bootstrap from passing. The final CLI inspection must report
current version 2, and both original migration receipts must remain byte-for-byte
unchanged.

## Bounds and outcomes

The shared session layer enforces a 4,096-byte command limit, 64 KiB output limit,
5-second connection timeout, 30-second statement timeout, 20-second lock timeout,
60-second idle transaction timeout, 12-second response deadline and 10-second
lock-observation deadline. This fixture's largest setup command is 3,940 bytes.
There is no retry that can hide a missed barrier, deadlock, unexpected SQLSTATE
or unknown commit response.

Each command response records exact SQLSTATE and row count. An unexpected result
is `FAIL`; EOF, timeout, malformed output, changed process/container identity or
unobservable cleanup is `UNKNOWN`. Unreached cases remain `NOT_RUN`. Successful
process exit without the exact durable-row oracle cannot pass.

The harness verifies the clean Git revision against the embedded application
source identity, the mounted binary against its build receipt, both migration
source hashes against the database ledger, PostgreSQL `160015`, synchronous
commit, `fsync=on` and `full_page_writes=on`. It writes only bounded identities,
lock observations, SQLSTATEs and synthetic row values to the report. Credentials
and raw psql errors are not copied into evidence.

Cleanup rechecks the owned container incarnations, then targets each session by
its complete `(PID, backend_start, database, user, application_name)` identity.
It observes backend absence and joins the local Docker wrapper. Missing identity
or incomplete termination makes cleanup and the overall result `UNKNOWN`; no
unrelated backend is signalled. Session identities remain in `result.json` for
manual recovery if the Python process cannot execute `finally`.

Successful execution retains:

- `<root>/<run-name>-ingest-concurrency/result.json`
- `<root>/<run-name>-ingest-concurrency/observed-rows.json`
- database `bb_<run-name>_ingest_concurrency`
- configuration `<root>/bb_<run-name>_ingest_concurrency.toml`
- `<root>/ingest-concurrency-mutation.lock`

All eight cases, final inspection and cleanup must pass. This experiment does not
qualify receipt adapters, real owner exclusion, power-loss behavior, runtime
scheduling, reclamation, or P02-T01 as a whole.
