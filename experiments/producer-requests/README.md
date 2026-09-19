# Producer reservation prerequisite

This trusted exact-Git Linux experiment exercises the connected initial adapter,
PostgreSQL16.15 and actual RocksDB, using the independently encoded two-visibility
bootstrap from `experiments/ingest-bootstrap/oracle.py`. Run only inside a separately
inventoried native laboratory. It is not an ordinary installation entrypoint.

`run.py --root <owned-lab>` requires `native_initialization.py --producer-ledger`
artifacts and its fresh isolated PostgreSQL peer. It checks successful exact retry,
policy refusal after durable reservation, statement rollback, SIGKILL/reap after
reservation before native, and the third COMMIT reply lost before forwarding/after
server completion. The first two COMMITs acquire Control and register Ingest;
proxy counters must prove those replies passed. Every case compares all native
key/value bytes, exact retained input, reservation metadata/counter and terminated
SQL clients. The script never treats a missing fixture or empty input as PASS.

The ledger is still a prerequisite. Logical input positions, outcome/checkpoint
closure, ACK/GC, successor replay, power loss and physical ENOSPC are not covered.
`schema_cases.sql` checks19 nonempty/constraint/rollback observations against the
actual successful bootstrap dataset, including the documented bulk refusal and
empty versus unresolved epoch retirement. SQL concurrency and fresh-only migration
compatibility still require their own practical schedules before qualification. The progress checkpoint and evidence
report record actual execution; the presence of this harness is not a PASS claim.

`concurrency.py --root <owned-lab>` follows successful reservation/schema cases.
It clones the quiescent disposable SQL fixture, without native copies, and uses
actual independently connected PostgreSQL sessions. Reservation-first and
closing-first schedules observe `pg_blocking_pids` before explicit COMMIT/ROLLBACK;
older RC/RR snapshots independently require preserved counters or SQLSTATE40001.
Every case checks retained rows, counter, state and zero remaining fixture backends.
These synthetic SQL schedules do not authorize native handoff or qualify restore.
Two additional harness controls inject a second-client readiness timeout and a
waiter blocked behind a running holder. Cleanup must collect both clients and
terminate only observed backends in the newly created fixture database, within
eight seconds. PID/start-time checks guard against terminating a reused PID.

After a fixture-only correction, `--producer-ledger-fixture` requires a retained
baseline source manifest. It permits differences only in explicitly named fixture
and checkpoint/inventory files, then builds exact-source binaries and executes the
changed fixture test and Clippy. Broader unchanged suites retain their earlier
source-bound evidence; this mode does not relabel them as newly executed tests.

`build_old_probe.py` builds the original a1da96a client from an exact Git export
mounted read-only in a separately inventoried native builder. `compatibility.py`
then uses that actual binary and the new executable, recording both artifacts and
the independent harness revision. Five schedules cover registration/migration in
both advisory-lock orders, backend loss while an old process retains the actual
POSIX Rocks lock, closed SQL history, and the new adapter's refusal of schema10.
The backend-loss marker precedes native FFI; it proves that losing a PG backend
does not exclude the native owner, without claiming a mid-FFI crash. Closed-history
rows are a synthetic SQL negative control, not an implementation of native handoff.
Failure must distinguish a precise compatibility refusal from timeout/connectivity
errors, preserve schema/data, and leave no owned SQL clients. Reported execution,
including any failure, remains the authority for qualification.
