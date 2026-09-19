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
SQL constraint concurrency and fresh-only migration compatibility require their
own practical schedules before qualification. The progress checkpoint and evidence
report record actual execution; the presence of this harness is not a PASS claim.
