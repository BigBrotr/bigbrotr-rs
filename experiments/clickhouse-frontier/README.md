# ClickHouse immutable frontier probe

Run probe.py against a disposable isolated ClickHouse 26.5.1.882 endpoint with
LAB_CH_URL and LAB_ROOT configured. It creates only its named test database; it
must not target a production server. The four safe tables intentionally share a
tiny synthetic schema: this is a storage identity/visibility experiment, not the
future event/tag/provenance schema or API implementation.

Unsafe control: a logical edge at sequence 10 has first_seen=200. A correction at
11 has first_seen=100. Replacing by logical edge and latest sequence, then forcing
a merge, leaves no row visible under sequence<=10. The old snapshot is lost.

Safe variant: the immutable sequence belongs to the replacing key. Inject the
same correction twice with engine insert deduplication disabled, delay one table
family, and force merges. Frontier 10 remains unchanged for every family; frontier
11 contains two distinct immutable records and reduces to first_seen=100.

The recorded server used the previous independent lab container (2 CPUs / 3 GiB,
no swap, loopback only), separate from all project containers. Tables explicitly
set fsync_after_insert=1 and fsync_part_directory=1. The client ran under 1 CPU /
128 MiB / no swap / 30 tasks; runtime 1.404 s, 54 ms CPU, peak 11.1 MiB. The own
container was stopped afterward.

Duplicate inserts model replay; no network ACK was actually dropped. The test
uses fixed frontier predicates, not a full PG receipt/publication coordinator.
No power-loss, restart, real schema joins, API pagination or scale behavior was
tested. Those remain required integration scenarios. This proves a concrete
counterexample and the narrow immutable-identity alternative on a real engine.
