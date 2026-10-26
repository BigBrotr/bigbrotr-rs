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

## Initial Index schema qualification

`initial_schema.py` is a separate finite SQL check for the selected
ClickHouse26.8.35.10 amd64 image. It creates a new container and database, compares
the ten-table literal inventory with actual server catalogs, and checks a separate
writer's persistent `max_concurrent_queries_for_user=1 MIN 1 MAX 1` constraint,
narrow grants and refusal controls. The inventory's fixed table-identity vector
and exact schema/inspection source hashes must match before engine work begins.

Run only on an authorized Docker host after checking free space and existing
resource ownership. The image pull is explicit; the script never adopts a
foreign container, directory or database. Choose a new absolute lab directory whose
parent exists, and a unique container name with the shown prefix:

```sh
docker pull --platform linux/amd64 clickhouse/clickhouse-server:26.8.35.10@sha256:324e2f39fa3be72b5838acf05af0f3cd0bfcd90f0096a9b0cf470b757fc355cc
python3 -B experiments/clickhouse-frontier/initial_schema.py \
  --lab-root /absolute/owned/research/new-index-schema-run \
  --container bigbrotr-index-schema-UNIQUE \
  --schema-source apps/bigbrotr/src/storage/clickhouse/schema.rs \
  --inspect-source apps/bigbrotr/src/storage/clickhouse/adapter/inspect.rs
```

The server has no network or published ports. Native clients use `docker exec`
inside its4GiB/no-swap,2CPU,256-task limit. The script checks a2GiB data/log stop
budget and8GiB remaining host space between bounded queries, rotates server logs,
and stops its server after either outcome. It retains the container, database,
raw outputs and generated initializer/writer credentials under mode0700/0600
storage. Never publish that directory. A crashed/interrupted driver requires an
ownership check against its recorded container ID before manually stopping it.

This is SQL/native-client evidence. It does not qualify the HTTP wire, Rust
adapter, Fact mapping, PostgreSQL proofs/publication, fsync under power loss,
restart/recovery, capacity or the packaged product. A catalog match establishes
the selected settings, not their complete durability guarantee. Preserve a failed
run and its recipe; change an expectation only after correcting the owning
physical contract and reviewing the independent reason.

The narrow `--complete-retained-tail` option is only for the recorded JSON integer
oracle failure: it first verifies the retained literal table/column/event facts,
then checks the same stopped container's ID, image, mounts, labels and limits.
It appends only the missing type/partition/concurrency/column-TTL controls and
preserves the original outcome. An already recorded continuation refuses another
run. It is not a general failed-lab recovery mechanism.

The October26 evidence retains two physical-declaration failures (constant
partition key, omitted persisted granularity), the corrected JSON representation
oracle, and the refused constant-TTL fixture. The maintained TTL expression now
depends on `recorded_at`; this revised negative control is **NOT_RUN** and is
scheduled with the next required real adapter qualification. See
[the scoped evidence](../../docs/implementation/evidence/p02-initial-index-clickhouse-sql-20261026.json).
