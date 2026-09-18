# Actual initial-registry transaction experiment

This is an isolated research/acceptance harness for P02-T01/P02-T02. It executes
the actual Rust `ingest_registry` adapter against PostgreSQL 16.15; it is not a
product installation, policy initializer, admission service or authority grant.
`seed.sql` deliberately supplies **synthetic coordination-only** configuration
and policy documents/digests. These must never become product bootstrap values.

Initial status: **NOT_RUN**. Building this harness or passing its pure input tests
does not execute the engine scenarios. Record subsequent revision-bound results
in implementation evidence; do not reinterpret this initial status as a run.

On Tux, use a clean exact Git candidate and a newly inventoried run directory.
Build the actual CLI with `tools/build.py`, then `build_probe.py` builds the actual
application example from the same verified source archive, same lockfile, native
toolchain and output target. It records the probe's distinct executable digest.
The initial CLI build is reused only for explicit schema migration. The probe is
an additional acceptance executable, not a promoted release artifact.

```sh
python3 tools/build.py --output /owned/build --jobs 2
python3 experiments/ingest-registry/build_probe.py --build /owned/build
python3 experiments/control-migrations/lab.py setup \
  --root /owned/runtime --build /owned/build --source "$PWD" --run-name registry
python3 experiments/ingest-registry/run.py --root /owned/runtime --build /owned/build
python3 experiments/control-migrations/lab.py stop --root /owned/runtime
```

Paths are examples and must name the actual inventoried resources. The setup
creates a fresh internal network, PG data directory and synthetic SCRAM secrets;
no host ports are published. The registry runner creates a separately labeled
client sharing only the owned PG network namespace, with read-only build/source,
2 CPUs, 4 GiB memory+swap, 256 PIDs and no capabilities. It stops and retains this
client in `finally`; the parent PG/client remain for inspection until explicit
lab stop. Failed/partial databases and reports are preserved, never overwritten.

Five independent fresh databases execute:

- Direct first registration, exact inspection, a new same-identity retry, and a
  changed count-limit conflict with unchanged durable rows.
- COMMIT intercepted before forwarding: the same owned attempt reports unknown,
  resolves absence under the registration lock, then retries the same identity.
- Backend COMMIT completion with its response suppressed: the same attempt
  resolves the already committed complete registration without a second insert.
- Registration future cancelled while COMMIT is held: the attempt retains
  CommitDispatched; a fresh resolver must visibly wait on the old transaction's
  registration lock before the external driver releases COMMIT.
- Two actual registration clients: first COMMIT held, second observed waiting on
  the registration lock, then both return the same complete registration.

All cases include wrong deployment, wrong Control epoch at u64 maximum, changed
policy digest, migration-role refusal, exact numeric round-trip, and inspection
after the producer enters closing. That last transition is synthetic PG
coordination, not the native closing barrier or its acceptance.

Each case compares all columns of all 23 control tables through a separate psql
observer, in one consistent snapshot. The expected added rows are independent
literals in `run.py`; unaffected initial rows must remain byte-semantically equal.
The final snapshot and expected closing state are retained separately. Held
transactions must remain externally invisible. Real backend IDs, start times,
blocking PIDs and granted/waiting lock modes establish the schedules. The existing
bounded loopback [fault proxy](../control-migrations/README-fault-proxy.md) must
report the actual selected wire boundary; an exit code alone is insufficient.

The probe takes only mode, synthetic database, loopback port, fixed role name and
protected credential-file path. It emits closed redacted JSON and joins connection
drivers, including deliberately failed wires. Plaintext/SCRAM is confined to the
owned network namespace. This does not requalify the production connection/TLS
adapter, least privilege for every future command, policy authentication, actual
native ownership, event durability, restart takeover, archive, or release support.

The lock/snapshot reasoning follows PostgreSQL 16
[explicit locking](https://www.postgresql.org/docs/16/explicit-locking.html) and
[Read Committed isolation](https://www.postgresql.org/docs/16/transaction-iso.html).
Pinned tokio-postgres 0.7.18 `transaction.rs` sets its done flag before awaiting
COMMIT; dropping that future is therefore not rollback evidence. Its actual
source, probe/client bytes, engine/image, source manifest, workload, outputs and
resource lifecycle must be bound in the run report.
