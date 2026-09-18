# First security revision: native acceptance

Run the actual `config activate-initial` executable against an isolated native
PostgreSQL 16.15 lab, with clean exact Git source and matching built artifact:

```sh
python3 experiments/control-migrations/lab.py setup --root /absolute/new/lab \
  --build /absolute/exact/build --source /absolute/clean/source --run-name activation
python3 experiments/initial-activation/run.py --root /absolute/new/lab
python3 experiments/control-migrations/lab.py stop --root /absolute/new/lab
```

Inventory the host first and retain the labeled resources/evidence. No host ports
or real relays are used. The runner refuses existing reports, dirty source and
artifact mismatches. It creates isolated databases and uses the application CLI
for initialization/activation; fixture corruption, concurrent schema setup and
permissions are explicit SQL from the separate lab administrator.

Independent expectations reuse the frozen complete 215-setting document from
[configuration inspection](../configuration-inspection/README.md), with its
stated oracle boundary. Python reconstructs the canonical policy digest and
binary command digest; it observes every Control table and checks exact heads,
whole policy rows, audit/receipt references, grants and absent runtime effects.
Refusals and replays compare complete schema dumps. Cases cover managed seed
drift, declarative drift/equivalence, incorrect deployment/revision, missing
migration, initialized upgrade, retired runtime history, digest corruption,
transaction rollback on denied receipt insert, content conflict and historical
receipt resolution after configuration advances.

The existing framed PG fault proxy loses COMMIT before forwarding and after server
completion, holds concurrent same/different requests, and is killed with the real
application before COMMIT. Each case verifies durable effects and serialized
same-ID recovery. Finally SIGKILL/restart of PostgreSQL compares all database
dumps. Process crash recovery is not a power-loss qualification.

Rows and successful commands do not prove live leases, native fencing, running
role revocation, disk exhaustion, maximum-size resource behavior, arm64 or release
installation. No such claim is made. A planned scenario is not executed evidence.

Additional bounded native checks use `fk_races.py --root LAB --source SOURCE`
and `enrollment_after.py --root LAB --source SOURCE`. Execute them from real
files: they bind their own file SHA-256, the clean product source and the mounted
executable. A helper-only revision can be extracted separately from Git without
switching an actively consumed product checkout; record both identities.
The FK runner holds a real child insert open while the CLI waits, tests commit
and rollback, and includes a weaker `NO KEY UPDATE` negative control. The owner
consumer runner enrolls/inspects a real account after activation and confirms the
initial receipt is unchanged. Run `schema_cases.sql` only in the populated owned
lab and compare the entire Control dump before/after; six expected SQLSTATE
refusals must leave no mutation. Execution at fe63862 with helper revision e9b9127 is recorded in the
[native evidence](../../docs/implementation/evidence/p01-initial-activation-native-20260918.json),
including the initial stdin-invocation failure and subsequent file execution.
