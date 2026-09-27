# PostgreSQL control constraint fixture

`lab.py` is the accompanying Docker laboratory orchestrator. It provisions a new
owned directory, internal network with no published ports, PostgreSQL 16.15 and
a finite-lived client container. It refuses existing resources, never deletes
them, and keeps randomly generated passwords in owner-only files. Run its clean
Git checkout on the native Linux host after building the exact application
revision with `tools/build.py`:

```sh
python3 experiments/control-migrations/lab.py setup --root /owned/lab/runtime --build /owned/lab/exact-build --source "$PWD"
python3 experiments/control-migrations/lab.py smoke --root /owned/lab/runtime
python3 experiments/control-migrations/lab.py schema --root /owned/lab/runtime --run-name constraints
python3 experiments/control-migrations/privilege_cases.py --root /owned/lab/runtime
python3 experiments/control-migrations/lab.py stop --root /owned/lab/runtime
```

The root's parent must already exist. Image digests are explicit laboratory
inputs in the script; this is not the ordinary Compose installer. `resources.json`
binds container ownership, exact artifact/source receipt, engine and harness.
The setup `--run-name` also namespaces its new container/network names. Explicit
initdb options require SCRAM on every TCP host rule, including loopback; the
container-local socket is trusted only for the independent synthetic lab observer.
This fixture socket rule is not production role/grant qualification.
The separate privilege script exercises the actual runtime and migration logins,
grants the runtime only schema usage/reads, and requires SQLSTATE 42501 for DDL,
ledger writes, TRUNCATE, trigger disabling, ownership change and role elevation.
These are the initial read-only fixture grants; permissions for future typed
runtime mutation adapters remain unimplemented and unqualified.
`smoke.json` retains each actual result and stops at the first failure. The smoke
run creates fresh dedicated databases, exercises the actual executable, compares
the external checksum receipt, refuses incompatible histories and bad plaintext
routes/passwords, and checks receipt survival after a controlled engine SIGKILL.
The separate `schema` action runs the SQL fixture below in another fresh migrated
database and checks its literal postconditions through a separate connection.
Use a fresh `--run-name` for each repetition: earlier reports and databases are
never overwritten. Refused migrations require the specific reason and identical
schema/data dump hashes before/after, excluding only pg_dump's random psql restrict
token. Wrong-password rejection requires successful same-route controls, a new
server authentication-failure record and SCRAM host rules. This does not qualify
host power loss, a full upgrade, TLS, concurrency or lost COMMIT responses. Failed/partial resources remain owned
and inventoried for diagnosis; `stop` requires matching ownership labels.

Status: **NOT_RUN**. This fixture is authored for the actual PostgreSQL **16.15**
engine and `0001_control.sql`; static inspection is not execution evidence. Root's
exact-revision Linux run records the eventual outcome separately. It has no Python
package, pgTAP, extension or production-planner dependency.

Use a newly provisioned, isolated application database containing only migration
0001 and its real receipt, applied by the actual BigBrotr migration runner. The
fixture refuses another engine patch, changed receipt or pre-existing application
rows. It does not initialize, drop, truncate or repair the schema. Its synthetic
rows remain committed afterward for independent inspection; dispose of only the
owned laboratory database through its recorded lifecycle. Do not run it against
an operator's database or a DuckLake catalog.

From the repository root, with the caller's protected libpq connection settings
already selecting that owned database:

```sh
control_migration_sha="$(sha256sum migrations/postgres/0001_control.sql | cut -d ' ' -f 1)"
psql -X --no-password --set=ON_ERROR_STOP=1 \
  --set=expected_migration_sha256="$control_migration_sha" \
  --file=experiments/control-migrations/schema_cases.sql
```

Use the migrator/schema owner to exercise constraints, not to claim runtime
privilege qualification. The runner supplies the schema and receipt in its real
transaction; this fixture compares that receipt to an independently computed file
hash. Record Git commit/tree and clean source state, SQL/fixture hashes, PostgreSQL
server and client versions, image digest, host architecture, caller role and owned
resource names with stdout/stderr and exit status. Never put a credential-bearing
DSN or password in a committed report or command argument.

The script uses session-local assertion functions and temporary result/snapshot
tables. It fixes statement, lock and idle-transaction timeouts. Every negative
requires an explicit SQLSTATE and compares all ten application relations before
and after its failed subtransaction. The expected states are independent literals:
`23514` check/trigger refusal, `23503` foreign key refusal and `23505` uniqueness
refusal. Wrong SQLSTATE, no exception or any surviving row change fails the script.
`SET CONSTRAINTS ALL IMMEDIATE` forces deferred checks inside the caught failure
boundary; this does not disable constraints. Separate successful transactions
exercise real COMMIT of the initial dataset/revisions/topologies and job cut.

One deliberate whole-transaction failure prints an expected `23514` error with
`ON_ERROR_STOP` briefly off. Its captured SQLSTATE must match and the complete
control snapshot must remain unchanged after ROLLBACK. An earlier unexpected
failure would change the captured state to `25P02` and fail the subsequent check;
a false success of the deliberate bad value would also fail. All other errors
remain fatal or are caught only by their exact expected-state assertion.

A successful run ends with **104** named case results, including **84** caught
negative cases, then one JSON result with `result=PASS`, `executed=104`, `failed=0`,
`skipped=0`, `not_run=0` and the server version and migration hash. The case-count
assertion refuses empty or incomplete execution. This result describes only
these SQL cases; absent final output, a disconnect or a nonzero exit is not PASS.

Covered boundaries include:

- Independent exact receipt and immutable ledger; full u64 maximum, zero and
  fixed-byte identity/digest domains; server base-type parameter casts; explicit
  demonstration of NUMERIC fractional rounding before domain checks.
- Initial instance/dataset identity and NULL-to-1/1/0 heads; no head skip/reset,
  no missing/foreign revision, no mutable policy history, bounded JSON object
  storage, stale expected-value update and full rollback of revision plus head.
- Complete sealed topology with explicit zero, closed handoff and u64 maximum;
  a separately committed empty topology; no unsealed commit, membership mutation,
  duplicate/late child or rewritten generation, including immediate constraints.
- Owner epoch origin/successor, exact tuple, release evidence shape, immutable
  history and refusal of expiry-only replacement or mutation under a draining or
  released owner. Exclusion records are synthetic: no real worker runs here.
- Initial finite job state/revisions, idempotency-key uniqueness, no missing,
  duplicate, extra or foreign stream, no bound above a closed handoff, no changed
  starting cut, no missing active owner, no stale/regressed checkpoint, and
  terminal/pause/cancel state rules with committed successful/cancelled examples.

Postconditions for external inspection are one dataset (ID ending `01`), one
config revision, one policy revision, two sealed topology generations (0 with
three streams, 1 empty), two Control owner epochs (1 released, 2 active), and three
jobs: ID ending `28` succeeded with unchanged generation-0 cut, `29` cancelled
with no cut, and `2a` accepted with an empty generation-1 cut. The ledger stays at
one exact receipt. These hex IDs encode the small integer fixture identifiers;
they are synthetic and have no deployed identity or cryptographic authority.

This single-connection fixture does **not** qualify concurrent editor/migrator/
owner/child races, commit-response loss, disconnect recovery, runtime grants,
TRUNCATE/trigger-disable denial, real process exclusion, policy grants, Rust
`ToSql`/`FromSql` encoding, fractional-input rejection in the typed adapter,
command/job/pin atomic acceptance, retention/reclamation or any signed-event
journey. The prepared statement verifies only PostgreSQL's explicit base-type
casts. Those actual-client and multi-process cases remain **NOT_RUN** until root's
separate runner and acceptance execution supplies their evidence. No P02-T01 step
is marked complete by merely adding this fixture.

## Ingest coordination fixture

`ingest_cases.py --root /owned/lab/runtime --run-name ingest` uses a fresh owned
database and the actual executable's default target 2. It checks two exact
receipts, absence of implicit application initialization, no-op reapply, then
runs `ingest_schema_cases.sql`: 74 named cases, including 56 SQLSTATE negatives.
It compares every column of all 13 added relations through separate observer
connections against independent literal final rows in the Python harness.

Two further databases exercise a populated, eligible 0001 upgrade that preserves
existing rows and receipt, and a populated topology lacking source registration.
The latter must report the specific preflight refusal, retain exactly ten control
tables, and preserve its full schema/data dump digest. Original 0001 suites now
pass target 1 explicitly; their original scope does not grow with the CLI default.

Results, raw SQL stdout/stderr, independently expected rows and observed rows
remain under the owned root, never overwritten. A parser pass, missing final
summary or skipped native run is not acceptance. This harness does not qualify
external RocksDB/ClickHouse receipts, process fencing, concurrent schedules,
physical reclamation, archive/read/export or native ARM PostgreSQL.

## Transactional job admission fixture

`qualification.py <commit> <tree> --job-admission` runs a fresh exact-Git Linux
build, the existing schema tests/Clippy and `job_admission_cases.py`. Its explicit
seed is the retained6474cursor runtime compiler cache; source comes from the new
Git tree, with the same locked dependencies and no network during compilation.
The old `cursor-registration/lab.py` remains a compatibility entrypoint for the
registration, enrollment and runtime profiles in this shared orchestrator.

The815fixture applies real migrations using the actual CLI, initializes and
activates Control, then uses finite synthetic backup job metadata through the
application SQL role. It checks deterministic backfill without rewriting existing
rows/xmin/receipts, identical retries, transaction rollback, privilege and isolation
refusals, mapping immutability, immediate constraints/multirow insertion, actual
lock barriers in both writer/cut orders, backend death and restart. Literal expected
rows distinguish transactional admission order from equal/backdated timestamps.
A1000row EXPLAIN records the actual backward-index lookup; this is not a mixed,
aged workload or capacity claim.

The actual `serve` process additionally probes the optional815privilege boundary
through its loopback management endpoint. A nonexistent session must reach401
with sufficient grants and503 when mapping SELECT or INSERT is missing. This
is neither authenticated listing nor actual export command/audit/pin acceptance.
Those installed journeys and cursor authority lifecycle remain required. No
existing SQL checksum changes and no implicit migration or initialization occur.

Results, SQL responses, observer snapshots and process streams stay in the owned
run. PASS requires all cases and session cleanup, then the enclosing orchestrator
positively stops its owned containers, preserves prior container projections and
retires only its empty network. Databases/source/build/evidence remain retained.
The [8fae53a report](../../docs/implementation/evidence/p08-job-admission-native-20260927.json)
records95PASS checks across11databases; the [e82attempt](../../docs/implementation/evidence/p08-job-admission-native-incomplete-20260927.json)
remains INCOMPLETE after a missing fixture private-listener DELETE grant. Source
review/AST checks and synthetic metadata do not qualify real export acceptance.
