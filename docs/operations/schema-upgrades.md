# Control schema migrations

Status: initial P02-T01 foundation. The checked-in catalog contains
`0001_control.sql`, `0002_ingest.sql`, `0801_management_identity.sql`,
`0805_owner_enrollment.sql`, `0802_management_commands.sql`,
`0007_control_owner.sql`, `0008_job_bounds.sql`, `0009_archive_attempts.sql`,
`0010_archive_coordinator.sql`, `0011_producer_requests.sql` and
`0012_producer_outcomes.sql`. The executable includes explicit inspection, planning and
application using the selected PostgreSQL driver. Its local planner, preparation
and entrypoint tests pass. Native Linux amd64 PostgreSQL execution now covers
the actual CLI, 104 independent SQL constraint cases, initial read-only runtime
privileges and an engine crash/restart; see the
[recorded engine evidence](../implementation/evidence/p02-control-engine-20260917.json).
The [subsequent fault/TLS evidence](../implementation/evidence/p02-control-faults-tls-20260917.json) records
six actual migration fault/concurrency cases and ten TLS/mTLS cases on the
corrected ffb94a8 binary. The [seven relational race cases](../implementation/evidence/p02-control-races-20260917.json)
subsequently passed with actual lock barriers and complete expected rows.
This is not completion of P02-T01 or a qualified installation/upgrade path.
The [ingest coordination schema](../reference/ingest-control-schema.md) adds
source/allocation and producer registration, complete consumer frontiers,
projection attestations and source-range retention. Its engine acceptance is
separate from the earlier 0001 evidence; source review alone does not qualify it.
The [task](../implementation/02-storage.md#p02-t01--typed-control-schemas-and-minimum-relay-registry)
and [upgrade contract](../specification/operations-contract.md) own the remaining
runner, adapter and practical acceptance work.

## Explicit executable commands

Generate and review a configuration using `bigbrotr config init`, then provision
the dedicated database and migration login separately. These commands use its
configured control endpoint/database/transport but require an explicit migration
login and protected password file, rather than the runtime application's password:

```sh
bigbrotr migrate inspect --file instance.toml --user bigbrotr_migrator --password-ref file:/run/secrets/bigbrotr-migrator
bigbrotr migrate plan --file instance.toml --user bigbrotr_migrator --password-ref file:/run/secrets/bigbrotr-migrator --target 2
bigbrotr migrate apply --file instance.toml --user bigbrotr_migrator --password-ref file:/run/secrets/bigbrotr-migrator --target 2
```

Apply refuses the configured runtime username. This check is not a privilege
audit: provisioning must also enforce the ownership/grants below. All commands
require PostgreSQL 16.15, `fsync=on` and `full_page_writes=on`; apply forces
transaction-local `synchronous_commit=on`. They do not initialize a dataset or
qualify a complete installation or release upgrade.

An omitted target selects the latest embedded migration, currently 12. An explicit
target 1 preserves the initial control-only boundary for its historical fixtures.
Migration 2 refuses existing topology members with SQLSTATE `55000`: migration 1
had no source registry, so their exact source authority cannot be inferred.
The CLI reports this preflight specifically and leaves the transaction uncommitted.
Eligible databases without topology members can retain existing control rows
while applying 2. There is no automatic backfill or destructive repair.

Migration10 preserves existing Archive attempts with a NULL coordinator binding;
it does not adopt them into the local custody protocol. Subsequent new issuance
requires an explicitly initialized binding and retained inputs. A dataset with old
attempts cannot initialize that binding; inspect/preserve it for separately qualified
recovery. No ordinary startup migrates, rewrites or discards old rows. The typed
initializer requires SELECT/INSERT on `control.archive_coordinators` and execution
of its invoker guards, without UPDATE/DELETE/TRUNCATE or DDL authority. Existing
runtime grants must be extended deliberately after the explicit migration.
The [protocol boundary](../specification/archive-storage-contract.md#initial-coordinator-protocol-boundary)
states current integration and practical qualification limits.

Migration11 is fresh-only for Ingest registration history. Migration12 adds
immutable producer outcome copies and reserves 64KiB per input for completion
within the retained 32MiB envelope. It refuses incompatible prior reservation
totals with55000, without modifying history or adopting native authority. See the
[producer input/outcome contract](../reference/producer-request-inputs.md) for
compatibility and unresolved recovery gates. Runtime provisioning needs SELECT/
INSERT on `control.producer_outcomes` and EXECUTE on its invoker guards; no UPDATE,
DELETE, TRUNCATE or DDL privilege. Original migration bytes remain unchanged.

Output is bounded JSON with format `bigbrotr-control-migration/1`, version/count
strings, exact pending SQL hashes and explicit absent version as null. Exit 0
means the reported inspect/plan/apply completed; 1 is backend/I/O/refusal, 2 usage,
3 invalid static configuration/preparation, and 4 an unknown COMMIT outcome.
Failure to deliver stdout after commit also requires fresh inspection. Never
interpret an absent success response as permission to replace a database.

One finite process owns one connection and transaction. It reserves a conservative
payload grant before reading credentials, permits no waiter, and retains that
grant through connection-driver shutdown. The allowance is not measured RSS or
an OS limit. Connect is bounded to 10 seconds including resolution; transaction
work to 60 seconds, with 30-second statement, 5-second lock and 10-second idle
transaction deadlines. Local driver shutdown has a 2-second grace before abort
and join. An uncancellable resolver may outlive the runtime grace until process
exit; this command never recycles its credits into a long-running daemon.

Plaintext routes require all of at most 16 resolved addresses to belong to the
declared boundary: loopback, or private IPv4/IPv6 ULA for local service DNS under
`private-bridge`. One accepted address is pinned through driver `hostaddr`, so
connection performs no second DNS lookup. This address check cannot attest host
network topology; the private bridge and its DNS remain operator responsibilities.
`localhost` selects 127.0.0.1 without DNS; use `[::1]` explicitly for IPv6 loopback.
TLS uses only the configured CA and expected server name, with optional paired
client identity. The recorded native Linux amd64 cases exercise SCRAM, TLS/mTLS,
wrong CA/name/client identity and plaintext route refusals. Rotation/revocation,
arbitrary TLS combinations and native arm64 acceptance remain unqualified.

The database-local advisory transaction lock uses the two int4 keys 1111651442
and 1296648018. Apply takes it exclusively; inspect/plan share it. Ledger reads
occur in a later READ COMMITTED statement after lock acquisition. Metadata is
projected with byte limits and at most catalog length plus one rows, then sorted
in Rust after validation. No database sort consumes an arbitrarily expanded
ledger, although dead/corrupt heap scan cost is not a hard I/O bound. The lock
serializes cooperating migrators on this same authoritative database; privileged
manual DDL or independent database clones are outside that guarantee.

## Catalog and authoritative receipt

The catalog in `apps/bigbrotr/src/storage/postgres/migrations.rs` embeds the exact
SQL UTF-8 bytes and computes SHA-256 without newline or whitespace normalization.
`plan(applied, target)` accepts an ordered authoritative receipt list and returns
its current version, requested target and borrowed slice of pending migrations.
An empty input means that the caller has established an absent ledger; a failed
query or inaccessible database is not absence. The planner performs no I/O.

`control.schema_versions` has primary key `(engine, migration)`. This catalog's
engine is exactly `postgres-control`. Each receipt stores the migration filename,
32-byte SQL checksum, `reader_min`, `reader_max`, `writer_floor`, signed UTC
microsecond `applied_at_us`, and bounded non-secret `applied_by` identity. Initial
compatibility values are all 1. Migration IDs are positive u32 values: reserved
number ranges do not require nonexistent intervening migrations. The
[manifest](../../migrations/manifest.json) records append-only application order,
which may differ from numeric order as cross-phase consumers arrive. The compiled
catalog is checked against that sequence; existing history is never reordered.
Current readers validate the full known prefix while requiring their actual
prerequisite migration. Older binaries refuse unknown 0801; there is no implicit
binary rollback after this schema extension.

The planner requires an exact ordered catalog prefix, including name, checksum
and compatibility metadata. It rejects duplicate, missing, unknown or changed
history, an unknown target, and downgrade. Both the current and requested
endpoint must allow the reader and writer versions of this binary. Old history
rows describe their historical formats; they are not mutable advertisements of
the current format. A newer unknown migration refuses this binary even if an
operator guesses that its tables are compatible.

The runner must serialize inspection and application with its documented
migration advisory transaction lock before reading the ledger. A clean bootstrap
is an absent `control` schema in an otherwise valid dedicated application
installation; a schema with unknown objects or no valid ledger is not clean.
`0001` creates the schema and tables without `IF NOT EXISTS`. It does not start a
transaction, insert its own receipt, initialize a dataset, or grant runtime
credentials. Execute its DDL and matching receipt in the same transaction.
Ordinary startup and diagnostic inspection must not create storage.

If the commit reply is lost, the outcome remains unknown. After obtaining
migration ownership on a fresh connection, read the receipt and validate its exact
identity. An exact receipt means this migration committed; an authoritative
absence permits the same plan to retry. Connection loss is not evidence that
rollback happened. A matching receipt does not certify against arbitrary manual
DDL drift; operator-controlled DDL access is part of the trust boundary, and
catalog/object verification remains a release acceptance responsibility.

## Database ownership and privileges

Use separate application-control and DuckLake catalog databases/ownership,
credentials, backups and version negotiation even if they share a PostgreSQL
server. This SQL creates only application-owned `control` objects. It neither
creates PostgreSQL login roles nor edits DuckLake-managed metadata.

The dedicated migrator owns the control schema, its domains, tables and functions.
The runtime application login is a non-owner without schema creation, DDL,
TRUNCATE, trigger-disabling or role-management authority. `PUBLIC` receives no
schema, table or function privileges from this migration. Deployment provisioning
must grant only the schema usage, typed domain/function usage, reads and specific
table mutations needed by implemented adapters. It must not grant application
writes to `schema_versions`. Backup readers have a separately bounded read scope.
No application grant is usable yet merely because the DDL exists.

The SQL functions are invoker functions with a fixed search path and qualified
object references; they do not elevate a caller to the migrator. Trigger execution
is not an authorization API. A privileged database owner can bypass constraints
and triggers, so normal runtime credentials must not own these objects.

PostgreSQL login roles are distinct from logical `WriterRole`. The latter binds
`dataset_id`, a resource, owner epoch and process incarnation at the application
commit boundary. An `owner_epochs` lease timestamp is coordination metadata. It
never proves that an expired process has stopped writing. Moving to a new epoch
requires positive exclusion under the [ownership contract](../specification/operations-contract.md),
not just a row update, elapsed TTL, controller restart or healthy cockpit.

## Initial relational responsibility

The application database owns one local instance/dataset. Imported dataset
identities stay in their source lineage; they do not initialize another local
writer authority in this database. Dataset creation begins with NULL authority
heads. [Explicit initialization](control-initialization.md) accepts config revision
1 with its installation actor/audit. Security policy and topology remain absent
until their own authorized, atomic transitions exist; initialization alone cannot
start an ingest owner or create a runtime grant.

| Relation | Initial responsibility |
|---|---|
| `datasets` | Immutable local instance/dataset identity and monotone active config, policy and topology heads |
| `schema_versions` | Immutable exact migration receipts and compatibility metadata |
| `config_revisions`, `policy_revisions` | Immutable same-dataset revision history, validated policy documents and digests |
| `role_instances`, `owner_epochs` | Registered roles, exact writer incarnation/epoch tuple and explicit exclusion record |
| `topology_generations`, `topology_streams` | Immutable complete required-stream manifest and optional closed-epoch handoff bound |
| `jobs`, `job_input_bounds` | Finite accepted job identity, request idempotency key/digest, config/policy references, immutable initial cut, typed revisions/state/result identity |

All qualified identities use fixed BYTEA domains; 16-byte local identities reject
the nil value. Digests are exactly 32 bytes. Unsigned 64-bit counters use bounded
`numeric(20,0)` so values above signed i64 remain representable, and unsigned
32-bit values use bounded BIGINT. Observed timestamps are signed UTC microseconds.
No successor wraps at exhaustion. PostgreSQL applies NUMERIC scale coercion
before CHECK, so an untyped fractional input can round before the range check.
The typed adapter must reject fractions before binding/casting; this SQL domain
preserves integer values and range, rather than validating original wire tokens.
See PostgreSQL 16's [numeric type](https://www.postgresql.org/docs/16/datatype-numeric.html)
and [constraint semantics](https://www.postgresql.org/docs/16/ddl-constraints.html).

Revision documents are bounded JSON objects (10 MiB stored textual form). The
closed Rust configuration/policy schema must select and validate their contents
before parameterized insertion: deployment roots, secret values and scientific
payloads do not belong here. JSONB shape/size checks cannot establish that semantic
allowlist. `accepted_source_digest` identifies accepted source bytes;
`document_digest` identifies the owning versioned canonical policy encoding.
Re-serializing JSONB does not recover original source bytes. The policy activation
consumer must define and verify its exact digest representation before use.

Head updates require an expected-value CAS in the caller's transaction. For
example, insert the next immutable revision and update `datasets.config_revision`
only where it equals the expected prior revision; zero updated rows roll back the
whole change. Use NULL-safe comparison for initial absence. The trigger enforces
one-step monotonicity, but it cannot know the caller's original expected revision.
An actor, accepted preview digest and current policy must be reauthorized by the
adapter. The same rule applies to owner/job checkpoint mutations.

Topology membership and job input bounds are built and sealed in one transaction.
Child insertion locks its parent; sealing prevents concurrent late appends.
Deferred checks reject an incomplete parent at commit. A cut includes exactly the
required topology streams, including explicit sequence 0 for known empty streams.
A NULL job topology means no source cut, whereas generation 0 is a present topology;
a present empty topology remains different from absence. A cut cannot exceed a
closed stream's authoritative `source_streams.last_complete_sequence`, even when
its older topology retains a NULL handoff. A non-NULL topology handoff remains an
additional upper bound. Live bounds still require the owner's committed
receipt proof; this foundation does not create a second PG admission authority.

[Migration 0008 native evidence](../implementation/evidence/p02-job-bounds-native-20260918.json)
covers 24 corrected cases, including both concurrent lock orders and rollback.
Migration 0008 locks jobs, their bounds and source rows against concurrent writes,
then refuses SQLSTATE `55000` if any existing job cut exceeds an authoritative
closed bound. The CLI identifies this refusal and the whole migration transaction,
including earlier pending entries, rolls back. It never lowers a job's immutable
input or raises a closed source's claimed receipt. Preserve the incompatible
history for explicit reconciliation/reconstruction from verified source receipts;
there is no automatic repair or qualified data-loss-free reconstruction command.
Existing valid cuts and original migration checksums remain unchanged.

Job cuts with a topology and source closure require **READ COMMITTED**, selected
explicitly by typed adapters. Other isolation levels refuse with `25001`; a fixed
older snapshot cannot safely observe a job committed while closure waited. The
volatile job check takes `FOR SHARE` on every source in dataset/shard/epoch order,
then reads authoritative bounds with a fresh snapshot. The closure UPDATE locks
the same row before checking all recorded job, consumer and committed projection
bounds. These locks last to commit/rollback, including deferred job validation.
Multiple source closures should acquire rows in that same order. Deadlock or lock
timeout aborts the transaction; retry the entire authorized operation, never a
partially accepted cut. Explicit zero and complete topology requirements remain.
See PostgreSQL 16's [snapshots](https://www.postgresql.org/docs/16/transaction-iso.html)
and [volatile queries](https://www.postgresql.org/docs/16/xfunc-volatility.html).

Job insertion starts at `accepted`, job revision 1 and checkpoint revision 0.
The initial cut and request identity never change on completion. The request key
is opaque bytes bounded to 128 bytes and unique per principal and kind; the
acceptance adapter must compare the digest on retry and reject changed requests.
Updates carry a next job revision, nondecreasing checkpoint and active exact
Control coordinator tuple. Locking that owner row serializes a PG job mutation
against its release; native worker exclusion remains a separate responsibility.
Terminal jobs cannot resume. Paused/cancel-requested and unsuccessful terminal
states require a bounded reason code; succeeded requires a result manifest digest.
No arbitrary JSON checkpoint or scientific output is stored in the job row.

The shared authority deliberately does not dispatch jobs yet. Acceptance still
needs command receipts and authorization/source pins in one transaction before
reply; required pin/recovery tables and typed adapters arrive with their actual
consumers. Browser disconnect must not cancel an accepted job. Cancellation cannot
rewind durable data or release a native grant before the worker joins or is
positively excluded. Generic job TTL deletion is refused until explicit obligation
transfer/reclamation exists. Principal access checks, role acknowledgements,
management lifecycle, profile limits and job-specific recovery units remain owned
by their planned tasks.

## Validation gates

Local planner tests exercise an exact already-applied receipt, changed SQL/name/
compatibility metadata, duplicate and out-of-order history, unknown target/history,
reader/writer compatibility, known downgrade, intentional catalog number gaps,
and the independent SHA-256 `abc` vector. They do not execute PostgreSQL.

The following engine cases are required, with externally inspected rows and exact
source/engine identities. The evidence above records single-session SQL, CLI,
role and crash cases, followed by migration lock concurrency, ambiguous COMMIT
and TLS, then config/owner/job/topology races with independently observed lock
barriers and expected durable rows. Runtime adapters and receipt-backed consumer
frontiers remain unimplemented. Each trigger
has a concrete invariant and corresponding negative control below; a passing
parser or successful DDL execution alone does not qualify them.

| Boundary / trigger | Positive and negative engine cases |
|---|---|
| Scalar domains and FKs | Round-trip u64 maximum, zero sequence and generation 0; reject overflow, nil/wrong-length identities, foreign dataset references; demonstrate NUMERIC fractional coercion and adapter rejection separately |
| `immutable_*` | Insert exact migration/revision history; reject update/delete, including attempted checksum repair |
| `dataset_head_guard` | Build initial NULL heads and CAS to 1/1/0; reject prepopulated initial heads, identity rewrite/delete, skip/backward/NULL reset; race two expected-revision updates and observe exactly one commit |
| `owner_epoch_guard` | Acquire epoch 1, drain/exclude/release, then epoch 2; reject expiry-only takeover, skipped epoch, wrong prior dataset/role, revived release, tuple rewrite and delete; race acquisitions |
| `topology_stream_guard`, `topology_generation_guard`, `topology_complete` | Commit a sealed complete or intentionally empty topology; reject unsealed commit, child mutation/delete, later append and reseal; race child append against sealing |
| `job_creation_guard`, `job_update_guard` | Accept/seal one immutable job and advance with active Control tuple; reject initially running work, changed idempotency/input/config identity, stale revision/checkpoint, old/released tuple, terminal resume and invalid transition; race owner release against job update |
| `job_input_guard`, `job_input_complete` | Commit exact membership including zero and closed-bound values; reject missing/extra/foreign stream, unsealed commit, late append, update/delete and bound above closed handoff; completion cannot replace the starting cut |
| Runner transaction/privileges | Run actual inspect/apply on clean/incompatible databases; race migrators, terminate before commit, lose commit reply and resolve; verify exact receipt and schema together; prove runtime login cannot migrate, TRUNCATE or disable triggers |

`0002_ingest.sql` supplies relational coordination, with closed transitions and
independent constraint fixtures. Actual engine qualification and typed runtime
adapters still own the external receipts, fencing and reclamation guarantees.
`0003_relay.sql` and endpoint/route enrollment remain P02-T01 work. Publication
references arrive with their actual owner. These remaining boundaries prevent
marking Step 1 complete and prevent advertising durable job,
read/export, acquisition or reclamation behavior from this foundation.

There is no shipped predecessor release or qualified binary rollback in this
slice. Never edit an applied SQL file to make a checksum match. Future forward
repairs add catalog entries; restoring a backup requires new fenced roots and
explicit reconciliation of post-backup data and external effects. Before any
writer-floor increase or irreversible change, implement and exercise the owning
upgrade plan, backup closure and old/new compatibility journey. A failed or
unexecuted scenario remains a release gate.

Migration 0009 supplies [retained archive intent issuance](../specification/archive-storage-contract.md#retained-publication-intent-issuance).
It refuses pre-protocol `owner_epochs` rows with role `archive` (SQLSTATE55000),
without changing history or assuming old processes stopped. The runtime Control
role needs SELECT/INSERT on `control.archive_attempts` and EXECUTE on its guard;
it needs no UPDATE/DELETE/TRUNCATE. Issue/resolve uses the existing owned Control
connection and requires an explicit sealed topology. The guarded catalog and
actual publication remain separate unimplemented boundaries; this migration
cannot authorize an archive worker by itself. The [native issuance evidence](../implementation/evidence/p02-archive-attempts-native-20260918.json)
records the scoped PG16.15/Linux amd64 cases; wire COMMIT faults remain unexecuted.
