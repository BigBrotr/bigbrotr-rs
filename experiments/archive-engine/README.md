# Native archive engine prerequisite

These are the exact scenario sources executed against candidate
`d459fcdc357ddb3b68d9ac89e044b22156cf72b0`, not production launchers or archive
publication adapters. [The evidence report](../../docs/implementation/evidence/p02-archive-engine-native-20260918.json)
records hashes, environments, observed failures and unimplemented boundaries.
Generated data stays under the inventoried ignored
`.research/implementation/archive-worker/` root.

`runtime.py` expects a verified native build at `/build` (readonly), a fresh
owned `/lab`, Linux `strace`/`ldd`, no network, an unprivileged user and a disposable
container with 2 CPUs, 1 GiB memory and 128 PIDs. It verifies the source and worker
receipt, runs seven packaged cases, and writes `result.json` plus observations.
It deliberately blocks stdout, then kills/reaps its process group. Its fixed
diagnostic inputs and post-read output assertions are experiment constraints,
not a suitable untrusted-output production supervisor implementation.

`thread_probe.py` and `thread_files.py` use the same mounts/limits with fresh lab
directories and trace thread creation; the latter correlates extension opens.
The stack-unwind attempt only resolved libc frames. The source-confirmed cause is
signature hashing, not a claim inferred from those incomplete stacks.

`pid_exhaustion.py` additionally requires PID cap 8 and disabled core dumps. It
records the actual child outcome, cgroup PID events and time; its own successful
exit means observation completed, **not** that the worker succeeded. The recorded
worker abort remains a native failure for future parent recovery tests.

`native_tests.py` runs the six actual Rust engine/bundle tests inside the exact
native build image. It expects `/source`, the original `/out/build` with its
read-only source archive, and a fresh `/out/native-tests`. It rechecks source and
tool identities before execution and retains the actual ELF test executable.

`native_event_rows.py` is the exact subsequent harness for candidate `2f4dd92`.
It executes eleven Rust cases, including native Arrow/event/Parquet readback, and
checks the actual pinned C API vector size (2048). Its two separately owned
containers are also stopped-retained; see the
[event-row report](../../docs/implementation/evidence/p02-event-rows-native-20260918.json).
It does not qualify later metadata/dictionary changes or a publisher.

Before reproducing, inventory the host, verify ownership of inputs and create new
resources. The original six containers are stopped and retained under
`/home/tux/bigbrotr-implementation-20260917/runs/archive-d459fcd`; do not restart
or overwrite them. Exact creation commands/owners are in the laboratory inventory.
The runtime probes do not authorize network installation, release, pending
acknowledgement or resource deletion. The runtime/thread probes do not qualify scientific row
fidelity. None of these cases qualifies catalog transactions, native memory
peaks, power-loss durability or Linux arm64.

`native_all_rows.py` is the exact harness for `5c3a8dd`. It executes all 18
engine/row cases, including all five families and the raw-binding null-struct
negative control. Its separate build/test containers are stopped-retained;
[the report](../../docs/implementation/evidence/p02-five-family-rows-native-20260918.json)
records identities, scope and cleanup. This does not exercise later handoff code.

`native_handoff.py` qualifies the bounded wire/private preparation at `0cdd80f`.
`native_supervision.py` is the exact `c365053` harness: fourteen Rust cases,
five CI controls and affected Clippy, with the trusted build's actual packaged
worker supplied through the shared CI helper. It asserts nonempty case counts
and records positive termination with proven-full output pipes. See the
[supervision report](../../docs/implementation/evidence/p02-private-supervision-native-20260918.json).

`parent_death.py` exercises a packaged worker after engine startup, stops it and
kills its actual parent. Its `--expect-survival` mode records the missing safeguard
in the old worker; success in that mode means the negative control survived.
pidfds and a subreaper bind cleanup/reap to the owned processes. Positive corrected
worker qualification is separate. No process experiment grants catalog authority.

`native_parent_binding.py` is the exact `e4e2a30` harness for two worker integration
cases, the connected signed case, actual packaged stopped-worker parent death and
affected application/worker Clippy. The [report](../../docs/implementation/evidence/p02-parent-binding-native-20260918.json)
retains distinct Cargo and packaged executable hashes, source/native tool audits
and positive process cleanup. No kernel-task, host-loss or PG fence is inferred.

`catalog_fence.rs` is the bounded PostgreSQL dependency experiment qualified at
`a531ea3`; see the [four-case report](../../docs/implementation/evidence/p02-catalog-fence-native-20260918.json).
It is an explicit Cargo example of the private worker package, using the same
locked Rust/native tuple; it is not copied into the packaged installation.
Build with `cargo build --locked -p bigbrotr-archive-worker --example catalog-fence-probe`
under the verified bundle environment. A lab must verify immutable assets before
exec, supply a new isolated catalog/data/temp root and protected DSN file, bound
resources/output/time, and retain ownership through positive kill/reap. Native
errors expose fixed phases only because driver errors can embed credentials.

Its modes `init`, `read`, `write` and `lost-ack` operate on three constant canary
relations, without Nostr/scientific authority. Arguments after mode are the
absolute bundle, empty private temporary directory, data directory, DSN-file path
and a case batch number1–16. `write` reports a read-transaction barrier and waits
for `publish` on stdin; `lost-ack` additionally waits after COMMIT and before its
result. The executed controls compare concurrent absent-receipt writers, sealed
per-job login, positive backend termination and lost acknowledgement. No role
revocation/fencing design is selected or qualified merely by compiling this probe.

`catalog_fence.py` owns the four actual-process cases inside a fresh isolated
runner with `psql`, no Docker socket and an outer finite deadline. It checks the
cluster identity before mutation and positively reaps its children. The matching
`catalog-fence.Dockerfile` pins the lab client package; record its actual image
and executable identities. Four synthetic cases pass; this is not a selected production fence.

Three earlier fresh catalogs failed before the cases: the pinned PG extension
looks for unavailable `local_file` secret storage when no secret is found. An
explicit TEMPORARY memory secret avoids that fallback while persistent secrets
remain disabled; PG secret storage is disabled separately. Redacted native attach
diagnostics stay in a0600 file inside the private lab root, never either pipe.
The first failed image build and all failed runs remain recorded.

`catalog_startup.py` is the separately executed PG16.15 startup negative control
at `de1d402`. It requires a fresh owned cluster, psql15.19 and the same protected
credential setup. Startup-packet post_auth_delay exposes an invisible connection
that commits after NOLOGIN plus empty activity. PASS means that inference was
**disproved**, not a product fence passed. The [report](../../docs/implementation/evidence/p02-postgres-startup-native-20260918.json)
retains the first failed setup, source/image/tool identity and cleanup. Six empty
networks from these completed owned labs were retired after Docker address-pool
exhaustion; stopped containers, data and evidence remain retained. Never treat a
sleep interval as an exclusion guarantee.

`catalog_fence.py --case-set startup` adds two native-client
startup cases. It reuses the verified `catalog-fence-probe`; record that artifact's
source identity separately from the newer Python harness revision. The bounded
`catalog_startup_relay.py` observes one actual SCRAM connection on runner loopback,
forwarding packets to the owned PG16.15 cluster without recording payloads. It
compares NOLOGIN/empty activity with an alive client against positive client
SIGKILL/reap and a replacement commit before late server EOF. The exact ordering
between replacement COMMIT and ReadyForQuery was not timed.
The observer must see no native query before ReadyForQuery and must drain actual
server EOF; a sleeping interval does not establish exclusion. This is dependency
qualification, not production authority, a general proxy or portable export.

The first startup run at `1b6c311` **failed before completing a case**: server
AuthenticationOk was buffered until after the injected delay, so using that as
the barrier missed the gap. The corrected trigger observes the actual client
SCRAM proof; successful late ReadyForQuery still has to establish accepted login.
The corrected run at `56ef588` passes both cases with the exact `a531ea3` native
probe. [The report](../../docs/implementation/evidence/p02-native-startup-exclusion-20260918.json)
records independent source/artifact checks, packet ordering, positive reap and
replacement membership, the first failure and four stopped retained containers.
Both empty owned networks were retired. Persistent product recovery remains open.

`catalog_fence.py --case-set guard` is an alternative experiment, two scoped native cases
PASS at `694be48`:
`catalog_commit_guard.sql` adds a PostgreSQL BEFORE STATEMENT guard on all 28
catalog relations created by the pinned DuckLake initializer. Its dedicated
row-lock-protected login must match `session_user` for every metadata mutation;
ordinary writers lose DDL privileges. Takeover updates that same row. The two
initial cases keep old clients/logins alive (ready and invisible-startup) and
require their native COMMIT to fail after replacement publication. This proposes
an actual storage boundary instead of inferring one from client coordination.
It is not a product migration or selected replacement. The subsequent runs below
exercise concurrent mutation/rollback and privilege escapes. Full mutation
coverage, maintenance/migration compatibility and practical cost remain open.
Native metadata values are never manually edited. Production adoption would
require an explicit update to the archive ownership contract and migration path.

`--case-set guard-races` passes four native transaction schedules at `df2a0db`:
writer versus takeover obtains the guard row first, followed by holder commit or
rollback. A lab-only later snapshot trigger can hold a native commit behind an
advisory barrier; it does not supply the candidate fence. Fresh pg_blocking_pids,
pg_locks and backend incarnations prove actual blocking. `catalog_pg_session.py`
owns bounded interactive psql controls and exact backend termination on cleanup.
Literal independent counts distinguish committed old work from permitted replay.

[The guard qualification report](../../docs/implementation/evidence/p02-catalog-guard-native-20260918.json)
records source/artifact verification and the initial two cases separately. All
six guard containers are stopped-retained, with no job backends; three empty
networks were retired. This remains a candidate with explicit mutation-coverage
and migration gates, not a product guarantee or full P02 step.

`--case-set guard-privileges` passes 44 cases at `d4f43af`: real limited-role
attempts to alter ownership/triggers/schema, escalate session/role privileges or
bypass with replication mode; zero-row DELETE on each of the 28 guarded tables;
role/search-path shadowing; and an authorized native publication positive control.
It checks exact PostgreSQL SQLSTATEs and final guard/table/owner identities. This
is an actual permission boundary probe, not a replacement for product schema
inspection, epoch/CAS or migration implementation.

The 44 cases comprise 14 permission refusals, 28 guarded-table refusals, one combined
role/search-path case and one native positive control. They are not 44 native
publication schedules. Source audit independently matches 614 tracked files to
Mac Git archive; actual PG logs contain 30 explicit guard refusals.

`native_archive_identity.py` passes seven scoped cases plus Clippy at `8d1289d`
on Tux Linux amd64. It checks the borrowed multi-range fingerprint/literal bytes and the
actual signed Rocks reopen-to-worker path at two explicitly supplied topology
values. The worker bundle is the retained immutable qualified artifact; it is
verified separately from the new parent/model source. Writable dependency caches
must be fresh copies, never edited retained build outputs. No catalog receipt,
publication, ACK, reclaim or new worker lifecycle qualification is claimed.

The [archive identity report](../../docs/implementation/evidence/p02-archive-identity-native-20260918.json)
records 621 independently matched source files and the stopped-retained container.
The five-minute signed check includes native compilation; it is not a publication
throughput result.

`native_archive_attempts.py` qualifies the Control intent prerequisite at `b245644`:
13 Rust cases, scoped Clippy and one actual PG16.15 adapter journey cover migration
fresh/upgrade/refusal, issuance/replay/CAS, copied issuer refusal, immutable history
and cancellation behind a real table lock. The
[report](../../docs/implementation/evidence/p02-archive-attempts-native-20260918.json)
records independent retained-row/digest/checksum inspection and resource cleanup.
The input is synthetic and the topology fixture empty; this checks intended-input
identity, not source membership validity or signed archive publication. Lost wire
COMMIT responses and the catalog activation consumer remain separate work.

`native_catalog_product.py` exercises the actual private native bootstrap and
Rust PG catalog guard/activation adapter. The
[944f3e6 report](../../docs/implementation/evidence/p02-catalog-product-native-20260918.json)
records 15 Rust cases, scoped Clippy, 9 actual adapter stages, mixed-case bootstrap,
unsafe privilege and schema negatives, immutable Control intent replay, and a
live stale native writer after a sealed-login successor. A separate fresh native
reader finds one synthetic facts/witnesses/receipts triple. Independent PG logs
must show the exact `require_writer` rejection; the generic worker COMMIT error
alone could also describe authentication failure and is insufficient evidence.
The report preserves 3 failed candidates, the corrected numeric epoch transition,
source/artifact/state audits and owned-resource cleanup. Scientific publication,
activation concurrency/ambiguity, credential provisioning and M1 remain open.

`native_catalog_recovery.py` builds the exact-source recovery probe and packaged
bootstrap worker. `catalog_recovery_cases.py <owned-root>` then uses the lab's
already inventoried runner sharing only its PostgreSQL network namespace. It
requires protected credentials, immutable source/assets, separate writable caches,
SCRAM host authentication (including loopback), and the retained Control fixture
created by `archive_attempt_probe`. It never creates or retires host containers.
The integrating owner must stop the identified runner and PostgreSQL and audit
resources even if a wrapper fails; terminating `docker exec` is not exclusion.

Fourteen actual-PG cases cover install/activation pass, pre-forward and
post-completion COMMIT loss; lower/higher/same-attempt races with release/rollback;
and cancellation at held COMMIT followed by either server outcome. A separate psql
observer checks backend incarnations, actual transaction-ID lock waits, all 28
table privilege sets, installation identity/checksum and immutable attempt values.
The fixture digest is independently frozen, not calculated by the adapter under
test. Failed/unknown execution stops later cases and retains partial results.
The [55dba36 report](../../docs/implementation/evidence/p02-catalog-recovery-native-20260918.json)
records all14 PASS on native Linux amd64/PG16.15, complete source/artifact/state
audits and terminal cleanup. The temporary initdb readiness race was resolved
before any application role/database existed; it is retained as a setup failure.
No scientific publication, worker launch, persistent service recovery or process/
power-loss durability is implied by an activation receipt.


`native_catalog_credentials.py` builds/checks the credential adapter and finite
supervised bootstrap parent. `catalog_credentials_cases.py <owned-root>` requires
an exact clean checkout, a fresh inventoried runner/PG namespace, SCRAM host auth
and the actual Control fixture. Explicit native bootstrap/guard installation uses
CatalogManager with temporary SUPERUSER for ownership transfer; the controller
then removes it before actual CREATEROLE-only provisioning. Never infer that an
ordinary non-superuser manager can take ownership of the bootstrap role's tables.

The [d2b2609 report](../../docs/implementation/evidence/p02-catalog-credentials-native-20260918.json)
records 8 actual-PG sequences: generated-password authentication/wrong password,
four membership controls, live-backend and future-login seal refusal, repeated/
historical seal and fresh native attach; before/after COMMIT loss for provision
and seal; same-intent commit/rollback races; pre-existing-role refusal. Independent
state checks precede recovery where necessary, so a retry cannot hide incomplete
initial sealing. The 16 native Rust cases include signed private preparation after
the shared parent refactor. All 648 source files/four actual ELF hashes and terminal
state are independently checked. Owned resources are stopped-retained; network
retired. No science, source ACK, unique persistent launch or installed recovery is
qualified by these scoped prerequisites.


`native_catalog_schema.py` builds the finite scientific initializer, shared staging
schema and supervised parent probe from a clean Git export. `catalog_schema_cases.py`
runs the parent against an exclusively owned empty PG database, records all native
metadata rows, and checks exact redacted refusals plus unchanged database dumps for
existing/unfamiliar catalogs. `catalog_schema_child.py` supplies fixture credentials
only on the private input pipe. A new canonical empty root isolates DB refusal from
filesystem refusal. The embedded-native Rust cases cover six-table atomic DDL and
late-statement rollback; do not report them as actual PostgreSQL failure/recovery.
No scientific guard binding, receipt insertion, source ACK or installed recovery
is qualified by this prerequisite alone. Native results are recorded separately.


`native_publication.py` also builds the actual signed publication and readback
fixtures. `signed_publication_cases.py <owned-root>` requires a fresh exact-Git
publication lab and runs real Control initialization/registration, signed Rocks
admission/reopen, guarded FreshWriter handoff and the supervised native publisher.
The fixture authorizes one literal network source with provenance. Invalid ID and
signature are rejected; the valid request's exact retry retains the same receipt.
Missing dictionaries precede successful bootstrap/event publication; normal mode
also refuses a duplicate publication. No pending source record is acknowledged.

The independent `signed_publication_oracle.py` authors all ten canonical fact
preimages, both admission/archive digests and every declared scientific/receipt
column from literal v1 fields and the frozen E_public event. Actual native readback
also reconstructs and verifies the signed event. Fault modes interrupt only the
event transaction's actual PostgreSQL COMMIT, with required proxy counters and
full catalog/physical-file inspection. A pre-forward rollback must leave its six
caller-owned files retained; a lost completed reply must preserve all committed
rows and files. Complete raw Rocks key/value hashes must remain unchanged.
This is a private risk fixture, not installed recovery, product Read/export,
protection or resource/load qualification. Candidate execution outcomes are
recorded separately; the existence of the harness is not a passing result.

The same signed harness adds three actual native fencing schedules: successor
activation obtains the owner lock before old metadata DML, or old metadata DML
holds it through COMMIT/ROLLBACK. A temporary scientific_schema table lock gates
manager inspection without changing guard/native SQL. Fresh PG blocker pairs
and backend identities establish the actual order. The old child must be reaped,
historical sealing must preserve the successor, and a separate native reader must
match every committed field before the next worker may launch. Committed old
work refuses duplicate replay; absent old work permits one new lifetime.

Pinned postgres defaults to REPEATABLE READ (41223e5 postgres_storage.cpp), and
pinned DuckLake retries concurrent-update errors (d8a1881
DuckLakeTransaction::RetryOnError/RunCommitLoop). The owner-first fixture therefore
expects exactly40001 followed by the specific55000 writer-guard refusal, two
metadata batches and no data COMMIT. The proxy classifies only fixed error
messages, never emits native SQL/credentials, and clears per-transaction state on
ReadyForQuery idle, including after rollback. The two other schedules hold actual
COMMIT and either forward it or close without forwarding. Expected final physical
files are14/8/14, including six retained old orphans in each rollback case; eight
files are catalog references in all three. [Exact-source b12c902 qualification](../../docs/implementation/evidence/p02-signed-fencing-native-20260919.json)
passes all six schedules, including the original three after proxy changes.
Source/artifact/terminal audits pass; owned containers are stopped and network
retired. This is finite native fencing evidence, not installed recovery or orphan
cleanup.

`custody_cases.py --root <owned-root>` adds three candidate local-journal schedules
using the actual signed admission fixture, publication parent and PostgreSQL
provisioning adapter. It kills only an identity-checked parent PID; the existing
native worker's parent-death SIGKILL must terminate its child. A separate explicit
Linux amd64 native test drops coordinator descriptors while the spawning thread
lives, requiring the worker alone to keep the lock through engine startup.

Two manager schedules cancel an actual queued provisioning COMMIT and detach its
driver, then destroy the transport or kill the manager. Recovery must remain
Occupied while the transport lives. After its destruction, local custody can be
recovered while a separately owned proxy still holds the catalog transaction;
fresh pg_blocking_pids must show product sealing waiting on that transaction.
Only explicit proxy release permits COMMIT/sealing. The final role is disabled,
the immutable attempt is retained, no scientific files are referenced and all
41 Rocks rows match the independent retained digest. The proxy never logs SQL,
verifiers or credentials. [The 5ad77af report](../../docs/implementation/evidence/p02-local-custody-native-20260919.json)
records all three PASS, 28 scoped Rust cases and all six signed journeys. It retains
the initial ead11f8 fixture failure (reused Control role identity) and its separately
retired lab. Installed recovery remains required.

`missing_import_cases.py` covers a live-driver control and destroyed-driver
schedule around the actual tokio-postgres owner Bind/Execute/Sync request. The
proxy retains that request before PostgreSQL executes it, then can keep the
server endpoint alive after the client is destroyed. Independent queries check
role/import/owner state, zero scientific files and the same 41 Rocks rows. At
`e571002` the live control completed provisioning; the destroyed driver sent no
mutation after delayed owner execution. Both required NotImported observations
returned Conflict and the baseline report remains FAIL, with terminal resources
retired. The [corrected exact-source run at770c485](../../docs/implementation/evidence/p02-missing-import-native-20260919.json)
passes both schedules and directly observes the later resolver's transaction-ID
blocker. The missing branch returns only after rollback; it creates no credentials.

`missing_import_refusals.py` creates ten disposable clones of the completed empty
catalog, exercises ordinal/attempt/worker/login collisions, orphan disabled/login
roles, a missing or actually recreated imported role, an inconsistent owner and a
valid unrelated owner of the same batch. Exact before/after state includes full
attempt fields, owner/role xmin and role OIDs. A renamed actual role isolates the
retained login-name collision from orphan-role detection; the unrelated owner
keeps LOGIN and a non-NULL generated fixture password. Ordinary DROP removes only successfully
created clones/fixture roles; no immutable history trigger is disabled and the
original catalog/journal is unchanged. All ten cases pass at770c485, together with
29 scoped native Rust cases and six signed publication/receipt regressions. Full
installed recovery and protection remain open.

`native_publication.py --retained-read` builds the same native worker and fixture
consumers, then runs the thirteen intent/schema cases and the actual Control
read/issuance probe. It selects those affected checks instead of the default
publication/receipt/custody unit collection. The resulting ready marker precedes
the separate actual custody and missing-import schedules.
[Executed evidence at `d86e147`](../../docs/implementation/evidence/p02-retained-attempt-read-native-20260919.json)
passes that profile and all five affected schedules plus ten clone controls.
The actual read blocker, five exact unchanged journals, four imported identity/OID
bindings, seven retained Control rows and sidecar absence are recorded. The earlier
fcd63ac baseline failed the retained-row assertion; both labs are retired.

`initialization_tls_cases.py ROOT` qualifies the exact installed initializer and
native receipt client with explicit server-enforced mTLS plus SCRAM in a fresh
owned lab. `native_initialization.py --tls` also builds the receipt fixture. It
checks immutable reservations on refusals, real 128 MiB initialization/manager
recovery, unchanged catalog reads, blocked native-query cancellation and a
retained older executable as a discriminating valid-mTLS refusal control. It
restores the owned server settings/HBA; final runner/database retirement and
source/artifact auditing remain required. The
[native report](../../docs/implementation/evidence/p02-native-mtls-20260919.json)
records 25 passing cases at `50d9496` and the retained earlier test compilation
failure; it does not qualify signed mTLS publishing.

`initialization_cli_cases.py ROOT --late-faults` selects nine additional installed
initializer schedules at the scientific-schema and guard-install commits: lost
replies before/after server completion, coordinator SIGKILL before forwarding or
after completion, and native-worker SIGKILL before the schema COMMIT. Explicit
proxy targets avoid the preceding DuckLake ATTACH transaction. Exact process
identities/pidfds, joined proxy endpoints and fresh PostgreSQL observations delimit
the fault before reading durable state. Recovery runs with the entire worker bundle
absent; native rows and root markers must remain unchanged, bootstrap retirement
must be complete, and a second recovery must be idempotent. An incomplete native
schema must remain refused. The [native report](../../docs/implementation/evidence/p02-initialization-late-native-20260919.json)
records all nine PASS at `3ad4c64`, native build/checks and terminal audit. The
initial Docker subnet-pool setup failure is retained; no product source changed.

`native_initialization.py --publication` builds the installed main and signed
publication/read/receipt fixtures against the same authenticated worker.
`signed_publication_cases.py ROOT --installed` uses the actual installed initializer
and NOSUPERUSER/CREATEROLE/NOINHERIT CatalogManager for provisioning, sealing and
inspection. PUBLIC database and public-schema privileges are revoked before
initialization; writer CONNECT/USAGE and manager delegation are independently
inspected. At `dbb3aea` the normal case actually failed: two generated roles lack
CONNECT/USAGE, manager delegation is absent, and PG reports both no-op schema
grants and refused authentication; no physical/catalog files were produced. The
other five schedules were not run. Its independently verified source/raw evidence
and stopped owned resources are inventoried.
The correction adds separate CONNECT/USAGE grant-option refusal controls with
ordinary access preserved, unchanged owner/attempt/role observations, and execution
of the exact documented privileged ACL upgrade. Final PUBLIC database/schema
privileges must remain absent. The original six scientific/fencing expectations
remain unchanged; the oracle additionally checks those explicit refusal events.
