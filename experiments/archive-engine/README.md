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

`catalog_fence.py --case-set guard` is a new, **NOT_RUN** alternative experiment:
`catalog_commit_guard.sql` adds a PostgreSQL BEFORE STATEMENT guard on all28
catalog relations created by the pinned DuckLake initializer. Its dedicated
row-lock-protected login must match `session_user` for every metadata mutation;
ordinary writers lose DDL privileges. Takeover updates that same row. The two
initial cases keep old clients/logins alive (ready and invisible-startup) and
require their native COMMIT to fail after replacement publication. This proposes
an actual storage boundary instead of inferring one from client coordination.
It is not a product migration or selected replacement: concurrent in-flight
mutation/rollback, isolation, privilege/DDL escape, full metadata coverage,
maintenance/migration compatibility and practical cost still need qualification.
Native metadata values are never manually edited. Production adoption would
require an explicit update to the archive ownership contract and migration path.
