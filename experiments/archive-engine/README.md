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
No script authorizes network installation, release, pending acknowledgement or
resource deletion. The runtime/thread probes do not qualify scientific row
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

`catalog_fence.rs` starts the next bounded **unexecuted PostgreSQL** experiment.
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
result. The planned controls compare concurrent absent-receipt writers, sealed
per-job login, positive backend termination and lost acknowledgement. No role
revocation/fencing design is selected or qualified merely by compiling this probe.

`catalog_fence.py` owns the four actual-process cases inside a fresh isolated
runner with `psql`, no Docker socket and an outer finite deadline. It checks the
cluster identity before mutation and positively reaps its children. The matching
`catalog-fence.Dockerfile` pins the lab client package; record its actual image
and executable identities. These cases remain NOT_RUN until recorded native
evidence exists.
