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

Before reproducing, inventory the host, verify ownership of inputs and create new
resources. The original six containers are stopped and retained under
`/home/tux/bigbrotr-implementation-20260917/runs/archive-d459fcd`; do not restart
or overwrite them. Exact creation commands/owners are in the laboratory inventory.
No script authorizes network installation, release, pending acknowledgement or
resource deletion. These cases do not qualify catalog transactions, scientific
row fidelity, native memory peaks, power-loss durability or Linux arm64.
