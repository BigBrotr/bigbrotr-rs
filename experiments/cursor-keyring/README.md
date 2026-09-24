# Protected cursor key preparation qualification

This bounded P06-T03 Step1 / P08-T03 prerequisite exercises the actual finite
`access cursor-keyring prepare/inspect` commands. It does not qualify Control
registration, AEAD tokens, rotation, job listing, scientific pins or release.

Transfer an exact clean owner-selected Git revision to a fresh
`/home/tux/bigbrotr-implementation-20260917/runs/cursor-keyring-SHORT/source`
checkout through the documented Git-bundle workflow. Run that checkout's
`lab.py REVISION TREE` on Tux. It inventories the host before use, verifies stopped
8db compiler-cache and strace-exit-9b30c35 ownership, copies the compiler caches
without editing the originals, exports and hashes the current Git tree and runs
one labeled Linux amd64 container with no network/ports/volumes. The pinned image
has a6GiB memory limit and two CPUs. The native helper builds a fresh parent with
no Archive-worker binding, verifies its embedded source identity, runs selected
Rust/CLI tests and Clippy, and executes the practical cases. It never treats the
old compiler cache as proof of the current source or artifact.

For a fixture-only correction, `lab.py REVISION TREE ARTIFACT_REVISION` may reuse
an earlier stopped lab's parent only after verifying its successful build/tests/
Clippy receipt, original source manifest and ELF hash and comparing every current
product/build/test input. Only documentation (except the generated configuration
reference consumed by CLI tests) and this cursor-keyring recipe may differ.
Other experiment fixtures remain compared because Rust tests may include them.
The artifact keeps
its original embedded identity; the current practical run records both revisions.
This mode copies no compiler cache, runs no redundant compilation/tests and mounts
only old source/build metadata/native-test artifacts read-only, never old key data.

Fifteen practical cases cover ordered sync and identical retry, the independent
public wire corpus, cross-process locks, eight concurrent preparers, injected
file/directory/parent sync errors, injected ENOSPC, four SIGKILL windows, a file
created by an external writer after the initial absence check, and a genuinely
full blocking stdout pipe. Expected bytes use independent Python struct/hashlib
and fixed public fixtures; generated key bytes are inspected only inside the lab.
The output distinguishes inspected from prepared state and checks observable
files/identities/inodes in addition to exit status. A15-case PASS is required;
unreached cases remain NOT_RUN and failures retain their original evidence.

The qualified strace artifact is pinned by SHA-256. Write syscall arguments are
rendered raw so traces do not contain keys. Keep `out/cases/*/keys/` and retained
staging private on Tux; do not include them in generic log/tar collection or Git.
Reports contain only metadata/hashes. Traces, safe CLI output, source manifest,
build identity, original errors and container/resource inventories are evidence.
The one container is stopped and retained, with preexisting resource projections
checked at teardown; no global cleanup or volume deletion is performed.

Injected syscall failure/process death tests do not simulate host power loss,
physical writeback corruption, network filesystems or a hung kernel. The local
keyring is a snapshot of protected bytes, not continuing pathname custody or
authority. Registration must follow a successful explicit preparation.

The [native report](../../docs/implementation/evidence/p06-cursor-keyring-native-20260925.json)
records15practical cases passing at7acfd32 with the verified unchanged1f251db ELF.
The [original run](../../docs/implementation/evidence/p06-cursor-keyring-native-incomplete-20260925.json)
remains INCOMPLETE after an observer NameError; its successful build/tests and
first ordered-sync case retain their original identity. The correction moved
misplaced ENOSPC assertions into their intended loop without changing product
code or expected answers. No broader cursor/Control/release gate is closed.
