# Delayed syscall exit discriminator

This research tool isolates the tracer failure recorded in
[the original blocked-checker run](../../docs/implementation/evidence/p08-export-checker-incomplete-20260924.json).
It is not a product dependency or an upstream strace release.

Pinned input: [strace 6.1 source archive](https://strace.io/files/6.1/strace-6.1.tar.xz),
SHA-256 `2579e9cec37dbb786f6ea0bebd15f40dd561ef2bde2a2a2ecdce5963b01859fd`.
Use the existing inventoried Linux amd64 runner image, unprivileged with no
network, fresh writable `/out`, exact committed source mounted read-only at
`/source`, and the archive read-only at `/input/strace-6.1.tar.xz`.
Run `python3 -B /source/experiments/archive-engine/strace_exit_probe.py`.

The unchanged pthread fixture enters one real `openat` from a worker and waits
for an explicit exit command on its main thread. Compare unmodified and patched
tracers, each without injection and with a 60-second syscall-entry delay. Require
an observed pending marker call, matching actual Tgid/TracerPid and no return to
the application across a one-second observation before asking for exit. The
original delayed run must reproduce its timeout/error; the corrected delayed
run must join with code4 within2s without completing the marker call. Preserve
original logs, compiler/kernel versions and both tool/binary hashes. These are
expected results; absence or a different outcome is not PASS.

The patch changes only `TE_STOP_BEFORE_EXIT` in
[exact 6.1 dispatch_event](https://github.com/strace/strace/blob/v6.1/src/strace.c):
clear the delayed flag and free its copied wait data. Preserve the global timer
because other threads may still be delayed. SIGALRM is masked during dispatch,
and [timer scanning](https://github.com/strace/strace/blob/v6.1/src/delay.c) uses the
live TCB flags. The current exit event has separate storage. Linux
[ptrace semantics](https://man7.org/linux/man-pages/man2/ptrace.2.html) require a
tracer resume/detach at this irreversible exit stop to finish the thread's death.
This does not resume ordinary application work or change a syscall's result.

Qualify this instrument before reusing it against BigBrotr. A delay at syscall
entry exercises non-returning blocking work under supervision; it does not prove
that every kernel I/O in uninterruptible D state is terminable. The original
BigBrotr run remains INCOMPLETE even if this isolated discriminator succeeds.

The [executed discriminator](../../docs/implementation/evidence/p08-export-checker-instrument-20260924.json)
at9b30c35 records the scoped result, including the original failed extraction.
The revised BigBrotr checker fixture consumes `/out/checker-instrument/` with
that exact ELF and `result.json`; it verifies both digests and the committed
patch. Copy from the inventoried stopped lab only after fresh ownership and hash
checks, mount the copied instrument read-only, and capture its identities again.
The fixture records request submission and requires at least1.25s remaining of
the ordinary5s deadline immediately after sending SIGTERM, with request/syscall
pending before sending. It also requires actual signal delivery in the trace
while at least1s remains. This distinguishes the signal from a deadline that
already expired.
Its pidfd tracks actual process death independently of tracer lifetime. These
revised BigBrotr assertions require their own native run; the tool probe alone
does not qualify them.
