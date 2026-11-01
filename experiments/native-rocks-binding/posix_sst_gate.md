# Experimental application SST delay gate

This Linux AMD64 fixture has not yet qualified an actual fault. It addresses one
specific scheduling problem in the application-owner tmpfs test: closing an old
WAL before an SST flush can release preallocated pages after the test's first
ENOSPC observation. A full-filesystem observation before that close does not
establish that the subsequent SST write encounters ENOSPC.

`posix_sst_delay_gate.c` delays the first successful writable
`O_CREAT|O_TRUNC` open whose real fd resolves to `/posix/db/[digits].sst`. The
open itself has already executed. The empty, unallocated regular-file identity
and Linux background-thread id are journaled before the fd returns to RocksDB.
The shim waits at most five seconds for one release byte. Native open, write,
pwrite and pwrite64 arguments, return values and errno remain those of the real
`RTLD_NEXT` functions. Raw syscalls avoid interposition in the control journal;
loader reentry before constructor initialization also uses the real Linux
syscall. There is no error substitution, product Env, option, or Rust change.

`posix_sst_gate.py` launches the existing exact ignored Rust test. It verifies
the live child's `/proc` fd, inode, device, zero size/blocks and `rocksdb:` thread
name while the open is delayed. It creates one separate owned pressure file in
the same dedicated 64 MiB `/posix` tmpfs, writes until actual raw ENOSPC, checks
available bytes are zero, rechecks the still-delayed SST identity, and releases
the gate. It then requires an actual selected-SST write/pwrite/pwrite64 result
of `-1, errno=ENOSPC`, plus the unchanged Rust native NoSpace, sticky guard,
restored-headroom refusal, original-input conservation and joined-close oracles.
Fallocate failure, filler ENOSPC or a latch alone cannot pass.

The C journal uses inherited nonblocking pipes, sequence numbers, an overflow
flag and mandatory ready/end records. Missing/duplicate gates, timeout, a lost
journal record, controller errors or child failure invalidate the attempt. The
controller holds its pressure file until the child has joined; every failed
attempt copies the DB before removing only that exact owned file, even if a
partial Rust retention manifest exists. If join or custody copy fails, the
controller stays alive for the outer envelope's freeze/copy procedure. Failure
cleanup logging is best effort: a broken journal cannot bypass custody.
Control files belong in a separate persistent `/control` directory;
`/evidence` starts empty for the Rust test.

Run the cheap synthetic journal controls from a checkout:

```sh
python3 -B experiments/native-rocks-binding/test_posix_sst_gate.py
```

These controls cover synthetic journal rejection and cleanup failures. They do
not load the shim, execute RocksDB or qualify POSIX NoSpace.

After source review, compile only the C fixture in the pinned qualification
compiler image. The existing product and application library-test ELFs remain
immutable inputs. For example, with an empty owned output directory and the
qualification's verified image identity:

```sh
docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --user "$(id -u):$(id -g)" \
  --cpus 2 --memory 512m --memory-swap 512m --pids-limit 64 \
  --tmpfs /tmp:rw,nosuid,nodev,size=32m \
  --mount type=bind,src="$PWD/experiments/native-rocks-binding",dst=/fixture,readonly \
  --mount type=bind,src="$GATE_OUTPUT",dst=/out \
  "$QUALIFICATION_IMAGE" \
  clang -std=c11 -fPIC -shared -O2 -Wall -Wextra -Werror \
    -Wl,-z,defs /fixture/posix_sst_delay_gate.c -ldl -o /out/posix_sst_delay_gate.so
```

Record the exact fixture revision/source hashes, compiler image/tool hash,
command, output hash and terminal container state. This is a fixture build, not
a new product build or release receipt.

The established outer fault envelope must supply: no network, read-only root,
dropped capabilities, bounded CPU/memory/PIDs, an empty owned exact 64 MiB tmpfs
mounted at `/posix`, separately owned empty persistent `/evidence` and `/control`
directories, and read-only mounts of the test ELF, C library and controller.
Delegate its inner supervisor to:

```sh
python3 -B /fixture/posix_sst_gate.py \
  --binary /subject/test --binary-sha256 "$TEST_ELF_SHA256" \
  --library /fixture/posix_sst_delay_gate.so --library-sha256 "$GATE_LIBRARY_SHA256" \
  --control /control
```

Retain the existing 90-second outer deadline and pause/copy-before-termination
custody. A nonzero controller result is failure. Acceptance also requires
`/control/result.json` PASS and the exact Rust evidence/retained-file hashes;
hash the script/library/test ELF before and after execution. Bind the current
test-tool source manifest, its actual Cargo artifact/generated-metadata receipt
and ELF digest separately from any reused product/worker source identity. Do
not claim source metadata is embedded in the library-test ELF.

Keep the original unsuccessful attempts and distinguish this experimental
interposition schedule from an unassisted full-filesystem run. It establishes
only the actual recorded SST write and application-owner behavior on that
tuple; other native error producers, filesystems, races and platforms remain
outside this fixture's scope.
