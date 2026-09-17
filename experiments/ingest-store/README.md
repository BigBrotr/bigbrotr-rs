# Ingest store parent-directory synchronization probe

This Mac-only, external dyld interposer observes the real `fsync` used when the
store initializes its absent directory. It forwards the syscall unchanged and
prints fixed metadata only. There are no product test hooks, credential inputs,
raw paths or file contents in its output.

The selected actual-engine test creates exactly `parent/db`. The probe recognizes
the parent descriptor by a directory entry named `db` while `db/CURRENT` remains
absent. RocksDB's later DB-directory and WAL synchronization do not match this
fixture. One successful parent call must be observed before accepting the result.

Build the product test with the repository's native build prerequisites and exact
locked dependencies. Use the test executable reported by Cargo; its hash-derived
filename can change. Keep the probe and its output in an owned ignored directory:

```sh
mkdir -p .research/implementation/rocks-store
xcrun clang -Wall -Wextra -Werror -dynamiclib \
  experiments/ingest-store/parent_sync_probe.c \
  -o .research/implementation/rocks-store/parent-sync-probe.dylib

# Replace this with the exact executable printed by the native Cargo test build.
store_test=.research/implementation/rocks-store/target/debug/deps/rocks_store-18e296d00dde16b5
DYLD_INSERT_LIBRARIES="$PWD/.research/implementation/rocks-store/parent-sync-probe.dylib" \
  "$store_test" \
  --exact explicit_initialization_persists_only_the_exact_manifest_and_reopens \
  --nocapture --test-threads=1 \
  > .research/implementation/rocks-store/parent-sync-observation.log 2>&1
python3 - <<'PY_CHECK'
from pathlib import Path
text = Path('.research/implementation/rocks-store/parent-sync-observation.log').read_text()
assert text.count('PROBE parent directory fsync returned success before CURRENT exists') == 1
assert '1 passed; 0 failed; 0 ignored;' in text
PY_CHECK
```

Record the source revision, dirty-source identity if applicable, compiler, native
archive, test executable and probe hashes alongside the output. A missing dyld
observation, empty test selection, unsupported platform or failed test is not a
passing result. Run this only on the owned local test executable, not unrelated
programs or production processes.

This observes the call ordering and successful syscall return. It does not inject
failure, cut power, prove a device honors flushes, qualify Linux or demonstrate
recovery after an ambiguous write. Those remain separate practical scenarios.
