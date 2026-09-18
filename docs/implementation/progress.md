# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint), then
verify Git, actual evidence and resource ownership before continuing.

## Mandate and active boundary

Owner-authorized implementation remains active ([Q30](../planning/questions.md),
[mandate](../planning/mandate.md)): complete the selected product. Continue trusted
native work on Tux despite unavailable hosted Actions. No public release, rename
or Linux arm64 qualification is implied. Go neighbor stays read-only.

Active tasks: P02-T03/T04. The initial private native engine/bundle prerequisite
is qualified for the [recorded scope](evidence/p02-archive-engine-native-20260918.json).
Next implement typed scientific rows and the real pending-batch publication path,
including parent ownership. No storage task checkbox or M1 gate is complete.

## Verified Git and ownership

Private origin/main is at `dcc514e` (engine qualification push observed). Local
main has unpublished event-row candidate `2f4dd9266d1f8fd22287f87232d43a75d7327e45`,
tree `4a416f86b8c38106bf5e5e8e862775048846b488`. Its native qualification is complete
for the event-only scope. Root owns the subsequent dirty five-family row adapter,
new metadata/dictionary tests, event native report/harness, dependency/archive/task
and row-format documentation, laboratory inventory and this checkpoint. No
contributor conflict or history rewrite. Verify status before continuing.

The candidate adds unpublished `apps/bigbrotr-archive-worker/`, exact native
registry/bundle/build/CI integration and scoped controls. Only the worker links
DuckDB. Current private command `inspect-engine` loads the offline signed bundle;
it does not publish rows, supervise a child, acquire a grant or certify readiness.
[Bundle documentation](../../packaging/archive-worker/README.md) describes the
required future parent pre-exec verification and sanitized loader environment.

## Actual evidence and findings

Raw root `.research/implementation/archive-worker/`; inventory entry
`implementation_archive_worker_20260918` in the
[laboratory inventory](../research/laboratory-inventory.json).

- Exact-source native Linux amd64 build of `d459fcd` completed exit0/no OOM.
  All 558 source entries, actual compiler tools, both ELF executables and native
  assets were independently compared to receipts. CLI SHA-256
  `6db13c0b97bc06c0643ac7f00c8c546ecbc635287f606a5a2a8a1b709c357b22`;
  worker SHA-256 `ada9b22386d14f71c79eb54ea5fcbb8093eb8dda1d16a826d60b19fbc8f27ec7`.
  `native-d459fcd/build-audit.json` owns details.
- Six actual native Rust tests and seven packaged runtime cases pass. The runtime
  runs with no network; strace observes zero network calls. Exact signed versions
  load; zero memory, nonempty temp and missing/altered assets refuse. Ordinary CLI
  works without DuckDB. A full unread 65,536-byte pipe blocks the worker; the
  external harness kills/reaps it in 0.0035s and observes EOF. This is not an
  implemented application supervisor. Results are in `native-test-evidence/`
  and `runtime-evidence/`; exact scenario sources are now in `experiments/`.
- Two thread probes observed 96 successful clones with query `threads=1`.
  Exact DuckDB source explains transient signature hashing: one thread per 1 MiB
  segment, 35 + 40 + 21 across sequential extension loads. A separate PID cap8
  case produced clone EAGAIN, worker SIGABRT in 0.165s, no output and restored
  baseline PID count. Observation harness success is not engine success. Preserve
  signed checking and account startup threads/stacks separately. Parent recovery,
  native peak RSS/stack usage and simultaneous thread peak remain unqualified.
- Mac arm64 incremental workspace build, worker Clippy/six tests and packaged
  executable-relative loading pass; ordinary CLI has no DuckDB linkage. This is
  development evidence, not a clean release build. Initial RED/default-autoload,
  invalid config-option and readonly corruption-fixture failures remain in logs.
- Build/CI controls: ten pass; intentional inner FAIL/NOT_RUN cases are negative
  controls. All 207 previous dependency versions/checksums remain, with 87 added
  packages. Exact-version license allowances/notices were reviewed and dependency
  policy passes. Full native distribution SBOM remains required.
- Four target asset sets are pinned, but Linux arm64 and Mac x86_64 runtime are
  NOT_RUN. Hosted Actions remain unavailable. No asset download qualifies a target.

Scientific rows/Arrow conversion, dictionary closure, PG DuckLake transactions,
positive fencing, ambiguous receipt recovery, protection/reclamation and complete
signed read/export/restart remain required. Pending data must not be reclaimed.
Binding caveat: Arrow list slices retain complete child arrays and native conversion
copies them; row count alone is not an allocation bound. Check appender flush
before commit. The previous reading covered the storage tasks/contracts, five
implemented fact families, dictionary descriptors, pending feed and exact binding
sources; reread changed boundaries as needed, not the entire repository.

## Independent monitor review

All five owner-supplied findings have scoped corrections and practical evidence:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json) at
`2c0dfc1`, [MON-005](evidence/p01-jsonb-limit-native-20260918.json) at `c7e0c09`;
qualification pushed in `a745f7f`. JSONB cap-1/exact10MiB work and replay; cap+1
and original compact-only counterexample refuse without Control mutation.
Historical failures remain unchanged. Signed admission qualification at `12f936d`
retains its [initial-owner scope](evidence/p02-event-admission-native-20260918.json).
No full P01/P02 or M1 completion follows. Do not repeat unrelated suites without
new changes or findings.

## Current typed-row slice and native state

The event-only candidate `2f4dd92` built on Tux, with all 568 source entries and
actual tools/artifacts independently audited. Native test ELF SHA-256
`9b904684765291e89c281bc9d51d5c2c0757f5307cc01f13a113338224a722de` executed all
11 cases (five new row cases, six engine cases), no failures/ignored tests.
The actual pinned C API vector size is 2048. Raw results and fresh cleanup audit
are in `.research/implementation/archive-worker/rows-2f4dd92/`; the concise
[event report](evidence/p02-event-rows-native-20260918.json) and exact
`experiments/archive-engine/native_event_rows.py` are currently dirty, not pushed.

The next uncommitted adapter now covers events, eligibility, receive, provenance
and all seven closed dictionary descriptor schemas through `Rows` constructors.
Mac scoped Clippy and all 12 row cases pass (`all-rows-mac-corrected.log`); six
unchanged engine cases were deliberately filtered in that last run. New metadata
and dictionaries have NOT_RUN native Linux qualification. A CREATE VIEW parameter
fixture failure and a fixture-only Clippy diagnostic were corrected; original
logs remain, and earlier RED outcomes are retained.

A real failure was found: nullable structs with valid placeholder children are
accepted by Arrow/native table SQL but can shift persisted values in Parquet.
The known observer method reference returned zeros after Parquet. Exact-source
inspection shows the pinned Rust binding only invalidates the struct parent,
while Parquet value writing uses child validity. The adapter now masks children
too, preserving the original logical nullability and required fields of present
references. Full ten-descriptor readback passes; the minimal paired raw-binding
control reproduces 73 becoming 0 without child masks and 73 with masks. A native
SQL-created null-struct control passes. This was never a qualified/published data
path. Raw failing/boundary/fixed logs and exact upstream sources are inventoried
under `nullable_struct_investigation`. No semantic hash or migration change.

All eight archive containers (six initial engine probes, two event-row build/test
containers) are stopped-retained. The latest event-row `terminal-preflight.json`
and `cleanup-audit.json` verify zero running BigBrotr containers, eight unrelated
workloads unchanged and the shared checkout clean/detached at
`6be851580ba415eed5ff0979ee7072480f9ee3ab`, with zero consumers. Do not reset it.
Latest native root is
`/home/tux/bigbrotr-implementation-20260917/runs/archive-2f4dd92`.
Build owner `0d56e74dab700b9c562cb4abc82328a5`; test owner
`bd273ab81927594e348f3cf26b8fb85d`. Full IDs/commands/exit observations are in the
inventory. No resources deleted; all earlier monitor/JSONB/admission labs remain
separately retained. Build collector 68799 and test collector 40443 completed;
there is no pending native job. Mac scoped session 43573 completed exit0.

Next: finish reviewing the five-family conversion/null-normalization diff, commit
it as a new unpublished exact-revision candidate, then run its actual native
adapter tests on Tux. Preserve the unmasked counterexample and fixed full-content
oracle, record exact identities and terminal resource state, then normally push
verified commits plus evidence. Continue directly with the real pending-batch
consumer, explicit archive schema/init, parent protocol/grants and positively
fenced PG DuckLake publisher. No publisher/receipt/protection/read/export/restart
or M1 gate is complete; do not stop at row conversion or reclaim pending facts.
