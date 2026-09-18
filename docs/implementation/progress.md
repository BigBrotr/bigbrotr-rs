# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint) and
verify actual Git, evidence and owned resources before continuing.

## Mandate and active boundary

Owner-authorized complete implementation remains active. Continue trusted native
work on Tux despite unavailable hosted Actions. No public release/rename or Linux
arm64 qualification. Go neighbor remains read-only. Active P02-T03/T04: bounded
typed rows and pending-to-worker preparation, then real PG DuckLake publication,
positive fencing, receipts/protection and M1 signed read/export/restart. No P02
task checkbox or M1 gate is complete; pending must not be reclaimed.

## Verified Git and ownership

Mac primary workspace, main. HEAD and origin/main were both freshly verified at
`c20e2ca86f19ddd3ad03a49dd645132ff1d2852f` after ordinary push, including the
qualified implementation `0cdd80fd0fa6991ee0b7957b9e65103a7b3e9fc1`.
Root owns uncommitted archive parent implementation/tests, target-specific asset
profile generation, the connected signed Rocks/native test, shared CI test-worker
binding and affected owning docs/inventory. Scoped Mac checks passed; a clean
unpublished candidate is next for native Linux qualification. No contributor
conflict or history rewrite.

## Qualified evidence

Raw root `.research/implementation/archive-worker/`; detailed ownership in
`implementation_archive_worker_20260918` in the
[laboratory inventory](../research/laboratory-inventory.json).

- [Initial engine qualification](evidence/p02-archive-engine-native-20260918.json)
  at `d459fcd`: exact source/tool/binary/native asset audit, six native tests,
  seven actual packaged runtime cases and offline signed load. External harness
  terminates a worker blocked on a full output pipe; production supervisor is
  still absent. Signature hashing creates transient threads independent of query
  threads (35+40+21 sequential extension loads); PID cap8 aborts the worker.
  Preserve signatures and separately budget startup threads/stacks. Peak RSS,
  production recovery and arm64 runtime remain unqualified.
- [Event-only native rows](evidence/p02-event-rows-native-20260918.json) at
  `2f4dd92`: all568 source entries audited; eleven tests pass, vector size2048.
  Signed extreme bodies survive native append/Parquet/fresh reader and fixture
  signature re-verification. Wide synthetic bodies are conversion evidence only.
- [Five-family native rows](evidence/p02-five-family-rows-native-20260918.json)
  at `5c3a8dd`: all574 source entries/tools/assets audited; eighteen tests pass.
  Every dictionary descriptor/envelope field and null survives actual Parquet.
  Pinned-binding negative control loses73 to0 with valid placeholders below a
  null struct; matching child+parent masks retain73. Exact upstream paths/hashes
  and failed intermediate logs are inventoried. Logical identity is unchanged;
  the failing path was never published and needs no migration.
- [Pending handoff/native preparation](evidence/p02-pending-handoff-native-20260918.json)
  at `0cdd80f`: all584 source entries/tools/assets audited; ten scoped Rust cases
  pass (five protocol, three native preparation, one executable, one Rocks).
  Independent wire includes all347 truncation positions. Four ELF test artifacts
  and the Cargo-executed worker are retained. Cargo relinked a different worker;
  both complete/truncated cases separately pass against the exact packaged SHA
  `642952149ddef947370d04f20811e59f43f4f7ee054b7d03f3eea235635a9995`.
  Preparation fixture is synthetic; Rocks and executable boundaries are tested
  separately, not a complete signed pending-to-worker journey.

Only the private worker links DuckDB. `inspect-engine` and `inspect-batch` grant no
publication authority. The new application parent implements bounded nonblocking
pipes, pre-exec hash checks, sanitized child environment and retained grants for
private inspection. Native/role resource qualification and parent-crash/backend
exclusion remain required. No hosted Actions or arm64 runtime PASS.

## Independent monitor review

All five forwarded findings have scoped closure, also confirmed by rereading the
external implementer-feedback note: [MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json)
at `2c0dfc1`, [MON-005](evidence/p01-jsonb-limit-native-20260918.json) at `c7e0c09`;
qualification pushed in `a745f7f`. Signed admission at `12f936d` retains its
[initial-owner scope](evidence/p02-event-admission-native-20260918.json).
Do not rerun unrelated suites or infer whole-task completion.

## Current implementation and review boundary

Private framing reuses canonical fact preimages and complete admission membership.
It checks sizes before allocation, per-fact digest/source, then batch digest/EOF;
errors poison the reader. One decoded fact enters private native staging at a
time. The Rocks producer keeps its pending-data permit during the write.
`PreparedBatch` consumes the engine and discards partial staging on failure.
`inspect-batch` returns bounded diagnostics only after complete validation.
No archive ACK, permanent catalog schema, publication or pending reclamation.

Read complete owning protocol, preparation/schema, executable, pending producer,
resource budget, build/bundle, CI and affected test helpers; reviewed the complete
new parent source and contracts inline. No independent agent review in this slice.
The parent retains memory/control capacity until positive reap. On unresolved
termination it fails closed and preserves charges; job-root cleanup/retry is
forbidden. Parent SIGKILL/orphan exclusion is a separate next gate before a
publisher can be admitted. No permanent catalog/schema or publication exists.

Pinned DuckLake `d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4` constructs its own
internal connection for PG metadata transactions (`DuckLakeTransaction::GetConnection`/
Commit and PostgresMetadataManager::ExecuteQuery). A lock in another connection
cannot fence those commits. Positive backend exclusion and actual paused-owner/
ambiguous-commit tests remain required; no speculative fence is selected.

## Lab resources and next bounded action

Latest native root:
`/home/tux/bigbrotr-implementation-20260917/runs/archive-0cdd80f`.
Local `.research/implementation/archive-worker/handoff-0cdd80f` owns raw build/test
receipts, audit/collector scripts, terminal preflight and cleanup audit. Build
owner `fb482a2e1f447c67a5031d4ec510bf9d`, container ID
`e8c5dbc8fe33ac633ece8d8bca1e8cdb26a288e914c955750113e7d61eb48de4`;
test owner `328a7081471b221be868fca195659326`, ID
`b230cf87271dca06e81c0bff50c81561c474a6d9ae39c48259466c6da7a41266`.
Both exited0/no OOM, stopped-retained. Collectors50502/52076 completed exit0.
Fresh terminal audit: all ten earlier archive containers still stopped; zero
running BigBrotr containers. Twenty unrelated containers (eight running) unchanged.
Shared source clean/detached at `6be851580ba415eed5ff0979ee7072480f9ee3ab`, zero
consumers. Do not reset it or delete retained experiments without lifecycle review.

Parent Mac source/log identities are recorded in `parent_supervision_mac` and
`.research/implementation/archive-worker/parent-mac/identity.json`. Seven real
process and two file-verification tests pass; the signed event admission/reopen/
pending-to-worker test passes twice with counts[1,1,1,1,0] and unchanged complete
Rocks KV state. Reduced transfer cap, wrong executable, cancellation, nonempty
root and unavailable capacity leave state intact. Three CLI and five CI controls
pass. Final all-target application Clippy passes (session75584 exit0); earlier
native rebuild/check session56698 and final Mac test session65242 completed exit0.
Initial REDs and the incorrectly imported non-executed attempt are retained. The
reduced transfer cap originally surfaced Process(Exit); its regression now returns
Transfer(Limit), with TerminationUnknown retaining precedence. Linux-only full
pipe capacity assertions and their timing diagnostics are NOT_RUN.

Shared CI now makes the exact-source package before workspace tests and supplies
the verified packaged worker through `--rust-tests-from-build`. Its positive
native build-receipt helper path still needs qualification; no complete CI PASS.
No new remote resource was started during this local slice.

Next: commit the reviewed Mac-checked parent slice as an unpublished candidate,
freshly inventory Tux, transfer a clean Git bundle and build its exact source in a
new isolated root. Run native archive/process tests (ten on Linux including truly
full stdout/stderr), the connected signed test, affected CLI and CI controls;
verify artifacts/cleanup before pushing. Then address parent-death/orphan
exclusion and full native grants/measurements before real PG-fenced publication.
Receipt/protection/read/export/restart and the complete product remain unfinished.
