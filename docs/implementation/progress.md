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

Mac primary workspace, main. Origin/main was freshly verified at
`e6f6a6980f9c0325343ff06ed65f8450204d9ced`. HEAD is qualified candidate
`0cdd80fd0fa6991ee0b7957b9e65103a7b3e9fc1`, tree
`01974974508f1b4c3380abef946665d864569135`. It was clean when bundled/transferred.
Only root-owned qualification report/harness, inventory, checkpoint and affected
documentation are dirty, ready for a coherent qualification commit and ordinary
push. No contributor conflict or history rewrite. Verify subsequent HEAD/remote
instead of treating this precommit observation as their final state.

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

Only the private worker links DuckDB. It implements `inspect-engine` and
`inspect-batch`; neither grants publication authority. Parent pre-exec executable/
library verification, sanitized loader environment, whole-process grants and
bounded termination remain required. Native assets are not runtime qualification.
No hosted Actions PASS. Earlier scoped Mac tests, all-target Clippy and failures
are retained in the inventory; no Mac or native job remains running.

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
resource budget, build/bundle and tests; reviewed the qualification harness and
actual outputs. The next parent must own blocked I/O and termination, retaining
capacity until exit is positively observed. General worker supervision cannot
reuse the process-exit-only lifecycle diagnostic thread as a transport.

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

Next: verify qualification documentation/artifact checks, commit and push. Then
implement the parent pre-exec verification and nonblocking supervised process
handoff with real blocked stdin/stdout/stderr, timeout/cancellation/crash and grant
retention tests. Connect actual signed Rocks pending to that process. Continue
with explicit archive init and positively fenced publication; receipt/protection/
read/export/restart and the complete selected product remain unfinished.
