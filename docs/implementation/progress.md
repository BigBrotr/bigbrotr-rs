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

Mac primary workspace, main. HEAD and origin/main freshly verified after ordinary
push at `3063d12e8d39195b031e767b1ef7a76a86b78276`, including qualified source
`c365053afae92eb2cf8b10760e0d8bec2f42a897`. Root owns the next parent-death binding:
Cargo.lock, worker manifest/main/tests, parent installation and owning contracts,
packaging docs, inventory/checkpoint. Mac source/log/package identities are under
`parent_death_mac` in the inventory. No contributor conflict. This next binding
is Mac-checked but not yet a native-qualified revision; prepare a clean candidate.

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

- [Private parent supervision](evidence/p02-private-supervision-native-20260918.json)
  at `c365053`: all591 source entries/tools/assets audited; fourteen Rust cases
  (ten archive, one signed connected Rocks/worker case, three CLI), five CI
  controls and application all-target Clippy pass. The shared CI helper validates
  the actual packaged worker. Both output pipes are proven full at65,536bytes;
  positive termination takes stdout2089/stderr2073 microseconds without draining.
  Signed pending reaches the exact package twice with counts[1,1,1,1,0] and
  unchanged complete Rocks KV state. Refusal controls retain state/grants.
  This qualifies private inspection while the parent lives, not publication.

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
`/home/tux/bigbrotr-implementation-20260917/runs/archive-c365053`, local
`.research/implementation/archive-worker/supervision-c365053`. All three owned
containers completed exit0/no OOM and are stopped-retained:

- Build owner `b8c4e91f5225198ffcd13804bcf4330a`, ID
  `153750054b9c73c5741ee921dc4029f37d26ba55c5a247e31e38a201ce3c828e`.
- Tests owner `09047a050b7b692edc70d23926ec997f`, ID
  `4da509c0963c125f99bed820a668fde9bf60bec13c2ccb17d7584e432c9dd628`.
- `orphan-baseline` owner `e9fba895100e2fa24e5015cf7c7a661b`, ID
  `fdde43718199b3970b9739f8bf207e1c060981e49b4924ee5d87301f9ec3fdc3`.

Collectors99084/71791 completed exit0. Terminal preflight/cleanup audit confirms
zero running BigBrotr containers, twelve earlier archive containers stopped and
all twenty unrelated containers unchanged (eight running). Shared source remains
clean/detached `6be851580ba415eed5ff0979ee7072480f9ee3ab`, zero consumers.
Do not reset it or delete retained resources without lifecycle review.

The orphan negative control verified actual engine startup/read(0), stopped the
packaged worker and killed its parent: the worker survived the full one-second
observation. The harness then killed/reaped that exact worker using pidfd/subreaper
and reaped the parent. This proves the missing guarantee, not a product PASS.

Mac parent-supervision source/log identities remain under `parent_supervision_mac`
and `.research/implementation/archive-worker/parent-mac/identity.json`. Original
REDs, mistaken-import non-execution and Transfer(Limit) regression are retained.
Native qualification now includes the formerly unexecuted full-pipe/CI-helper cases.
No full CI, release authentication, peak-memory or PG fence PASS.

The next dirty implementation supplies/checks expected parent PID before engine
work and arms Linux SIGKILL on spawning-thread death, with a post-registration
parent check. Mac checks startup parentage only. Exact rustix1.1.5 source and Linux
manual were inspected; negative PID must be checked before Pid::from_raw. Actual
Mac RED showed missing parent accepted; another RED showed negative PID panic.
Corrected missing/malformed/foreign parent refusal, complete/truncated handoff,
worker Clippy and signed supervised handoff passed. Session91281 completed exit0.
Logs `parent-binding-{red,green,negative-red,final-mac}.log` and
`parent-death-connected-mac.log`; new Mac package/environment under
`.research/implementation/archive-worker/parent-death-mac`. Positive Linux
parent-death, persisted recovery and PG exclusion remain NOT_RUN.

Next: commit the reviewed Mac-checked parent-death implementation as a separate
unpublished candidate; freshly inventory Tux and qualify it through an exact Git
bundle/new isolated root, including the stopped-worker positive case. Run both
worker integration cases and the signed supervised case plus affected Clippy.
Preserve the successful old-worker negative control and all stopped resources. Full native grants and persistent/PG exclusion still gate publication.
Receipt/protection/read/export/restart and the complete product remain unfinished.
