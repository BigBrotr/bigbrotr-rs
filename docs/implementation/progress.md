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

Mac primary workspace, main. HEAD `913c6ccffca14d2217b961327a439128e672afde`
contains candidate `afc70f37626a94750df5933a345839e3ffb03fac` plus the isolated
runner Dockerfile and early-exit diagnostic corrections. All unpublished. Origin/main last freshly verified
at `c6fface491bffe3d774d77c5debe0d337b762ce9`. Candidate sources were clean at Git
bundle creation. Root owns only checkpoint/inventory/evidence updates. No other
writer or contributor conflict; Tux source copies are immutable Git inputs.

## Qualified evidence

Raw root `.research/implementation/archive-worker/`; detailed ownership in
`implementation_archive_worker_20260918` in the
[laboratory inventory](../research/laboratory-inventory.json).

- [Initial engine qualification](evidence/p02-archive-engine-native-20260918.json)
  at `d459fcd`: exact source/tool/binary/native asset audit, six native tests,
  seven actual packaged runtime cases and offline signed load. External harness
  terminates a worker blocked on a full output pipe; production supervision
  was absent at that revision. Signature hashing creates transient threads independent of query
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

- [Parent binding](evidence/p02-parent-binding-native-20260918.json) at `e4e2a30`:
  all595 source entries/tools/assets audited. Two worker integration cases,
  signed connected case and application/worker Clippy pass. Exact packaged SHA
  `3133542df86c6d2992911434aff6292b232c6ea1f06235ed8e0d437765e3c136`
  reaches real read(0), is proven stopped, then exits on parent SIGKILL in2.99ms;
  pidfd and waitpid establish positive reap. Cargo-relinked binary has a different
  retained hash. No persisted job recovery or PG/backend fence is inferred.

Only the private worker links DuckDB. `inspect-engine` and `inspect-batch` grant no
publication authority. The new application parent implements bounded nonblocking
pipes, pre-exec hash checks, sanitized child environment and retained grants for
private inspection. Native/role resource qualification, persisted recovery and
backend exclusion remain required. No hosted Actions or arm64 runtime PASS.

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
new parent source and contracts inline. No independent agent review in these slices.
The parent retains memory/control capacity until positive reap. On unresolved
termination it fails closed and preserves charges; job-root cleanup/retry is
forbidden. The Linux parent-death case is qualified; persisted job recovery and
positive PG/backend exclusion remain gates before a publisher can be admitted.
No permanent catalog/schema or publication exists.

Pinned DuckLake `d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4` constructs its own
internal connection for PG metadata transactions (`DuckLakeTransaction::GetConnection`/
Commit and PostgresMetadataManager::ExecuteQuery). A lock in another connection
cannot fence those commits. Positive backend exclusion and actual paused-owner/
ambiguous-commit tests remain required; no speculative fence is selected.

## Lab resources and next bounded action

Latest native root:
`/home/tux/bigbrotr-implementation-20260917/runs/archive-e4e2a30`, local
`.research/implementation/archive-worker/parent-death-e4e2a30`. Both owned
containers completed exit0/no OOM and are stopped-retained:

- Build owner `233ad227d3bdd5b8bb3e26b21e5f3974`, ID
  `3a2dcd1945acd801a90baad31733819969732871ac767b5f6c5b81ea87e95bde`.
- Tests owner `9294675fc34813f3818f8966b954bf58`, ID
  `9a3cbe025727fd56eb66d14678a85feb4657fc97fcbd85b742030e93ed06f49f`.

Collectors83989/34981 completed exit0. Fresh terminal preflight/cleanup audit
confirms zero running BigBrotr containers, fifteen earlier archive containers
stopped and twenty unrelated containers unchanged (eight running). Shared source
remains clean/detached `6be851580ba415eed5ff0979ee7072480f9ee3ab`, zero consumers.
The earlier `archive-c365053` root retains build, supervision and orphan negative
control; its three containers remain stopped. Do not reset/delete these inputs.

Mac source/log/package identities are under `parent_supervision_mac` and
`parent_death_mac`. Original stub REDs, mistaken-import non-execution, corrected
Transfer(Limit), missing-parent acceptance and negative-PID panic are retained.
Corrected Mac worker cases/Clippy and signed connected case passed; session91281
completed. Linux refusal/transfer, signed connection and stopped-parent case are
now qualified. No full CI, release auth, native peak memory or PG fence PASS.

Focused next-fence source inspection is inventoried under
`postgres_fence_source_inspection`: read-only reference checkout
`.research/checkouts/postgres-archive-fence` at
`41223e51559cd581f1c06e170b71c71df25bbaac`. Pool/header, transaction, connection and
catalog implementations read completely; scanner forced-acquisition and setting
boundaries only partially read. ForceAcquire can exceed the pool limit. Reset
uses DISCARD ALL or PQreset, clearing session state/reconnecting. A health-check
name or pool limit is not a commit fence. No catalog fencing design is selected
and no new PG runtime experiment ran. Ordinary catalog attach must also disable
optional PG secret storage explicitly; do not assume native persistent-secret
settings alone cover the extension's separate registration path.

The candidate `catalog_fence.rs` probe is an explicit Cargo example of the
private worker package (not an installed product command). Three canary relations,
init/read/write/lost-ack modes and stdin barriers expose actual native transaction
boundaries. Mac build/Clippy passed; actual PostgreSQL cases remain NOT_RUN.
The finite orchestrator validates cluster identity, bounds cases and reaps children.
No scientific data, archive ACK or selected fencing authority is inferred.

Native build completed exit0 with600 source entries/tools/assets audited at `/home/tux/bigbrotr-implementation-20260917/runs/archive-afc70f3`:
owner `0d4ac95f90da6fec55afba6709b662cb`, container ID
`a3fa76ce33e3f9edfdf18b8c510b5bb53895df66d28fd6fafc78dbe195fc4aa2`.
Collector32410 completed. Explicit native probe build/collector34762 also passed;
artifact SHA `a35342747758e5f06422c0efb671e01ec2c3f4afc2f546d8372d9be353d0f26a`.
The first fresh PG16.15 run failed during initialization before all four cases.
Its harness omitted the child's fixed failure phase on early EOF; the current
correction records that phase. Raw `cases-first` evidence retains this FAIL.
Launcher98951 completed exit1; both PG and runner stopped-retained, every child
reaped, zero remaining job backends. Do not reuse this catalog as a fresh run.
Initial image build failed because BuildKit resolved a local config digest via
Docker Hub. Candidate `1fbddcb` corrects the local tag with pre/post image-ID
checks. The retry built image
`sha256:9d30ac361d158c281070d274427241bee955ead10904d6dbe4ff866200548f3d`
with psql15.19; logs/source/launcher are retained locally under
`.research/implementation/archive-worker/catalog-fence`. No hosted CI claim.
Fresh preflight: shared checkout unchanged/clean, zero source consumers, zero
previously running BigBrotr containers, twenty unrelated containers,306GiB free.

Next: commit/transfer the diagnostic correction, repeat against a separately
owned fresh PG16.15 lab with the same exact probe binary, and diagnose the phase.
Its four native cases exercise unfenced duplicates, sealed login with a live
transaction, terminated backends with paused old client, and lost COMMIT ACK.
Record exact artifacts and terminal resource state even on failure.

Additional PG16.15 source inspection found `InitializeSessionUserId` checks
rolcanlogin before `pgstat_bestart` publishes backend status; `PostAuthDelay`
lies between them. This identifies an untested startup race for any protocol
that assumes NOLOGIN plus an empty activity query is sufficient in isolation.
Downloaded exact-tag reference files are under the same ignored root in
`pg-source`; they are reference material, not an edited PostgreSQL build.
Do not infer production exclusion from the four quiesced-client cases.
Read the remaining owning catalog/identity/operations dependencies before editing.
A possible investigation is a dedicated per-job PG role with native connection
setup quiesced, new logins revoked before write authorization, and positive
exclusion of every established backend on recovery. This is an unselected
hypothesis: test late connection/reset, paused old owner and ambiguous commits;
NOLOGIN or an unrelated advisory lock alone cannot certify exclusion.
Persisted job recovery and complete native resource admission/measurements remain
production gates. Keep P02-T04 foundation versus acceptance splits explicit;
receipt/protection/read/export/restart and the complete product remain unfinished.
