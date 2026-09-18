# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).
Verify Git, dirty work, evidence and resources before continuing.

## Mandate and active boundary

Owner-authorized complete implementation remains active. Continue trusted native
work on Tux despite unavailable hosted Actions. No public release/rename, Linux
arm64 qualification or changes to the read-only Go neighbor. Active P02-T03/T04:
real PG DuckLake publication after private pending preparation, positive fencing,
receipts/protection and M1 signed read/export/restart. No P02 task or M1 gate is
complete. Pending must not be reclaimed. Whole selected product remains required.

## Git and ownership

Mac main, integrating root only. Freshly verified HEAD
`d4f43af4f2f264886053338cffa664611065569c`, origin/main
`8d8cdf58af95262d713876a5be97ab0a1b513f63`, clean before evidence updates.
The unpublished HEAD is the exact native44-case candidate; earlier MON/startup/guard
evidence is pushed. Root owns report/inventory/README/checkpoint updates. No other writer.
Source transfers through Git, readonly Tux source, root review only.

## Qualified product prerequisites

Earlier evidence remains retained under `.research/implementation/archive-worker/`
and [the inventory](../research/laboratory-inventory.json), key
`implementation_archive_worker_20260918`:

- [Engine](evidence/p02-archive-engine-native-20260918.json), [event rows](evidence/p02-event-rows-native-20260918.json),
  [five families](evidence/p02-five-family-rows-native-20260918.json) and
  [handoff](evidence/p02-pending-handoff-native-20260918.json): pinned native tuple,
  actual Parquet fidelity and private framing within their stated scopes.
  Nullable compound children need matching masks. Extension startup creates
  threads independently of query threads; PID cap 8 aborts the worker.
- [Private supervision](evidence/p02-private-supervision-native-20260918.json)
  at `c365053`: bounded nonblocking pipes/hash checks/retained grants; full stdout/
  stderr termination; signed Rocks pending reaches the package twice with all KV
  state unchanged. No archive ACK. Unknown termination cannot release grants.
- [Parent binding](evidence/p02-parent-binding-native-20260918.json) at `e4e2a30`:
  Linux expected-parent checks/death SIGKILL positively reap an initialized stopped
  child in 2.99 ms. Old native worker survives the negative control. Neither
  persisted recovery nor PG exclusion follows from parent-death registration alone.

## Catalog findings to carry into the publisher

[Native catalog investigation](evidence/p02-catalog-fence-native-20260918.json)
at `a531ea3` verifies 600 source entries/tools/assets and probe SHA
793bd08ce5f99b32941da8485c0080c4656ed807ded41a7623b326a37b85e12d.
Four real Rust DuckLake/PG 16.15 synthetic cases: unfenced writers produce [2,2,2];
NOLOGIN rejects fresh attach but an existing writer commits [1,1,1]; after backend
termination the paused old writer fails at COMMIT and replacement leaves [1,1,1];
lost COMMIT acknowledgement leaves exactly one triple. Counts do not prove body
fidelity or physical durability. There is no product catalog/schema/publisher yet.

Three original initialization FAILs remain retained. Pinned PG extension falls
back to unavailable local_file secret storage when no explicit memory secret is
provided. Qualified fix uses a TEMPORARY memory postgres secret via META_SECRET,
checks nonpersistent memory storage and retains META_SECRET_STORAGE_TABLE=''.
Persistent secrets/autoload/community extensions remain disabled. Exact upstream
pool can ForceAcquire beyond its nominal limit and Reset DISCARD ALL/reconnect;
an unrelated Control advisory lock cannot fence native DuckLake COMMIT.

[PG startup counterexample](evidence/p02-postgres-startup-native-20260918.json)
at `de1d402`: real psql 15.19/PG 16.15 with startup-packet post_auth_delay proves an
invisible old connection writes marker 73 after NOLOGIN and empty activity. A
fresh same-credential connection refuses. The old client remained alive, then
exited/reaped after 6.03 s. A one-second observation delay is not a fencing timeout.
Exact REL_16_15 source places rolcanlogin checking before PostAuthDelay and
pgstat_bestart. NOLOGIN plus an empty activity scan is not a general exclusion
proof. Product recovery must positively exclude old clients/processes as well as
resolve old backend transactions; this stronger protocol is still unselected.

Read scope/hashes and original failed roots are inventoried under
`catalog_fence_probe`, with raw local root
`.research/implementation/archive-worker/catalog-fence` and native root
`/home/tux/bigbrotr-implementation-20260917/runs/archive-afc70f3`.
No native peak/mixed/aged workload, full CI, product fence or ARM claim.

## Independent monitor corrections

Earlier five findings remain scoped closed:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json) at `2c0dfc1`,
[MON-005](evidence/p01-jsonb-limit-native-20260918.json) at `c7e0c09`. Signed admission
at `12f936d` keeps its initial-owner scope. Do not rerun unrelated suites or infer
full lifecycle/recovery completion.

The three newly forwarded findings have now been independently reproduced and
corrected, without interrupting the preceding catalog laboratory:

- **MON-009 / P02-T01:** [native report](evidence/p02-job-bounds-native-20260918.json).
  Old migrations 1/2 accept job 6/closure 5 in both orders with NULL old handoff.
  Additive 0008 preserves earlier SQL and uses authoritative source caps, sorted
  SHARE source locks for jobs and reverse closure checks with an indexed lookup.
  Explicit READ COMMITTED is required; fixed-snapshot isolation refuses 25001.
  Incompatible existing rows refuse 55000 with a specific CLI error and unchanged
  whole schema/data/ledger. Exact02d2a9b binary SHA
  65cb511e0d016436b1d6df90103f04b5820a9ae7ea471824429c7a5431a9c7d2
  passes 24 actual PG 16.15 cases: eligible/refused upgrade, N/N+1, inverse order,
  eight observed lock schedules with commit/rollback, multiple streams/zero/
  omissions and isolation refusals.606 source files independently match Git
  archive/manifest/artifact. Mac 11 planner tests and scoped Clippy pass. No job
  dispatch, source receipt, production load or full upgrade claim.
- **MON-010 / P01-T04:** [offline report](evidence/p01-tls-presence-20260918.json).
  Missing CA/server name independently reproduces on the old Mac executable.
  Selected PG Control/catalog and ClickHouse now require both; unselected
  dependencies permit incomplete staged declarations with syntax checks.
  Actual CLI 24 configuration tests, including 24 new combinations and nonexistent
  certificate/secret paths, plus scoped Clippy pass on Mac. Generated reference
  remains in parity. No new native Linux TLS/runtime qualification claimed.
- **MON-007 / P01-T03/P02-T04:** [cache report](evidence/p01-archive-cache-retry-20260918.json).
  Real HTTPS truncation poisons old deterministic partial; new private attempts
  publish only verified assets via an exclusive atomic hard link. Owned failures
  clean up; another attempt/historical partial stays intact. Five tests pass on
  Mac and exact `303763d` Linux amd64, including actual HTTPS retry and concurrent
  downloads, size/hash/read failures, preexisting-path preservation and ZIP checks.
  Cache requires hard links; SIGKILL can leave unique harmless staging residue;
  no cache power-loss durability is claimed.

No invariant or expectation was weakened. Root reviewed all changed source and
relevant callers/contracts. Artifact/handoff checks report no errors; their three
historical dependency-cycle candidates remain unchanged navigation warnings,
not product tests. No full CI or independent agent review is claimed.

## Current qualification and resources

[Native startup exclusion](evidence/p02-native-startup-exclusion-20260918.json)
now passes two cases at harness56ef588 with exact a531ea3 Rust probe SHA
793bd08ce5f99b32941da8485c0080c4656ed807ded41a7623b326a37b85e12d.
610 source files independently match Mac Git archive and clean Linux readonly
checkout. Actual PG extension binary embeds libpq18.3 (PQlibVersion180003);
matching source tar verifies against the extension overlay SHA512.

The actual live native negative control publishes after NOLOGIN/empty activity.
The positive case reaps the old client in3.16ms, commits its replacement before old
server EOF, then observes late ReadyForQuery/server EOF with
zero old SQL and final counts[1,1,1]. A bounded loopback relay observes message
order/counts, never payloads. Startup delay triggers the case; it is not a fence.
Initial1b6c311 attempt FAILED before completing a case because AuthenticationOk
was buffered until startup completion; corrected trigger observes the actual
SCRAM proof. Both results retained. No production schema/receipt/protection claim.

The [archive authority contract](../specification/archive-storage-contract.md#configuration-and-authority)
now explicitly requires exclusion of all old credential holders, then resolution
of remaining backends and a fresh receipt transaction. Unknown old host/process
state cannot authorize replacement. P02-T04 still needs persisted ownership before
credential handoff and actual successor recovery; this is not finished by the probe.
Source read scope: blocking connect/connection AUTH_OK/query-start/ReadyForQuery
branches in exact libpq18.3; PG16.15 relevant InitPostgres/PostgresMain/status-cache
branches; native extension connection wrapper and existing private Process/worker.
Not a complete libpq/server review. Persistent product recovery callers do not yet
exist. Scope, source URLs/hashes and exclusions are in the report/inventory.

All sessions terminal: source transfers68652/72187, native launch5731(FAIL)/36184
(PASS), result transfer9386. Four new owned containers are stopped-retained with
no OOM; zero job backends before PG stop. Both exact-label/ID empty networks retired.
Fresh independent audit: zero BigBrotr containers running;20 unrelated identities,
process incarnations, mounts and networks unchanged,8 running. Dynamic healthcheck
logs and nondeterministic mount ordering are not identity changes. Shared checkout
last observed clean/detached6be8515 in the prior slice; never used here.

Inventory entry: `implementation_archive_worker_20260918.native_startup_exclusion`.
Local raw root `.research/implementation/archive-worker/catalog-fence/native-startup`;
exact upstream under sibling `libpq-18.3`/`pg-source`. Native roots beneath
`/home/tux/bigbrotr-implementation-20260917/runs/`:
`native-startup-1b6c311` and `native-startup-56ef588`. Retain source, data and reports;
do not restart containers whose networks were retired. Earlier laboratories and
MON qualification resources remain stopped as inventoried. No active experiment.

## Current catalog guard alternative and next bounded action

[Six native guard cases](evidence/p02-catalog-guard-native-20260918.json) PASS:
-694be48: ready and invisible-startup old native clients remain alive and LOGIN;
 replacement publishes; old COMMIT fails with actual PG guard refusal; counts[1,1,1].
-df2a0db: actual native writer versus takeover in both lock orders, each holder
 commit/rollback. pg_blocking_pids/pg_locks and both backend incarnations establish
 blocking. Writer-first commit resolves present receipt; writer rollback resolves
 absence. Takeover-first commit rejects old writer; takeover rollback permits it.
 Independent reopened reads match literal expected before-replay and final counts.

The isolated SQL guards all28 exact initialized native metadata relations with
BEFORE STATEMENT triggers. SECURITY DEFINER uses session_user and FOR SHARE on a
separate owner singleton; takeover updates that row. No native values/columns are
rewritten. Ordinary writer loses DDL/trigger privileges. The lab-only later
snapshot/advisory pause trigger establishes race order, never the candidate fence.
Full PostgresMetadataManager cpp/hpp, DuckLakeInitializer, PostgresTransaction and
PostgresStorage read; relevant InitializeDuckLake/Commit/RunCommitLoop branches
read. Default native PG isolation is REPEATABLE READ by source, not a directly
queried runtime setting. No full DuckLake mutation/maintenance audit is claimed.

Source audits independently match612/613 files to Mac Git archives and clean
readonly Tux checkouts. Reused native probe remains a531ea3/SHA793bd08ce5f99b32941da8485c0080c4656ed807ded41a7623b326a37b85e12d.
Current raw root `.research/implementation/archive-worker/catalog-fence/commit-guard`;
native roots `runs/catalog-guard-694be48` and `runs/catalog-guard-df2a0db` under the
same inventoried implementation root. Four additional owned containers stopped,
no OOM, no job backends; two exact-label/ID empty networks retired. Fresh audit
zero BigBrotr running;20 unrelated identities/processes/mounts/networks unchanged,
8 running. Source transfers18138/40940, runs36205/86873, copies22627/69729 all
terminal. Inventory key: `implementation_archive_worker_20260918.catalog_commit_guard_probe`.
No active experiment; do not restart retired-network containers.

The candidate remains UNSELECTED as a product fence. `guard-privileges` at
`d4f43af` passes44 cases:14 actual limited-role permission refusals, all28 zero-row
table guards, one role/search-path shadow case and one native positive publication.
Independent source audit matches614 files to Mac Git archive;30 actual PG guard
errors. Two new containers stopped-retained, no OOM/backends, empty network retired;
20 unrelated containers unchanged,8 running, zero BigBrotr running. Source transfer
86579/native run27074 terminal. Raw privileges-result/resources/log and
audit-privileges.json retained under the same raw root; native root
`runs/catalog-guard-d4f43af`. Inventory has the exact container/network identities.
Next: review/commit/push this qualification, then audit native ordinary mutation
coverage with inlining disabled/no runtime DDL.
Check all owned schema/trigger identities, altered/missing guard refusal, fixed
snapshots and exact fresh setup/upgrade behavior before adopting it. Production
adoption needs an explicit owning-contract exception for versioned metadata
triggers, typed epoch/token/CAS and initialization/migration; the experimental
login singleton is not that implementation. Keep resource/orphan cleanup separate
from catalog commit safety. If it fails, retained positive client/process exclusion
is the fallback, not NOLOGIN/empty activity.

Then implement persistent publisher attempt identity, scientific catalog/schema
and atomic receipt-backed signed publication. Shared finite user jobs do not
include internal archive attempts; do not shoehorn these into export/import or
mistake the existing Control session fence for native authority. Existing pending
and PreparedBatch are real consumers, but have no archive semantic batch type yet.
No pending reclamation, full P02 checkbox or M1 completion before actual protection,
read/export/restart acceptance. Do not reopen architecture without a new failure.
