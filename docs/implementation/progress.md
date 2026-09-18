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

Mac main, integrating root only. Freshly verified HEAD and origin/main both
`33267d018e6ee4e37db5e7a92d2e37c97acbf4e4`, clean before this slice. The three
monitor fixes and final reports are pushed. Root now owns the native startup
qualification harness/README and this checkpoint/inventory; no other writer.
Exact inputs travel through Git; Tux is never an independently edited product
workspace. Independent agents unavailable; root review only.

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

## Resources and next bounded action

All sessions from this slice completed: native build 60668, SQL 74814, retirement 5765,
Linux cache 34139, Mac tests 58123/24489 and Clippy 85606. All owned SQL sessions were
positively removed before stop. Four new PG/client containers, the native builder
and cache test container are stopped-retained. The two empty owned SQL networks
were retired by exact ID/label/endpoint checks; catalogs/data/reports remain.
Previous sixteen catalog/startup containers remain stopped; their six retired
networks remain retired. Zero BigBrotr containers run. Twenty unrelated containers
are unchanged, eight running. Shared Tux checkout remains clean/detached `6be851580ba415eed5ff0979ee7072480f9ee3ab` with zero consumers.

Detailed inventories: `implementation_job_bounds_20260918` and
`implementation_archive_cache_20260918`. Raw local roots:
`.research/implementation/job-bounds`, `tls-static`, `archive-cache`.
Native exact runs: `runs/job-bounds-6a6f796`, `runs/archive-02d2a9b`,
`runs/archive-cache-303763d` below the inventoried implementation root.
No active experiment or unfinished mutation needs joining.

Startup harness candidate `1b6c3116f118a51fe441460b90180c15e5600d69` ran and
FAILED its first case: AuthenticationOk arrived only after the injected six-second
delay, so waiting for it missed the invisible gap. No completed case is claimed.
Both owned containers stopped, zero remaining job backends, empty network retired.
Raw `native-startup/failed-result.json` retains the packet timings. The corrected
trigger observes the client SCRAM proof, then measures actual ReadyForQuery/SQL.

Next: commit the corrected startup harness as an unpublished test candidate,
transfer it through Git into a new clean Tux checkout and run its two cases with
the already qualified a531ea3 native probe/bundle (record both identities). New
`catalog_fence.py --case-set startup` uses a bounded loopback protocol observer,
real SCRAM and startup-packet post_auth_delay: compare an invisible live native
writer with positive SIGKILL/reap before a replacement commit. This is NOT_RUN.
The observer records frame counts/order, never credential/SQL payloads; startup
sleep is fault injection, not an exclusion timeout. All previous labs stay retired.

The exact PostgreSQL extension embeds libpq18.3 (actual PQlibVersion machine code
returns180003), despite server16.15. The upstream18.3 source tar matches the SHA512
pinned by extension41223e5. Read scope: PQconnectdb/pqConnectDBComplete, connection
AUTH_OK branch, PQsendQueryStart/PQexecStart and ReadyForQuery parser branch;
PG16.15 InitPostgres end/PostgresMain initialization/query-loop ordering and
backend-status snapshot cache. Blocking connect waits for ReadyForQuery; status
publication precedes that message. Activity reads must use fresh transactions.
These source findings support the proposed client-reap plus backend-exclusion
protocol, not a completed persistent product recovery implementation. Raw hashes,
read scope and files are under catalog-fence/libpq-18.3 and pg-source; inventory
tracks the narrowly added upstream sources. No pending reclamation.
