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

Mac main. HEAD and origin/main freshly verified at
`0bef27c1ca8abcc9ad3b62b4623d0f9efb8b85c3`, including qualified `a531ea3`.
Root owns the new uncommitted `experiments/archive-engine/catalog_startup.py` and
this checkpoint. No active experiment or contributor conflict. The startup
counterexample is written/reviewed but NOT_RUN. Exact candidate inputs continue
through Git; Tux is not a separately edited product workspace.

## Qualified boundaries

Earlier evidence remains retained under `.research/implementation/archive-worker/`
and [the inventory](../research/laboratory-inventory.json), key
`implementation_archive_worker_20260918`:

- [Engine](evidence/p02-archive-engine-native-20260918.json), [event rows](evidence/p02-event-rows-native-20260918.json),
  [five families](evidence/p02-five-family-rows-native-20260918.json) and
  [handoff](evidence/p02-pending-handoff-native-20260918.json): pinned native tuple,
  actual Parquet fidelity and complete private framing within their stated scopes.
  Nullable compound children require matching masks. Signed-extension startup
  creates threads independently of query threads; PID cap8 aborts the worker.
- [Private supervision](evidence/p02-private-supervision-native-20260918.json)
  at `c365053`: nonblocking bounded pipes, hash checks and retained grants;
  proven-full stdout/stderr termination; signed Rocks pending reaches the actual
  package twice with unchanged complete KV state. No archive ACK.
- [Parent binding](evidence/p02-parent-binding-native-20260918.json) at `e4e2a30`:
  Linux expected-parent checks/death SIGKILL positively reap an initialized stopped
  child in2.99ms. Old worker survives the negative control. Neither persisted
  recovery nor PG exclusion follows from parent-death registration alone.
- [Native catalog investigation](evidence/p02-catalog-fence-native-20260918.json)
  at `a531ea3`:600 source entries/tool/assets verified, explicit Rust probe SHA
  `793bd08ce5f99b32941da8485c0080c4656ed807ded41a7623b326a37b85e12d`.
  Four synthetic cases on PG16.15 pass: unfenced writers produce[2,2,2]; NOLOGIN
  refuses a fresh connection but an existing transaction commits[1,1,1]; after
  backend termination a paused old writer fails at COMMIT and replacement leaves
  [1,1,1]; lost COMMIT acknowledgement leaves exactly one committed triple.
  Counts do not establish body fidelity or physical durability. No product fence
  is selected, no production catalog/schema or publisher exists.

All five forwarded monitor findings have scoped closure, confirmed by rereading
`/Users/vincenzo/.codex/reviews/bigbrotr-rs-monitor/implementer-feedback.md`:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json) at
`2c0dfc1`, [MON-005](evidence/p01-jsonb-limit-native-20260918.json) at `c7e0c09`.
Signed admission at `12f936d` retains its initial-owner scope. Do not infer full
lifecycle/recovery completion or rerun unrelated suites.

## New findings and actual failures

Raw latest root `.research/implementation/archive-worker/catalog-fence`; native
root `/home/tux/bigbrotr-implementation-20260917/runs/archive-afc70f3`.

Three separately fresh catalogs failed before the four cases. First harness lost
the child phase; `913c6cc` records fixed early-exit phases. `449e63f` adds protected
redacted attach diagnostics. Actual cause: the pinned PG extension falls back to
missing `local_file` secret storage when no default secret exists, even though
persistent secrets are disabled. `a531ea3` supplies an explicit TEMPORARY memory
secret through META_SECRET and verifies its nonpersistent memory storage. Keep
META_SECRET_STORAGE_TABLE='' independently. No invariant or expected result was
weakened. Original FAILs remain in `cases-first`, `cases-phase`, `cases-diagnostic`;
`cases-memory` is the four-case PASS.

Initial research image build failed because BuildKit tried resolving a local
config digest at Docker Hub. `1fbddcb` uses a local tag with pre/post exact image
identity checks; resulting lab image is
`sha256:9d30ac361d158c281070d274427241bee955ead10904d6dbe4ff866200548f3d`,
psql15.19. This is not a BigBrotr distribution image or hosted CI success.

Exact PG16.15 source inspection found InitializeSessionUserId checks rolcanlogin
before pgstat_bestart publishes backend status; PostAuthDelay lies between them.
Reference files/hashes and scoped read boundary are inventoried under
`catalog_fence_probe.pg_startup_source`. In-flight startup is still NOT_RUN.
NOLOGIN plus an empty activity query is not established as a general fence.
Pinned postgres pool can ForceAcquire beyond its limit and Reset uses DISCARD ALL
or reconnect; an unrelated advisory lock cannot fence native DuckLake COMMIT.

Read full relevant probe/orchestrator, native pool/catalog/transaction/connection,
DuckLake initializer/storage and selected PG startup functions; relevant source
boundaries/hashes are inventoried. Root inline review only; no independent agent
review. No native peak, mixed/aged workload, full CI or arm64 runtime claim.

## Resource state and next bounded action

Collectors32410/34762/70858/56792 completed. Case launchers98951/21713/15247
completed exit1 for initialization failures; launcher3923 completed exit0.
Terminal audit confirms all12 newly owned containers stopped-retained, every
probe child reaped, zero remaining job backends and zero running BigBrotr
containers. All148 prior BigBrotr containers remain stopped; twenty unrelated
containers unchanged, eight running. Shared checkout remains clean/detached
`6be851580ba415eed5ff0979ee7072480f9ee3ab`, zero source consumers. Exact container,
network, path and owner identities are in `catalog_fence_probe.runs` and raw
resource receipts; do not reuse a prior catalog as a fresh experiment.

Next: commit/transfer the bounded `catalog_startup.py` and reproduce the PG16.15
post_auth_delay window with a real client in a fresh isolated lab. Show whether
an invisible connection can write after NOLOGIN plus empty activity; finish
cleanup and record the actual result. Then prioritize MON-009 below before any
new product consumer of job limits, followed by MON-010/MON-007. Return to full
publisher fencing/recovery and the signed M1 journey after those corrections.

## Newly forwarded independent review

Owner forwarded three static findings after monitor review through `a531ea3`.
Taken in charge at this safe boundary; no claimed reproductions or accepted fixes
yet. Use existing owning tasks, not another plan. Verify current code/contracts
and record counterevidence if a finding is invalid or already fixed.

- **MON-009, medium, P02-T01 ingest/job authority:** inspect complete owning
  0001/0002 migrations and later replacements/callers. Reproduce on isolated PG
  old topology with NULL handoff followed by authoritative closure N: job bound N
  must pass, N+1 must fail. Check job-first closure, both concurrent lock orders,
  multiple streams, zero prefixes and rollback. Published checksummed migrations
  stay immutable; use an additive migration with explicit incompatible-row
  handling and coherent locking of both operation directions. Resolve before
  consolidating dependent job-limit components. No data loss was observed.
- **MON-010, low, P01-T04 static configuration:** compare `resolve::tls` against
  real connection preparation. Offline verify-full must enforce the selected
  dependencies' required CA/server-name fields without opening files or connecting.
  Regress missing each/both, complete syntactically valid nonexistent paths, and
  unselected dependencies. Runtime currently refuses rather than falling back.
- **MON-007, low, P01-T03/P02-T04 archive bundle tooling:** inspect deterministic
  exclusive partial-file lifecycle in obtain/copy_verified. Reproduce failed
  download then success in the same cache and concurrent downloads. Publish only
  verified assets; isolate/clean owned temporary files without deleting another
  invocation's work. Add discriminating practical regression and exact evidence.

These three are open. Passing previous suites does not close them. Preserve the
completed five earlier findings and catalog investigation; do not conflate them
with these new triggers.
