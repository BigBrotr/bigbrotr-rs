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

Mac main. Last observed HEAD `6a6f796` (unpublished negative-control harness);
origin/main verified at `03819daa517024ce4f08d7e2cdbc48d0e47f4baa`.
Root owns MON-009 additive migration, fixture, contracts and evidence. No other
writer/conflict. Tux receives clean exact Git inputs, never independent edits.

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
- [PG startup counterexample](evidence/p02-postgres-startup-native-20260918.json)
  at `de1d402`: real psql15.19/PG16.15 with startup-packet post_auth_delay proves
  an invisible old connection commits one marker73 row after NOLOGIN and empty
  activity. A fresh same-credential connection refuses; old client exits0 after
  6.03s and is reaped. This disproves a general exclusion inference, not a product
  safeguard. The one-second observation delay is not a fencing timeout.

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
`catalog_fence_probe.pg_startup_source`. The startup window is now reproduced with a real client; NOLOGIN plus empty
activity alone is demonstrably insufficient as a general fence.
Pinned postgres pool can ForceAcquire beyond its limit and Reset uses DISCARD ALL
or reconnect; an unrelated advisory lock cannot fence native DuckLake COMMIT.

Read full relevant probe/orchestrator, native pool/catalog/transaction/connection,
DuckLake initializer/storage and selected PG startup functions; relevant source
boundaries/hashes are inventoried. Root inline review only; no independent agent
review. No native peak, mixed/aged workload, full CI or arm64 runtime claim.

## Resource state and next bounded action

All catalog and startup collectors/launchers completed. Startup setup first
failed before creating a Docker network because default address pools were full.
After verified retirement of four empty terminal owned networks, first PG setup
failed because post_auth_delay cannot be set by ALTER ROLE; corrected startup
packet at de1d402 passed. Both failures are retained, not counted as test success.

Fresh terminal audit: all16 containers from this catalog/startup slice are
stopped-retained; owned children reaped and zero job backends. Zero running
BigBrotr containers; twenty unrelated containers unchanged, eight running.
Six terminal empty owned networks were removed after exact ID/label/endpoint
checks. Containers, catalogs and evidence remain retained. Detailed records are
`catalog_fence_probe.runs`, `startup_runs`, `network_retirement`; no prior unrelated
network was removed. Shared checkout remains clean/detached
`6be851580ba415eed5ff0979ee7072480f9ee3ab`, zero consumers. Sessions87744/49089
completed; no current experiment/process needs joining.

MON-009 negative controls at clean harness `6a6f796` reproduced both orders on
actual PG16.15, actual CLI `afc70f3` SHA c008a1fdacc6acf6d9fbd0727313a17da835d096428546ddf8f367a78b5056ad.
Old topology handoff NULL: closure5 then job6 commits; job6 then closure5 commits.
Independent literal rows/old SQL checksums match. Sessions reaped. Raw evidence:
`.research/implementation/job-bounds/negative-result.json`. New owned PG/client
remain RUNNING for follow-up under
`/home/tux/bigbrotr-implementation-20260917/runs/job-bounds-6a6f796/runtime`;
resource identities in local `job-bounds/resources.json` and laboratory inventory.
No active case; previous catalog resources remain terminal as above.

Candidate migration0008 and24 corrected-case scenarios are authored. Formatting,
Python parsing and original SQL immutability checks pass; Mac control_schema test
build is still running (session58123), native corrected execution NOT_RUN.
Next: commit the candidate for exact-source native build, then actual CLI
upgrade/concurrency/rollback qualification. Existing application transactions select READ COMMITTED; explicitly
address stale snapshots in stronger isolation, rather than assume locks refresh
them. Then MON-010/MON-007 and return to publisher/M1. Earlier SQL stays immutable.

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

MON-010 has scoped Mac execution evidence in
[evidence/p01-tls-presence-20260918.json](evidence/p01-tls-presence-20260918.json):
old binary accepts missing fields; corrected24 configuration tests and24 new
matrix combinations pass. Clippy session85606 still runs; no Clippy PASS yet.
MON-007 reproduced with actual HTTPS truncation then poisoned retry, plus concurrent
downloads. Candidate unique temporary publication passes5 local Python tests;
code/docs/evidence remain owned dirty work awaiting their separate commit.
MON-009 exact02d2a9b native build succeeded (binary65cb511e...), collector60668
complete. Corrected24-case execution session74814 is active on fresh owned PG/client
`archive-02d2a9b/runtime`; do not stop it. Original negative laboratory remains
running and retained. Update terminal results before closure.

MON-009 remains open pending these native results. Passing previous suites does not close new findings. Preserve the
completed five earlier findings and catalog investigation; do not conflate them
with these new triggers.
