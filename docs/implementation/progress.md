# Current execution checkpoint

Observed 2026-09-18. Navigation only; follow the
[resumption procedure](agent-handoff.md#resume-from-one-verified-checkpoint).
Verify Git, dirty work, evidence and resource claims before continuing.

## Mandate and active boundary

Owner-authorized complete implementation remains active. Mac is the editing/Git
workspace; use trusted exact-source Tux execution despite unavailable hosted
Actions. No public release/rename, Linux arm64 qualification or changes to the
read-only Go neighbor. All selected product work remains required.

Active P02-T03/T04: signed durable pending → actual PG DuckLake publication,
positive fencing, receipts/protection, then M1 read/export/restart. No P02 task or
M1 gate is complete. No product catalog/schema/publisher exists yet. Pending must
not be acknowledged/reclaimed. Do not mistake private preparation for publication.

## Git and integrating ownership

Mac main; root is the only writer/integrator. Last verified HEAD
`f678a38` equals origin/main; the identity slice/report are pushed. Root now owns
the dirty additive0009 archive intent migration, ControlSession adapter/request,
planner/request tests, actual PG acceptance example/harness and owning docs.
Mac13 selected tests and scoped Clippy pass; native issuance checks NOT_RUN.
The next commit is an unpublished exact-source candidate for that native run.
No force push, independent Tux product edits or concurrent writer.

## Implemented prerequisites and retained evidence

- P02 engine/Arrow prerequisites: [engine](evidence/p02-archive-engine-native-20260918.json),
  [event rows](evidence/p02-event-rows-native-20260918.json),
  [five families](evidence/p02-five-family-rows-native-20260918.json),
  [private handoff](evidence/p02-pending-handoff-native-20260918.json).
  Nullable compound children require matching validity masks; unmasked actual
  Parquet readback loses values. Signature-hash startup threads are independent
  of query threads; PID cap8 aborts the pinned worker. No RSS/throughput claim.
- [Private supervision](evidence/p02-private-supervision-native-20260918.json)
  at `c365053`: bounded nonblocking pipes/verified artifacts/retained grants,
  full-output termination and signed Rocks pending → packaged worker with unchanged
  state. [Parent binding](evidence/p02-parent-binding-native-20260918.json) at
  `e4e2a30`: expected parent plus Linux death SIGKILL, positive reap of a stopped
  initialized worker. Unknown termination retains grants and cleanup obligations;
  a signal or dropped handle is not remote catalog exclusion.
- **Current identity slice:** [native report](evidence/p02-archive-identity-native-20260918.json)
  at `8d1289d`, tree `a311b3e28bdd43fecdb4cf9478d8341e72fbe475`.
  Borrowed `bigbrotr_model::ArchiveBatch` validates nonempty ordered disjoint
  same-dataset complete admission values, coalesces adjacent stream ranges and
  hashes the selected canonical recipe without copying references/bodies.
  `Preparation::inspect_pending` now accepts explicit topology and returns schema1
  semantic input digest after checking actual worker membership. The worker wire
  remains version1. Never infer current topology from the Rocks creation manifest.
  The [identity contract](../specification/identity-and-encoding.md#exact-domains-and-field-order)
  clarifies maximal ranges before any product archive receipt exists; the old
  golden vector is unchanged and no archived product bytes require migration.
  This type establishes structure, not retained topology or publication authority.

Mac54 model tests, actual signed Rocks reopen/private worker and scoped
model/application Clippy PASS. Exact Linux candidate passes five membership cases,
one literal-byte case, one signed connected case and scoped Clippy. Signed case
uses independent Python/OpenSSL expectations at topology0/u64max, counts[1,1,1,1,0],
unchanged complete KV state and existing refusal controls. Its5-minute check
includes native compilation; this is not a publication throughput measurement.
Initial missing-type/API/field compiler REDs are retained and labeled, not claimed
as executed assertion failures. Root reviewed complete changed code/callers and
owning contracts; no independent agent or full-CI review is claimed.

## Catalog findings that constrain the next implementation

[Native catalog investigation](evidence/p02-catalog-fence-native-20260918.json)
at `a531ea3`: unfenced publishers duplicate[2,2,2]; NOLOGIN does not stop an existing
writer; positive backend exclusion rejects old COMMIT; a lost COMMIT response
leaves one committed triple. Synthetic receipts/counts are not scientific fidelity
or physical durability. An unrelated Control lock cannot fence native COMMIT.

The pinned native extension needs an explicit TEMPORARY memory postgres secret
through META_SECRET, plus META_SECRET_STORAGE_TABLE=''. The original absent-secret
attach failures are retained. Keep persistent secrets/autoload/community extensions
disabled. Native pool ForceAcquire can exceed the nominal connection-pool limit.

[PG startup](evidence/p02-postgres-startup-native-20260918.json) and
[native libpq startup](evidence/p02-native-startup-exclusion-20260918.json) establish
that a live invisible old connection can commit after NOLOGIN/empty activity.
Actual embedded libpq is18.3, server16.15. Positive native kill/reap plus late
server EOF prevents later SQL in the scoped blocking-client case. Initial1b6c311
harness FAIL and corrected56ef588 PASS remain retained. Fresh PG stats snapshots
are necessary. Unknown old host/process state cannot authorize cleanup or release
its resources. The [authority contract](../specification/archive-storage-contract.md#configuration-and-authority)
requires exclusion of all old credential holders or an equivalent enforced
storage fence, resolution of old transactions, then fresh receipt inspection.

[Catalog guard candidate](evidence/p02-catalog-guard-native-20260918.json):
- `694be48`: two live stale native clients (ready/invisible startup) fail COMMIT
  after successor publication; login/client kept alive to discriminate the fence.
- `df2a0db`: four real native lock schedules, writer/takeover first with holder
  commit/rollback; actual blocking and backend incarnations observed. Fresh
  independent read determines presence or permitted replay; final counts[1,1,1].
- `d4f43af`:44 cases—14 limited-role permission refusals,28 guarded-table refusals,
  combined role/search-path shadow case, one authorized native positive control.
  Exact PG SQLSTATEs and30 guard errors;614 independently matched source files.

The experimental SQL guards all28 pinned metadata tables with BEFORE STATEMENT
triggers, SECURITY DEFINER/qualified search path, session_user comparison and an
owner singleton locked FOR SHARE through native COMMIT/ROLLBACK. Takeover updates
that same row. Runtime writer loses DDL/trigger privileges. Native values/columns
are never manually rewritten. The singleton login is NOT a product epoch/CAS/
attempt protocol; adoption still needs an explicit owning-contract compatibility
exception and actual initialization/migration/inspection implementation.

Scoped exact-source inspection: full PostgresMetadataManager cpp/hpp,
DuckLakeInitializer, PG extension PostgresTransaction/PostgresStorage; selected
Commit/RunCommitLoop, catalog option/insert planning and inlined metadata writer
methods. Default native PG isolation is REPEATABLE READ by source, not a directly
queried runtime result. Table/schema inlining overrides take precedence and
inlined data/deletes may create extra PG tables. Ordinary publication must reject
incompatible settings/layout and keep runtime DDL refused. No complete native
mutation/maintenance audit or broader isolation matrix is claimed. Source hashes,
read ranges and official links are in the inventory/report.

## Independent monitor findings

Scoped corrections are pushed and remain closed within their actual evidence:
[MON-001/002/004/006](evidence/p01-monitor-corrections-native-20260918.json),
[MON-005](evidence/p01-jsonb-limit-native-20260918.json),
[MON-009](evidence/p02-job-bounds-native-20260918.json),
[MON-010](evidence/p01-tls-presence-20260918.json),
[MON-007](evidence/p01-archive-cache-retry-20260918.json).
MON-009 uses additive0008, authoritative caps, sorted SHARE locks, inverse checks,
READ COMMITTED and incompatible-history refusal;24 actual PG cases passed.
MON-010 keeps selected/unselected TLS validation distinct and offline.
MON-007 uses unique private download attempts and exclusive verified publication;
actual HTTPS retry/concurrent cases passed Mac/Linux. Do not rerun unrelated
suites mechanically or infer whole lifecycle/release acceptance from these cases.

## Verified current resources and continuity

Inventory key `implementation_archive_worker_20260918.archive_identity` in
[the laboratory inventory](../research/laboratory-inventory.json).
Local raw root `.research/implementation/archive-worker/archive-identity`;
native root `/home/tux/bigbrotr-implementation-20260917/runs/archive-identity-8d1289d`.
All621 source files and exact Git archive independently match Mac/Tux. Native
artifact identities and tools are retained. The readonly reused worker SHA is
`3133542df86c6d2992911434aff6292b232c6ea1f06235ed8e0d437765e3c136`, from retained
`archive-afc70f3/out/build` (same worker previously qualified at e4e2a30).
Writable Cargo/target caches are separate copies; retained builds were not edited.

Container `bigbrotr-implementation-archive-identity-8d1289d`, ID
`d879db9a3fdb957b3b42da490db447016c8ab7b164786e21739e0696a2493531`, owner
`b4ddb738209962f7a8cf08cb887fd686`, is stopped-retained, exit0/no OOM.
Network none; no new job network/volume. Fresh terminal audit: zero BigBrotr
containers running;20 unrelated identities/processes/mounts/networks unchanged,
8 running. Sessions15902(source/launch),80286(observer),45483(copy),67812(final Mac)
are terminal. No active experiment or unresolved cleanup. Earlier guard/startup/
MON resources remain stopped as inventoried; never restart retired-network
containers. Shared checkout last observed detached6be8515 in an earlier slice;
not used or freshly requalified here.

## Next bounded action

Qualify the new immutable archive intent issuance on fresh isolated Tux PG16.15.
The migration preserves previous checksums, refuses pre-protocol archive owners,
requires the actual Control backend, stable catalog, expected predecessor and
sealed topology. The typed request binds schema1 input and unique worker lifetime;
replay under a successor preserves the original issuer. Test real migration
fresh/upgrade/refusal, CAS/replay/conflicts, copied issuer, retained history and
cancellation. Local assertion RED was UnknownTarget(9); API RED was missing module.
Raw local root `.research/implementation/archive-worker/archive-attempts`.
Then implement the actual catalog guard with a first real initialization consumer. Bind monotonic allocated ownership and immutable attempt
identity so a late old coordinator cannot reactivate an excluded login; a Control
check before an independent remote write alone cannot provide this ordering.
Qualify the product's checksum/version/layout/trigger inspection, fresh init,
eligible/refused upgrade, CAS/lost takeover response and fresh receipt resolution.
Use the selected guard evidence; do not restart architecture research without a
new failure. Keep resource/orphan cleanup separate from catalog commit safety.

Then connect scientific schema/all families/atomic semantic receipt and protection
to signed M1 read/export/restart. Shared finite user jobs are not internal archive
attempts. No pending reclaim or P02/M1 completion before those actual boundaries.
