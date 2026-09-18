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

Mac main; root is the only writer/integrator. Last published origin/main is
`d24cd876d6d9f97d7e3bdf6e12d64b76b634de24`. Current local candidate/evidence and
dirty work are recorded below; unpublished native candidates are not releases. Product candidate `b245644`
and its evidence are pushed. Mac 13 selected tests and scoped Clippy pass; exact
native 13 tests, Clippy and one actual PG adapter journey PASS. No force push,
independent Tux product edits or concurrent writer.

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
are terminal. The identity experiment is terminal. The new issuance lab is also terminal:
`/home/tux/bigbrotr-implementation-20260917/runs/archive-attempts-b245644`;
owner `0f4f43dd37438ce31fbf33efee4d55ab`, runner
`bigbrotr-implementation-archive-attempts-b245644`, PG suffix `-pg`. Both stopped
exit0/no OOM; owned internal network retired, data/artifacts/caches retained.
627-file readonly source independently matches Mac Git archive. The executed ELF
probe SHA is `863cecfe98f3f27abfbe1eb308cec648e9095cce5af7e882bf845e7e4a22569b`.
Full IDs/preflight/resources/audits are under `archive_attempts` in the inventory
and `.research/implementation/archive-worker/archive-attempts`. Sessions90183 and
26070 are terminal. Independent actual PG inspection finds exactly 2 immutable
rows, ordinals[1,2], predecessor[0,1], issuer_epochs[1,1]; literal CBOR/OpenSSL
matches both retained batch digests. All 3 databases have expected migration
checksums. Final audit: 20 unrelated unchanged, 8 running,zero BigBrotr running.
No active experiment or unresolved cleanup. Earlier guard/startup/
MON resources remain stopped as inventoried; never restart retired-network
containers. Shared checkout last observed detached6be8515 in an earlier slice;
not used or freshly requalified here.

## Current catalog product candidate

Mac main is now local candidate2e732f6; origin/main remains d24cd87. Root is the
only writer. Dirty work adds the confirmed collation/NOLOGIN fixes, stronger
native privilege/freshness cases and a mixed-case bootstrap login. Three clean
exact-source native runs are retained under `catalog-product` in the laboratory
inventory; none is an overall PASS yet:

-652e043: native bootstrap/tests/Clippy/Control passed; fresh guard install failed
 on String→REGROLE binding. Fixed by exact pg_roles name→OID lookup.
-dbc5211: guarded install, activation1/2, live stale native writer rejection and
 independent read[1,1,1] passed; Control epoch11 acquisition failed because its
 epoch::text alias sorted lexically (9 above10). Fixed with qualified numeric sort.
 One extra helper container never started (permission denied under image uid);
 its proposed negatives are NOT_RUN, not product failures.
-2e732f6: explicit Control epochs9/10/11/12 and full guard journey passed; new
 collation and NOLOGIN regressions failed as expected. The changed collation was
 accepted, and sealed login inspection returned UnsafeRole. Both fixes are now
 local, not yet qualified. Dropped-column refusal passed.

All three labs are terminal: two main containers each stopped-retained, internal
networks retired; the dbc5211 helper remains never-started/retained.20 unrelated
identities unchanged,8 running,zero BigBrotr running. Exact IDs/owners and audits
are retained in the inventory and `.research/implementation/archive-worker/catalog-product`.
No active experiment at this checkpoint.

Independent read-only review identified the role-bind and collation issues and
confirmed the numeric epoch fix and NOLOGIN recovery semantics. Root also fixed
the copied-issuer probe to use the actual active issuer; actual PG rejects it for
missing backend commit exclusion. Old reports retain their original scope.

## Next bounded action

Run the corrected exact-source candidate: native bootstrap with mixed-case role,
pre-install collation/unsafe-role/nonempty refusal, enabled function/trigger shape,
five unsafe writer privilege cases, successor while old login NOLOGIN, no implicit
re-enable, historical replay, old live native rejection and independent read.
Preserve both failed regression logs. Complete diff review, independent source/
artifact/state observations and resource cleanup before pushing verified work.
The catalog SQL remains separate from applied Control migrations; no old SQL was
rewritten. Inspection rejects prior incompatible candidate layouts rather than
silently adopting them. No scientific product schema/publisher/ACK exists yet.

Then complete catalog activation ambiguity/concurrency/credential custody and
connect scientific schema/all families/atomic semantic receipt/protection to
signed M1 read/export/restart. P02/M1 and every selected later phase remain open.
