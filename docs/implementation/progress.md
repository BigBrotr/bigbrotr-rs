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
M1 gate is complete. The catalog guard prerequisite is implemented; no scientific product schema or
publisher exists yet. Pending must
not be acknowledged/reclaimed. Do not mistake private preparation for publication.

## Git and integrating ownership

Mac main; root is the only writer/integrator. Last freshly verified HEAD,
origin/main and remote refs/heads/main are equal at
`2ab50cf9522234678dcfc872483e4cb22495191e`, with a clean working tree after ordinary
push. This checkpoint update follows that observation. All catalog candidates and
the final evidence are pushed; none is a public release. Product candidate `b245644`
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

## Qualified catalog guard prerequisite

Mac main product candidate `944f3e6ee5898747d5ecce5278bc3acfdb38a06d` is qualified
within [the native report](evidence/p02-catalog-product-native-20260918.json).
The source and evidence are committed and pushed through2ab50cf.
Root is sole writer; this checkpoint refresh is the only subsequent edit.
Mac14 application tests plus1 bootstrap-wire test and scoped Clippy passed.
Exact native15 Rust cases,5 build/test checks,9 adapter stages and actual packaged
bootstrap/native writer/read processes passed. No full CI/product gate is claimed.

Implemented: separate catalog SQL; pristine native bootstrap; actual column/key/
collation/ownership/function/trigger/hash inspection; restricted dedicated roles;
monotonic import of privately retained Control intents; no automatic re-enable of
sealed logins. The improved issuer test copies the active issuer and is rejected
for missing backend exclusion. Numeric Control reacquisition crosses9/10/11/12;
actual retained epochs are1..15. Applied Control SQL is unchanged.

Independent audits match all637 Mac/Tux source files and all4 executed ELF hashes.
Actual metadata has28 manager-owned tables, attempts[1,2], owner2, old login still
NOLOGIN, three data files each with1 record, native read counts[1,1,1]. Frozen
literal CBOR/OpenSSL matches both intended-input digests. PG backend156 first
fails serialization then raises the exact require_writer exclusion: the generic
native COMMIT marker alone was deliberately insufficient. Input and science are
synthetic; this is not signed publication/protection or an archive ACK.

Three failed candidates remain recorded:652e043 String→REGROLE bind;dbc5211
lexical epoch ordering at11;2e732f6 accepted collation change and rejected sealed
login inspection. The latter two regressions now pass. One dbc5211 extra helper
never started due to executable uid permissions; its proposed cases are NOT_RUN.
Independent review found/confirmed these defects and reviewed the corrective diff;
root verified actual state/logs. Prior candidate layouts are not silently adopted.

Final native root:
`/home/tux/bigbrotr-implementation-20260917/runs/catalog-product-944f3e6`.
Owner `bf1b832499f02415b2f391c65b5f1244`; runner
`bigbrotr-implementation-catalog-product-944f3e6`, PG suffix `-pg`.
Both stopped-retained exit0/no OOM; owned network retired. All3 earlier runs are
terminal too; their data/caches and the never-started helper remain inventoried.
Final audit:20 unrelated identities unchanged,8 running,zero BigBrotr running.
Full IDs/source/artifact hashes and raw paths are under `catalog_product` in the
laboratory inventory and `.research/implementation/archive-worker/catalog-product`.
Session43878(cleanup) and97954(copy) are terminal. No active experiment/cleanup.

## Next bounded action

The next exact-source candidate adds an actual-PG recovery probe plus14 proposed
install/activation fault/race/cancellation cases. Initial independent review found
two incomplete observation oracles; they now check every native DML grant and
prohibited table right, bootstrap revocation/ownership, complete installation
identity and attempt topology/login/OID as well as the frozen input digest.
Mac compilation/Clippy is running (session33868); no new Tux run is active yet.
Local raw root `.research/implementation/archive-worker/catalog-recovery` contains
the candidate-only transfer/launch helpers. Existing labs remain terminal.
These scenarios are not yet executed evidence. Transfer a clean committed
candidate, inventory a fresh lab and run them before recording PASS.

Qualify the product adapter's activation ambiguity/concurrency and wire
actual credential custody/unique worker launch and recovery. A matching observed
attempt never permits a second native client; replacement gets a new lifetime.
The current finite bootstrap and PG adapter require an external bounded owner;
the installed initialization command/provisioner remains unfinished.

Connect scientific schema/all families/atomic semantic receipt and local protection
to signed M1 read/export/restart. No pending ACK/reclaim, task completion or P02/M1
closure before those actual boundaries. S3, full native mutation/maintenance
coverage, aged mixed load/RSS, Linux arm64 and full Compose/release journeys remain
selected work. Keep file/orphan cleanup authority separate from catalog fencing.
