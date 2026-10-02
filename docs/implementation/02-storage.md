# Phase 02 — Durable admission, archive and online representations

**Outcome:** full accepted evidence survives the declared process-crash model,
can be exported independently and reaches coherent online generations. Different
durability classes, archive cuts and serving frontiers remain visible.
Checked steps have their recorded implementation evidence; unchecked steps remain work.

**Depends on:** P01-T01–T08 as needed per task; P01-T09 is a later integration
consumer, not a prerequisite creating a cycle. Read
[data contracts](../specification/data-contracts.md),
[component contracts](../specification/component-contracts.md) and
[storage semantics](../architecture/storage-services.md).
Local/S3 placement, credentials, qualified protection and relocation follow the
[archive storage contract](../specification/archive-storage-contract.md).
The prior Python/engine experiments are evidence for their recorded scope, not
proof that an unbuilt Rust binding has passed these tasks.

The [storage catalog](../specification/storage-catalog.md) selects logical names,
keys, writer authority, retention and consumer mappings. Implement P02 tables now
and reserve later owned migrations without creating empty placeholder services.
P02-T01/T03/T04/T05 round-trip every applicable catalog family through actual
engine schemas and portable reconstruction. Introspected DDL, generated types,
manifest schema and API spelling must agree with the [naming contract](../specification/naming.md).
P05 owns the separate derived working-state/NEG directory, not a second writer
to the ingest RocksDB directory; P02 supplies its binding/fencing primitives.

## P02-T01 — Typed control schemas and minimum relay registry

**Depends on:** P01-T02; P01-T04 static configuration core (steps 1–5), not its
PG-backed authority step 6; the P01-T06 authority core (steps 1/2/4/5) and P01-T07
interface/transport core (steps 1–4), not their later storage acceptance. This
task supplies the PG schema/CAS used to finish P01-T04's durable authority. **Files:**
`migrations/postgres/{0001_control,0002_ingest,0003_relay}.sql`,
application `src/storage/postgres/`, `src/interfaces/migrations.rs`,
`contracts/control/schema.json`, `apps/bigbrotr/tests/control_transactions.rs`,
`docs/operations/schema-upgrades.md`.
**Contract:** bounded typed control commands → atomic owner/revision/job/cut state.
Minimum endpoint identity/enrollment exists here so P03 does not wait for all P04.

Migration 0001 establishes the shared typed finite `jobs` authority used by early
CLI consumers; P08-T03 adds management controls over it. Migration 0002 includes
P02-T07's `snapshot_leases` and `retention_claims`; P06 migration 0603 extends their
references with cursor pins. This prevents later UI/read phases from becoming
prerequisites for early durable jobs, exports or reclamation.

MON-009 extends this task with migration 0008: authoritative closed-source bounds
for jobs in both operation orders, sorted source locks, explicit isolation and
incompatible-row refusal. Preserve earlier checksums. Native negative controls
reproduced both missing guards; [24 corrected native cases](evidence/p02-job-bounds-native-20260918.json)
qualify exact `02d2a9b` upgrade/race/rollback checks on PG 16.15.

- [ ] **Step 1:** Define database owners and roles for configuration/policy, lease epochs,
  source ranges, projection frontiers, jobs, snapshot pins, publication references
  and endpoint enrollment; body/measurement payloads stay outside control rows.
- [ ] **Step 2:** Add primary/unique/foreign keys and checked state transitions; bounded
  versioned JSON is diagnostic detail, not the sole checkpoint semantics.
- [ ] **Step 3:** Separate PG application-control and DuckLake catalog ownership, privileges,
  backups and version negotiation even when one PG server hosts both.
- [x] **Step 4:** Implement explicit `bigbrotr migrate inspect/apply` with a dedicated
  migration role, serialized ownership, checksums and compatible version checks.
  [Initial catalog/engine evidence](evidence/p02-control-engine-20260917.json) and
  [serialized commit/TLS evidence](evidence/p02-control-faults-tls-20260917.json)
  cover migration 0001. The [0002 engine cases](evidence/p02-ingest-control-engine-20260918.json)
  add latest-target application, eligible upgrade and atomic unregistered-source
  refusal; [0002 races](evidence/p02-ingest-control-races-20260918.json) exercise
  relational coordination. Typed runtime adapters, later schemas and full release
  upgrades remain separate steps.
- [ ] **Step 5:** Implement compare-and-swap updates for config revisions, owner epochs and
  complete frontiers; preserve missing/zero/empty distinctions.
  The [same-Control initial Ingest renewal](../reference/ingest-registration.md#same-control-scheduling-renewal)
  is the scheduling prerequisite for the installed initial owner. It preserves
  existing identities and reuses the registry/policy locks; it cannot revive an
  expired owner, grant native exclusion or activate a successor. The
  [eleven-case native evidence](evidence/p02-ingest-lease-native-20260928.json)
  qualifies its initial renewal/refusal/cancellation boundary, not the complete
  CAS/handoff step or installed lifetime.
- [ ] **Step 6:** Establish normalized endpoint dictionary/enrollment and explicit route/
  policy references; later lifecycle migrations add observations/scheduling.
  The first [private operational identity consumer](../reference/relay-endpoint-registry.md)
  appends 0003 after 0816. [Identity/SQL](evidence/p02-relay-registry-native-20260928.json)
  and [fault/concurrency](evidence/p02-relay-registry-faults-native-20260928.json)
  evidence closes that bounded consumer. It does not close this step:
  The initial [no-auth enrollment command](../reference/relay-enrollment.md)
  appends0019; [actual-engine](evidence/p02-relay-enrollment-native-20260928.json)
  and [COMMIT/lock schedules](evidence/p02-relay-enrollment-faults-native-20260928.json)
  qualify those bounded command guarantees. [Focused live-owner relational checks](evidence/p02-relay-enrollment-relations-native-20260928.json)
  cover the closure consumed by the next acquisition issuer. The
  [committed visibility reader](evidence/p02-ingest-visibility-native-20260928.json)
  qualifies exact native mapping/dependency resolution under initial Control
  authority; immutable descriptor data is not a source grant. The
  [policy-version prerequisite](evidence/p01-policy-versions-native-20260928.json)
  qualifies fresh explicit bans and legacy compatibility. The
  [initial-owner installed enrollment HTTP](evidence/p02-initial-enrollment-http-native-20261001.json)
  qualifies existing-endpoint enable/retry/withdraw and table/function privilege
  startup refusals, with the original failed probe retained. Authenticated
  [URL registration](evidence/p02-initial-registration-http-native-20261001.json)
  now supplies that ID on the same owner;0025 preserves earlier applied SQL,
  with explicit populated upgrade and full retained-state checks.
  [Seven registration COMMIT schedules](evidence/p02-relay-registration-faults-native-20261001.json)
  qualify actual adapter cancellation/loss/expiry and exact fresh-owner recovery.
  [Eleven authority races](evidence/p02-relay-registration-races-native-20261001.json)
  add both lock orders, rollback and historical retry after changed heads/grants.
  [Live-owner relational checks](evidence/p02-relay-registration-relations-native-20261001.json)
  verify atomic linkage, current attribution, immutable history and session pruning.
  [Installed HTTP fault schedules](evidence/p02-initial-registration-faults-native-20261001.json)
  now qualify actual ninth-COMMIT loss, admitted client reset/retry and timely
  SIGTERM with full SQL/native preservation and fresh adapter receipt recovery.
  CLI enrollment, installed successor, complete acquisition/restrictive drain
  and full routes remain; no whole-step completion follows.
- [ ] **Step 7:** **Practical P02-T01:** real PG transactions race two revision/frontier
  updates, disconnect after commit and retry. Inspect committed rows externally,
  including zero-output progress. Negative control: updating a watermark before
  its required receipt or overwriting the starting input cut on completion.
- [ ] **Step 8:** Document migration privileges, compatibility failures and rollback/forward
  repair semantics; test the actual migration CLI on clean and incompatible DBs.

The next P02-T01 Step6/P03-T01 source prerequisite implements additive0020
[operational request binding](../reference/producer-request-inputs.md#operational-relay-source-binding-0020).
The adapter checks retained source/visibility and current enrollment before both
reservation and native dispatch. First-wave and relational evidence plus the
[eight actual lock/COMMIT/recovery schedules](evidence/p02-relay-source-faults-native-20260928.json)
qualify this bounded metadata responsibility. The current issuer, bans, committed
visibility and socket custody remain open. No complete task step is inferred.

## P02-T02 — Atomic RocksDB admission and exact identity inventory

**Depends on:** P01-T02/T03/T05, the P01-T06 authority core and P02-T01. This task supplies the real owner used to finish P01-T06/T07 practical cases.
**Files:** application `src/ingest/{owner,admission,receipts,sequence}.rs`,
`src/storage/rocks/{keys,options,inventory,pending}.rs`,
`apps/bigbrotr/tests/admission_recovery.rs`,
`tests/acceptance/cases/admission.py`, `docs/reference/durability.md`.
**Contract:** Admit/AdmitEvidence → typed durable result after synchronized batch;
separate CertifyExisting → retained dependency proof without mutation semantics.
The [identity/encoding contract](../specification/identity-and-encoding.md) and
[effect table](../specification/operations-contract.md) fix exact bytes and recovery.
One owner serializes exact identity decisions for each writable directory.

- [x] **Step 1:** Specify versioned key prefixes for event-ID metadata, event ordinals,
  scoped eligibility/receive minima, provenance keys, dictionaries, sequence facts, producer receipts and owner
  metadata. Begin with exact full identities and fixed-width ordered compact
  ordinal keys; no truncated hash accepted as definitive identity.
  The [key reference](../reference/ingest-key-format.md) and
  [codec evidence](evidence/p02-ingest-keys-20260917.json) cover key bytes only;
  value codecs and actual engine/admission remain the subsequent steps.
- [ ] **Step 2:** Store new shared EventPayload plus first verified signed
  representative per eligible scope, scoped first-receive/earlier corrections,
  optional witness facts, dictionary dependencies, pending records, strict
  request digest/result and contiguous sequence allocation in one synced batch.
  Retain complete pending-batch boundaries independently of request-detail GC;
  the archive consumer must still recover their exact membership after an ACK.
  A known payload in a new scope is NewFacts even with provenance off; no private
  original signature/envelope/time is copied into its public representative.
  The initial [dictionary batch](../reference/ingest-bootstrap.md) connects the
  Control backend to one synchronized native bootstrap and exact retained replay.
  [Native evidence](evidence/p02-ingest-bootstrap-native-20260918.json) records five
  writer tests and eight connected cases, including external full-byte inspection
  and process kills before write / after commit before receipt delivery. This
  prerequisite does not complete event admission or this step; power loss, ENOSPC,
  mid-FFI errors and successor recovery remain unqualified.
  The [initial-owner event writer](../reference/ingest-admission.md) adds owned
  verification, atomic event/scope/receive/witness effects and strict replay. Its
  signed fixture passes on Mac and native Linux amd64.
  [Native evidence](evidence/p02-event-admission-native-20260918.json) records eleven
  writer tests and fifteen connected event/bootstrap cases, including independent
  complete-byte comparison and before/after-commit process kills.
  ACK/retirement, archive resolution and installed source-policy/runtime remain
  required before this step or the first end-to-end milestone is complete.
- [ ] **Step 3:** Verify outside expensive critical sections where safe, then serialize the
  final exact admission check/commit. Same-ID concurrency cannot overwrite first
  admission or suppress a distinct permitted provenance edge.
  The initial `verify_owned` prerequisite shares actual ID/BIP340 checks with
  the existing JSON consumer without reparsing buffers;
  [Mac](evidence/p02-owned-event-mac-20260918.json) and
  [native evidence](evidence/p02-owned-event-native-20260918.json) record its scope.
  The initial writer now serializes native decisions and moves verified owned
  input through the connected Control bridge. The installed initial owner now
  retains native/Control custody; its same-owner private session consumer has
  [bounded installed fault evidence](evidence/p01-initial-session-faults-native-20261001.json).
  Concurrent producer dispatch remains unimplemented; sequential
  fixtures do not qualify that concurrency boundary.
- [ ] **Step 4:** Implement NewFacts/AlreadyDurable/Excluded/Invalid/Retryable,
  RequestConflict and IncompatibleSchema outcomes and
  ambiguous receipt resolution plus RetiredRequest. Same ProducerRequestId with
  changed bytes errors while detailed receipts exist; retired ranges return
  RetiredRequest without reconstructing reclaimed digests. Implement the registered
  producer epoch, bounded outstanding window, contiguous acknowledgement and
  fenced retirement/GC contract; restart
  cannot reuse a request identity or forget unresolved effects.
  Initial [native detail inspection](../reference/ingest-receipt-format.md#native-detail-inspection)
  binds a stored receipt to its exact request, dataset and digest with retained
  resource ownership; see [Mac](evidence/p02-receipt-reader-mac-20260918.json) and
  [native evidence](evidence/p02-receipt-reader-native-20260918.json).
  It does not resolve missing detail, grant authority or implement retirement.
  The [exact producer-input staging prerequisite](../reference/producer-request-inputs.md)
  now precedes new native bootstrap/event/rejection effects and feeds bounded
  recovery decoding. PostgreSQL reservation/checkpoint closure, partial-input
  recovery and the versioned successor protocol remain required; legacy receipt
  replay does not qualify missing producer inputs. The [scoped native evidence](evidence/p02-producer-inputs-native-20260919.json)
  at a1da96a covers staged-input recovery, complete crash oracles and six installed
  signed publication schedules; it does not complete this step.
  The connected [reservation prerequisite](../reference/producer-request-inputs.md#initial-connected-reservation-protocol-0011)
  adds fresh-only migration 0011, immutable PG input metadata and a distinct reserved counter.
  Stage and PG COMMIT precede a fresh authority check and native dispatch; no
  outcome closure, ACK or successor recovery is implied.
  [Native reservation evidence](evidence/p02-producer-reservations-native-20260919.json)
  records six connected/fault cases, 19 SQL observations, six closure/snapshot
  schedules, two harness cleanup controls and five actual old/new compatibility
  cases; six installed signed schedules at 57cce75 match 24 reservations. This
  qualifies that prerequisite, not this step's remaining ledger/ACK/handoff work.
  The [native outcome copy](../reference/producer-request-inputs.md#connected-native-outcome-copy-0012)
  adds migration0012 and a private native witness to the existing bridge, before
  logical position/checkpoint closure. [Native outcome evidence](evidence/p02-producer-outcomes-native-20260919.json)
  qualifies five outcome/COMMIT-loss cases,19 outcome SQL observations, four actual
  migration/client compatibility cases, all24 signed-run copies and a distinct
  AlreadyDurable request/retry with no new facts. Reservation/race regressions
  also pass; initial fixture/audit failures remain recorded. This qualifies the
  durable copy only, not this step's logical checkpoint, ACK/GC or handoff work.
  The finite [event-only import checkpoint](../reference/import-input-bindings.md#event-only-logical-checkpoint)
  adds native-witness closure and a contiguous logical shard head in that same
  outcome transaction. [Native evidence](evidence/p03-import-checkpoints-native-20260919.json)
  records signed reverse-order/retry/fault cases, actual SQL lock schedules and
  additive compatibility. Complete claim/proof checkpointing, mutation ACK/GC,
  retirement and successor recovery still prevent completion of this step.
  The [native producer barrier](../reference/ingest-bootstrap.md#native-producer-closing)
  adds exclusive recovery and an explicit v1-to-v2 closing transition under a
  current Control actor, without new facts/requests/ACK. [Native evidence](evidence/p02-producer-closing-native-20260920.json)
  records 35 store tests, 12 connected scenarios and 11 old writer-library snapshots.
  Exact retries and SQL interruption preserve the barrier. Full outcome drain,
  ACK/GC, source closure and fresh-process successor activation remain required.
  The [positive original-request resolver](../reference/producer-request-inputs.md#positive-original-request-resolution)
  copies validated original outcomes under current Control without changing native
  state or advancing logical progress. [Scoped evidence](evidence/p02-producer-recovery-native-20260920.json)
  records 46 native-store tests, 19 connected captures including all three outcome
  classes/refusals/ambiguous commits, and actual held-owner exclusion followed by
  recovery in a distinct process. Missing receipt or reservation stays Unresolved;
  corrupt existing copy stays Conflict. This does not complete drain, ACK/GC or
  this step; reserved-but-uncommitted resolution remains separate work.
  That negative path additionally requires recovery provenance: the
  [WAL reopening risk slice](../../experiments/ingest-wal-recovery/README.md)
  found default point-in-time recovery could hide a lost synchronized batch.
  Strict reopening refuses detected damage, but does not prove a complete tail
  was never lost or salvaged earlier. Do not infer noncommit from an issued
  counter until that remaining boundary has an implemented, qualified witness.
- [ ] **Step 5:** Make provenance-off allocate no P inventory or event/endpoint side channel;
  retain scope eligibility and scoped first local receive/earlier corrections.
  Implement CertifyExisting against one committed inventory view, exact verified
  intent, complete dependencies and existing retention/protection. Return
  NeedsAdmission for any missing effect; checkpoint/cut compaction and mixed-backup
  rejection preserve proof closure without a second per-delivery proof ledger.
  Producer handoff accepts Receipt/ExistingProof bases; proof-only classified
  prefixes/pages need no request sequence or ingest ACK. Resolve ambiguous Admit
  requests before choosing a new certification path, and ACK only mutation slots.
  The [initial existing-effect read](../reference/ingest-admission.md#existing-effect-certification)
  now passes [native qualification](evidence/p02-certification-native-20260919.json):
  14 actual native tests, four connected cases/twelve reads and independent full
  proof/state oracles. No mutation request, original, fact, sequence or PG row is
  added by these reads, including a full mutation window. Proof-only checkpoint,
  compaction, GC/restore and successor recovery still prevent step completion.
- [ ] **Step 6:** Use bounded positive cache with full equality; account for pending/flushing
  maps, native caches, memtables and compaction alongside the cache's own slab.
- [ ] **Step 7:** **Practical P02-T02:** use the actual Rust binding, kill before/after synced
  commit and after commit-before-response; reopen and compare every batch member
  and receipt externally. Inject ENOSPC and simultaneous duplicate/new-edge input.
  Reorder producer ACKs, leave a hole, interrupt receipt GC, resubmit an old epoch
  and restore watermarks; no retired request causes a second scientific effect.
  A two-request outstanding window must progress across longer pages/live streams
  using incremental durable outcome handoff before final EOSE coverage. Pause a
  verified request before commit while retiring its epoch; the owner-side closing
  barrier must exclude its late commit.
  Add both private/public arrival orders, alternate valid signatures, new scope
  with provenance off, import→network, certification after receipt GC and missing
  proof dependency on restore. Reuse independent signed/golden fixtures.
  Negative controls: identity before durable payload, global first signature, or
  bare existence used as a complete retained-effects proof.
- [ ] **Step 8:** Report process crash separately from power loss/host failure; expose
  pending bytes, native write stalls, receipt latency and exact inventory growth.

## P02-T03 — Portable dictionaries and deduplicated source documents

**Depends on:** P02-T02. **Files:** application
`src/storage/archive/{dictionary,documents,encoding}.rs`,
`contracts/archive/v1/{dictionary,document,measurement}.json`,
`apps/bigbrotr/tests/archive_references.rs`,
`tests/fixtures/storage/documents/`, `docs/reference/archive-format.md`.
**Contract:** canonical fact + source documents → versioned portable rows with
resolvable namespaced references and unchanged original bytes.

- [ ] **Step 1:** Implement dataset/event/endpoint/observer/access/method dictionaries with
  immutable canonical values, normalization versions and overflow rejection.
  Typed row conversion now covers the seven admission descriptor schemas;
  actual publication/closure remain required. Include sparse nullable references
  in real Parquet readback: the pinned binding needs parent/child null masks.
  [Fixed-reference append evidence](evidence/p02-dictionary-append-native-20261002.json)
  qualifies native atomic append/replay and a connected private observation/signed
  socket journey after the original bootstrap, plus nine actual append COMMIT
  ambiguity/cancellation/recovery schedules. The same report now qualifies bounded
  read-only intern preparation and its three-case actual-engine signed socket
  consumer. Its installed startup consumer now preserves original bootstrap and
  explicitly appends a private root, with nine lifetime/window and nineteen COMMIT
  cases. Finite execution and installed archive/export closure remain required.
- [ ] **Step 2:** Create A06 objects keyed by visibility namespace and digest; verify bytes
  on first creation and reject conflicting bytes under the same identity.
- [ ] **Step 3:** Separate document object bytes from each fetched/type/method occurrence;
  unchanged documents reuse payload while actual measurements retain their times.
- [ ] **Step 4:** Preserve canonical event arrays/content and unsigned timestamp domain in
  the chosen Arrow/Parquet mappings; do not parse content as arbitrary JSON.
  The initial [event row adapter](../reference/archive-format.md) covers typed
  conversion/native appending; full archive publication, independent native
  qualification and portable export remain required.
- [ ] **Step 5:** Define unknown/null/empty/value variants for measurements and structured
  failures; credentials/raw exception payloads are excluded from retained fields.
- [ ] **Step 6:** Include required dictionary/object additions before or with references;
  encode parsed interpretations as versioned derived results.
- [ ] **Step 7:** **Practical P02-T03:** write full nested/Unicode/extreme-time records,
  1,000 unchanged document fetches, changed bytes and a second visibility scope.
  Reopen solely with standalone DuckDB and resolve every reference. Expect one
  object per exact bytes/scope and all actual measurements. Negative controls:
  creating a new logical object per poll, dropping empty tag values, cross-scope
  dedupe leaks. A measured wide physical layout may still repeat bytes.
- [ ] **Step 8:** Document byte fidelity, unsigned time limitations in generic clients and
  the complete difference between source objects and parsed hot fields.

## P02-T04 — Transactional Parquet archive publication

The current steps3/4/5 prerequisite is the bounded
[native batch-membership observer](../reference/archive-handoff.md#bounded-native-batch-membership-candidate),
wired to `archive batch-inspect`. It compares retained complete source/digest and
original-file membership in one selected snapshot before the P02-T04/T07
frontier/pin bridge. [Native3369091 qualification](evidence/p02-archive-membership-native-20260920.json)
passes23 CLI cases, two actual blocked-reader cases, two PG crashes/restarts and
28 selected tests. No frontier, reference-closure, protection or task-completion
claim follows from this census.

The [private exact-reference hydration consumer](../reference/archive-format.md#exact-historical-signed-event-hydration)
now reconstructs original signed events from payload/eligibility facts in one
native transaction. [Twenty connected cases](evidence/p02-archive-event-native-20260920.json)
at `3f29952` consume actual admission/publication, reject missing/corrupt files,
verify original signatures and read after actual PostgreSQL postmaster crashes.
This supports M1 read/export wiring; it supplies no complete archive frontier,
certified cut, policy grant or retention pin and completes no P02/P06 task.

Current service connection: persistent `storage.archive_state_root` and explicit
`archive state-initialize` bind one state directory to the retained initialization
reservation; installed Control acquires coordinator custody before its connection
and keeps it through actual driver join. [Sixteen native CLI/Control cases](evidence/p02-coordinator-state-native-20260919.json)
now pass at `952fd9a` with harness `f822978`, including physical bind aliases and actual
SIGKILL/restart; 98 Rust tests pass and one holder fixture is explicitly ignored.
This is a P02-T04/P01-T05 prerequisite, not step completion. Pre-protocol
history adoption and actual recovery/dispatch remain required. Immutable complete
input headers now precede issuance, with exact grouping replay checks and receipt reconstruction without facts plus complete-wire preflight.
[Native qualification](evidence/p02-retained-inputs-native-20260919.json) at `d442125`
passes all six installed signed schedules and six fresh-process receipt inspections
while Rocks is unreadable. All 22 input files match independently frozen bytes;
47 native Archive tests, 8 binary tests, 11 CLI tests and scoped Clippy pass
(one older holder fixture ignored). Installed dispatch and protection remain open.
No step is complete from these prerequisites. See [the owning boundary](../specification/archive-storage-contract.md#persistent-coordinator-state).


The next P02-T04 steps3/4/5 slice establishes the
[initial coordinator protocol boundary](../specification/archive-storage-contract.md#initial-coordinator-protocol-boundary)
through additive0010 and explicit fenced typed initialization. Bound issuance
consumes actual retained inputs; old history stays inspectable and cannot be adopted.
The installed signed fixture now consumes that path.
[Native b203a41 evidence](evidence/p02-coordinator-protocol-native-20260919.json) covers
eight actual binding/history/retry/refusal cases and six signed schedules with22
bound inputs and fresh-process receipts. [Native d355c5b faults](evidence/p02-coordinator-faults-native-20260919.json)
add both candidate legacy-API/migration lock orders, cancelled blocked initialization
and before/after binding COMMIT-loss retry. Complete service dispatch remains open;
no checkbox or complete recovery gate is closed.

The installed coordinator-initialize command and ordinary startup require are
a qualified P02-T04 consumer slice. They share the existing Control owner/policy/
transport executor, retain local custody through join, and explicitly refuse old
unbound history after migration10. The previous local-only state command remains
unchanged. [Eleven native cases](evidence/p02-coordinator-cli-native-20260919.json)
pass at `5050999`: actual installation, retry, startup/restart, old-history preservation,
full stdout and both blocked signal cancellations. The native run passes 47 Archive,
8 binary and 12 CLI tests plus scoped Clippy; one older holder test remains ignored. No dispatch,
complete recovery or whole-task gate is closed.

The current P02-T04/T07 bridge adds a complete native cut (whole retained batches,
zero prefixes and same-cut reference closure), a live catalog capture barrier and
[fresh-only local persistence participation](../specification/archive-storage-contract.md#qualified-local-persistence-participation)
through additive0015. The installed coordinator initializer consumes the marker;
new issuance requires it. [Scoped native qualification](evidence/p02-archive-cut-native-20260920.json)
at `354de75` passes 54 selected tests, two signed full cuts, 15 installed CLI cases and 11
updated protocol cases, with separate exact fixture revisions. Manager loss before
final verify/during COMMIT and identical retry are exercised. Both failed initial
TRUNCATE fixtures remain recorded. The actual Archive frontier/snapshot/lease
binding and populated 10/14 history recovery remain open. No task checkbox closes.

The next connected candidate consumes that observed cut in the existing Control
owner: additive0016 binds the actual Archive owner, complete frontier, selected
original input identities and native snapshot atomically. Identical retries preserve
attribution; historical result resolution is independent of latest native snapshot.
[Native frontier qualification](evidence/p02-archive-frontier-native-20260920.json)
at `e64ee85` passes two signed histories,17 installed SQL/upgrade cases and15
restart/commit/manager-loss/cancellation cases using fixture `4bdc20f` and harness
`1865da9`. Complete old rows/xmin and eight-relation publication transitions are
checked independently. Preserved harness failures remain INCOMPLETE. Source
handoff, actual pins and served read/export remain open; no checkbox closes.

**Depends on:** P02-T01/T02/T03 and P01-T05/T06.
**Files:** application `src/storage/archive/{publisher,receipts,worker}.rs`,
`contracts/archive/v1/batch.json`, `packaging/archive-worker/`,
`apps/bigbrotr/tests/archive_publication.rs`,
`tests/acceptance/cases/archive.py`, `docs/operations/archive-publication.md`.
**Contract:** deterministic complete source batch → committed DuckLake snapshot
and archive receipt. Preparation can parallelize; logical publication has one owner.

- [ ] **Step 1:** Package a pinned DuckDB engine and compatible verified extensions in an
  isolated worker; disable uncontrolled extension/network installation at runtime.
  The initial [private engine diagnostic and build bundle](../../packaging/archive-worker/README.md)
  implements pinned linking/loading and resource settings. The private inspection
  parent now owns nonblocking pipes, pre-exec checks and retained memory/control
  grants. [Linux amd64 expected-parent/death-signal qualification](evidence/p02-parent-binding-native-20260918.json)
  proves exit/reap of an initialized stopped worker after parent SIGKILL; the old
  worker survives in the retained negative control. Persisted recovery/exclusion, full
  host/disk admission, native peak measurements and the actual publisher still gate
  this step; downloading
  another target's assets does not qualify that target.
  The [native prerequisite evidence](evidence/p02-archive-engine-native-20260918.json)
  includes signed-startup PID exhaustion; budget this separately from query
  threads and qualify parent termination/recovery without disabling signatures.
- [ ] **Step 2:** Encode rows with the selected soft starting targets: 256 MiB files,
  64,000 rows per group, Zstd level 3, a 30-second batch-formation/dispatch timer
  while resources/authority are available (not a commit-latency SLA), and two
  bounded preparation workers. Bound application decoded input separately from
  engine targets, verify effective options/vector overshoot and inspect actual
  Parquet metadata. Disable DuckLake inlining explicitly with no persisted
  override; single-small-record publication must follow the same contract.
  The private [pending handoff/preparation](../reference/archive-handoff.md)
  connects canonical complete batches to typed native staging. It uses per-fact
  appends initially; production parent grants, batching and actual catalog/file
  publication still gate this step. The [native handoff evidence](evidence/p02-pending-handoff-native-20260918.json)
  qualifies ten scoped Rust and two packaged cases, with Rocks and executable
  boundaries exercised separately. Later [private supervision evidence](evidence/p02-private-supervision-native-20260918.json)
  connects actual signed admission/reopen/pending to the packaged worker twice
  with unchanged Rocks state. Full catalog/body/reference readback remains open.
- [ ] **Step 3:** Use the PG-backed DuckLake catalog; publish events/evidence/dictionaries/
  document references and deterministic batch receipt in one transaction.
  Use the native writer on either local files or the configured S3 destination;
  validate catalog/root identity and disable automatic migration at ordinary attach.
  The [native COMMIT-response loss](evidence/p02-native-commit-file-loss-20260918.json)
  at `c9a29e0` exposes deletion of committed files by normal INSERT error cleanup.
  Qualify caller-owned native file registration for every family and the receipt
  before enabling the publisher. Preserve complete transactional membership,
  nested mappings, immutable file ownership and recovery; no normal-INSERT fallback.
  The [registration experiment](evidence/p02-native-registration-20260918.json)
  at `ea270ac` preserves all3 synthetic files/full reads after completed-but-lost
  native COMMIT and retains unregistered rollback files. The
  [signed guarded publication qualification](evidence/p02-signed-publication-native-20260919.json)
  at `2075a39` now connects actual Control/source registration, signed Rocks
  admission/reopen and FreshWriter supervision to all five current families and
  receipt. Three native schedules compare complete literal rows/signatures,
  retained source bytes and relative file membership; completed-but-lost COMMIT
  preserves all files, while pre-forward rollback retains unregistered files.
  The source policy and reader are explicit lab fixtures. Product receipt/read
  services, relocation, orphan recovery and
  protection remain open.
  [Signed native fencing](evidence/p02-signed-fencing-native-20260919.json) at
  `b12c902` adds three actual publisher/activation races, with directly observed
  blocker chains, exact serialization/guard refusals, positive old-child reap and
  fresh complete readback before successor launch. Both old commit and rollback
  preserve the expected files/rows; all six signed schedules pass. This does not
  implement persistent lifecycle recovery or the product receipt reader.
  The first finite fresh-only initializer and shared schema declaration now create
  the five existing families plus an empty technical receipt relation;
  [native qualification](evidence/p02-scientific-schema-native-20260918.json) covers
  initialization/refusal only. The [scientific guard binding](evidence/p02-scientific-guard-native-20260918.json)
  at `a91ae01` qualifies 37 PG sequences for immutable schema binding, full 203-column
  inspection, failed activation effects, atomic install ambiguity and both SQL lock
  orders. Applied0001 is unchanged. Init recovery, native publication and full
  signed body/reference readback remain required.
  The [actual native catalog investigation](evidence/p02-catalog-fence-native-20260918.json)
  qualifies four synthetic cases and explicit memory-secret attach after a real
  dependency failure. It is not the product schema/publisher. Connection-startup
  exclusion, persisted recovery and full signed membership remain gates.
  Installed initializer work additionally requires durable database/root
  reservations, one newly generated bootstrap credential, positive native/driver
  completion and explicit refusal of partial/ambiguous state. Before that handoff,
  the shared native route now requires a completed SCRAM exchange; existing trust
  or other non-SCRAM-only routes refuse. The closed bootstrap/initialize response
  now rejects duplicate fields. Discriminating local regressions and
  [16 old/new native authentication cases](evidence/p02-native-catalog-auth-20260919.json)
  pass, including incomplete SCRAM before SQL and verify-full TLS refusals.
  That authentication slice left configured native mutual TLS and the installed
  initializer to subsequent work below.
  Its atomic ownership transfer must validate the configured manager and retired
  bootstrap names/OIDs, complete guard/native ACLs (including column/default ACLs),
  required manager privileges and existing wrong-owner refusal. Qualify actual
  non-owner writes, positive runtime manager use, rollback and ambiguous install;
  a structural checksum alone does not prove those authority properties.
  [Native ownership qualification](evidence/p02-catalog-ownership-native-20260919.json)
  records two baseline counterexamples, both intervening failures and 56 passing
  cases at df4a598. The configured non-superuser manager can inspect; missing
  rights/default ACLs refuse, both COMMIT-loss directions resolve, and concurrent
  installers produce one install and one existing observation. Complete reserved
  initialization and fresh publication under that manager remain required.
  Permanent local initialization custody and its actual native consumer now have
  [scoped native evidence](evidence/p02-initialization-custody-native-20260919.json)
  at `3efde78`: 22 practical cases, actual driver/native descriptor retention,
  immutable journal/recovered-launch refusal and unchanged full database/filesystem
  observations on invalid inputs. Corrected fixtures isolate symlink/inode checks;
  a raw reopened descriptor demonstrates the documented trusted-parent boundary.
  Atomic database reservation and newly generated bootstrap credentials now have
  [scoped native qualification](evidence/p02-database-reservation-native-20260919.json)
  at `bb8be6d`: 29 actual cases including the generated credential's native consumer,
  exact returned identities, unsafe privilege rollback, competing roots and global
  nonce collisions, both lost-COMMIT outcomes and coordinator SIGKILL before remote
  rollback/late commit. Failed baseline/oracle candidates and review corrections
  are retained. Bounded reservation observation and exact retirement now have
  [39 native recovery cases](evidence/p02-initialization-recovery-native-20260919.json)
  at `660df73`, including actual post-manager-installation observation, both lost
  COMMITs and graceful cancellation with real advisory waiters. Independent durable
  effect checks cover ambiguous outcomes too; the failed baseline and weaker first
  oracle remain recorded. That recovery slice did not implement SQL-capable backend
  drain, the complete initialization/recovery CLI or configured native mutual TLS;
  see later evidence below.
  The real [initializer/recovery CLI](../user/archive-initialization.md) now has
  [17 scoped native scenarios](evidence/p02-initialization-cli-native-20260919.json)
  with binaries at `6265286` and separately pinned harness `3335e06` (unchanged production).
  It connects reservation, native initialization/join, recovery, exact retirement,
  manager installation and fresh inspection. Actual driver/native cancellation and
  a full output pipe are exercised. The malformed first fixture remains recorded.
  [Five actual CLI drain cases](evidence/p02-initialization-drain-native-20260919.json)
  at artifact `78e7734` / harness `1978e12` now qualify nonempty waits across databases,
  timeout, SIGTERM/SIGINT and retained retry after natural backend completion.
  The producer is explicitly a custodied tokio-postgres residual-session fixture;
  this does not qualify a blocked native libpq write. Protected sealed-FD client
  identities now wire initializer, publisher and receipt consumers without changing
  the 32 KiB wire format. [25 actual native TLS/mTLS cases](evidence/p02-native-mtls-20260919.json)
  at `50d9496` pass for installed initialization, fresh manager inspect/recover and
  SELECT-only receipt reads, including server clientcert+SCRAM enforcement and
  blocked native-query cancellation with pidfd-observed child exit before unblock.
  Review's libpq CA-path truncation finding has local RED/GREEN and a real long-CA
  refusal with unchanged permanent state. The initial Linux-only test compilation
  failure is retained. [Nine installed late-fault cases](evidence/p02-initialization-late-native-20260919.json)
  at `3ad4c64` now qualify lost scientific/guard COMMIT replies in both directions,
  coordinator SIGKILL at both boundaries and native-worker SIGKILL. Independent
  PostgreSQL state distinguishes rollback from commit; recovery with the entire
  worker bundle absent preserves native rows/root markers, retires bootstrap and
  remains idempotent. Partial scientific state refuses. Signed mTLS publication
  and installed service/M1 acceptance remain open;
  this step is not complete.
  The installed signed consumer at `dbb3aea` now reproduces a missing delegation
  failure with PUBLIC CONNECT/USAGE revoked: initialization succeeds but generated
  writers cannot connect. The [correction at `61271e2`](evidence/p02-installed-publication-native-20260919.json)
  passes all six signed publication/fencing schedules and two isolated missing
  grant-option refusals without durable mutation. PUBLIC access stays absent;
  22 writer lifetimes are sealed with nondelegable CONNECT/USAGE. The exact
  documented privileged ACL grants run against a fresh installed catalog before
  writer creation; populated historical upgrades remain separately unqualified.
  Existing migration bytes/checksums and credential lifetimes remain unchanged.
- [ ] **Step 4:** After an ambiguous commit, query the receipt in a new transaction; do not
  repeat while receipt state is unknown. Reject mismatched batch digest.
  Include loss of the actual native PostgreSQL COMMIT response, plus physical file
  existence and full body/receipt reads; a kill after COMMIT returned and metadata
  counts do not cover this boundary. The original normal-INSERT case fails;
  caller-owned scientific registration now passes the scoped native schedules
  linked above. The installed [finite receipt command](../user/archive-receipts.md)
  now consumes retained Control/local inputs under supervised accepted policy,
  shared process budgets and independently inspected SELECT-only credentials.
  [Sixteen native CLI cases at `d78bcc5`](evidence/p02-receipt-cli-native-20260919.json)
  qualify 22 original retained observations, missing/corrupt inputs, scan/resource
  limits, accepted-policy seed drift, effective privilege refusals, readonly data/
  state mounts and actual blocked-reader supervision/cancellation. All catalog
  statements/data (apart from one ACL block's grant ordering), original Control
  rows and 106 data/state files remain unchanged. Three failed development matrices
  are retained with their fixture corrections. This observation grants no replay,
  writer exclusion, protection or reclamation; lifecycle recovery remains required.
  The new [private receipt consumer](../reference/archive-handoff.md#fresh-receipt-observation-candidate)
  uses the actual supervised worker, complete retained batch/attempt identity,
  readonly binding/attach and a bounded full-field scan through EOF. It refuses
  malformed receipts (including unrelated rows), target-identity conflicts and
  duplicates, unreadable files and truncated work. [Native receipt qualification](evidence/p02-archive-receipts-native-20260919.json)
  at `0b3a427` passes six integrated signed recovery schedules and eighteen SELECT-only PG/
  readonly-mount cases, including real blocked cancellation/deadline, missing/
  corrupt objects, identity conflicts, limits and identical restored retry.
  All 19 scoped native Rust cases pass. Persistent lifecycle recovery, aged scan
  capacity and protection remain required; pending stays retained in every case.
  The borrowed model archive identity and pending-preparation consumer bind schema,
  explicit topology, maximal ranges and complete source/digest membership. Frozen
  single/multiple-range and signed pending fixtures have independent expected
  bytes/digests. [Exact native qualification](evidence/p02-archive-identity-native-20260918.json)
  passes seven scoped cases plus Clippy at `8d1289d`. This input identity is not a
  persisted attempt or commit receipt;
  those consumers and actual ambiguous recovery remain required.
  Additive0009 and `ControlSession::register_archive_attempt` now implement
  immutable CAS issuance, binding this input identity and coordinator/worker
  lifetime under the actual Control backend. [Native qualification](evidence/p02-archive-attempts-native-20260918.json)
  at `b245644` passes 13 Rust cases plus actual PG issuance/replay/refusal/cancellation;
  the catalog must still import and enforce monotonic authority before any
  credential exposure/publication. No active archive owner or ACK is created.
- [ ] **Step 5:** Enforce a single positively fenced publisher and bounded native worker
  ownership. An unrelated advisory lock cannot certify catalog commit authority.
  The [PG startup counterexample](evidence/p02-postgres-startup-native-20260918.json)
  also disproves NOLOGIN plus empty activity as general exclusion: an invisible
  old connection later commits. Qualify actual process/client exclusion and
  credential/startup ordering before selecting the product protocol.
  The [native libpq 18.3/PG16.15 startup cases](evidence/p02-native-startup-exclusion-20260918.json)
  now exercise both an invisible live native writer and positive kill/reap before
  replacement. Persisted attempt/credential/process ownership and successor
  recovery remain required; neither this probe nor parent-death signaling is that
  implementation. Apply the [complete exclusion ordering](../specification/archive-storage-contract.md#configuration-and-authority)
  to all credential holders and use fresh backend observations.
  A bounded [PG metadata-trigger alternative](evidence/p02-catalog-guard-native-20260918.json)
  now passes six actual native cases with live stale clients and both transaction
  lock orders/commit/rollback, plus 44 actual privilege/coverage cases at `d4f43af`.
  The [narrow compatibility exception](../specification/archive-storage-contract.md#versioned-catalog-commit-guard)
  now selects this fence. [Product guard prerequisite evidence](evidence/p02-catalog-product-native-20260918.json)
  qualifies native bootstrap, versioned fresh install/inspection, immutable
  Control-intent activation and live stale native rejection at `944f3e6`. It also
  records the confirmed role-bind, collation, NOLOGIN and Control numeric-epoch
  corrections with their failed candidates. [Actual activation recovery](evidence/p02-catalog-recovery-native-20260918.json)
  at `55dba36` now qualifies14 install/activation fault, race and cancellation
  cases with independent identity/privilege/lock observations. Scientific input
  is synthetic; no worker is launched from an observed activation. Full ordinary
  mutation coverage, native publication/recovery, credential custody and installed
  recovery remain gates. It can
  enforce commit exclusion without inferring remote process death; resource and
  orphan cleanup still require their separate positive lifecycle evidence.
  Credential provisioning/sealing and the finite supervised bootstrap parent are
  now qualified at `d2b2609` within [8 actual-PG sequences and 16 native Rust cases](evidence/p02-catalog-credentials-native-20260918.json).
  New creation uses a local
  SCRAM verifier and atomically imports/activates the retained intent; observed
  attempts cannot issue another credential. The consuming handoff remains distinct
  from persistent unique-process recovery. Executed cases check non-superuser
  manager membership, real authentication, sealing, both COMMIT ambiguity branches
  and same-intent races. The signed native races linked above additionally qualify
  finite scientific publication versus activation in both lock orders. Persistent
  lifecycle and installed recovery remain required. The new
  [local attempt custody candidate](../reference/archive-handoff.md#persistent-local-attempt-custody-candidate)
  synchronizes immutable intent before credentials, binds actual provisioning
  drivers and native exec to one close-only lock, and refuses another launch from
  recovered state. [Native custody qualification at 5ad77af](evidence/p02-local-custody-native-20260919.json)
  passes 28 scoped Rust cases, three actual worker/driver/manager schedules and all
  six signed publication/receipt journeys, with 25 exact journals and unchanged
  retained source rows. Incomplete journal and cross-boot
  recovery, installed resource reconciliation and protection remain required.
  The missing-import slice adds a recovered-only identity gate and the
  actual delayed owner Bind/Execute/Sync discriminator in
  `experiments/archive-engine/missing_import_cases.py`. The baseline seal-only
  adapter at e571002 failed the two required NotImported observations while
  completing both driver schedules. [Native qualification at770c485](evidence/p02-missing-import-native-20260919.json)
  now passes both corrected schedules, ten collision/retained-role controls,
  29 native Rust cases and six signed regressions of the shared seal path.
  The actual late-query blocker, all 24 exact journals and23 imported identity/OID
  bindings are recorded; unrelated owner/credential state is preserved.
  The recovery consumer reads the original Control attempt by ID/catalog without
  re-registering it or reconstructing its batch from fixture sidecars.
  [Native qualification at `d86e147`](evidence/p02-retained-attempt-read-native-20260919.json)
  retains the failed fcd63ac assertion and passes the actual bounded read probe,
  13 intent/schema cases, three custody and two missing-import schedules with
  ten clone controls. Five journals, four imported identities/OID bindings and
  seven Control rows match independent values; actual blocked cancellation and
  fresh-session retry pass. No sidecar is created. Installed lifecycle/resource
  reconciliation, malformed metadata/extreme values and protection remain open.
  Installed compact recovery preparation shares the protected PostgreSQL adapter
  with Control: the returned actual driver remains unpolled until the catalog
  consumer attaches custody. The real publication fixture consumes it, including
  seal/recovery. [Native qualification at `6606103`](evidence/p02-shared-postgres-transport-native-20260919.json)
  passes six actual CLI TLS/signal/cancellation/restart cases, the same pinned
  query-before-driver discriminator, three custody/two missing-import schedules,
  ten clone controls and six signed journeys. All 27 journal bytes and Control
  requests plus 26 imported identities/OID bindings are checked; resources retired.
  Installed service, shared process grants and complete recovery remain open.
  The installed worker selector now binds the exact builder-produced executable
  length/hash inside the main binary. `archive verify-worker` consumes this anchor
  and the existing offline asset checks; an adjacent receipt or runtime hash
  cannot supply authority. [Native qualification at `53707a2`](evidence/p02-installed-worker-binding-native-20260919.json)
  passes12 installed bundle cases, five invalid build assertions, seven Rust cases
  and actual same-target binding/change/unbinding/restoration. This used retained
  build caches; the complete fresh builder and installed Archive lifecycle remain
  unqualified. Explicit `archive.catalog_id` and
  [`archive inspect`](../engineering/configuration-and-runtime.md#explicit-catalog-inspection)
  now connect configured expected identity to privileged finite metadata checks;
  [native qualification](evidence/p02-catalog-cli-native-20260919.json) passes 18
  installed CLI cases at 3e87a53, with unchanged logical dumps and terminal driver
  checks. Identical production sources at 5262c84 pass 38 scientific guard cases
  (both lock orders, activation and repeated current sealing) plus six shared
  Control CLI cases. Untrusted owner shape
  is locked through bounded fixed scalars before owner values are read by any
  guard entrypoint. Separate reader credentials, retained-batch recovery inputs
  and explicit initialization remain prerequisites.
  No step is complete from these prerequisites.
- [ ] **Step 6:** Handle empty and one-small-event timer batches; publish useful progress
  without waiting for a size threshold that may never be reached.
- [ ] **Step 7:** **Practical P02-T04:** run real PG/DuckLake transactions; kill preparation,
  kill after catalog commit before ACK, and attempt a competing owner. Reopen the
  catalog independently and compare exact logical fact/reference membership.
  Negative control: two unfenced receipt-check-then-insert publishers.
  Include a separate read-only worker without a source-data mount, refused object
  writes, bad credentials, incomplete upload and missing committed objects. A
  metadata-only count is not a content-read test.
- [ ] **Step 8:** Document catalog/object backup coupling, orphan preparation visibility and
  the archive receipt's durability class; record latency/space/native memory.
  Verify actual file/directory/object persistence before pending reclamation under
  that class; a transactional catalog/process-kill test alone cannot qualify
  power-loss durability or weaken a stronger previously issued receipt.
  Track uncaptured/captured file hashes independently of semantic receipts;
  qualify native persistence/integrity before reclamation without requiring an
  extra full S3 GET merely to create a first checksum. Backup/scrub/full semantic
  verification carry their actual budgets and evidence.
  The local file-persistence prerequisite retains the original pre-COPY file
  descriptor and synchronizes it plus directory ancestry before native registration. Exact
  DuckDB source inspection and a native inode test require `USE_TMP_FILE false`.
  A Linux baseline trace at `e1852b8` confirms no prepared-file fsync despite
  successful publication/readback. [Native qualification](evidence/p02-archive-file-sync-native-20260920.json)
  at `b9ddd74` passes nine syscall cases (six EIO and two ENOSPC refusals plus
  ordering), nine native tests and31 actual signed PG cases. Each injected fault
  executes the otherwise ignored fixture and leaves no committed membership;
  completed files survive refused same-attempt retry. No power-loss/protection/
  reclamation claim follows.

The installed Control consumer now inventories retained publication intents before
idle when Archive is locally assigned. [Native evidence](evidence/p02-control-archive-inventory-native-20260919.json)
at `a010c15` covers fixed-cutoff paging in batches of 64, all historical obligations, stale
cursor/gap/mismatch refusal and eight actual startup/restart/cancellation cases.
It implements the enumeration prerequisite in P02-T04 steps 3/4/5 without granting
publication authority or completing any step; source-input linkage, custody,
CatalogManager/reader and Ingest dispatch remain required service connections.

## P02-T05 — Full online representation and immutable projections

**Depends on:** P02-T01/T02/T03. **Files:**
`migrations/clickhouse/{0001_events,0002_evidence}.sql`, application
`src/storage/clickhouse/{rows,chunks,insert,readiness}.rs`,
`contracts/serving/generation.json`,
`apps/bigbrotr/tests/serving_visibility.rs`,
`docs/reference/online-indexes.md`.
**Contract:** immutable facts → full online body plus narrow indexes in an
identified serving incarnation. Logical correction history survives merges.

- [ ] **Step 1:** Create shared payload and scope-qualified signed eligibility
  representations, losslessly joined into full signed events, plus narrow ID/time/author/tag
  access paths; provenance adds both narrow orders only when enabled. Parsed
  monitor fields reference document objects rather than repeat wide payloads.
- [ ] **Step 2:** Key raw fact retries by complete immutable SourceRecordId; projection row
  identity adds generation and subrow discriminator. Replacing merges may collapse
  only byte-identical retries, never successive logical first-seen corrections.
- [ ] **Step 3:** Build deterministic per-table/per-physical-partition chunks containing
  schema, ordered membership and content digest; finite insert dedupe is optional
  acceleration, not correctness.
- [ ] **Step 4:** Configure synchronous durable inserts with the selected fsync settings;
  record exact engine/version/table settings in the serving manifest. Native
  projections are optional rebuildable acceleration. Verify base and projection
  health after recovery; degraded projection state must use a budgeted correct
  base plan or explicit unavailability, never certify incomplete required data.
  Qualify exact release behavior against upstream projection-fsync issue #111318;
  INSERT settings alone cannot establish projection persistence.
- [ ] **Step 5:** Apply the required source cuts and permitted eligibility/evidence
  scopes before resolving signed representatives and correction minima; deduplicate
  identical retry rows before logical counts and limits. Bind query values and
  qualify stored-column references independently of output aliases. Exercise a
  binary-ID versus hex-output alias collision against the actual engine and an
  independent full-row oracle; an empty successful query is not proof of absence.
- [ ] **Step 6:** Treat table replacement, restore or unclean loss as a new invalidated
  serving incarnation until revalidated/rebuilt. A PG receipt cannot certify
  missing CH parts.
- [ ] **Step 7:** **Practical P02-T05:** force actual merges, expire insert-token history,
  delay one tag/inverse-edge chunk and replay. Compare before/after-cut answers
  externally. Negative controls: logical-edge ReplacingMergeTree or base-table
  existence used to skip an incomplete secondary index.
- [ ] **Step 8:** Record body/index duplication, candidate/body lookup costs and part debt;
  P02-T09 selects the measured physical variant before production DDL activation.

## P02-T06 — Replay, frontiers and actual recovery progress

**Depends on:** P02-T02/T04/T05. **Files:** application
`src/ingest/replay.rs`, `src/storage/indexer/{service,frontier}.rs`,
`apps/bigbrotr/tests/replay_frontiers.rs`,
`tests/acceptance/cases/replay.py`, `docs/operations/replay.md`.
**Contract:** complete pending/archive source units → all required projections,
dependency notices and contiguous serving frontier with real restart position.

- [ ] **Step 1:** Read pending first and committed archive fallback using the same immutable
  source identities; replay cannot create new first-admission timestamps.
- [ ] **Step 2:** Bound by rows and bytes (20,000 rows/16 MiB starting batch, one-second flush);
  separately reserve lookahead, probe arrays and driver staging before allocation.
- [ ] **Step 3:** Track zero/nonzero chunks for each required family. Disabled projections
  have explicit scope; absent required sinks cannot report success.
- [ ] **Step 4:** Persist deterministic recovery units/receipts and document exactly which
  unit restarts after an interrupted insert; displayed progress is not a seek index.
- [ ] **Step 5:** Publish affected-key/partition notices before their consumer frontier,
  including legitimate no-output/control-only source ranges.
- [ ] **Step 6:** Stop at poison/unrecoverable source with typed diagnostics; allow unrelated
  independent streams to progress without claiming one global complete cut.
- [ ] **Step 7:** **Practical P02-T06:** interrupt each family and restart the binary; compare
  event/tag/provenance/document membership and cuts against the immutable source.
  Include first huge unit and a quiet single record. Negative controls: first-item
  byte-limit exception, progress emitted before dependency notices, zero-input trap.
- [ ] **Step 8:** Document backlog age, resume granularity, repair procedure and when readers
  can use a stale complete generation.

## P02-T07 — Snapshot leases, retention and safe reclamation

**Depends on:** P02-T04/T06. **Files:** application
`src/storage/archive/{pins,retention,compaction,scrub}.rs`,
`src/storage/postgres/snapshot_leases.rs`,
`contracts/archive/v1/retention.json`,
`apps/bigbrotr/tests/retention_references.rs`,
`docs/operations/retention.md`.
**Contract:** complete snapshots/reference claims → bounded maintenance that
cannot reclaim required evidence, pending data or readable generations.

- [ ] **Step 1:** Register readers, exports, backups and active derived/publication inputs
  against immutable snapshot identities; keep lease expiry distinct from evidence
  retention and policy authorization.
  The [exact native selector](evidence/p02-archive-snapshot-native-20260920.json) at
  `e1852b8` passes31 connected cases and nine native tests. It supplies the reader
  prerequisite only; actual protected frontier and retained pin registration remain
  required before this step can close.
  Native event/cut readers now require the local coordinator and inherit its
  close-only custody; decoded event bodies retain custody through their consumer.
  [Native qualification at `5ba3051`](evidence/p02-archive-reader-custody-native-20260920.json)
  passes 38 actual signed cut/read/refusal/restart cases and 17 scoped Rust tests.
  Native FD inspection, the unbound old-reader control, blocked cancellation and
  deadline, stopped-child parent death and lock reacquisition pass. Fifteen Mac
  custody tests and consumer Clippy also pass. No pin/policy grant or step closure
  follows; concurrent maintenance by the same coordinator remains a separate gate.
  [Historical capture at `a670f03`](evidence/p02-archive-historical-capture-native-20260920.json)
  qualifies exact retained snapshot3 under head4 in two signed cut journeys.
  The new migration0017 and Control pin adapter bind this selection to the original
  frontier, current holder and atomic positive claims, resolve identical retries
  without renewal and release after joined consumption.
  [Native pin qualification](evidence/p02-archive-snapshot-pins-native-20260920.json)
  at `27150f2` passes two signed cuts,14 SQL constraints, unchanged legacy upgrade
  and independent row/xmin checks. Fixture `583f085`/oracle `e828eb6` passes22 fault
  cases, including both lock orders, omitted positive claim, expiry and lost COMMIT
  replies. Original parser failure remains recorded. Typed renewal, old-holder
  recovery, actual concurrent reclamation,
  other consumer purposes and actual authorized read/export remain required.
  [Native recovery evidence](evidence/p02-archive-pin-recovery-native-20260920.json)
  at51d35ae qualifies a fresh recovery-only custody lifetime: readers/new pins
  cannot share it, including rewrap and retained drivers. Fourteen native cases,
  eleven SQL cases, unchanged populated17/legacy16 upgrades and a real omitted
  positive-claim release cover explicit recovery/atomicity/result resolution.
  Applied0017/0018 bytes remain unchanged. Permanent-blocker cancellation, typed
  renewal, actual authorized read/export and concurrent reclamation remain open.
- [ ] **Step 2:** Reclaim pending payload only after committed archive and safety policy;
  preserve identity/receipt metadata for its correctness horizon.
- [ ] **Step 3:** Compact files without changing source identities or reference resolution;
  record new membership atomically and retain old generations while pinned.
- [ ] **Step 4:** Default full events/provenance/actual measurements to permanent retention.
  Optional lossy monitor expiry emits resolution/loss/coverage manifest first.
- [ ] **Step 5:** Delete unreferenced objects only after accounting for snapshots, exports,
  backups, staged ownership and concurrent publication; no live-file glob cleanup.
- [ ] **Step 6:** Use one resumable scrub executor with byte/CPU/I/O budget, persisted cursor
  and declared full-byte versus sampled semantic verification.
- [ ] **Step 7:** **Practical P02-T07:** hold an export/read pin while compacting, crash between
  object preparation and snapshot commit, expire an unrelated generation and
  resume. Independently open both retained snapshots. Negative control: GC based
  only on latest catalog membership or wall-clock object age.
- [ ] **Step 8:** Document retained bytes, maintenance headroom and why local compaction/
  retention cannot erase already exported or remote copies.

## P02-T08 — Restore, loss detection and repair skeleton

**Depends on:** P02-T01–T07 plus P01-T06/T08.
**Files:** application `src/storage/recovery/`,
`contracts/backup/manifest.json`,
`tests/acceptance/cases/storage_recovery.py`,
`docs/operations/{backup,restore,corruption}.md`.
**Contract:** versioned consistent backup → positively owned restored deployment
with original identities and correctly invalidated/rebuilt derived readiness.

- [ ] **Step 1:** Define backup closure: control/catalog plus objects, required pending WAL
  or quiesced archive cut, dictionaries, owner/source topology, consumer progress,
  policy and separately protected secret material.
  Include causal closure of owner ACK/receipt-GC with the producer checkpoint
  that authorized it; refuse post-GC owner plus pre-checkpoint producer mixtures.
- [ ] **Step 2:** Restore ownership/policy/catalog/object integrity before enabling writers,
  then embedded identities/pending state, projections and dependent workers.
- [ ] **Step 3:** Preserve original event ordinals/receipts/observation metadata; importing a
  dataset and restoring an instance must take separate command paths.
- [ ] **Step 4:** Invalidate serving/read cursors after incompatible incarnation changes;
  rebuild from immutable evidence instead of trusting surviving PG frontiers.
- [ ] **Step 5:** Bound forensic work and preserve suspicious bytes; ordinary startup cannot
  allocate an entire damaged tail or silently truncate later valid records.
- [ ] **Step 6:** **Practical P02-T08:** restore into fresh isolated volumes while the source
  exists, compare exact IDs/body digests/edges/dictionaries and actual read output,
  then exercise missing object, corrupt part, wrong schema and unavailable key.
  Negative control: counting equal rows while substituting another event.
  Exercise local→S3 and S3→separate-prefix relocation through pinned catalog/file
  closure, destination readback and explicit activation; a DATA_PATH edit alone
  is refused as a migration. Restore all scientific families, not only events.
- [ ] **Step 7:** Prove old-owner exclusion before enabling the restored writer; rehearse
  interrupted restore and keep the source untouched.
- [ ] **Step 8:** Record time/temporary space and failure-class limits. P08 completes the
  operator UX and full release drills; these safety contracts are implemented now.

## P02-T09 — Physical design and mixed workload decision

**Depends on:** P02-T02–T08; uses real Rust adapters and P01 harness.
**Files:** `tests/workloads/storage/`, `contracts/workloads/storage.json`,
`docs/decisions/storage-layout.md`,
`docs/operations/sizing.md`, versioned CH/embedded schema migration chosen below.
**Contract:** fixed logical semantics + candidate physical layouts → measured
release selection and explicit capacity limits, not a best-engine slogan.

- [ ] **Step 1:** Use the complete scope-qualified provenance key from A02 as the baseline;
  compare grouped sorted relay values and bitmap-plus-required-metadata only
  when they preserve times, scopes, receipts and correction/recovery behavior.
- [ ] **Step 2:** Compare ID-ordered full bodies plus narrow chronological/author/tag paths
  against chronological bodies with ID access. Control physical part counts,
  partition fanout, body duplication and cold/warm cache state. Compare physical wide repeated
  document columns against object/reference tables under real dictionary
  compression, visibility and standalone export; logical deduplication alone
  does not establish lower Parquet bytes.
- [ ] **Step 3:** Vary U, new P, repeated D, actual M measurements, document-change rate,
  tag fanout, late corrections, wide records and ongoing imports/queries.
- [ ] **Step 4:** Exercise 256 MiB/64k-row archive targets around smaller/larger bounded
  application inputs; record actual group/file sizes, freshness, native peak RSS,
  file requests, compression and compaction/recovery debt. Include quiet workloads
  across many tables with the selected inlining setting. Engine row/byte/file
  targets are not memory caps; verify supported settings and failure behavior.
  Compare local storage and a qualified S3 provider with request counts, real
  data movement, credential rotation/outage, backup verification and relocation;
  loopback object serving does not qualify WAN or independent-host failures.
- [ ] **Step 5:** Measure all representations, temporary/pending/merge headroom and backup
  restore cost; report read/write amplification and worst admitted query behavior.
- [ ] **Step 6:** Select a candidate only if correctness/recovery scenarios pass and it fits
  the intended host envelope under mixed work. Compare p95/p99 read latency,
  sustainable catch-up rate and total bytes; if tradeoffs cross, retain the
  simpler exact baseline and document the workload-specific alternative.
- [ ] **Step 7:** **Practical P02-T09:** age real stores with many parts, run acquisition,
  queries, ranking-sized scans and maintenance concurrently, then interrupt/
  recover. Negative control: a warm isolated insert loop that hides part/compaction
  debt is not accepted as sustainable throughput.
- [ ] **Step 8:** Record the chosen DDL/engine settings and evidence before enabling release
  migrations; label unrun billion-scale/host-loss cases explicitly. No capacity
  certificate is inferred from the small planning experiments.

## Phase evidence and integration

P02-T01/T02 can progress before every later P01 integration slice is finished.
P02-T04/T05/T06 feed P01-T09's first-event journey. Phase 03 consumes durable
admission and the minimum endpoint registry; phase 05/06 consume coherent source
cuts. Later phases extend workflows without weakening these invariants.
Review schema/config/docs and actual engine settings together with implementation.
