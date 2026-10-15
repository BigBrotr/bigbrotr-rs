# Initial ingest registration

`storage::postgres::ingest_registry` implements one bounded coordination
transaction for the first Ingest resource/source and initialization producer in
an **already initialized** control database. It does not initialize configuration
or policy and cannot activate a native writer. Its returned observation is data.

`InitialRegistration` fixes dataset/instance, the current Control tuple, expected
config/policy revisions and document digests, previous topology, new Ingest role
and resource/incarnation, shard, ordered allocation domains, producer slot, count
limit and initial times. The new owner/source/producer epochs are all one. A
missing topology advances to zero; an existing sealed empty topology advances
by exactly one. A populated predecessor needs the separate handoff operation.

The transaction checks PostgreSQL 16.15, `fsync`, `full_page_writes`, both exact
migration receipts, and the caller's application privilege boundary. A superuser,
control schema/table owner, effective owning-role member or caller with control
schema creation rights is refused. Runtime connections must come from the actual
configured trusted adapter and retain their driver/resource/deadline ownership.
The dedicated migration credential is not an application connection.

Registration takes the shared migration transaction lock, then its dedicated
registration transaction lock. The latter is acquired in a separate statement
before Read Committed lookups, so an earlier holder's commit precedes their
snapshot. Dataset and relevant authority rows remain locked through the final
head CAS. Only the required role, owner, topology/member, source, allocation
domains and initialization producer are inserted. Source membership and topology
sealing commit together. No scientific event or Rocks receipt is written here.

Every untrusted row projection is bounded: exact fixed identity/digest lengths,
canonical full-domain u64 text, a maximum of nine domains with an excess-row
discriminator, and fixed small schema/membership result sets. Identity comparison
includes the complete expected domain set. Domain hash equality never grants
allocation authority. Per-statement/lock/idle transaction timeouts are 20/5/10
seconds; these do not replace the caller's whole-operation deadline.

`RegistrationAttempt` owns an immutable specification and is neither Clone nor
Deserialize. It changes to InFlight before the first await and CommitDispatched
before awaiting COMMIT. Cancellation or a lost result preserves that state.
Another registration call on an unresolved attempt is refused. `resolve` uses a
fresh connection and the same lock, then observes committed rows after the old
transaction has ended. Absence permits retry of the **same** specification;
matching/conflicting rows terminate that attempt. An unavailable lookup leaves
the attempt unresolved. Ordinary errors after a confirmed rollback permit retry.

`Matching { current: false }` retains exact historical identity while reporting
changed heads or closed/retired state. Even `current: true` supplies no policy
lease, action grant, local root binding, producer closing barrier, native lock,
positive old-writer exclusion or durable external effect. The native owner must
establish all of those through its real configured factory and commit boundary.

The [registry experiment](../../experiments/ingest-registry/README.md) exercises
this adapter against actual PostgreSQL with synthetic coordination prerequisites.
The [2032042 native evidence](../implementation/evidence/p02-ingest-registry-native-20260918.json)
records five executed Linux amd64/PostgreSQL 16.15 scenarios, complete external
rows and actual COMMIT/lock schedules. Product Control bootstrap, durable policy projection,
native activation, successor/recovery registration and first signed-event
admission remain separate work.

## Historical registration lookup

The installed recovery consumer must reconstruct the original registration from
the exclusively held native manifest and retained PostgreSQL rows. The selected
`load_historical` read takes an `IngestRecovery` and configured instance, uses the
existing migration/registration serialization, and returns checked attribution
only. It performs no COMMIT or DML and releases its transaction by confirmed
rollback. Callers retain the bounded native worker, application connection and
driver through cancellation or completion.

The original manifest selects dataset, Ingest tuple, shard, topology, policy and
allocation domains. Its policy revision selects the original configuration and
both digests; current dataset heads must never substitute for that history. A
bounded producer lookup requires exactly one initial slot, with an excess-row
discriminator. The slot identifies its original registering Control tuple.
Complete existing registration checks validate all reconstructed fields, including
source, topology membership, domain set and policy schema. Missing, ambiguous or
inconsistent rows refuse; absent predecessor stays distinct from generation zero.
The retained scheduling expiry may reflect renewal and is not an authority grant.

This initial loader refuses noninitial owner epochs. It does not close a producer,
resolve missing outcomes, establish recovery provenance or activate a successor.
Existing connected recovery revalidates the complete registration under fresh
Control authority before any effect. No SQL migration or native format changes.
The [historical lookup evidence](../implementation/evidence/p02-ingest-registry-native-20260918.json)
records ten actual PostgreSQL 16.15/RocksDB 11.8.1 cases at `fc695d2`, using two
installed initialized stores. All 69 Control tables and 17 native rows remain
unchanged per read. Original heads survive changed current revisions, and null
versus zero predecessors are distinct. Missing/ambiguous registration, changed
source identity, unknown policy schema and wrong instance refuse. This verifies
bounded attribution only: historical setting validation, blocked-query cancellation,
installed recovery and successor activation remain separate requirements.

## Same-Control scheduling renewal

The explicit installed first lifetime constructs its attempt from
`ControlSession::initial_ingest_basis`: a bounded fenced read of both accepted
documents, the actual Control deadline/server clock, and absent or sealed empty
topology. Zero is a valid previous topology. Any existing source refuses fresh
initialization. This observation grants nothing; registration and native dispatch
still perform their complete checks. Cancellation poisons reuse and the real
driver remains owned. No server clock or accepted managed limit is invented from
the startup file. The [installed boundary](ingest-store.md#explicit-initial-owning-lifetime)
does not yet promise recovery of partial initialization.

The installed initial owner needs its own scheduling renewal: Control heartbeat
alone does not change the Ingest row. `ControlSession::renew_initial_ingest`
checks the same complete initial registration and both accepted documents, under
the original dedicated Control backend. It takes Control, dataset, Ingest UPDATE
and dependent registry/producer locks in that order. It requires both old owner
deadlines to remain live and sets the Ingest deadline to the current Control
deadline. Refresh Control first. No new identity, producer state or native data
is created; expired or draining owners and closing producers are refused.

The suspend-aware operation window starts before coordination I/O, is at most
five seconds, and is also bounded by the remaining old scheduling deadlines.
The write rechecks both old deadlines after lock waits. The caller additionally
bounds I/O and retains its driver, native owner and memory reservations. Failed or
cancelled coordination poisons session reuse. A lost COMMIT may have renewed
the row; an expired response does not provide new authority. There is no durable
heartbeat receipt or claim of server-side expiry fencing at delayed COMMIT.

This operation changes scheduling data only. Actual native exclusion, fresh
admission/source/policy checks, caller observation custody and successor recovery
remain independent requirements. It cannot make a standalone initializer's
registration writable by a later Control process. No applied SQL or native
format changes. The finite probe and `bindings.py --lease` have
[eleven executed native cases](../implementation/evidence/p02-ingest-lease-native-20260928.json)
at e86439e: actual read after the old deadline, exact sole scheduling-row change,
complete refusal-state equality, and observed dataset-lock cancellation with
retained fencing. Root checked original native/input bytes and all 64 Control
tables. Lost/delayed COMMIT, renewal-write cutoff discrimination and policy/closing
races remain unexecuted for this method. No installed lifetime claim follows.

## Prepared initial successor

Migration0030 and `ControlSession::prepare_initial_successor` introduce a
prepared-only continuation of the fully retired initial Ingest owner. The exact
[native handoff](ingest-manifest-format.md#initial-owner-handoff-receipt) retains
both manifests, closure digest and source/request bounds. Its local plan must
match the exclusively held configured store before PostgreSQL mutations begin.

Current Control and accepted policy custody are independent of the original
slot registrar. Under Control, dataset, historical owner/domain/source/producer
locks, one transaction inserts the fresh role/owner2, source2, producer2, fresh
allocation domains, and sealed topology containing closed source1 plus source2.
The immutable `control.initial_ingest_successions` intent retains exact bytes,
original preparing Control and policy attribution. Confirmation of that COMMIT
precedes native dispatch. A second transaction reacquires the same custody and
checks accepted policy without extending the original local policy deadline.
Only then may the one synchronized native metadata transition execute.

An unknown preparation COMMIT must be excluded and observed under fresh current
Control before retry. Exact retry uses the same retained plan and request fields;
it cannot invent a new attempted incarnation over the old intent. The original
retired producer and native detail remain untouched. Migration0030 refuses
preexisting unrecognized successor history and vetoes successor request/input
binding INSERTs, even when their producer lacks an intent. This preparation call
does not reconstruct a lost plan. The adapters below inspect completed preparation or complete an existing gap; activation, writer conversion and abandoned-empty-attempt retirement
remain required work. An OPEN scheduling row or completed native batch grants none
of that authority.

The [connected recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json)
records four actual PostgreSQL 16.15/RocksDB 11.8.1 cases at harness `ac644ef`,
using exact-source probe `4b89687`. A frozen retired signed store advances from
49 to 52 native rows in four writes; all four original inputs survive unchanged.
All 71 Control tables, historical row versions and the exact immutable intent
are checked through same-Control and fresh-Control retries. A refusal after
confirmed preparation COMMIT leaves PostgreSQL prepared and native unchanged;
changed intent refuses before an exact retry completes. Schema 29 and a proven
intent-insert SQL rollback leave no preparation/native effects. The earlier
Control claim/renewal/release changes are accounted for separately.

Both successor request and input-binding attempts reach the named activation
veto with SQLSTATE 23514 under the application role. The latter is a synthetic
envelope, not a completed import workflow. The probe retains its local proof
inside one process. The subsequent `64419a5` fixture with `3969a61` binaries
interrupts the actual preparation COMMIT before forwarding and after server
completion but before the client reply. Both return CommitUnknown without native
dispatch. The failed driver is joined and the original backend's exact
PID/start/database tuple is independently observed absent before fresh Control
claims authority. Before forwarding, retry creates the absent intent with fresh
attribution; after completion, it preserves the original intent and row version.
The independent oracle checks 24 full snapshots and rejects 34 corrupted captures.
That wave retains the local plan in one process. The read-only extension below
qualifies a separate replacement process. Abandoned-attempt retirement, writer
activation and installed restart remain unqualified. Prepared metadata does not
grant admission.

## Read-only prepared history

`ControlSession::inspect_prepared_initial_history` reads an already completed
epoch2 preparation under fresh actual Control and the exclusive configured native
owner. It decodes one bounded exact intent and validates the retired original
registration, original preparing Control attribution, retained policy/configuration,
complete prepared target and native handoff. The original Control may already be
released; the new Control must positively exclude its backend through the existing
claim protocol. Expired historical scheduling times never become new authority.

The returned `PreparedInitialHistory` borrows the native owner and retains its
metadata grant. It selects epoch1 input/receipt interpretation without changing the
epoch2 singleton manifest. Each `inspect_prepared_initial_request` refreshes policy
custody, checks the complete history and compares the original reservation, exact
input, positive native effects and existing PostgreSQL outcome copy. Confirmed
rollback ends each read transaction; cancellation or unknown delivery poisons
Control reuse. Explicit caller Control renewal is a separate scheduling write.

The source accumulator requires every original request in order and the exact
retained count, cut and closure digest. Only this positive proof may reconstruct
the local handoff guard. Missing intent/detail, nonempty target, target sidecars,
inconsistent attribution and obsolete native observations refuse without repair.
The bounded intent/registration grant is at least 128KiB; callers independently
retain policy-document, input, outcome, native and driver grants and deadlines.

This API introduces no migration or persistent-format change. At this wave the
installed recovery command accepted only its original epoch1 path; the explicit
empty-attempt consumer below extends that entrypoint. Exact-source build39e7468 passes seven focused native reconstruction cases and
affected replay regressions. Harness e92f3d7 passes seven actual PostgreSQL16.15/
RocksDB11.8.1 cases from copies of an already completed prepared signed store.
A genuinely new process receives configuration only, claims fresh Control10 and
reconstructs four original positive outcomes/cut12 while preserving registrar3
and preparing Control8. All71tables,52native rows/sequence62 and four input hashes
are checked; explicit claim/renewal/release effects are accounted for separately.
Missing/changed intent, missing native handoff, nonzero successor, missing original
input and missing PostgreSQL outcome all refuse without repair. The independent
oracle checks67snapshots and rejects90corrupted captures, including a consistently
fabricated future renewal. Actual server-clock bounds and observed backend absence
are retained. The original seed and six frozen artifacts remain unchanged.

The preparing process had already finished; this wave does not inject concurrent
live-owner loss or interruption during preparation. Completing a PG/native gap,
retiring an empty attempt, activating a writer, installed restart/resend and
repeated lifetimes remain separate requirements in the existing plan.


## Completion of an existing initial gap

`ControlSession::inspect_prepared_initial_gap` handles the complementary state:
PostgreSQL has an exact committed preparation, while the excluded native store
still has its original epoch1 manifest. The loader uses the same bounded intent,
attribution and prepared-target checks as completed-history inspection. It also
requires absent native transition/target markers, empty target namespaces and
absent target input files. Missing or mixed state refuses without initialization.

`PreparedInitialGap` retains the actual native borrow and metadata reservation.
Ordinary initial-retirement reads reconstruct every original input, native effect
and existing PostgreSQL copy. Consuming that metadata together with the complete
positive source witness produces an owned `InitialGapPlan` only if all encoded
handoff bytes agree. The caller ends the source borrow before mutable dispatch;
the plan retains its exact native directory/sequence guards and metadata grant.

`complete_prepared_initial_gap` checks the same existing intent under fresh actual
Control, accepted current policy and retained row custody, then applies the four
native writes and confirms a read-only PostgreSQL rollback. It cannot insert a
missing intent or rewrite the original target incarnation, timestamps, policy or
preparing Control. Current policy may have advanced; it must be at least the
retained target policy and pass current validation. No preparation COMMIT is
needed because the intent must already be durable.

Cancellation before dispatch leaves native unchanged. Failure during or after
dispatch can leave the batch durable; retain and join actual worker/driver custody
and inspect state under fresh authority. Same-process exact plan replay may
observe completed native2 without another write. A replacement process observing
native2 uses the separate completed-history reader. Neither path activates a writer
or retires the empty attempted owner.

Exact-source build162caeb passes nine native reconstruction tests, affected
replay/codec regressions, entrypoint/schema checks and Clippy. Harness51a6b86 then
creates a real gap through confirmed preparation followed by an authority refusal
and process exit. Seven separate recovery processes qualify normal completion,
missing/changed intent, target input, missing original input/outcome and cancellation
after proof. Fresh Control9 fences the exited preparing Control8 and preserves the
original registrar3 and immutable intent. Normal native state advances from 49 rows/
sequence58 to 52/62; cancellation and all refusals preserve the original state.
Explicit Control lifecycle writes are separated from all read/completion phases.

The same binaries pass the seven affected completed-history cases on separate
copies. In total, the independent oracles check 158 full snapshots and reject 198
corrupted captures, including the real gap preparation. Original retired/prepared
seeds and the newly created gap seed are preserved. Exact captures and scoped
independent review are in the
[recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json).
The original process exited after a returned refusal; no kill was injected.
The `4c283e2` qualification adds six actual processes: guarded current policy2
with historical policy1 retained, loss before completion's transaction starts,
and loss of the read-only ROLLBACK reply after native completion, each followed
by a new config-only recovery. Before-native loss leaves 49 rows/sequence58;
fresh recovery completes to52/62. After-native loss retains52/62; fresh recovery
inspects history without another write. Failed Control reuse refuses, its driver
is joined and its exact backend is observed absent before the next claim.
All 114 full snapshots and 123 corrupted-capture controls pass. Policy advancement
is a privileged isolated fixture with normal guards, not a product activation
writer. Installed recovery, empty-attempt retirement, writer activation, native
syscall ambiguity and general subsequent lifetimes remain separate requirements.

## Empty prepared attempt retirement (under implementation)

The separate recovery consumer retires only the exact unactivated epoch2 attempt.
It retains the complete predecessor proof and immutable preparation. A separate
native context accepts an explicit OPEN/zero or CLOSING/zero target producer;
ordinary handoff application and completed-history reconstruction remain strict
OPEN/zero operations. Decoding a handoff alone cannot construct a retirement plan.
Every original input, native effect and existing SQL outcome must be revalidated.

The owned plan retains its metadata grant, exact directory identity and Rocks
sequence after the source borrow ends. Native dispatch rechecks the singleton,
handoff, explicit zero source, exact producer marker, empty target namespaces
and absence of target input sidecars. OPEN/zero becomes CLOSING/zero in one
synchronized producer-marker write. An exact closing retry makes no write.
An OPEN-derived plan recognizes only that one-write completion; unrelated sequence
changes, equal-sequence foreign roots, missing/nonzero markers and sidecars refuse.

The empty closure digest is SHA-256 of the following concatenation, with fixed
width binary inputs and unsigned BE64 integers:

```text
ASCII("bigbrotr/initial-empty-attempt-closure/1") || NUL
|| SHA256(exact retained handoff bytes)
|| SHA256(exact target manifest bytes)
|| producer_slot[16] || BE64(2) || BE64(0 issued) || BE64(0 source cut)
```

This distinct digest binds the abandoned attempt and empty target. The original
source closure digest is preserved. It does not certify a new scientific outcome,
ACK, restored-state guarantee or permission to reuse the attempted incarnation.

The PostgreSQL consumer requires additive0031 and current accepted policy. It
holds the actual current Control row FOR UPDATE before dataset and ordered
target parent custody, then rechecks authority after waits. This also excludes
concurrent generic import claims whose existing guard takes SHARE on that Control
row. Dataset custody alone does not exclude a zero-request import claim.

Before a prepared attempt can retire, acquisition/import claims attributed to its
preparing Control through current Control must be absent, including zero-request
claims. These are Control lifetimes, not the target Ingest owner or original slot
registrar. Target requests/outcomes, input bindings, checkpoints (even zero) and
gap history also refuse. Unclaimed jobs remain unchanged. After native closing,
one SQL transaction closes source2 at explicit zero, retires producer2, closes
domains2, releases owner2 with storage fencing and retires role2. All terminal
fields share one database timestamp and the empty closure digest. Applied0029,
0030 intent immutability and the successor admission veto remain in force.
Sealed topology membership, including the target NULL handoff bound, is unchanged.

Only these durable combinations are supported:

| Native target | PostgreSQL target | Operation |
|---|---|---|
| OPEN/zero | Exact prepared | Close native, then retire SQL |
| CLOSING/zero | Exact prepared | Finish SQL retirement |
| CLOSING/zero | Exact terminal | Observe unchanged completion |
| OPEN/zero | Terminal, or either side partial | Refuse |

Transaction-internal monotone staging is not a loader-accepted partial state.
An exact terminal observation revalidates original proof, target identities,
digest, common timestamp and absent target-bound detail without rewriting xmin.
It does not repeat the claim-window assertion against later unrelated Control
jobs: no retained retiring-Control attribution exists to reconstruct that earlier
upper bound. The absence assertion is made while performing the terminal change.

Unknown native completion or SQL COMMIT retains custody and poisons failed Control
reuse. Fresh recovery must positively exclude the old backend, rebuild the proof
and distinguish exact prepared from exact terminal state. Exact build30a88ae
qualifies the native component and affected regressions:13checks/122test executions,
including14reconstruction tests. Connected83db9d8 uses26 actual processes, including
11 empty-attempt probes and15 affected history/gap regressions. The independent
oracles check333full snapshots and reject465corrupted captures. One27-byte native
marker write precedes one SQL transaction changing13target rows with common time
and digest. Same-Control and fresh-process terminal retries preserve both stores.
A real SQL rejection after native closing leaves prepared metadata; new Control
reconstructs all four original requests and finishes without another native write.
Fourteen raw SQL cases reject partial lifecycle commits and current zero-request
claims, while a later unrelated unbound claim preserves terminal classification.

The original be4f284 fixture diagnostic mismatch remains FAIL; only its expected
legacy function display name changed before the corrected run. Connected5938617
also qualifies actual terminal COMMIT loss before forwarding and after server
completion: four processes,86 full snapshots and128 corrupted-capture controls.
Both failed owners refuse reuse and their exact backends are observed absent.
Fresh config-only recovery completes prepared SQL or preserves already terminal
SQL, including its original timestamp/xmin, without another native write.
Connected1d3b93c adds four actual current-Control claim/expiry schedules:86 full
snapshots and201 corrupted-capture controls. Claim COMMIT prevents retirement;
ROLLBACK permits it. A held retirement COMMIT6 blocks the claim, then its later
completion leaves terminal retry unchanged. Real Control UTC expiry while waiting
refuses before native closing. These are guarded relational import claims, not an
installed import executor. Connected17ccb8a creates a genuine claim while preparing
Control8 is still live, then ends that process and confirms its backend absent.
A new config-only Control9 refuses at empty-load with the named historical-claim
veto. Sixteen full snapshots and39 corrupted-capture controls preserve claim8,
immutable intent, originals and native52/sequence62; no target retirement occurs.
Native syscall ambiguity remains a separate gate in the existing recovery evidence
and task plan. The installed entrypoint is qualified below. No epoch3 or writer
activation is added.


### Installed empty-attempt retirement (under implementation)

`bigbrotr ingest recover --retire-empty-attempt --file ROOT` selects only this
existing-attempt operation. It conflicts with `--close-source`,
`--abandon-acquisition` and `--retire-initial`; the default original-producer
recovery keeps its existing behavior. The command requires local Control/Ingest,
existing exclusive native custody, migration0031 and immutable preparation intent.
It accepts no caller-supplied replacement identity and creates no new attempt.

The existing supervised blocking owner opens the configured native store before
reading protected credentials or claiming Control. This mode then uses configured
dataset/instance and a fresh actual Control claimant; the original-only registry
loader cannot classify a prepared epoch2 manifest. The dedicated empty-attempt
inspector validates the exact retained state. One bounded input/outcome pair
replays each original request in order, with current accepted policy and the
existing64-request spool bound. The metadata grant remains held while the original
source proof becomes an owned retirement plan. Preclaim admission verifies the
simultaneous native/connection, metadata, source-witness and replay grants, plus
the later plan/dispatch overlap. Missing originals, unresolved claims, mixed state
or insufficient resources refuse; no missing outcome is synthesized.

After positive replay, the existing native-first/SQL-terminal adapter retires the
exact empty target, or observes its unchanged completion. The worker destroys
native custody before releasing current Control. The parent retains the actual
driver/grant through its join and emits success only afterward. Cancellation,
timeout, failed output or unknown completion preserve retained state for the same
root/mode; an unjoined blocking worker follows the existing process fail-stop.
No listener, acquisition, Archive worker or writer activation is started.

The `bigbrotr-ingest-recovery/1` success state is `empty-attempt-retired`.
Top-level `producer_epoch`, `issued_through` and `reserved_through` describe the
empty target:2,0,0. `resolved_original_requests` counts the positively replayed
predecessor requests. `original_source_closure` names predecessor epoch1, its
issued/resolved counts, complete source sequence and original digest.
`empty_attempt_retirement` names owner epoch2, its attempted incarnation, retained
terminal timestamp, empty closure digest, domain count and committed target owner
release. Native acknowledged/retired watermarks remain0. Integer counts and times
are decimal strings. Common dataset/resource/slot identities, `native: closed`,
current `owner_release: committed` and `ready: false` retain their existing meaning.
A second invocation must preserve terminal timestamps and row versions.

Exact build7e820b9 passes compilation, five actual entrypoint tests and Clippy.
Connected788f971 then passes eleven actual recovery commands on eight new
signed-seed clones, with22 external full-state observations and296 rejected
capture perturbations. Normal retirement changes one native marker and13 target
SQL rows; a second process preserves terminal timestamps and row versions. A
guarded SQL rejection after native closing is completed by fresh Control11
without another native write. Missing input/outcome, schema30, occupied native
custody and ordinary epoch1 recovery all refuse as specified. Original signed
inputs, registrar3, preparer8 and immutable intent remain exact.

Observed preclaim SQL waits qualify SIGTERM and deadline. The actual CLI and
its driver finish while the server backend can remain blocked until fixture
unlock. Positive global and exact PID/start/database absence is required before
external native reopening; client exit alone is not a rollback receipt. These
commands make no target or Control claim changes. This wave does not exercise
installed CLI COMMIT loss, blocked native syscalls, power loss, writer activation
or full restart/resend. The adapter COMMIT-loss evidence remains separately pinned.

## Live initial successor activation (under implementation)

The next selected responsibility continues P02-T01 Step5 / P02-T02 Step4 with
one genuinely new native-owning process preparing and activating owner/producer
1→2, then admitting a signed event. It preserves all original signed data and
request history. Repeated lifetimes, replacement after a retired empty epoch2,
installed Sync/Archive restart and M1 remain required subsequent consumers.

An opaque live candidate owns the exclusive `IngestRecovery` and internally
mints its new incarnation, role identity and fresh allocation domains. It cannot
be cloned, deserialized or reconstructed from retained identity bytes. Positive
original replay constructs the existing exact handoff using those identities;
preparation reuses the existing confirmed-PG-before-native protocol. Ordinary
historical handles, `InitialHandoff` and `ObservedHandoff` remain observations,
with no public conversion into a writer. Continuous native custody and the
actual locally minted candidate survive through activation.

An additive immutable PostgreSQL activation receipt distinguishes an activated
owner even before its first request. It binds dataset/resource/target owner,
role/incarnation, producer identity, exact handoff/manifest digests, preparing and
activating Control attribution, accepted configuration/policy and database time.
This first live consumer requires the same actual current Control that prepared
the candidate, unchanged accepted preparation policy and both live scheduling
deadlines. It cannot revive an expired attempted owner. Control custody precedes
dataset and ordered historical/target parent locks; deadlines are rechecked after
waits. Applied migration0030's prepared-only classifier remains strict.

Only confirmed activation COMMIT, final native/authority validation and private
consumption of the live candidate return the active writer. Unknown COMMIT returns
no writer and poisons Control reuse. A replacement process can observe retained
activation but cannot adopt the departed incarnation. No extra native marker or
format is selected: the exact schema2 manifest/handoff remain unchanged. The
activation receipt must make history, gap and empty-attempt retirement refuse
activated state, including an otherwise empty target. A separate active validator
and renewal operation account for subsequent scheduling changes.

Epoch1 keeps its existing reservation behavior. Epoch2 reservations/input bindings
require the exact activation and still-current activating Control. Even the first
request/input binding must match the activated target-manifest digest; absence of
a prior request cannot supply a missing identity check. Other epochs
remain refused by this bounded consumer. Share the existing reservation, input
staging, native dispatch and outcome-copy protocol through a validated authority
context. Decoded database data alone cannot construct the local live capability.

A live successor starts at request1 with an explicit source counter0 and fresh
allocation domains, while retaining old dictionary/visibility references. It does
not rerun bootstrap. Source0 remains valid after a rejected or AlreadyDurable
request, provided no target facts or pending batches exist. Missing/malformed
source state and zero allocation counters remain distinct refusals. Positive
Evidence replay treats request1 as bootstrap only for the actual initial epoch1
profile; epoch2 request1 is an ordinary append.

Acceptance must check the actual locally minted versus foreign/observed candidate,
first dictionary append1, no-fact request1 followed by new-fact request2 starting
at source sequence1, exact retry/conflict, original-byte preservation, resource and
authority refusal, activated-zero rejection by earlier recovery routes, renewal
and actual activation COMMIT loss on both sides of server completion. Connected
signed admission and resend must preserve first-admission semantics, including
provenance disabled. Exact native buildc640b2f passes135 tests including six new
live-writer groups. Finite connected2c142c6 passes five actual candidate processes,
48full snapshots and116 independent capture controls: zero activation, first
append/signed admission/retry/alternate signature and COMMIT13 loss on both sides.
Its import event has no network observation; the provenance-off setting therefore
does not qualify real network provenance suppression. Connected5fe7cc0 additionally
qualifies14 rollback-only SQL discriminators on the actual preparing backend
(one positive and13 named23514 refusals), catalog restoration, confirmed activation
and typed live scheduling renewal followed by signed admission. Its independent
oracle checks16 complete snapshots and35 rehashed perturbations. The renewed
Ingest deadline follows current Control; it can shorten the initial deadline.
Connected983342e now qualifies the first-manifest fence after committed activation,
renewal under a different genuine Control, actual Ingest expiry with live Control,
and renewal COMMIT15 loss before/after server completion: five processes,67 full
snapshots and108 rehashed capture controls. Both manifest attempts reach the named
23514 veto on actual Control8; a subsequent typed first append succeeds. Control9
gets InvalidInput before an operation and remains usable. Expiry gets Backend and
poisons Control; the observed SQLSTATE remains null in default server logs. Both
COMMIT-loss cases return CommitUnknown and poison Control; only after-complete
persists the new scheduling deadline. Immutable receipts and all original bytes
remain unchanged. Installed/general restart remains unqualified. Connecteda80710a
qualifies three fresh-process activated-zero refusals plus an unactivated positive
history control (39full snapshots/84capture perturbations). History and empty
retirement reach the named activation veto; gap rejects native epoch2 before SQL.
The default server log omits SQLSTATE, recorded as null. All target/original state
remains intact; this grants no writer adoption or general activated-owner recovery.
Detailed identities and exclusions are in the existing
[recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json).


### Installed live initial successor (under implementation)

The selected next consumer is explicit `serve --activate-initial-successor`, with
local `control,ingest` and optional `sync`. It is mutually exclusive with fresh
`--initialize-ingest`. The caller must first stop and fully retire the signed
initial predecessor. Configuration and durable state are the only startup inputs;
no supplied target identity, in-memory historical plan or implicit retirement is
accepted. Existing epoch2, missing original proof and competing native ownership
refuse. Repeated lifetimes and activated-owner recovery remain subsequent work.

The existing supervised blocking owner acquires exclusive native custody before
protected connection preparation. It loads the historical registration, positively
replays each original request, mints a local candidate and prepares/activates it
under one actual Control. A fenced basis supplies current accepted policy, server
UTC and the actual Control deadline without weakening fresh initialization's empty
source requirement. The active writer, candidate or retained native recovery must
occupy the native-first custody field across every fallible post-claim phase.
Cancellation and failed/unknown delivery preserve state; no writer adoption follows.

Share the installed command/Sync loop and validated native authority kernel.
The live branch uses activation policy, current accepted request limit, producer2
and request1, including actual Control/Ingest renewal. Historical registration
identifies retained data; it is not the new policy snapshot. Opaque live visibility,
observation and certification operations expose no mutable native store. Current
source/job authorization, bounded decoding, receive-clock evidence and signed
admission keep their existing semantics. Successor dictionaries use fresh domains;
provenance-off still retains local receive evidence while suppressing new witness
and provenance effects. Original event bytes, first receive and existing provenance
must survive resend unchanged.

Preclaim resource admission must cover retained activation metadata (128KiB data
and8192 native bytes), native ownership, Control/driver, retained inputs, positive
replay and future Sync overlap. Keep socket/native close before Control release
and actual driver/grant join before terminal success. The initial Archive consumer
still assumes epoch1, its topology and frontier0; selecting Archive with this mode
must refuse before startup until a separate lifetime-aware consumer is qualified.
The deployment may declare its required Archive counterpart without selecting it.

Acceptance requires an actual installed process and authenticated finite-job path,
a real controlled WebSocket carrying a predecessor resend and a distinct signed
event, renewal beyond the original target deadline, complete SQL/native/input
preservation and shutdown. Compare provenance-off with provenance-on; include the
three startup refusals above and affected original-initial regressions. This finite
Sync consumer does not claim coverage, ACK/GC, Archive restart or M1 completion.

Connected61a3de1 qualifies five installed Serve invocations: provenance off/on,
existing epoch2, missing original and competing native owner. Root independently
replayed ten intermediate plus ten complete before/after observations and98
adversarial controls. Off retains70 native rows/sequence84 and on84/98; both retain
four original inputs and add two target inputs. Current policy2/window2 wins over
startup managed drift, and renewal remains live beyond the initial target deadline.
The selected quiet socket is observed outstanding at SIGTERM and later reaches
EOF; actual native/driver/Control shutdown is joined. Three wire event frames do
not establish three individual certifications or completed job coverage. Exact
build787e1fe passes15 affected tests and Clippy; three signed-recovery and two
initial Sync-window regressions pass with the same binaries. First2032894 failure
and corrected retrospective off replay remain retained in the existing evidence.

## Activated initial producer recovery (under implementation)

The selected command is explicit `ingest recover --activated-successor --file
PATH`, mutually exclusive with the earlier source-closure/retirement actions.
Configuration, the existing local native store and retained database/input state
are its only startup inputs. It uses fresh Control and current accepted policy to
close and positively resolve the already activated epoch2 producer. It starts no
listener or acquisition and returns no writer. The existing initial and empty
recovery modes retain their stricter historical classifiers.

A separate checked recovery context binds the actual schema2 manifest, retained
1→2 handoff, both exact source streams, retired predecessor and activated target.
Require both existing producer markers, exact dataset/shard/owner/producer/domain
identities and sealed topology generation1 with two streams. Missing target state
cannot mean zero work. Source sequence0 and issued0 are valid when positively
consistent with the target state. The native OPEN→CLOSING barrier is durable and
one-way; retries may observe the same closing state. Bind the proof to actual native
root identity and sequence, rechecking before replay. This observation cannot
become a writer or a source-closure witness.

Positive replay admits references to the predecessor stream only through its exact
retained closed handoff cut, and target references only through the checked current
target counter. New target batches belong to epoch2. Preserve full fact decoding,
digest/reference closure and forward/reverse dictionary checks. Target request1 is
ordinary evidence append, never initial bootstrap. One positive request establishes
neither a contiguous source2 closure nor proof of every unreferenced old request.
Each retained original input and reservation must match byte-for-byte; absence or
corruption is unresolved, never inferred noncommit. Stream requests sequentially
through reserved_through, preserving issued/reserved distinction and unresolved tails.

A separate bounded PostgreSQL recovery owner validates immutable preparation and
activation, both manifests, all18 allocation domains, exact retired predecessor,
target lifetime and topology. Never fabricate an epoch2 initial registration or
reuse a live writer capability. Acquire current Control before dataset, then ordered
historical/target parents and producer. Recheck authority after waits. An expired
historical Ingest scheduling lease does not prevent positive recovery under current
Control; current policy may advance beyond the retained activation policy. Historical
issuer, policy, input, outcomes and existing recorder attribution remain immutable.
Only a missing PostgreSQL copy of a positively verified native outcome is inserted,
with the current recovery recorder. Identical retries preserve existing rows/xmins.

Add migration0033 without changing applied migrations. Dispatch only the exact
activated producer OPEN→CLOSING path to its own complete validator; retain0031's
unactivated retirement checks and0032's activation, source/domain/owner/role retirement
vetoes. No broad activated-state exemption is allowed. Native closing precedes SQL
closing. Unknown closing/outcome COMMIT yields no reusable operation authority;
another configuration-only process observes retained state and retries. Never reopen
native state or infer rollback from an undelivered response.

The first installed recovery exposed an unsupported0033 equality between the
historical Control release and role-retirement timestamps. Ordinary Control
release samples server UTC in separate statements. Preserve applied0033 and use
additive0034, which requires the exact released, storage-fenced Control identity
and a retired matching role, as the established0031/0032 classifiers do. Native
Ingest retirement's shared timestamp remains exact. Activated recovery requires
schema34; upgrade qualification preserves the33 ledger and all prior rows.

Reserve native ownership, retained metadata/closing proof, Control/driver and one
input/outcome replay pair before a claim or barrier. Retain native custody until
close and the actual driver/grant through joined shutdown. The finite report names
producer2, issued/reserved/resolved counts, native closed and committed Control
release; source2 remains open and readiness false. It does not retire the target,
complete acquisition coverage, acknowledge/reclaim data, activate epoch3 or publish
Archive data.

Acceptance uses copied installed off/on states and actual zero/source-zero states:
fresh recovery, identical retry and missing-PG-copy repair; corrupt/missing old
dependencies and new inputs, wrong activation/handoff/extra producers/foreign streams;
held native before Control; advanced current policy and expired historical scheduling;
actual closing and outcome COMMIT loss on both sides of server completion followed
by separate installed recovery. Preserve full original SQL/native/input captures,
attribution and artifacts, and distinguish build-only or finite-probe evidence from
installed qualification. Source2 closure/retirement and repeated-lifetime/Archive
consumers remain subsequent selected work.

## Activated initial source closure (under implementation)

The next selected responsibility is `ingest recover --close-activated-source --file
PATH`, mutually exclusive with every other recovery action. It owns the same
configuration-only fresh Control/native/driver lifecycle as activated recovery,
then proves and closes source2. It starts no acquisition, converts no observation
to a writer and does not retire the producer or its workflow. The existing positive
recovery command remains strictly source2=open. This separate mode accepts either
an open source or its exact completed closure; a persisted intermediate closing
source is not an adoptable completion.

Use distinct opaque activated source-resolution proofs, borrowing the actual
native recovery and its checked closing proof through completion. Preserve the
initial source APIs and digest. Recheck actual root/sequence, both manifests,
retained handoff, both producer markers and exact two-stream context. Require
issued=reserved=N and N<=the immutable activation request limit<=64. Missing or
unissued reserved requests refuse. Replay exact original inputs/reservations and
positive outcomes in order1..N; no missing effect can be inferred from absence.
NewFacts advance source2 contiguously from1. AlreadyDurable and Rejected advance
only the request prefix. Source1 references remain positively verified within the
retained handoff cut; source2 dependencies may not exceed the accumulated prefix.
The final reconstructed source2 cut must equal its positively checked counter.
Zero requests and issued-positive/source-zero are valid distinct cases. The proof
covers every reserved request, without claiming to resolve unrelated staged
sidecars, reclaim inputs or inspect every unreferenced predecessor fact.

The new closure digest is SHA256 of this exact fixed-width concatenation:
`bigbrotr/activated-initial-source-closure/1\0`, SHA256(exact retained handoff),
SHA256(target manifest), producer_slot16, BE64(2), BE64(N), then for every ordered
request BE64(sequence), input_SHA256_32, outcome_SHA256_32, followed by
BE64(source2_cut). The domain ends in one NUL byte. This binds source1 transitively
through the handoff. The independent PostgreSQL validator binds the immutable
activation. The accumulator retains fixed-size state rather than receipt vectors;
no new native format or write follows the existing durable closing barrier.

A distinct PostgreSQL source-purpose classifier preserves all preparation,
activation, policy, topology1, original registration and predecessor checks.
All nine predecessor domains remain closed and all nine target domains active;
the target Ingest owner may be active/draining, with an unretired matching role.
An expired historical scheduling lease is not recovery authority. Require current
Control and accepted policy, ordered Control UPDATE then dataset UPDATE locks,
ordered historical/target parents, source UPDATE and producer UPDATE locks.
Recheck authority and server UTC after waits. Preserve the established historical
Control release/role-retirement distinction; do not require equal timestamps.
An open source permits producer open/closing during inspection; a closed source
requires producer closing and complete closure fields. No terminal producer,
domain, owner or role shape is accepted by this slice.

For an open source, replay may copy only missing positively verified native
outcomes under current recorder authority. For a closed source, every exact
PostgreSQL outcome copy must already exist; missing or mismatched copies refuse.
The final typed close compares the full ordered SQL input/outcome digest prefix
against the native proof. It commits source open→closing→closed atomically, with
its cut/digest/server timestamp, or observes exactly the same closure without
rewriting its row/xmin. Use additive migration0035 with two narrow deferred
transition branches: producer OPEN→CLOSING keeps the existing complete validator
for owner active/source open, using the complete source-purpose classifier for
its additional supported draining/closed shapes; source open→closing→closed
requires the source-purpose classifier plus the final source=closed shape. This
preserves the ordinary recovery command's existing application grants. Every
source-purpose typed operation explicitly requires the new classifier. Preserve all applied
migrations, the ordinary typed recovery classifier and existing retirement/domain/
owner/role vetoes, including job/frontier cut consistency. Ordinary positive
recovery still requires owner active and source open. No broad activated-state
exception is permitted.

Native closing precedes SQL closing. Retain native custody until close and the
actual transport driver/grant through joined shutdown. Unknown COMMIT poisons the
session and yields no reusable capability; a fresh installed process replays and
observes retained state. Preclaim resource checks cover retained128KiB metadata,
4096-byte data/native closing grants, one bounded input/outcome pair, and final
64KiB SQL publication overlap. The report names producer2, issued/reserved/resolved
counts, source2 cut/digest, native closed and committed Control release, ready=false.
It does not certify coverage, abandonment, retirement, ACK/GC, epoch3 or Archive.

Acceptance includes installed off/on first closure and separate-process exact
retry, zero-work and Rejected/source0, plus an actual AlreadyDurable/source0 case;
independently encoded digests and complete preserved source1/handoff/activation.
Exercise omitted/duplicate/out-of-order originals, unresolved reservation tails,
wrong/stale native proof, forbidden dependencies/counter and extra stream/producer;
missing/corrupt input or SQL copy; both concurrent job-versus-closure lock orders;
final source-close COMMIT loss before forwarding and after server completion with
fresh-process recovery. Preserve captures and ambiguity; distinguish actual CLI
exit, supervisor join and backend absence. Epoch2 workflow abandonment/retirement
remains subsequent work because the current gap protocol owns epoch1 only.

## Activated initial acquisition abandonment (under implementation)

This bounded responsibility continues P02-T01 Step5 / P02-T02 Step4 in the
[storage tasks](../implementation/02-storage.md), enabling P01-T09 Step4 in the
[first useful composition](../implementation/01-foundation.md). Qualification
belongs in the existing
[producer recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json);
this selection does not complete those steps or M1.

The explicit command is `ingest recover --abandon-activated-acquisition --file
PATH`, mutually exclusive with every other recovery action. Share the installed
activated source path: own the configured native store first, claim fresh actual
Control, positively replay the complete target request prefix and close source2
or observe its exact closure. Then record uncertainty for the activated owner's
queued acquisition intents. Source closure and positive outcome copies may already
have committed when abandonment later refuses; preserve and report those effects.
The command starts no listener or acquisition and never adopts the old writer.

The terminal adapter consumes no portable authority. It borrows the same
`ClosedActivatedInitialSource` and `ResolvedActivatedSource` through dispatch,
checks their exact association, counts, source cut and digest, and revalidates
native root/sequence and the complete two-stream context before and after waits
and before COMMIT. Reuse the ordered SQL input/outcome comparison against this
positive witness. The final transaction requires the exact already closed source2
and closing producer2. A decoded handoff, copied digest or fabricated epoch2
`InitialRegistration` cannot replace either proof. No native format, additional
native write, source digest or writer conversion is introduced.

Use additive migration 0036 to extend `control.acquisition_gaps`, retaining its
job primary key, foreign keys, producer index and immutable history. Replace the
epoch1-only checks with this exact paired union:

| Producer epoch | Source owner epoch | Reason |
| --- | --- | --- |
| 1 | 1 | `initial_producer_abandoned` |
| 2 | 2 | `activated_initial_producer_abandoned` |

No other combination is supported. Preserve applied migration bytes, every
epoch1 row and its reason, the initial API and its existing application grants.
The epoch2 INSERT guard selects the complete activated source classifier and
binds the previous job owner to the immutable activation's Control tuple. This
is the activating Control, not the original producer-slot registrar or target
Ingest incarnation. Require that historical Control to be positively fenced,
the exact closed source2 cut/digest, complete original outcome copies and the
current actual recovery issuer. The gap time must follow source closure and job
creation and remain within current server UTC and Control authority.

Select every acquisition claim attributed to that activating Control, including
jobs with no mutation request. Accepted unclaimed jobs and jobs of unrelated
Control lifetimes remain unchanged. Handle only typed protocol1 queued jobs with
sealed input and checkpoint zero. Read retained producer2 gaps and selected claims
through indexed, sorted queries with an excess-row discriminator; their combined
count must be at most64, independently of the producer's request count. Validate
the complete batch and all retained gap/job relations before the first new gap
INSERT. Refuse unsupported or advanced selected history, inconsistent retained
gaps or excess count without partial abandonment.

Each new gap retains the exact original job revision and claim, producer2,
source2 cut/digest, current recovery issuer and server timestamp. Immutable job
detail supplies the original actor, scope, selectors and requested duration.
Record whole-intent uncertainty, without inventing an execution interval or
partial traversal. Gap insertion and queued→cancel_requested→cancelled commit
atomically, preserving the established two revision advances and using the gap's
reason and timestamp. Only this exact gap permits transfer of the terminal write
to current Control; it grants no execution. Deferred gap INSERT checks reject an
incomplete cancellation, and completed jobs and gaps remain immutable, including
after an early `SET CONSTRAINTS`. Exact retry revalidates retained gaps and returns
the same total without rewriting their issuer, timestamp, job revision or xmin.

Preserve 0028's ordinary-job privilege correction. Add no deferred jobs UPDATE
trigger that makes acceptance, same-owner claims or ordinary mutations query
recovery tables. The existing owner-change guard gains only the exact epoch2
gap-backed terminal transfer; the epoch1 branch keeps its behavior. The new
abandonment transaction requires schema36 and its narrow recovery privileges.
Rebuilt existing consumers retain their earlier minimum prerequisites while
validating the complete known additive ledger. Old binaries cannot treat the
unknown migration as compatible history.

Acquire actual current Control UPDATE before dataset UPDATE, followed by the
existing ordered historical/target parents, source UPDATE, producer UPDATE and
sorted job UPDATE locks. Recheck current accepted policy, Control identity and
server UTC after waits and before final effects. Expired historical Ingest or job
scheduling, revoked old actor/session/grants, withdrawn enrollment or changed
historical execution policy do not authorize adoption and must not prevent
recording this uncertainty under current recovery authority. Preserve original
accepted intent and browser-session rows; this operation does not revoke or renew
sessions. The installed local executor borrows native custody throughout its
socket callback, so exclusive native recovery and the separate actual Control
fence exclude that executor. No remote-worker exclusion guarantee follows.

Before any claim or barrier, reserve the existing native, Control/driver,
128KiB retained metadata, 4096-byte data/native closing proof and one bounded
input/outcome replay pair. Preflight a 128KiB final abandonment grant alongside
retained metadata/closing custody; release replay grants before this operation.
The source publication's 64KiB grant and abandonment's 128KiB grant are sequential.
Cancellation, failed delivery and unknown COMMIT leave no reusable session
authority. Keep native custody through close, then drop or release Control as
appropriate and join its actual transport driver while retaining its grant.
Only a fresh configuration-only process may resolve an unknown outcome by
replaying the same retained state.

The successful report uses `state=activated-acquisition-abandoned`, producer2,
decimal issued/reserved/resolved counts and the exact source2 closure digest/cut.
Its `acquisition_abandonment` names the total jobs, reason
`activated_initial_producer_abandoned` and `coverage=unknown`. Native close,
committed Control release and joined shutdown precede publication; `ready=false`.
Neither an observed EOSE nor positive native outcomes certify traversal coverage.

Acceptance uses preserved copies of the qualified installed off source and the
genuine AlreadyDurable/source0 closure, each with first abandonment and a separate
process retry. Under a genuinely live activating Control, add a typed claimed
but unexecuted zero-request job and an untouched accepted unclaimed job; do not
forge a historical claim after that backend exits. Check all SQL rows/xmins,
source1/handoff/activation, native rows/sequence, exact original inputs, job detail
and sessions. Narrow raw SQL cases must discriminate paired epoch/reason identity,
activator versus registrar, source cut/digest, incomplete cancellation and retained
immutability. Exercise final abandonment COMMIT loss before forwarding and after
server completion, followed by fresh-process recovery with exact backend
exclusion. Verify ordinary HTTP acceptance/claim on schema36 with its existing
grants, preserving the 0028 compatibility boundary. Retain failures and distinguish
synthetic controls, finite probes and actual installed qualification.

Keep the source classifier's active/draining observation, but implement no
active→draining transition. Producer2 remains closing, target domains remain
active and target Ingest remains unreleased with its role unretired. Producer/
domain/owner retirement, import obligations, ACK/GC, epoch3 and Archive remain
separate responsibilities. Event classification from the positive native input
must be retained for eventual retirement, so unbound Event requests cannot evade
its workflow veto through nullable SQL fields; exposing that classification is
deferred and is not a prerequisite of this acquisition-only operation.

## Activated initial producer retirement (under implementation)

This bounded responsibility continues P02-T01 Step5 / P02-T02 Step4 and enables
P01-T09 Step4/M1 through the existing [storage tasks](../implementation/02-storage.md).
Qualification remains in the
[producer recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json).
It does not complete those steps or select a replacement lifetime.

The explicit command is `ingest recover --retire-activated-initial --file PATH`,
mutually exclusive with every other recovery action. Source2 closure and any
required acquisition abandonment must already have completed through their own
operations. The command takes configuration and retained SQL/native/input state;
it starts no listener, acquisition or writer. Missing prerequisites refuse rather
than being repaired or completed implicitly. Ordinary activated recovery, source
closure, acquisition abandonment and all earlier recovery modes retain their
existing state classifiers.

Retain exclusive native custody before connection preparation or a fresh actual
Control claim. A distinct checked retirement context binds the same exact native
root/sequence, schema2 manifest, retained handoff, both producer markers and both
source streams as positive activated recovery. The target native producer must
already be CLOSING, including when issued is zero; this operation makes no native
barrier or other native write. Observe that closing state with its retained grant.
SQL producer closing or retired never makes native OPEN an acceptable completion.
No decoded row, portable digest, copied proof or fabricated epoch2
`InitialRegistration` supplies this authority.

Positively replay every target request in order1..N with its exact retained input,
reservation and already present PostgreSQL outcome copy. Require issued=reserved=N
and N<=the immutable activation limit<=64. Missing or mismatched detail refuses;
this path inserts no outcome copies. Reuse the activated source accumulator and
its existing digest without changing native formats. Source1 dependencies remain
bounded by the retained handoff cut; source2 dependencies and NewFacts must fit
the contiguous reconstructed target prefix and exact native counter. Zero requests
and issued-positive/source-zero remain distinct supported proofs.

Preserve the native-derived Event classification already computed while replaying
the prefix. The activated witness retains its fixed-size request mask, including
Rejected and AlreadyDurable Event requests. Every such request must have an exact
acquisition job and its completed epoch2 gap. A native Event with no job refuses
even when every optional SQL source field is NULL. Unbound dictionary Evidence
remains supported. The mask is local positive proof, not a new stored field or
caller-supplied classification. Before final effects, compare the complete ordered
SQL input/outcome digest prefix with the native witness and check its association
with the exact borrowed retirement closing context. Revalidate native root,
sequence and the complete checked source interpretation after waits and before
COMMIT; preserve refusals for stale or equal-content foreign-root proofs.

Use additive migration0037, preserving all applied migration bytes and existing
rows. This mode requires schema37; existing consumers retain their minimum schema
requirements while validating the complete known additive ledger. New recovery
privileges do not become ordinary acquisition privileges. A dedicated retirement
classifier retains the full immutable preparation,
activation, historical configuration/policy, retired predecessor and exact18-domain
identity checks. Sealed topology1 and both members remain unchanged, including the
target member's NULL handoff bound. Current accepted policy may advance; neither
expired historical Ingest scheduling nor obsolete execution policy grants authority
or prevents this positive recovery. The original activating Control must be
positively fenced with its exact released/storage-fenced owner and retired role.
Its separately observed release and role-retirement timestamps need not be equal.

Before retirement, validate all target outcome copies and complete retained
epoch2 acquisition gaps. Bind previous claims to the immutable activating Control,
not the original slot registrar, target Ingest incarnation or current recovery
issuer. Require every such acquisition claim to have resolved, including claims
that issued no mutation request. Each retained gap must match the exact source2
cut/digest, original job revision and owner, reason, terminal cancellation,
checkpoint0 and final issuer/time relation. Retained gaps remain bounded at64,
independently of request count. Their historical issuers need not still be live;
validate their retained identity and terminal relation under current Control,
without replaying an old issuer's live-authority check.

Any import claim attributed to the activating Control, target input binding or
producer checkpoint refuses, including a checkpoint with complete-through zero.
Import logical closure is a separate selected responsibility, not inferred from
native outcomes. Unclaimed accepted jobs, unrelated Control claims and browser
sessions remain unchanged. Use bounded producer queries and owner-indexed workflow
checks; do not scan unrelated job histories or silently ignore zero-request work.

Only these durable PostgreSQL target shapes are supported, with the native target
already CLOSING in both cases:

| Shape | Required target state |
| --- | --- |
| Ready to retire | Producer closing; source already closed with the proved cut/digest; all nine domains active; exact Ingest owner active/draining and its role unretired |
| Completed | Producer retired; all nine domains closed; Ingest owner released with storage fencing; its role retired; exact common terminal timestamp and closure digest |

The final transaction acquires actual current Control UPDATE before dataset UPDATE,
then the ordered historical/target owner, role and domain parents, source UPDATE,
producer UPDATE and required ordered job custody. Recheck accepted policy, actual
Control identity and server UTC after lock waits and before terminal effects.
Current expiry or cancellation is not proof that an earlier transaction rolled
back. This operation implements no active-to-draining transition; it only accepts
that existing observation as the source classifier does.

For a ready target, sample one database timestamp T no earlier than the committed
source closure. Atomically retire producer2, close its nine domains, release its
Ingest owner with `storage_fenced` and retire its role: exactly12 target rows.
Producer retirement, domain closure and owner exclusion retain the already proved
source2 digest; their terminal times and role retirement all equal T. Preserve the
source2 row, its earlier close time and xmin. Preserve source1, predecessor history,
preparation/activation receipts, requests, copies, gaps, jobs and sessions. Account
for current Control claim, renewal and release separately from these12 target rows.

The producer transition guard must require the exact activated retirement staging
and workflow obligations, even for reserved zero. A complete deferred guard on
target lifecycle changes must re-read final rows and reject partial commits;
transaction-internal staging is not a loader-accepted durable state. Preserve the
epoch1 retirement and unactivated empty-attempt branches, ordinary recovery/source
transition branches and ordinary acquisition privilege boundary. SQL alone cannot
prove retained native inputs or exclusion; only the typed composed operation
supplies that proof. Do not turn the new classifier into a generic state exemption.

Completed retry replays the whole native/SQL proof and revalidates the complete
terminal relation. It preserves the original T, digest, attribution and all row
versions. Mixed terminal state, a changed cut/digest or timestamp, missing copies
or unresolved workflows refuse without repair. Existing terminal rows remain
immutable even after early `SET CONSTRAINTS`; ordinary earlier loaders still refuse
retired state. The dedicated classifier alone permits this exact observation.

Protect the activated target role explicitly, rather than relying only on the
existing deferred trigger's `retired_at_us` change predicate. Bind this protection
to the immutable activation's target role. Its only permitted update is the
selected NULL-to-T retirement with every other column unchanged; after retirement,
reject UPDATE and DELETE, including changes to `created_at_us` that leave
`retired_at_us` unchanged. TRUNCATE must not erase this protected history, including
through cascading table truncation. Preserve Control and epoch1 role behavior and
their ordinary grants; this is not a generic role-lifecycle redesign. Qualification
must include forcing constraints early, then attempting a non-retirement-column
rewrite without changing `retired_at_us`, as well as terminal timestamp rewriting
and deletion/truncation protection.

Before claiming Control, preflight retained native/driver ownership,128KiB metadata,
4096-byte data/native closing proof, one bounded input/outcome pair and a128KiB
terminal SQL grant. Replay grants are released before the final operation; metadata
and closing custody remain held. The Event mask adds fixed-size local state, not
a receipt vector. Cancellation, failed delivery or unknown COMMIT yields no reusable
session authority. Keep native custody through close, then release or drop current
Control and retain its actual driver/grant through joined shutdown. A separate
configuration-only process resolves unknown outcomes by positively fencing the
preceding backend and distinguishing ready from exact completed state.

Successful `bigbrotr-ingest-recovery/1` output uses
`state=activated-initial-retired`, producer2 and decimal issued/reserved/resolved
counts, the exact source2 cut/digest and `ingest_retirement` naming nine domains,
retained acquisition-gap count and committed Ingest release. Native acknowledged
and retired watermarks remain0. `native=closed`, current `owner_release=committed`
and `ready=false` are published only after actual native/Control/driver shutdown.
No capacity is reclaimed and no acquisition coverage is certified.

Acceptance uses preserved copies of the qualified installed off source and genuine
AlreadyDurable/source0 after acquisition abandonment, with first retirement and
separate-process exact retry. Include activated-zero completion and unbound
Rejected/Event refusal; genuine zero-request workflow claims and import
binding/checkpoint vetoes; missing or conflicting original/copy and mixed terminal
state. Independently check Event classification, complete SQL/native/input state,
the exact12-row transition and retained source/predecessor/activation attribution.
Exercise partial SQL lifecycle commits, final retirement COMMIT loss before
forwarding and after server completion, fresh-process recovery, and actual blocked
operation cancellation/current-authority expiry. Reuse qualified unchanged replay
and custody evidence explicitly instead of inferring a new boundary from it.
Preserve original failures and distinguish native components, finite probes and
installed execution. Repeated lifetimes, import closure, ACK/GC, epoch3 activation,
Archive restart and completion of M1 remain separate obligations.

## Repeatable ingest succession (selected, not yet implemented)

P02-T01 Step5 / P02-T02 Step4 next implements one versioned protocol for
successions n to n+1, starting at2 to3 and exercised again at3 to4. A separate
classifier or migration for every numerical epoch is not the selected design.
The responsibility includes preparation, continuous local activation, positive
recovery, source closure, acquisition abandonment and terminal retirement,
including prepared attempts that never activated. Codec support or activation3
alone does not complete this responsibility or installed restart.

The existing initial1-to2 protocol, singleton preparation/activation records,
native handoff bytes and historical manifests remain unchanged. The new family
uses manifest key schema3, version2 values under the existing epoch-indexed
owner-handoff keys and epoch-indexed PostgreSQL preparation/activation records.
A receipt retains a bounded pair of exact manifests, the predecessor's terminal
kind/count/cut/digest and the parent receipt hash. Contiguous epochs determine the
frontier count; the exact chain binds every cut without a redundant stored frontier
digest or growing history vector. Portable records remain observations;
neither decoding nor finding a PostgreSQL activation recreates a writer.

There are exactly two legacy entry shapes:

| Predecessor | Required positive evidence |
| --- | --- |
| Activated epoch2, fully retired | Original0032 activation, complete0037 terminal relation, existing native CLOSING marker, closed source2, original inputs/effects and exact retained SQL copies |
| Unactivated empty epoch2, fully retired | Absence of0032 activation, complete0031 terminal relation, CLOSING0/source0, absent target requests/inputs/detail and recomputed empty closure after positive epoch1 replay |

Both entry shapes retain the immutable1-to2 handoff as their parent anchor.
An activated producer with zero requests is the first shape, never the second.
Later predecessors use the same activated-versus-unactivated distinction and the
immediately preceding new-family receipt. No recovery adopts an earlier attempted
incarnation, role or allocation namespace. A new candidate generates all of them
internally, checks freshness against the complete retained lineage/registry, and
owns the actual exclusive native handle continuously through writer delivery.

### Complete historical interpretation

The checked lineage is contiguous from epoch1 through the current native head,
with one stable dataset/resource/shard/recovery identity and producer slot. Every
retired stream, including a zero-length stream, has an explicit terminal cut.
The legacy epoch1 zero-source representation may have no physical SourceStream
counter: accept that exact absence only with its retained handoff cut0, a genuine
CLOSING producer whose issued count matches that handoff, and no epoch1 facts
or pending batches. A positive issued count is valid when its retained requests
and input sidecars pass the complete census and positive ordered replay; zero
facts does not imply zero requests.
It still contributes terminal cut0 to the checked frontier. Epochs2 and later
require their explicit source marker, including zero; never repair or infer a
missing nonzero or later-epoch marker.
Reconstruct and verify its ordered frontier from exact retained receipts and
registry identities. Check the entire bounded producer/source/receipt/domain
census and reject extra, missing or foreign state, including empty extra streams.
Hash agreement does not replace positive original-input/effect validation.
Bind every retained sidecar filename to the actual slot/epoch/request lineage;
syntactic filename validity alone is insufficient. Extra staged or reserved tails
remain unresolved and prevent terminal proof, including files that change without
a Rocks sequence increment. Do not silently filter foreign or future input files.
For this new-family bridge, reject unresolved historical staged extras before
preparation and preserve them for explicit resolution. This is a conservative
additional prerequisite: legacy retirement did not claim to resolve every staged
sidecar. Do not rewrite that historical result as failure or delete its extras.

Historical request replay uses that request's immutable manifest, policy decision
and protection coordinates while separately binding the actual current manifest,
native root and sequence. Dependencies must name a verified earlier stream within
its terminal cut, the already resolved contiguous prefix of the request's own
stream, or that request's exact current NewFacts batch after positive validation.
Advance the accumulated cut only after validating the complete contiguous batch
and outcome; no dependency can borrow a later request's facts. Resolve the
referenced native fact and its meaning, not just its ordinal
or a receipt hash. Preserve the original source-closure digest profiles for epochs1
and2; new epochs use one explicitly versioned profile independent of epoch number.
Replay streams one input/outcome pair at a time, retaining no receipt vector.
An earlier request may not depend on a later stream merely because that stream is
retired by the time of replay. The live writer also retains a checked bounded
frontier for old-fact admission/certification; a schema or epoch-number test alone
does not authorize those references.

Legacy loaders deliberately require their original topology head and exact one-
or two-stream native shape. They continue to refuse later states. The new family
needs its own historical terminal checks, which verify the sealed ancestor
topology without requiring it to remain the dataset's current head. Do not weaken
legacy classifiers or fabricate an `InitialRegistration` for a later epoch.
Each succession creates and seals a new topology containing all earlier terminal
cuts and its one new open stream. Old sealed memberships, including their original
NULL handoff coordinates, are immutable.

### Authority and interrupted attempts

Preparation first locks actual current Control, then dataset and ordered lineage
parents. It verifies current policy, positive predecessor retirement and resource
admission before atomically registering the next role/owner/domains/source/producer,
sealed topology and immutable preparation. Only confirmed preparation COMMIT can
dispatch the synchronized four-write native handoff. The batch appends its receipt,
changes the singleton manifest and creates explicit source0 and OPEN/zero producer
markers; it changes no historical key or retained input.

Activation consumes the private local candidate only after its own confirmed
COMMIT and a final check of actual current Control, accepted policy, deadline and
native identity. Preserve the transaction fence between activation and the first
request or obligation, including after early `SET CONSTRAINTS`. Unknown commit,
failed delivery or cancellation returns no writer or reusable session authority.
Retain and join native/driver custody before a replacement process proceeds.

A replacement process classifies the exact preparation and presence or absence of
the committed activation before selecting recovery. The unactivated path must
support all four states below with the same protocol at every later epoch:

| Retained state | Permitted recovery |
| --- | --- |
| PostgreSQL prepared, native still at predecessor | Positively reconstruct the exact recorded handoff and complete the native gap |
| Prepared native target OPEN0 | Validate absence of target detail/input and close that exact empty attempt |
| Prepared native target CLOSING0, SQL not terminal | Finish the complete empty retirement transaction |
| Exact native/SQL terminal result | Verify it and preserve all existing bytes, timestamps, attribution and row versions |

After empty retirement the next candidate advances the epoch again. A recorded
activation, even with no requests, selects activated recovery instead. Activated
source closure, acquisition abandonment and retirement retain their separate
prerequisites; retirement repairs no missing outcome copy and implicitly closes
no source or workflow. Preserve Event-derived workflow checks, genuine zero-request
claim vetoes and refusals for unresolved import bindings/checkpoints. Import logical
closure remains a separate required product responsibility.

New SQL dispatch must retain the old epoch1/2 branches exactly. New lifecycle
guards must enforce complete terminal state and immutable history through UPDATE,
DELETE, TRUNCATE and cascading truncation, including after early constraint checks.
This includes both anchored legacy Ingest roles, especially epoch1 and unactivated
epoch2: their old deferred guards alone do not protect changes to every column.
The new anchor must protect those retained identities once succession starts,
without broadening old live consumers. Do not claim general Control-role history
immutability from the Ingest-specific guard; validate the required historical
Control identity/fence separately.
Ordinary Control/epoch1 consumers must not acquire invoker SELECT requirements on
new relations merely because a trigger runs. Current authority is rechecked after
lock waits; historical scheduling expiry is not exclusion or recovery authority.
An earlier Control release time need not equal its separately recorded role
retirement time.

### Resource admission and qualification

The64-file/32MiB retained input spool and per-producer64-request bound remain
independent limits; a succession reclaims none of that capacity. Empty attempts
consume lineage metadata even when they consume no input. The128KiB grants used
by the old two-stream adapters are reservation floors, not a lifetime bound.
The new profile bounds both the retained compact frontier and transient SQL
projections, with checked arithmetic and one-element overflow sentinels. Admission
of the next preparation must reserve its future recovery/retirement envelope,
alongside the actual concurrently retained native, transport and replay grants.
Recovery of an existing attempt must not require capacity for another epoch or
another input. Metadata exhaustion refuses before new durable preparation.
An exact PostgreSQL-prepared/native-old attempt counts as the next epoch for this
reservation even while the native census still has only its predecessor's streams
and receipts. Its new-family historical checker must accept that precise gap
without calling legacy loaders that require the old dataset topology head.

Retaining every immutable topology creates E(E+1)/2 membership rows through E
epochs in this one-stream-per-epoch profile. A linear frontier-memory reservation
does not bound that persistent growth. Select a ceiling of32,768 logical retained
memberships per supported lineage, enforced by raw SQL under dataset custody and
by the typed caller. Require the complete expected dataset topology shape; the
formula does not excuse extra memberships or unrelated generations. Support at
most one registered sealed empty topology before genesis, with no earlier
generation, and refuse a longer or foreign prefix with a bounded query. It
contributes no membership; its generation offset does not change the epoch count.
Thus at most E+1 generations are inspected, without unbounded traversal of empty
ancestors. E is the contiguous owner/producer epoch,
not `topology_generation + 1`.

This ceiling admits at most255 total epochs, including1 and2:32,640 memberships
fit, while epoch256 would require32,896. Select a separate256KiB lineage metadata
reservation ceiling:128KiB bounded scratch plus512 bytes per compact entry.
At255 entries the reservation is261,632 bytes. An entry retains only one manifest
and bounded terminal coordinates/digests; assert its complete allocation fits512
bytes. Retain each raw receipt and SQL projection only while processing it, inside
the scratch/transport budgets, with no additional uncharged history vector. Include
vector headers, decoding temporaries and allocation capacity in the accounting.
If an implementation cannot fit those bounds it must revise the accounting before
admitting work, not silently exceed the reservation. All concurrent metadata
objects need their own admitted share; these numbers do not replace native,
transport, input, outcome or process headroom.

Epoch255 remains recoverable and retirable without creating a new topology.
New preparation256 refuses before any target intent or native write. These are
explicit initial profile limits, independent of64 retained inputs and not a claim
of255 useful acquisition lifetimes. Physical PostgreSQL pages/indexes/WAL, native
maintenance and RSS are measured separately: a logical row or reservation count
is not a physical byte bound. The fixed portable bytes are specified in the
[manifest format](ingest-manifest-format.md#repeatable-owner-handoff-selected-not-yet-implemented).

Qualification includes both legacy2-to3 entry shapes; activated admission and
complete retirement3-to4; and interrupted unactivated preparation3, fresh-process
gap/empty recovery and activation4. Verify a dependency older than the immediate
predecessor, exact historic manifests and all sealed memberships. Include missing,
extra and conflicting chain/frontier records, equal-content foreign-root and stale
sequence proofs, old-loader refusal, full-spool empty recovery, metadata-limit
refusal before preparation, ambiguous prepare/activation/retirement COMMIT and
current-authority expiry/cancellation. Raw SQL must refuse partial terminal writes
and same-transaction activation obligations. Distinguish component codecs, native
engine checks and actual installed journeys in the existing evidence register.
ACK/GC, Archive restart, backup/restore closure and M1–M5 remain open until their
own complete selected journeys are demonstrated.

### First installed repeatable lifetime (under implementation)

`serve --activate-successor` consumes a genuinely terminal epoch2-or-later head,
using the same bounded history, preparation and activation adapters as the finite
consumer. It owns the exclusive native handle before protected connection setup,
verifies original inputs and exact retained outcomes for every earlier stream,
and transfers only a freshly delivered `LiveSuccessorWriter` into the installed
HTTP/Sync worker. A historical `InitialRegistration` never stands in for the
current writer. Existing initial and epoch2 consumers retain their original
dispatch and refusal boundaries.

The new lifetime uses the current writer's manifest, policy, request window and
checked frontier for dictionaries, new admission, duplicate certification and
scheduling renewal. Its native, retained-input, transport, history and preparation
reservations must coexist with the selected session and Sync work. Preparation
reserves the future lineage envelope before durable mutation; shutdown preserves
all receipts and inputs and does not retire the live generation. The command is
selected separately from both initial flags and excludes Archive.

The first installed qualification must exercise both legacy terminal2 shapes and
an actually terminal later generation through the same command, with genuine
authenticated acquisition, historical/new/retried signed events, provenance on
and off, renewal beyond the original deadline and joined SIGTERM. Original SQL,
native data and input attribution remain independently checked. This is the first
continuous installed writer consumer, not completion of installed recovery,
source/workflow closure, retirement, tails, capacity or Archive restart. Its build
and actual installed qualification are still NOT_RUN.
