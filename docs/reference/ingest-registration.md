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
First-request manifest mismatch after committed activation, refused/expired/lost
renewal, activated-zero old-loader refusals and installed/general restart remain
unqualified. Detailed identities and exclusions are in the existing
[recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json).
