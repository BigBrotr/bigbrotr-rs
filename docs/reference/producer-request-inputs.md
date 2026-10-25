# Retained producer request inputs

This P02-T02 step 4 prerequisite retains the exact initial co-located producer
input before every new synchronized dictionary, event or rejection batch. Its connected reservation prerequisite is described below; logical progress,
outcome closure, ACK checkpoints and successor ownership remain separate work.
PostgreSQL continues to hold coordination metadata rather than event bodies.

The sole open `IngestStore` owns the staging directory `producer-inputs-v1` inside
its native root. Its mutable serialized writer and actual RocksDB directory lock
exclude another writer; staging never opens another descriptor for RocksDB's
`LOCK`. The existing stable, operator-controlled root boundary applies. Hostile
rename, independently writable copies and mount aliases remain outside that
boundary. Synchronous filesystem and engine work must stay on the retained,
joined owner worker.

Each file name is the lowercase producer slot hex (32 characters), a hyphen,
producer epoch hex padded to 16 characters, a hyphen, request sequence hex padded
to 16 characters, and `.cbor`. Its complete value is the unchanged profile-1
`bigbrotr/producer-request/1` preimage, including its NUL separator. The file
contains the full caller-declared dictionary order or actual signed event,
scope, origin, lineage, observation and witness. The scientific fingerprint
recipe and Rocks key/value schemas do not change.

For a new accepted request, the writer validates the native state/window, retains
the input, synchronizes its file and directory, then checks authority again at
the actual synchronized native commit. Parent directory synchronization covers
staging-directory creation. Directories require mode 0700 and the effective UID;
files require mode 0600, the effective UID and one link. A newly created file is
explicitly chmodded and verified before dispatch so a restrictive umask cannot
make a successfully staged input unreadable by recovery. No symlink or special
file is followed. An existing name must contain exactly the same complete bytes;
it is synchronized again before an identical retry may proceed.

File existence does not prove admission. A complete input can survive cancellation
or process death before the native commit. A changed input under that name is
refused; no code deletes it and retries with a different identity. A partial file
from interrupted write, ENOSPC or other I/O failure is preserved and can block
an identical retry. Explicit resolution/repair is still required for that case;
the adapter never converts it into authoritative absence or silently overwrites
it. No power-loss or ENOSPC qualification is implied by ordinary write success.

## Bounds and recovery reads

The starting retained spool is bounded to 64 files and 32 MiB of aggregate logical
file bytes per store, with at most 16 MiB per input. All retained inputs count;
there is no GC or reclaimed capacity yet. Native admission also enforces the
registered producer window. Each attempted new write checks the finite directory
inventory, refusing unexpected names/types, empty files and exhausted bounds.
It compares existing input with an 8 KiB buffer. A 64 KiB staging allowance covers
comparison, directory and fixed bookkeeping; native/libc allocations remain
within the wider qualified owner profile. This is an allocation reservation,
not an RSS ceiling or an aged-state capacity measurement.

`PreparedEvent` encodes the original input before cryptographic verification can
discard an invalid body. Its existing input grant covers original owned buffers
and the overlapping exact preimage, within the 16 MiB input allowance. Bootstrap
adds one encoded-input allowance to its operation grant. Rejection detail remains
operational: retaining its original input does not make the event valid or create
scientific facts.

`read_request_input` reads one explicitly named request with a caller-supplied
expected digest under a transferred byte/item grant. The closed decoder preserves
all typed fields and compares a complete re-encoding against the input. It rejects
proof-only identities, unknown operations/profiles, noncanonical CBOR and trailing
bytes, with limits checked before caller-directed allocations. Dictionary
uniqueness scratch is included. It does not verify signatures or authorize policy;
recovered events still pass the actual verifier before any new dispatch.

The reader binds the decoded request and dataset to the requested ID and store.
Missing, incomplete, unreadable, mismatched and incompatible inputs are distinct
from successful recovery. Reduced decoding limits remain resource errors, not
corruption; identical bytes can be retried under an adequate grant. Returned
ownership includes the grant for transferring decoded input to its real consumer.
No raw database or unbounded iterator escapes.

## Compatibility and remaining gates

The reader never creates a missing input. Historical native receipt replay stays
available without this sidecar, preserving the earlier bounded primitive; it does
not retrospectively qualify old data as having complete producer inputs. Before
handoff or receipt reclamation, the future protocol must verify the retained input
set against the committed producer ledger and watermarks, and refuse incompatible
history or require explicit recovery. Old executables can ignore this added
sidecar, so it cannot serve as a new ownership barrier or grant until that versioned
protocol is implemented and enforced.

Beyond the initial reservation protocol below, logical source obligations,
outcome/checkpoint closure, ACK/GC, partial-input recovery and owner handoff
remain required. A new process must not impersonate the old incarnation.
This slice preserves inputs and enables bounded inspection; it does not resolve
ambiguous native outcomes, abandon workflow positions or issue a successor lease.

The independent model fixtures cover complete canonical bytes, including the
existing real signed import and an invalid-signature request. Native-owner tests
exercise original input recovery, signed/rejected outcomes, exact replay after
reopen, cancellation before effects, changed-input conflict, reduced limits,
partial data, file permissions, restrictive umask and finite spool limits. Process tests additionally kill the actual owner before/after its native commit.
Execution and unexecuted scenarios must be recorded separately in the progress
checkpoint and slice evidence; the presence of these tests is not a PASS claim.

The [native a1da96a report](../implementation/evidence/p02-producer-inputs-native-20260919.json)
records seven input-boundary cases within 25 actual Rocks tests, including exact
pre/post-commit SIGKILL/reap state, filesystem substitutions and the packaged
pending consumer. Six installed publication schedules independently check 24 input
files. The crash oracle covers a one-dictionary bootstrap; populated publication
state is checked for invariability against its complete original digest. These
results do not qualify power loss, partial-input repair, a producer ledger or
successor authority.

## Initial connected reservation protocol (0011)

The connected initial bootstrap/event adapters require additive migration0011.
The same mutable native handle first synchronizes its exact input under the
borrowed operation grant. The adapter then commits an immutable
`control.producer_requests` row: original slot/epoch/sequence, dataset, profile1
request digest, encoded byte length, exact native manifest SHA256, original
issuing Control identity and reservation timestamp. Bodies remain in the spool.
A newly validated Control/policy/registry transaction follows successful COMMIT;
only then can the native primitive run, compare staged bytes again and perform
its final authority check. Four validator calls cover the two complete policy
snapshots. The validator is a trusted internal contract, not a request callback.

`producer_epochs.reserved_through_request` starts at zero and advances atomically
with each contiguous reservation. It is distinct from native issued/ACK/retired
watermarks. Request rows and counters survive rejected events and failed dispatch.
The SQL trigger updates the producer row, so concurrent closing and reservation
serialize on that actual row; an older repeatable-read snapshot cannot advance a
stale counter after another transaction changes it. The supported write is one
reservation per statement; same-epoch multi-row INSERT is refused atomically.
The adapter resolves an identical retry by reading all bound metadata under the
producer lock, preserving the original row and timestamp. A different input or
native manifest refuses. SQL alone cannot verify a filesystem synchronization;
the internal staging witness has no externally constructible capability.

This prerequisite retains all rows: at most min(registered window,64) requests
and 32MiB declared input bytes per epoch, plus the stricter existing per-store
spool bounds. It implements no reclaimed capacity. Retirement with any reserved
request is refused until typed outcome/progress closure is implemented; a bare
caller-supplied checkpoint digest is insufficient. Empty epochs may still close.
Neither the counter nor row presence proves a native admission result.

Cancellation or PG error before reservation COMMIT leaves no dispatch. If COMMIT
is dispatched but its answer is lost, the result is Reservation CommitUnknown;
the session remains poisoned and the input remains retained. Native DeliveryUnknown
is a different boundary. A fresh current Control session can inspect one bounded
reservation by the original identity, without impersonating its original process.
Missing metadata does not establish native absence or permit identity reuse.
There is no automatic replay under a new incarnation in this prerequisite.

Migration0011 refuses all preexisting producer slots, source streams, allocation
domains or Ingest owner epochs, including retired/released history. It acquires
relation locks under the existing exclusive migration advisory lock before the
preflight. There is no backfill, input-completeness inference or implicit adoption.
Old registration that commits first leaves veto history; an old adapter starting
after migration sees an unknown migration and refuses. New registration/dispatch
explicitly require0011. This is a fresh-only compatibility boundary, not a claim
that a PG lock excludes native FFI: losing its PG backend can release locks while
an old Rocks write still runs. Legacy adoption needs explicit positive exclusion
and recovery. Previously applied migration bytes and native formats remain intact.

The concurrency design follows PostgreSQL16's documented
[trigger visibility](https://www.postgresql.org/docs/16/trigger-datachanges.html)
and [transaction isolation](https://www.postgresql.org/docs/16/transaction-iso.html).
Practical qualification must check rollback, ambiguous COMMIT, preserved original
metadata and both closure orders using actual processes/engines; source inspection
and a catalog unit test do not substitute for those cases.

The [reservation evidence](../implementation/evidence/p02-producer-reservations-native-20260919.json)
records six actual connected reservation/fault cases and 19 SQL observations at
dddefd5, six closure/snapshot schedules, two bounded harness-cleanup controls and
five old/new migration schedules. Actual old a1da96a process/LOCK observations
show that losing its PG backend does not exclude native ownership; fresh-only migration 0011
still refuses its committed history. The marker precedes FFI, so this does not
qualify a mid-FFI crash. Six installed signed-publication schedules at 57cce75 also
match all 24 PG reservations to retained inputs and native context. Earlier failed
fixture runs and the lost supplemental SQL transcript remain recorded separately.
These results qualify the reservation prerequisite only; logical progress,
outcome closure, ACK/GC and successor recovery remain required.

## Connected native outcome copy (0012)

P02-T02 step 4 next binds each reservation to an immutable producer-side copy of
the complete native outcome. Migration0012 and the initial bootstrap/event bridges
own this responsibility. A successful connected return requires both the native
commit (or exact validated native replay) and the PostgreSQL outcome COMMIT.
This is still not a logical position checkpoint: these bridges have no import or
traversal position and must not derive one from the mutation request sequence.

The native owner alone produces the internal outcome witness. Ordinary structural
receipt inspection cannot produce it. It binds the exact native manifest, request,
dataset, digest, outcome kind and original policy epoch, retaining the complete
unchanged receipt/1 or rejection/1 bytes. Receipts include their original protection
context, batch and dependency closure; rejection preserves its original policy
epoch rather than the current retry's epoch. NewFacts, AlreadyDurable and Rejected
are distinct. A rejection creates no event progress basis. Retryable errors,
uncertain commits and RetiredRequest produce no witness.

The ledger stores typed keys/kind/policy, the canonical operational outcome bytes
and their SHA256, original reservation binding and recording Control tuple. No
event body is copied into PG. The trusted adapter validates the native witness;
SQL checks reservation identity, retained bounds, current Control/Ingest authority,
immutability and the byte checksum. A database checksum alone is not native
commit authority. Identical retries preserve the original outcome row and timestamp;
different bytes or context refuse. A current Control owner can inspect bounded
outcome metadata without impersonating the producer. Absence is not native absence.

Before dispatch, each input reservation also reserves 64KiB for its outcome within
the existing 32MiB retained producer envelope. This covers the supported maximum
256 bootstrap records/dependencies or 5 event records/256 dependencies; encoders
refuse larger results before mutation. The operation grant adds 256KiB for the
retained encoded copy and PG handoff staging. These are application bounds, not RSS
or physical-disk ceilings. All rows and inputs remain retained; no capacity is
reclaimed yet. Migration0012 refuses preexisting reservation sets without this
headroom (55000), preserving applied checksums and all history. Compatible earlier
reservations remain unresolved until an actual native outcome is recovered; no
backfill or successful outcome is inferred. Unknown new migration fences old SQL
starts, but PG backend loss still does not exclude native work.
Migration compatibility does not authorize a successor: the existing restriction
to the original Control/producer incarnation remains. A new process still needs
the separately implemented and qualified recovery/handoff protocol. Native
bootstrap replay now checks its complete retained batch in a populated store,
without claiming a complete later cut or rewinding any counter.

The native-operation PG transaction records the outcome after native completion.
Failure before its COMMIT leaves native effects intact and the session unavailable;
loss of the COMMIT reply is a distinct Outcome CommitUnknown. A retry must resolve
the original native request and compare the retained ledger identity. Cancellation,
lease expiry or backend loss after native dispatch never become a negative native
result. The three boundaries (reservation, native write, outcome copy) remain
separately identifiable. No producer_checkpoints, ACK, retirement or GC watermark
is advanced by this copy. Required practical fault/compatibility evidence is
recorded separately; this contract is not a PASS claim.

The [native outcome report](../implementation/evidence/p02-producer-outcomes-native-20260919.json)
records five actual connected outcome-copy/fourth-COMMIT cases and19 SQL constraint
observations at9537667. Four old/new schema compatibility cases preserve all rows
and DDL on refusal; they do not qualify upgraded runtime grants or owner recovery.
Six installed signed-publication schedules independently compare all24 retained
NewFacts/Rejected copies. A separately built fixture with identical product inputs
also copies AlreadyDurable for a new request without adding facts; its exact retry
preserves the original PG row/xmin, and all27 native rows match the independent
oracle. No logical position, producer checkpoint, ACK or GC is inferred from these
results. The report preserves the initial SQL expectation failure, later observer
correction and terminal audit recovery rather than relabeling them as first-run success.

## Positive original-request resolution

P02-T02 step 4 includes a bounded positive-only resolver after the
[native producer barrier](ingest-bootstrap.md). The
[scoped qualification](../implementation/evidence/p02-producer-recovery-native-20260920.json)
records 46 native-store tests, 19 connected captures and actual separate-process
recovery after backend loss and joined SIGKILL. Rejected and AlreadyDurable have
connected fresh-Control evidence within the same process; the separate-process
case covers NewFacts. Full drain/handoff and successor authority remain required.

`ControlSession::resolve_initial_producer_request` takes an exclusively open
`IngestRecovery` and the complete original registration. Its shared closing
preflight validates current Control, the full current policy, historical native
identity and registration, then retains Ingest/role SHARE and producer UPDATE
locks. Both PostgreSQL closing and the actual native closing marker are required.
The original expired Ingest scheduling lease is not renewed or impersonated.
The short current-Control lease starts before coordination I/O and is checked
before/after bounded native reads, before COMMIT and before delivery.

The immutable reservation supplies request digest, input byte length and native
manifest identity. The native reader binds the exact retained canonical input to
that request and dataset. Bootstrap reconstructs and compares its complete
original first batch. Event replay verifies the signed original for a positive
receipt and checks its own historical body, scoped eligibility, sufficient
receive/witness effects and complete dictionary closure, plus the original pending
marker for NewFacts. It does not consult newer minima to replace historical facts.
The original receipt/rejection bytes and policy remain the historical decision.
Foreign receipt topology/recovery generations are unsupported by this initial
path; all facts and original input must still be locally retained. ACK and retired
watermarks must be zero and owner/producer epochs must be one.

Only that internal native witness can enter the existing outcome-copy adapter.
Before SQL insertion, it compares request, digest, manifest and the actual encoded
input length with the locked reservation. Identical copies retain every original
row field, including recorder, timestamp and xmin; new copies attribute the
current Control recorder separately from the original issuer. No migration or
backfill is needed: migration0012 already supports that distinction. Native state,
input files, producer reservations, logical checkpoints and ACK remain unchanged.

Missing original input, missing request detail at or below issued, or a request
above issued never establishes noncommit. The latter two remain Unresolved, with
no negative outcome, cancellation marker or identity reuse. Reduced resource
limits can be retried against identical bytes. Any error after the connected
operation starts leaves its Control session unavailable; a fresh claim resolves
the same request. Lost COMMIT is CommitUnknown, including when the exact outcome
row was committed; never undo native effects or insert under a replacement ID.

Separate transferred grants cover input decoding and native outcome/dependency
inspection plus PostgreSQL handoff. Dictionary closure keeps two bounded vectors
of fixed-size references and decodes one fact at a time. This is not a whole-store
scrub, an archive fallback, causal backup validation, contiguous resolved prefix,
input repair, ACK/GC or logical import completion. Those remain required tasks.

The planned reserved-but-uncommitted resolver has an additional prerequisite:
healthy recovery provenance must exclude silent loss of a previously committed
tail. The [native profile](ingest-native-profile.md#selected-options) now refuses
detected WAL damage, but a complete-record truncation and an earlier permissive
recovery cannot be disproved by today's successful open or issued counter alone.
No negative resolution API, durable noncommit row or retirement permission is
provided by this fix. Preserve unresolved reservations and their original inputs.

## Operational relay source binding (0020)

P02-T01 Step6 and P03-T01 extend the existing reservation with an optional,
immutable `(EndpointId, AccessScopeId, enrollment generation, RouteId)` tuple.
Its dataset is the reservation's dataset. All four fields are present together,
with a positive generation, or all are absent. Existing rows remain absent;
there is no backfill or adoption. Migration0020 appends after0019 without changing
previous SQL, original native request preimages, digests or receipt formats.
The tuple is operational recovery metadata even when scientific provenance is off;
it does not allocate endpoint/access/method archive dictionaries.

On PostgreSQL16, adding these constrained-domain columns rewrites the retained
`producer_requests` table even with NULL defaults; logical preservation does not
imply stable tuple `xmin` or a metadata-only upgrade. The exact
[16.15 implementation](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/commands/tablecmds.c#L6709)
explains the domain coercion. Plan the explicit migration maintenance window and
free space for that table and its indexes, plus WAL; the small populated fixture
does not establish time or disk requirements for a large retained ledger. Preserve
the applied0020 checksum. Verification compares all original application values
and other relations, and separately observes the physical rewrite and idempotent
reapplication.

The trusted source owner supplies attribution for the socket that delivered the
bytes. Constructing this data is **not** a source grant: the consumer must still
check current bans, service/selector permission, root route policy and committed
visibility, and keep socket custody through arrival. Arbitrary caller-selected
IDs could otherwise attribute a private arrival to another public endpoint. The
current binding adapter checks the event's visibility reference against its
immutable no-auth access context; no-auth does not itself imply public visibility.
The finite issuer, private-route visibility enforcement and actual socket consumer
remain required integration work.

Initial event admission compares the complete retained tuple, including omission,
and checks the exact enabled historical and current generation/route under
Control → dataset → enrollment → Ingest → producer ordering. It repeats these
checks in both the reservation transaction and the post-COMMIT native dispatch
transaction. Enrollment changes do not require a policy-epoch change. Absence is
allowed before initial reservation; it is an error before native dispatch.
An existing NULL tuple cannot become bound; a bound tuple cannot be omitted or
changed on retry even when the signed bytes and native request digest are identical.
A bound event must have network origin. The original trusted full-policy callback
continues to own source/scope/provenance authorization.

SQL checks source shape and current/historical enrollment before the existing
producer reservation guard. The route foreign key alone is insufficient: the
exact historical generation must also name that route and be enabled. A separate
post-producer-lock check excludes import-position bindings. Import's existing
producer lock and refusal to attach an already reserved request close the opposite
insertion order. Unbound import insertion retains its existing lock order and does
not acquire a dataset lock after a job/producer lock. Product transactions keep
READ COMMITTED and make a separate observation after waiting for serialization.

Bound requests require explicit0020 migration. Unbound existing consumers remain
usable at their earlier migration floor: the already checked ledger is observed
under its shared migration lock before selecting columns introduced by0020. Old
executables reject the unknown migration. New code still compares omission after
the upgrade; earlier-schema compatibility cannot bypass a retained binding.

Positive original-outcome recovery continues to use the original immutable
reservation, synchronized native input and complete native outcome. It does not
require today's enrollment, bans or route to authorize yesterday's committed
result, and it performs no fresh admission. Missing native detail stays unresolved;
no negative resolution, replacement identity or new grant follows from this tuple.
Reservations/outcomes remain within their existing finite retention envelope.

The [first practical wave](../implementation/evidence/p02-relay-source-native-20260928.json)
qualifies populated upgrade, exact/changed/omitted retry, legacy adoption refusal,
visibility mismatch and post-reservation refusal. [Relational evidence](../implementation/evidence/p02-relay-source-relations-native-20260928.json)
checks both relay/import insertion orders and exact guard refusals with rollback.
The [eight lock/fault schedules](../implementation/evidence/p02-relay-source-faults-native-20260928.json)
qualify both source-lock orders, writer rollback, reservation/outcome ambiguity
and original recovery after real authenticated withdrawal. Independent complete
native/input/receipt bytes, all-table effects and original attribution are checked.
Concurrent writers are explicitly privileged fixture injections; they do not
establish management authorization. These results close the bounded operational
binding responsibility. Actual source issuance, bans, routes and socket custody
remain required before claiming acquisition or the complete M1 journey.

## Explicit installed positive recovery

The selected finite command is `bigbrotr ingest recover --file ROOT`. It targets
only the configured existing Ingest root and reconstructs its original identity;
there is no caller-supplied replacement manifest or producer. It requires local
Control and Ingest. It starts no listeners, acquisition or Archive worker.

Native inspection and positive exclusion precede the application connection and
fresh Control claim. A held/missing/foreign native store therefore cannot fence
its original Control owner. A supervised blocking owner retains the native grant,
protected connection inputs, current Control and driver; a signal/deadline stops
further work and joins it, or explicitly terminates the process after the shutdown
grace. A diagnostic pipe cannot prolong that grace.

Closing captures PostgreSQL's reserved-through request under the same producer
lock as the native barrier. This is separate from native issued-through, which
may be smaller after interruption. Native issued beyond the reservation bound
refuses. The command streams every original sequence from one through that bound
through the positive resolver, with separate bounded input/outcome grants and
current policy checks. Missing reservations, input or native detail never count
as resolved. The original registration bounds the count; no vector of requests
or whole-store read is required. Current policy changes cannot rewrite historical
identity/outcome bytes. Exact retries preserve the original copies and barrier.

A successful finite result reports `positive-outcomes-resolved`, both bounds and
the number actually resolved, after native close, Control release and driver
join. This is not readiness, a durable recovery checkpoint, an absence proof,
source closure, ACK, retirement or successor activation. Failures can leave a
native barrier or committed outcome copy; preserve state and retry the same root.
The [installed supplement](../implementation/evidence/p02-producer-recovery-native-20260920.json)
records nine original-state cases plus three interruption/diagnostic cases with
product `5512161` and final fixture `851d116`. Two actual installed dictionary
requests and all 17 native rows/69 Control tables are checked, including missing
copies/inputs/reservations, pending reservation beyond issued, held native owner,
changed heads, retries, SIGTERM, query deadline and a filled 64 KiB stdout pipe.
The signed supplement uses the same product with fixture `e8317e9`: all four
original requests from the retained installed acquisition/Archive/export journey
resolve, an identical retry preserves them, and a missing PostgreSQL event copy
is reconstructed with original bytes and a separately attributed current recorder.
All 49 native rows/four inputs and 69 Control tables are checked; the original
laboratory's Control, catalog and native/archive/export files remain unchanged.
This is bounded P01-T09 Step4/P02-T01 Step5/P02-T02 Step4 evidence. Source closure,
retirement, successor activation and the full restart/resend journey remain open.

SIGTERM and deadline terminated the tested CLI in about 2.22s and 3.08s with no
state changes; the configured operation/shutdown limits were 1s/2s. The driver has
a separate 2s drain. A PostgreSQL backend still waited on the fixture lock after
each process exit and terminated after unlock: transport shutdown does not promise
synchronous server-query cancellation. Saturated stdout terminated in about 0.32s
with exit1 after verified durable effects; diagnostics are not a recovery receipt.
These measurements do not qualify blocked native syscalls, COMMIT ambiguity,
production capacity or another target architecture.

### Initial source closure extension (under implementation)

The next P02-T01 Step5/P02-T02 Step4 consumer adds explicit
`ingest recover --close-source --file ROOT`. The default command retains its
positive-outcome-only behavior. The extension requires the initial single
producer and complete retained positive resolution of every reserved request;
it cannot turn missing input/detail or an unissued reservation into noncommit.

An ephemeral bounded accumulator consumes the opaque native recovery results in
original request order under continuous native exclusion. NewFacts batches must
cover the source contiguously from one; AlreadyDurable/Rejected do not advance
that source bound. The final bound must equal the native source counter. Zero
is an explicit empty prefix. No vector of all receipts, duplicate outcome ledger,
native format change or receipt reclamation is needed for this responsibility.

The source closure digest is SHA-256 over ASCII
`bigbrotr/initial-source-closure/1` plus NUL, the native manifest SHA-256 (32 bytes),
producer slot (16 bytes), producer epoch and reserved-through (each unsigned
BE64), then each ordered request sequence (BE64), input digest (32 bytes) and
original outcome SHA-256 (32 bytes), followed by the final source bound (BE64).
The count binds the fixed-width request entries; the manifest binds the source,
topology and original allocation namespaces. This digest attests the resolved
initial prefix; it is not a backup, restored-state or absent-tail proof.

Current Control and complete accepted policy authorize a transaction using the
existing source row and monotone open→closing→closed transitions. It rechecks the
original producer/reservation bound under established owner/source/producer locks,
preserves authoritative job/frontier caps, and stores only the verified bound and
digest. Exact closed retries compare the complete retained closure without
rewriting its time/xmin. Unknown COMMIT requires the same exclusive historical
inspection and positive resolution before retry; it cannot activate a successor.

Source closure leaves producer/domain retirement, workflow abandonment, ACK/GC,
Ingest owner release and successor activation to their explicit later operations.
The original topology remains immutable, including its historical NULL handoff;
a later topology must carry the closed predecessor's exact bound. This extension
passes the first scoped wave with product `f873d92` and fixture `d90b718`: three
actual native prefix cases and four installed signed cases (close at12, identical
retry, missing event outcome copy, incompatible retained job bound13). The
[initial-source supplement](../implementation/evidence/p02-producer-recovery-native-20260920.json)
retains all49 native rows/four original inputs,69-table comparisons and the
independent closure digest. Fixture `10c2eca` adds actual UPDATE/SIGTERM rollback
and COMMIT/deadline recovery: the latter commits after the client exits, and the
identical retry preserves the exact retained closure. Both retries check positive
Control fencing. Server query termination is observed separately from CLI/driver
termination. Fixture `d824180` qualifies both current-schema concurrent orders:
a job cut13 committed first prevents closure12; closure12 committed first makes
job13 COMMIT refuse and roll back. Migration0815's earlier dataset lock remains
active, and the historical topology NULL remains unchanged. The competing job
writer is a privileged relational fixture, not authenticated acceptance. These
bounded results do not complete producer/domain retirement, successor or M1.

### Explicit initial acquisition abandonment (under implementation)

`ingest recover --abandon-acquisition --file ROOT` includes positive resolution
and source closure, then explicitly abandons the original initial owner's queued
acquisition intents. It is mutually exclusive with `--close-source`. This finite
operation is a retirement prerequisite, not producer/domain retirement or ACK/GC.
It starts no network work and never claims traversal, EOSE or event completeness.

The initial Sync executor borrows its native owner throughout its finite socket
callback; that callback cannot retain the socket. Exclusive native recovery
therefore excludes this executor as well as its admission writer. Fresh actual
Control exclusion independently fences its PostgreSQL effects. This combination
is specific to the initial local lifetime, not a generic remote-worker fence.
Current policy authorizes recovery even after the old actor, scope, enrollment or
deadline loses execution authority. Those changes must not erase existing facts
or prevent recording uncertainty. Original accepted intent remains immutable.

Additive migration0027 introduces the selected `acquisition_gaps` relation. This
first protocol retains one whole-intent gap per job, keyed by that existing job
identity; its immutable acquisition detail supplies scope, exact selector,
endpoint/access and requested duration. It records no observed start/end or
partial traversal because those are unknown. The gap binds the original claim,
original producer and closed native source cut/digest, current recovery issuer
and timestamp, with reason `initial_producer_abandoned`. Gap insertion and
queued→cancel_requested→cancelled commit atomically; existing job revision rules
remain in force. Only this exact gap authorizes transferring the terminal write
to current Control. It grants no new execution. Deferred checks refuse incomplete
transitions. Gaps, original inputs/outcomes and scientific records are retained.

The qualified recovery setup grants `SELECT, INSERT` on `control.acquisition_gaps`
and `EXECUTE` on `control.guard_acquisition_gap()` and
`control.require_acquisition_gap_complete()`, alongside its existing job/source
read/update privileges. Initial retirement also needs the function privilege
specified below. A login prepared only for positive outcome recovery does not
implicitly receive these terminal-operation permissions.

Select all typed jobs claimed by the original Control, including jobs with no
mutation request. Unclaimed accepted jobs belong to no old execution and remain
unchanged. The first consumer handles at most64 queued jobs with checkpoint zero;
advanced/foreign or excessive history refuses without partial abandonment. An
indexed excess-row discriminator bounds reads, independently of receipt count.
Control/dataset UPDATE precede historical registry/source/producer and sorted
job locks. Source closure and prior positive outcome copies may already have
committed when a later abandonment refuses; they are never undone or hidden.

Identical retry revalidates the complete native/SQL prefix and retained gaps;
it preserves their original issuer, timestamps and row versions. Cancellation
and ambiguous COMMIT require the same original root and mode. Later producer
retirement must check these obligations explicitly; migration0011 continues to
refuse nonempty retirement until that separate consumer is implemented.

The [first scoped evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json)
uses product `257d232` and fixture `bff0bc3`: real additive migration/reapply,
original signed job abandonment and identical retry, a synthetic zero-request
queued intent plus untouched unclaimed intent, revoked historical actor/grants,
and advanced checkpoint/missing0027 refusal. It checks70 tables/49 native rows
and preserves the original signed catalog/files. Later qualified guard, limit and
interruption supplements are recorded below; the first-wave outcome is unchanged.

Independent review and an installed HTTP reproduction at fixtureb14489d identified
an ordinary-job grant regression introduced by0027. Additive0028 removes its
redundant deferred jobs UPDATE trigger; the deferred gap INSERT check and terminal
job immutability retain atomicity. The prior six abandonment observations remain
valid for their scoped recovery grants. Product20ce916 now passes ordinary26/28 HTTP acceptance and28 signed Sync with
old grants, plus27 startup refusal for both consumers. Fixture0137713 adds actual28
abandonment/retry, eight raw incomplete/immutable transaction refusals, and a
discriminating missing-trigger negative. All70 tables retain their rows/xmin on refusal.
The original64fda0f SET ROLE fixture setup failure remains recorded; the correction
uses a real application login with explicitly synthetic backend preparation.
Fixture97f9a8d also checks the64/65 boundary, including64 retained gaps plus one
queued intent, and preserving populated27→28 migration/reapply/recovery. Excessive
history refuses without partial job/gap writes. The additional jobs are synthetic
complete relational inputs, not a throughput measurement. Fixture3ef3777 adds actual
INSERT/SIGTERM rollback and COMMIT/deadline followed by durable completion after
client death. Both same-root retries positively fence the prior Control and
preserve original gap/job/source attribution. All70 tables/49 native rows/four
inputs and original catalog/files are checked. CLI termination is bounded in these
schedules; server backends wait until the fixture unlocks. Blocked native calls,
crash/ENOSPC, aged/load, producer/domain retirement and successor remain separate
acceptance. See the same report for exact artifacts and original failures.


### Explicit initial retirement (under implementation)

P02-T01 Step5/P02-T02 Step4 add `ingest recover --retire-initial --file ROOT`
as a separate operation after explicit source closure and acquisition abandonment.
It is mutually exclusive with the other recovery modes. Additive0029 preserves
all applied migration bytes, native formats and existing rows. Old binaries refuse
its unknown schema. A retained historical empty retirement is not implicitly
adopted; the new consumer requires the complete terminal shape described below.

The consumer retains exclusive `IngestRecovery`, fresh actual Control and current
policy throughout its operation. It only observes an existing native closing
barrier and revalidates every original request/result against already present PG
outcome copies. It cannot insert missing copies or turn an unissued request into
noncommit. The native witness records Event classification within the existing
64-request spool bound; a native Event with no acquisition job refuses even if
all optional SQL source fields are absent. Unbound dictionary Evidence remains
supported. No new persistent witness or native closure digest is introduced.

Before any retirement, check the full single initial producer/source, all copied
outcomes, complete retained acquisition gaps and all original-Control workflow
claims, including claims with no request. Import bindings/checkpoints or import
claims refuse pending their own qualified logical closure. This is a declared
initial path limit, not removal of Importer or general recovery from the product.
Unclaimed accepted jobs remain untouched. Owner-indexed acquisition/import checks
avoid scanning unrelated job histories; retained gaps are bounded at64.

Under Control then dataset UPDATE, lock Ingest/role/domains before source and
producer. One final SQL transaction retires the producer, closes every allocation
domain, releases Ingest with `storage_fenced` and retires its role. Each shares the
same observed database timestamp and previously resolved source digest. If the
current wall clock precedes the already committed source closure, refuse before
any terminal UPDATE; never commit a shape that completed retry would reject. Ordinary
closing/admission still refuse retired/released history. Only this explicit mode
can observe completion, revalidating the whole native/SQL witness and terminal
state without rewriting original timestamps, attribution or row versions. Mixed
terminal state refuses rather than being repaired opportunistically.

The producer trigger checks closure/workflow obligations even for reserved zero;
SQL alone does not prove native exclusion or inputs. The internal adapter supplies
that proof and composed owner/domain atomicity. It follows PostgreSQL16
[trigger visibility](https://www.postgresql.org/docs/16/trigger-datachanges.html)
and [row-lock semantics](https://www.postgresql.org/docs/16/explicit-locking.html).
Application recovery privileges include EXECUTE on
`control.require_initial_retirement(bytea,numeric,bytea)` and the existing recovery
read/update grants; this does not expand ordinary acquisition grants.

Successful output reports `initial-retired` after native close and current Control
release/driver join. Native v2 remains closing, ACK and retired watermarks stay0,
and all inputs/receipts/facts remain. No capacity is reclaimed and no successor
is activated. Cancellation or unknown COMMIT requires the same root/mode; a
committed terminal result survives client loss. The
[retirement evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json)
preserves the original 6bc2005 clock-regression failure: that run committed a
terminal time older than source closure. Corrected product/fixture f12dbf0 passes
all ten installed cases, including the discriminating refusal, completed retry
and populated additive migration/reapply. Fixture0d81400 with the same f12 artifacts
also passes actual AFTER producer UPDATE/SIGTERM rollback and deferred retirement
COMMIT/deadline followed by durable completion after client death. Each same-root
retry positively fences the preceding Control; completed retirement preserves
its original timestamps/digest/xmin. All 70 tables, 49 native rows/four inputs and
the original signed Control/catalog/files/report are checked. These schedules
do not establish server cancellation on client exit. Fixture3f36784 adds four
passing cases with the same artifacts: current policy expiry during observed SQL
execution rolls back retirement; same-root fresh-Control retry succeeds; Ingest
draining before retirement is preserved, and draining after retirement refuses
23514 against released history. Product owner guards remain enabled; actual
blocked SQL and complete row/native effects are captured. Current Control UTC
expiry, other row-contention schedules, true zero-issued interrupted startup
and successor retain separate qualification. Fixture 115d888 also passes three
independent import-obligation refusals: an original-Control claim with no request,
a retained input binding without such a claim, and a retained producer checkpoint
whose complete-through bound is zero. Exact PostgreSQL veto/context is correlated
to the fresh Control backend; all 226 product triggers remain enabled and the
retirement function unchanged. Complete 70-table/native effects and original seed
preservation pass. These are explicitly synthetic incompatible relational inputs,
not qualification of genuine import admission or logical closure. No full M1 or
whole-task completion is claimed.

## Typed local classified progress (0039, partial qualification)

P02-T02 Steps4/5/7 add a cumulative producer head consumed by the actual initial,
activated and successor Sync dispatch. This first increment keeps native ACK and
retired watermarks zero and retains every input/outcome. It grants no window
credit, completes no acquisition job and claims no remote resume cursor, EOSE
coverage or backup qualification. The new acquisition job's
`jobs.checkpoint_revision` stays zero; historical jobs retain their original
values. Explicit whole-intent abandonment still records uncertainty and preserves
scientific facts.

Product `d4e1b85` passes 76 focused tests, 19 required controls and both Clippy
selections. Fixture `75df760` passes three actual successor Sync lifetimes at
epochs 3/3/4, full 77-table and independent signed native/input checks, and 63
corrupted-capture controls. Raw replay binds 1013 retained files and repeats the
science/controls. The [recovery report](../implementation/evidence/p02-producer-recovery-native-20260920.json)
preserves all original failures and exact build/source/artifact identities.
Fixture `738637f` additionally passes fresh config-only recovery and exact retry
of the retained epoch3 typed state, plus missing-closure and foreign-issuer
refusals on separate privileged corruption copies. All77 tables/xmins, native
bytes and inputs match independent expectations; 60 corrupted-capture controls
and raw replay binding856 files pass. First recovery changes only the explicit
closing state and Control lifecycle; retry adds no native write. Activated epoch2
Sync and raw replay pass at9653725. Initial7239's complete capture passes the
corrected50316df raw reader; its original failed verifier result remains retained.
Fixture ecedbcaf adds both real proof-only COMMIT-loss outcomes and their fresh
active-Control recovery/retry, with82 controls. Other recovery, refusal and
resource scenarios remain required; all of these results retain ACK0.

`CapturedEventInput` decodes the actual retained correlated EVENT, checks the
accepted collection and actual REQ filters, and derives observation from that
frame's receive sample and validated source/attribution. Its private binding
carries raw JSON SHA256, actual 48-byte REQ ID, wire sequence, job/scope/source and
full semantic certification digest, including rejected-signature inputs. Generic
`PreparedEvent::new` and `PreparedCertification::new` remain usable by earlier
consumers but cannot authorize this progress. Neither a structural ExistingProof
nor a PG outcome copy can mint the native witness. The opted-in native operation
stamps its actual result with the retained root device/inode, native sequence and
manifest, rechecked before progress COMMIT. Equal receipts from another root do
not acquire this authority.

`acquisition_progress` retains one current local prefix per producer/job, not one
row per delivery. Ordinals 1..3 count retained correlated EVENTs in one actual
REQ; a different REQ starts at ordinal1. Parse/size/membership preparation holes
freeze that REQ's classified prefix. Later valid mutations retain their captured
frame binding, original reservation/outcome and complete dependency union; the
producer mutation cut and native ACK advance without replacing the job head or
its physical xmin. This returns credit without classifying skipped input. A
certification after the hole remains an ordinary read with no progress or ACK;
a later genuine REQ may start a new classified prefix at ordinal1. Existing
untyped or incomplete history is never silently adopted or backfilled. Native
rejection is a classified failure only within a continuous prefix, distinct from
an unclassified preparation hole. The preparation-hole source increment's build
and installed qualification are tracked separately in the recovery report.

Classifications 2/3/4 are actual native NewFacts/AlreadyDurable/Rejected, and
5/6 are ExistingProof/certification rejection. Each new classified position
advances the job's progress revision and classified count; 2/3/5 advance success,
4/6 advance rejection. Exact retained-position replay preserves the original
revision. Dictionary setup advances only the producer cut, without inventing a
job or frame. Proof-only progress advances revisions with the mutation cut
unchanged. `producer_checkpoint_dependencies` retains the complete deduplicated
FactRef union. No dependency is removed or replaced by an unproven maximum cut.

The producer digest is SHA256 of ASCII `bigbrotr/producer-progress/1` plus NUL,
dataset16, slot16, producer epoch BE64, manifest SHA256, checkpoint revision BE64,
complete-through mutation BE64, job count BE64 and dependency count BE64, followed
by job records sorted by job ID bytes and dependencies sorted by full source ID.
Each job record is exactly216 bytes: job16, scope16, revision BE64, REQ48, ordinal
BE16, wire sequence BE64, raw SHA256, intent digest32, classification BE16,
mutation request BE64 (zero for proof/rejection certification), classified,
successful and rejected counts (each BE64), actual received time signed BE64,
clock resolution BE64 and anomaly flags BE32. The Sync clock has no uncertainty
estimate; observer/clock/witness identities come from validated attribution.
Retaining actual time/quality permits independent intent reconstruction without
echoing the stored digest. Each dependency is dataset16, shard BE64 (u32 domain),
source epoch BE64, source sequence BE64 and payload digest32: exactly72 bytes.
SQL's canonical encoder and Rust's fixed-width encoder must match independent
goldens. Physical row metadata and checkpoint commit timestamps are not digested.

The mutable head has nullable format/context/count/issuer extensions; all are
NULL for historical opaque heads. No migration adopts or rewrites an old head
as typed progress. Nullable additions to existing tables use the domains' base
types and equivalent checks: PostgreSQL16 would otherwise rewrite retained rows
when adding a constrained-domain column. Constraint validation still scans under
DDL locks; this is not an online or constant-time migration guarantee.
No-head legacy history remains readable through old recovery;
the new dispatch refuses to synthesize completed setup from old reservations.
Historical retirement validators accept only a structurally valid typed head and
retain all existing workflow/import/gap checks and unactivated-target vetoes.
They also reconstruct the head's historical Control issuer from its exact
dataset/resource/epoch/incarnation, retained Control role instance and immutable
backend fence metadata. This nonlocking historical read requires neither the
old live backend/lease nor an unretired role instance; write-time authority still
uses the current fenced Control owner.
Native recovery separately revalidates its complete retained dependency closure;
SQL checksum equality is not a native presence or authority proof. Unknown COMMIT
keeps the session unavailable and requires inspection under actual recovery
custody; it never authorizes a new request identity.

This finite first slice allows64 job heads and16,384 exact dependencies per
producer. The latter is an explicit selected profile (64×256), not a consequence
that proof-only traffic has at most64 deliveries. Logical charges are1024 bytes
for the head,1024 per current job and256 per reference: each covers its fixed
payload, keys and conservative row metadata, not PostgreSQL physical pages or
RSS. These charges share the existing32MiB cap with **all historical** retained
input bytes plus64KiB completion allowance per request. Exhaustion refuses before
new dispatch; freeing an active slot does not reclaim retained state. The existing
absolute64 request/recovery envelope remains and does not satisfy indefinite live
traffic or the two-slot ACK acceptance on its own.

A separate2MiB operation grant owns the typed union and current heads:
16,384 `FactRef` values,64 `JobHead` values,128KiB bounded SQL/parameter scratch and
fixed bookkeeping; a compile-time size inequality checks this representation.
Rows stream with excess-count and fixed-width sentinels before vector insertion.
Native decoding/result handoff and the Control connection/driver retain their
own grants; the latter survives cancellation and driver drain. The SQL canonical
aggregate is bounded separately to64×216+16,384×72 bytes before hashing and is
server work, not charged as Rust allocation. Native handoff headers and the
larger certification result are included in their derived existing allowances.
No measured process/resource bound follows from these reservations.

The Control login retains its existing migration0038 privileges and additionally
needs `SELECT, INSERT, UPDATE` on `control.producer_checkpoints` and
`control.acquisition_progress`, and `SELECT, INSERT` on
`control.producer_checkpoint_dependencies`. It needs `EXECUTE` on
`control.acquisition_progress_bytes(control.acquisition_progress)`,
`control.producer_progress_digest(control.producer_checkpoints)`,
`control.require_producer_progress_capacity(bytea,numeric,bigint,integer,integer)`
and `control.require_retained_producer_progress(bytea,numeric)`.
No delete/truncate grant is required. Existing privileges on the unchanged
Control/acquisition authority helpers and replaced historical validators remain
necessary. The migrator owns trigger installation; application SQL does not
disable those guards or seed checkpoint authority.

### Native ACK and returned credit (partial qualification)

This P02-T02 increment connects confirmed mutation checkpoints to the existing
native acknowledged watermark. Product `fcad4c9` passes 258 selected component
tests and both Clippy selections. Actual initial, activated epoch2 and successor
epoch3 consumers pass with a registered two-request window, three new signed
events before EOSE and a separately gated three-event proof-only resend. Complete
independent SQL/native/input checks and raw replay pass: initial I=A=C6,
epoch2/epoch3 I=A=C4; proof-only adds no native writes or request originals.
The original result-packing, quiet-wire selection and standalone reader import
failures remain recorded. The owning
[recovery report](../implementation/evidence/p02-producer-recovery-native-20260920.json)
pins exact fixture/build identities and invocation premises. Fixture `70bb00f`
also passes two real third-mutation COMMIT losses and four fresh recovery/retry
invocations, 140 negative controls and strict raw replay binding 2,313 files.
Before-forward leaves I4/A3/C3 and no SQL outcome4; recovery copies outcome4
without inventing checkpoint progress or ACK4. After-complete leaves I4/A3/C4;
recovery validates the retained originals/closure and synchronizes ACK4. Both
recoveries close native ownership; exact retries add no native write. Historical
typed issuer/xmins, scientific facts and original inputs remain preserved.
No intermediate cut or post-ACK durability loss was injected. Full-window
proof-only, native ACK uncertainty and remaining refusal/resource gates stay open.

Product `9cef25f` additionally passes 262 component tests and both Clippy
selections. Installed fixture `dfbb8dc` exercises actual REQ1 `[A, malformed, B]`
then a distinct REQ2 `[C, D, B-proof]` within the two-slot window. B advances only
the mutation cut/ACK while the first classified prefix and its xmin stay frozen;
REQ2 resumes classification. Final I=A=C7, revision8, classified4 and all27
dependencies match the independent oracle. Fresh config-only recovery preserves
all seven originals and scientific history, adding one closing write; exact retry
adds none. The live/recovery/retry controls total95, and strict raw replay binds
2,185 files. The first `6065977` fixture failure remains recorded separately from
its corrected offline context and this successful fresh run. Same-REQ proof-only
after a hole is qualified by the later fixture below; full-window typed proof
and the other listed gates remain open.

Fixture `9f859ba` also qualifies two fresh initial recovery refusals derived from
that complete recovered state: native A7 with a valid typed C6 head, and missing
original request5 despite its retained outcome and ACK. Both exit at the expected
distinct boundary, preserve all scientific rows/xmins and native bytes, and add
no native write. The missing original is quarantined intact on the isolated copy.
Forty-six controls and strict raw replay binding 4,329 files pass. The earlier
origin-session preflight failure remains recorded with its cause unknown.

Fixture `8470860` exercises REQ1 `[A,B,C]`, then actual REQ2 `[A,malformed,A]`.
Only REQ2's first proof advances the typed heads: revision7, classified4, ordinal1;
C=I=A6, all 22 dependencies, six originals and native 75 rows/sequence95 remain
exact. A later healthy REQ supplies the serial-dispatch observation; the
third proof's no-op interpretation also depends on the reviewed caller path,
since no per-proof success response exists. All 52 controls and strict raw replay
binding 2,136 files pass. Recovery reuses the separately qualified ordinary proof
head evidence; no new persistent form is introduced. The existing real native
full-window proof component remains valid. Current healthy serial typed Sync
ACKs each mutation before the next event, so it cannot exercise a full-window
typed PostgreSQL proof transaction; that broader consumer gate remains deferred.

The private native test target at `b005d91` also qualifies one actual ACK write
whose WAL `fsync` result is replaced with EIO by the retained tracer. Exact trace
and WAL bytes bind its single producer-state update; ACK, credit and a previously
valid capacity request return CommitUnknown, with no later WAL write/sync before
abrupt process exit. Both the positive control and fault reopen in distinct
processes at A1/sequence11, preserving all nine frozen rows except the ACK and the
original input. Nineteen negative and two positive diagnostic controls pass.
The report retains two preceding fixture failures and their bounded corrections.
This exercises the native kernel with a private test target: kernel page cache
survives, and no connected PostgreSQL error delivery or physical loss is claimed.

Additive migration `0040_producer_ack.sql` separates the absolute retained limit
from the coordination window: the next reservation is at most validated typed
C plus the registered window, and never exceeds64. An absent or opaque legacy
head contributes zero. SQL still does not observe or manufacture native credit;
the live owner independently requires native A. The replaced historical validators
retain their lifecycle and closure checks while allowing retained request counts
above the active window. No earlier migration or retained record is rewritten.

The connected mutation and dictionary operations retain their progress grant
through confirmed PostgreSQL delivery and synchronized native ACK. The grant
reserves 2 MiB data plus 4096 native bytes before effects. Only the private connected
operation can authorize ACK; a decoded head or caller-supplied sequence cannot.
The ACK batch preserves issued, closing, retired and all original request,
receipt, input and scientific data. It advances acknowledged only when the
validated contiguous mutation cut is greater. Uncertain completion ends usable
ownership and requires positive recovery of the original state.

Proof-only certification sends no ACK, including when a previously confirmed
mutation cut is ahead of native acknowledged. It may advance classified progress
while the mutation window is full without changing native sequence or detail.
An ordinary successful mutation operation returns only after its own ACK is
positively observed; the PostgreSQL cut alone never returns producer credit.

Installed Sync uses that owner's native issued/acknowledged observation for
scheduling. Its next request must still be exactly issued+1. Initial setup
acknowledges the two dictionary requests before a two-slot Sync lifetime can
continue. The active window is separate from the retained64-request/32MiB
profile: ACK reclaims nothing. Initial Archive likewise bounds retained complete
batches independently of the active request window, within its existing complete
cut protocol and resource grants.

Restart must validate the typed checkpoint, complete retained dependencies and
original inputs/outcomes before accepting nonzero ACK or completing a cut ahead
of it. Missing or inconsistent history refuses; a larger head cannot excuse a
mixed restore cut. Recovery must preserve exact native sequence and proof
ownership across any ACK write. Authenticated schemas before40 retain their
original recovery behavior with A=0 and no native ACK; schema39 typed closure is
still validated. Nonzero A cannot be accepted through a pre40 schema.
Ordinary activated epoch2 recovery replays the exact epoch1 originals before its own;
whole-lineage successors retain their ordered original replay. Preparing a new
lifetime may preserve a verified closed ancestor with A<C: it supplies no live
credit, and preparation neither rewrites that watermark nor re-stamps its proof.
Empty, unactivated targets still require zero issued/acknowledged/retired state.
These bounded two-slot demonstrations do not qualify GC, indefinite traffic, workflow completion or
backup/restore.

The compacted activated recovery extension is scoped-qualified at product
`fc9388fc` and installed fixture `cf1a2ef9`. Only positive
`--activated-successor` recovery selects an inspected
initial floor: it authenticates the retired ancestor, typed checkpoint and
canonical outcomes, then replays genuine epoch2 originals. The ACK-only proof
must revalidate that ancestor and target checkpoint in its final transaction,
and retain the complete input census through synchronized native ACK. Compacted
source closure and retirement are unsupported at that product revision. A reserved unissued request
stays unresolved; a staged unreserved tail is preserved and supplies no credit.
ACK of a verified contiguous issued prefix reclaims no file or outcome.
The [recovery report](../implementation/evidence/p02-producer-recovery-native-20260920.json)
binds 172 component tests/63 mandatory controls and Clippy,24 installed commands
with291 checker controls, and exact raw replay over7827 bound files. The connected
matrix uses a seven-request compacted ancestor and zero/one genuine target request:
fresh typed ACK, before/after outcome COMMIT response loss and exact retries,
missing input, reserved-but-unissued refusal, and original-input regression.
The native tests separately exercise optional tails, proof/root/sequence binding
and logical request counts above the unchanged physical64-file limit. Source
closure, retirement success, later consumers, GC and physical-loss/restore/capacity
remain unqualified by this slice. Both original failed attempts are preserved.

The compacted Source/abandonment/retirement extension is scoped-qualified at
product `7bc63d97` and installed fixture `b1b2dff6`: source review, CLI/probe
compilation,179 component tests/70 mandatory controls and Clippy passed. The
installed positive/refusal profile passed25 commands/338 checker controls on
eight copies, followed by exact raw replay over9299 bound files. It covers
zero-request lifetimes, a genuine Event with its activating-Control job, exact
source/abandonment/retirement retries, missing-copy repair before closure, and
incompatible head, missing input, reservation tail, unbound Event and partial
terminal refusals. The unbound Event is refused during original replay; it does
not exercise the final Event-mask recheck. Original failures remain preserved.
The separate fixture `85891017`, using the same product, then passed56 commands
and865 checker controls on13 copies, with exact raw replay over7186 bound files.
It measures final COMMIT baselines for all three operations and exercises response
loss before forwarding and after server completion, fresh retries, cancellation
and actual UTC expiry across a real job lock, retirement post-proof ancestor H/D
and final Event-mask refusal, and a genuine zero-request claimed job. The separate `b6c4315a` fixture then passed15 commands/264 checker controls
on six copies, with raw replay over6163 bound files and32 proxy wire/metadata
tests. Measured post-proof refresh reply holds establish final Source and
abandonment H/D refusal before terminal effects. Fixture `0ceff580`, using the
verified import-gate probe at `e75aaa2f`, passed eight commands/133 case controls
and twelve isolated-mutation controls on three copies; exact raw replay bound
7351 files. An ordinary application-role import claim under the live activating
Control remains queued through genuine Event source closure and acquisition
abandonment, then causes the named retirement veto. A draining owner permits the
exact twelve-row retirement and unchanged retry. Otherwise-ready SQL with an
unchanged native OPEN target refuses at the native/SQL inspector, before the
deeper native observation guard. Import execution is not exercised. No GC or
physical-loss/restore/capacity qualification follows.

It uses a distinct source-capable proof: confirmed canonical
ancestor replay precedes genuine target originals, whose ordered digests, source
cut and Event mask retain their existing meanings. The ACK-only proof cannot
construct that source capability. Each final transaction must revalidate the
sealed ancestor checkpoint/canonical outcomes and exact target copies; an earlier
confirmed read is insufficient. Retirement observes an existing native closing
barrier and cannot create one. Workflow gaps and the exact twelve-row terminal
transition retain their existing predicates. Every post-COMMIT return checks the
complete native input census and current delivery authority; no terminal proof
supplies permission to unlink inputs or discard scientific state.

## Closed-producer reclamation (reader under qualification; deletion unavailable)

The first reclamation profile covers one nonempty, completely retired initial
producer, before successor preparation. Let N be its reserved request cut:
require `0 < N <= 64`, native closing with `issued = acknowledged = N`, and
validated typed format1 progress complete through N. Require the exact completed
retirement tuple, including closed source/allocation domains, released and
storage-fenced Ingest owner, retired role, matching closure digests/timestamps,
and the existing workflow, acquisition-gap and import checks. Native closing or
ACK alone is insufficient. Keep all PostgreSQL reservations, outcome bytes,
progress/dependencies and terminal rows unchanged.

Before the first reclamation, replay every original and validate the complete
scientific state under exclusive native custody. Reuse the existing source
closure digest, which binds the original manifest, producer identity, ordered
request digests/outcome hashes and source cut, together with the typed checkpoint
revision/digest. The closure is not a checksum of every SQL column or the store.
No second per-request proof ledger is introduced.

The existing ProducerState key gains this exact 139-byte version3 value; all
integers are unsigned big-endian. Earlier values retain their existing meaning.

| Byte interval (end excluded) | Value |
|---|---|
| 0..2 | `03 31` |
| 2..10, 10..18, 18..26 | issued, acknowledged, retired; all N |
| 26..27 | closing byte `01` |
| 27..59 | SHA-256 of the original manifest bytes |
| 59..67 | complete source sequence |
| 67..99 | original source closure digest |
| 99..107 | typed format1 checkpoint revision |
| 107..139 | typed format1 checkpoint digest |

A positively confirmed current-Control transaction validates the retained PG
closure before authorizing any native mutation. One bounded synchronized native
WriteBatch installs this floor and deletes at most64 exact receipt keys. Only
observed native success permits unlinking authenticated original input files,
followed by directory synchronization. PG or native unknown completion authorizes
no unlink. An exact floor retry performs no native write; validated residual files
after interruption can be removed idempotently. Unexpected or corrupt files remain
refusals, not deletion candidates.

Compacted recovery authenticates the exact floor against retained PG history,
canonical outcomes and progress dependencies. It reuses the structural and semantic
census to retain complete pending-batch boundaries, source cuts, Fact integrity,
dictionary forward/reverse mappings, allocations, index subjects and correction
ancestry. Hashing only the progress dependency union is insufficient. Original
request semantics were established before the floor; a PG row alone cannot
manufacture that proof. Reclaimed requests return `RetiredRequest` even with a
changed digest. Missing detail without the matching floor, or post-GC native
state paired with older/incompatible PG state, refuses recovery.

The non-destructive codec, compacted-prefix witness and shared census are
implemented. The isolated installed reader passed with preserved originals,
quarantined originals and incompatible-head/closure/pending refusals using a
synthetically installed floor; this does not qualify product reclamation.
The existing `--retire-initial`
recovery action recognizes an already compacted initial producer. It checks the
complete retained PostgreSQL closure and reports `resolved_requests = 0`,
`compacted_requests = N` and `recovery_basis = closed-producer-floor-v3`; it does
not replay absent originals or install the floor. Other recovery actions retain
their original-reader restrictions.

This compacted branch additionally needs the current Control login to have
`SELECT(owner_resource_id)` on `control.initial_ingest_successions`,
`control.ingest_successions` and `control.ingest_activations`, alongside the
existing initial-activation read privilege. These column reads prove absence of
prepared or activated successors. Provision them explicitly for an installation
using this branch; the command does not grant itself privileges. No mutation
privilege is added, and ordinary original-input recovery keeps its existing ACL.
An absent permission refuses the compacted operation.

The explicit compacted initial-succession implementation passed the bounded
synthetic-floor journey: preparation, exact retry, confirmed SQL/native gap, first
dictionary write and installed startup/renewal/shutdown (16 cases, 149 negative
controls). This qualifies those consumers; product floor installation remains gated.
Its sealed handoff binds the verified floor, native root/sequence and complete
input census while preserving the existing portable intent bytes. Preparation
checks the exact typed checkpoint, canonical outcomes and historical epoch1
retirement before confirmed PostgreSQL COMMIT and again before the same four
native writes. Exact prepared retries write nothing. Historical gap/completed
preparation readers require the existing intent, an empty open epoch2 target and
confirmed ROLLBACK before returning a reusable plan. They cannot activate a writer.
Historical retirement checks cover epoch1 allocation domains; the existing
prepared-target validator separately requires all epoch2 domains to be active.

Only the retained continuous candidate can use this basis for initial activation.
The existing `serve --activate-initial-successor` entrypoint selects the compacted
current-initial reader before claiming Control and reserves both census and
canonical-outcome work. Its predecessor report separates zero original replays
from N compacted requests. The current `--retire-initial` reader keeps its strict
no-successor guards and confirmed COMMIT. Installed activated recovery, later live and Archive consumers retain their
separate qualification gates; initial preparation does not qualify those paths.

The explicit `ingest recover --complete-initial-preparation --file ROOT` command
passed its bounded installed profile (23 commands, 232 altered-evidence refusals)
and exact-source raw replay. It completes only an already recorded epoch1-to-2
preparation, or verifies its exact empty OPEN epoch2 retry. It accepts complete
original predecessor evidence or a verified closed-producer floor; it never
selects a different basis after a failed proof. Current Control and accepted
policy authenticate the original intent, preparing identity, predecessor closure
and target state. A gap performs the existing four-write native transition;
a prepared retry writes no native state. Activated or later attempts, nonempty
or closing targets, missing original evidence and incompatible compacted closure
refuse. Neither path adopts the attempted incarnation or activates a writer.

Successful output reports `state=initial-preparation-completed`, the starting
`from_owner_epoch`, exact target incarnation/handoff digest, predecessor cut and
closure digest, and separate `resolved_original_requests` and
`compacted_requests`. The target remains producer2 with zero issued/reserved
requests. Output follows native teardown, confirmed Control release and driver
join; an interrupted or unknown outcome requires inspection of the same retained
root and intent. The command performs no ACK, retirement, floor installation or
unlink. The compacted completion branch also passed bounded wire loss before
native dispatch and after rollback completion, plus fresh installed retries
(13 commands, 164 altered-evidence refusals and exact raw replay). An entry loss
leaves a native1 gap unchanged; a lost completion reply may leave the four writes
already durable, and retry writes nothing. Original-input delivery faults,
cancellation/lease expiry, process/power/native syscall uncertainty and a genuine
epoch2/SQL-pending3 gap remain separate qualification gates. Original evidence
remains required by recovery modes not explicitly adapted to a compacted predecessor.

The compacted `ingest recover --retire-empty-attempt --file ROOT` extension is
scoped-qualified at product and installed fixture `30ab2e45`. It selects only an exact unactivated empty
epoch2 target, OPEN0 or CLOSING0, with no pending successor. Its distinct proof
authenticates the retired initial ancestor, typed checkpoint, canonical outcomes
and complete native/input census. The final transaction repeats those checks
before the existing one-marker native close and atomic thirteen-row SQL
retirement. A prepared CLOSING0 target can finish SQL retirement; terminal SQL
with native OPEN0 refuses. An exact terminal retry preserves native state and
the original thirteen timestamps and row versions. The proof remains bound
through native dispatch, SQL COMMIT and result delivery; it supplies no writer,
activation, ACK or deletion authority. The command keeps native custody ahead
of Control teardown, including interrupted acquisition, and reports compacted
requests separately from original replays. Original-input retirement retains
its own proof and requires all original evidence. Exact product `30ab2e45`
passed CLI/probe compilation,185 components/76 mandatory controls and Clippy.
Its first installed profile passed13 commands/239 controls on nine copies;
exact raw replay bound7698 files. It covers both evidence bases, unchanged
terminal retries, a real SQL failure after native closure and fresh completion,
and the selected H/D/input/activation/nonempty refusals. All four origins remain
unchanged. Separate fixture `b9c61f59`, using the same product, passed five
commands/125 controls on three copies, with raw replay over5876 bound files.
Its observed final COMMIT3 baseline distinguishes loss before forwarding from
loss after server completion. Both leave exactly one native close and report
CommitUnknown; fresh recovery completes prepared SQL or preserves all thirteen
terminal timestamps/xmins without another native write. Fixture `38c99d96`
additionally passed nine installed commands, 279 case controls and 24 setup
controls, with raw replay over6027 files. It exercises genuine current-Control
generic import claims in both lock orders, rollback, unchanged terminal retry,
SIGTERM during a held lock, and actual database UTC expiry followed by fresh
completion. No cancelled or expired attempt writes a native close. These guarded
relational claims do not qualify import execution or original preparing-Control
claims. Fixture `b88029c1`, with the same product, passed six commands/122 case
controls plus25 fixture controls and raw replay over6013 files. A held,
server-completed refresh COMMIT2 confirms the initial proof precedes isolated H/D
corruption; both exact refusals preserve native state and target SQL. All three
releases occurred within the unchanged1s renewal deadline. One source-only
partial SQL state and a missing native handoff refuse; pairing independently
retained native OPEN0 bytes with genuinely retired SQL also refuses, preserving
all thirteen terminal timestamps/xmins. These are explicit corruption fixtures,
not a supported mixed-restore procedure. Separate fixture `2b556790` with product
`628f955` passed one installed malformed-floor refusal,14 case and8 fixture
controls; raw replay bound5737 files. A fixed fixture changes only floor byte25
(retired7 to6), retaining all84 rows. The installed command refuses before
Control connection/claim, preserving all77 SQL tables/xmins and native sequence126
without a product write. Corrected fixture `16f32106`, with product `628f955`,
passed one probe and two installed commands,62 case and11 setup controls, with
raw replay over5768 files. An ordinary import is admitted before the probe and
claimed by the actual original preparer inside its held second transaction.
Fresh completion preserves that claim; fresh retirement refuses at the named
claim guard before refresh or native close. The first fixture failure remains
recorded separately. Import execution, remaining proof-use/physical uncertainty
and original-input delivery remain separate qualification gates.

Later compacted preparation, SQL/native gap completion and the existing
`ingest recover --retire-empty-successor --file ROOT` have bounded qualification
at product `2bdf513c` with fixture `28cd644d`. A separately retained compacted
check authenticates the initial floor and canonical PostgreSQL history while
later ancestors still require their original inputs. Preparation keeps the
existing four-write handoff; recovery of a committed intent never creates a
writer. An unactivated empty target can close once, finish retirement from an
existing native CLOSING state, or verify an unchanged terminal retry. Compacted
ancestor requests remain separate from original predecessor and target counts.

The four-copy/eight-stage profile and exact-source raw replay cover a measured
preparation COMMIT response loss, fresh gap completion, installed OPEN retirement,
a real SQL rejection after native close, fresh CLOSING completion and exact retry.
Complete SQL rows, terminal timestamps/xmins, native bytes and input maps are
checked. Prepared OPEN/CLOSING cases retain only the required later original;
preparation itself retained the optional initial originals. Separate fixture
`82103420` qualifies preparation without those optional originals, required-input
refusal despite residual initial files, and H/D changes at measured preparation
and installed proof-use boundaries. D corruption reaches the earlier SQL
closed-source commitment guard; it does not qualify the later Rust D recheck.
Remaining delivery/cancellation/native uncertainty and Archive remain gated.

The same product `2bdf513c` also passes an ordinary-basis regression at fixture
`c3bec54c`: one genuine PreparedOpen4 copy, installed empty retirement and exact
terminal retry. The unchanged independent75-table model compares all rows, four
original inputs and native bytes. Retirement adds one native close and the exact
terminal transaction; retry preserves terminal timestamps/xmins, receipt and
native sequence. Thirty counterexamples and raw replay over787 files pass. The
ordinary report contains no compacted basis/count. This does not qualify broader
ordinary lifecycle faults, activation/live or Archive.

Product `5df6b942`, with fixture `fa76466a`, additionally qualifies a compacted
current writer for later activation. The existing `serve --activate-successor`
selects this basis; the profile exercises admission through its finite consumer
and installed quiet activation separately. The writer retains one charged lineage
allocation and a private compacted ancestor proof. Dictionaries, Event admission,
reads, certification, checkpoint/ACK and renewal recheck the same native/input
census and retained PostgreSQL H/D authority. Failed or interrupted proof use
poisons that owner; it cannot supply a fresh recovery or Archive capability.

The independent four-copy profile covers full, partial and absent initial
residuals, with every later original still required. Finite dictionary progress
reaches checkpoint1/ACK1; subsequent existing/new Event admissions and retries
preserve that watermark. A separate installed quiet activation renews beyond its
initial deadline and joins after SIGTERM without issuing a target request.
Exact-source raw replay binds all SQL rows/xmins, native bytes and input files.
The [recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json)
owns counts, identities and preserved failures. Real Sync Event/certification
progress, delivery faults, fresh activated-later recovery, Archive and capacity
remain separate requirements; this does not implement floor writing or unlink.

Reclamation stays unavailable until initial recovery and the
existing successor preparation, activation and recovery consumers support that
witness without weakening their guarantees. Fresh-process replay, exact retry,
interrupted synchronization/unlink, mixed restores, missing scientific/index/
pending state and old-reader refusal remain required qualification. The existing
[recovery report](../implementation/evidence/p02-producer-recovery-native-20260920.json)
owns the source decisions and measured results; no deletion has been implemented
or qualified by this selection.
