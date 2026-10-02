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
is not implemented or qualified yet; the preceding measurements do not cover it.
