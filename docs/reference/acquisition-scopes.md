# Initial acquisition scope registration

P03-T01 Steps1/2/3 now have an internal registration adapter.
The [native first-wave evidence](../implementation/evidence/p03-acquisition-scope-native-20260928.json)
covers six actual-PG schedules. [Ten fault schedules](../implementation/evidence/p03-acquisition-scope-faults-native-20260928.json)
cover lost COMMIT, cancellation, both policy lock orders and corrupt buckets.
[Three raw SQL runs](../implementation/evidence/p03-acquisition-scope-guards-native-20260928.json)
cover58 immediate refusals, exact2MiB boundary and complete rollback.
This is neither an installed Sync command nor authority
to request/admit events or advance coverage. The [selector component](protocols.md#collection-selector)
retains its earlier bounded Mac/native evidence.

`register_acquisition_scope` accepts normalized collection intent, an existing
EndpointId, operational AccessScopeId, complete visibility QualifiedRef and an
explicit positive policy epoch. Retain that epoch with the original request across
retries. Registration creates no archive dictionaries. Source enrollment generation
and route are per-attempt execution context, outside this stable scope tuple.
A scope may remain registered after enrollment withdrawal or policy advancement;
its existence never permits fresh work.

Migration0004 appends after0020 and preserves all35 prior SQL checksums. One
`acquisition_selectors` row per exact normalized document in a dataset avoids
repeating a potentially large selector for every endpoint. SelectorId and
AcquisitionScopeId are opaque nonzero bytes16 identities, distinct from a semantic
fingerprint. The candidate SHA256 over document bytes locates at most64 rows;
complete bytes decide equality. Neither candidate equality nor a random ID collision
can adopt another selector. The semantic CBOR fingerprint remains unchanged.

Profile1 persists compact UTF-8 JSON in bytea, never JSONB. Its exact document
order is version, filters, history_since, ephemeral. Each normalized filter emits
present fields in ids, authors, kinds, sorted ASCII tag keys, since, until order.
There is no whitespace. IDs use lowercase hex, integers plain unsigned decimal.
Strings preserve UTF-8, including non-ASCII and `/`; quote/backslash are escaped,
BS/TAB/LF/FF/CR use their short escapes and other U+0000–001F use lowercase
`\u00xx`. This freezes the inspected serde_json1.0.151 compact formatter behavior,
not an arbitrary future serializer. Order/escaping changes require compatibility
handling rather than duplicate operational identity. Literal JSON regressions
complement the independent CBOR vectors.

Encoding counts escaped bytes before allocation, reserves exact output capacity,
and checks the same lexical replay budget as decoding. A typed selector can fit
its model limits while its escaped document exceeds2MiB or the replay working
ceiling; registration reports Capacity, never truncation or an altered predicate.
The caller retains grants for selector capacities and encoded bytes. SQL bounds
stored bytea; the trusted Rust codec owns semantic normalization. The internal
reader below checks persisted intent before its future admission consumer.
No unchecked stored bytes become acquisition authority.

`acquisition_scopes` is unique over dataset, endpoint, selector, access, full
visibility reference and policy epoch. Composite foreign keys require the exact
access/visibility relationship and an existing retained endpoint/access enrollment.
Both new tables are immutable and refuse truncation. Fresh registration requires
the actual Control owner and current accepted configuration/security revisions.
The adapter takes Control then dataset locks and performs lookups afterward under
READ COMMITTED. Selector and scope insertion share one visible transaction; a
scope failure rolls back the provisional selector too.

The existing final registration fence is reused because the new rows have exactly
its required attribution columns. Deferred checks reject changed policy or an
expired/lost owner before ordinary adapter COMMIT. As with endpoint registration,
raw SQL that forces deferred checks early with SET CONSTRAINTS cannot claim
commit-time expiry. No generic registration framework or backend trait is added.

A fresh owner resolves an exact existing historical scope before checking whether
a *new* scope may use the requested epoch. Therefore lost COMMIT followed by policy
advancement can recover the original identity without recreating old work. Missing
old-epoch intent refuses without leaving a selector. A received BB001 is a rejected
commit; transport uncertainty remains CommitUnknown. Errors/cancellation poison
the session; its caller retains and joins the driver before retrying.

`read_acquisition_scope` looks up an ID only inside the current Control dataset.
It returns immutable historical or current intent, not permission. SQL bounds
the returned document and identity/numeric fields; an orphan selector, wrong
profile or inconsistent scope is InvalidMetadata. Profile1 decoding compares
normalized serialization directly against the original bytes without allocating
a second encoded document. Alternate escaping, order, duplicates or whitespace
are refused rather than repaired. Parser resource bounds remain Capacity, distinct
from malformed or noncanonical input.

The reader requires8MiB plus an item before querying and retains that permit in
`StoredAcquisitionScope` until its selector is dropped. A smaller/missing-item
reservation refuses before poisoning the session. The connection owner separately
reserves PG response/framing capacity and keeps that grant through driver closure;
dropping a query does not release the driver's buffers. Cancellation or a failed
read after begin poisons reuse. No permit resizing or smaller retained-allocation
claim is introduced. The32MiB producer queue is distinct from the process-wide
network/parser envelope. The finite consumer must account their actual overlap.
[Mac codec/compile checks](../implementation/evidence/p03-acquisition-scope-read-mac-20260928.json)
and [fourteen actual-PG reader cases](../implementation/evidence/p03-acquisition-scope-read-native-20260928.json)
pass at a127c61. The latter observes the actual scope-table lock before cancellation,
checks original rows/xmin, full identity and document bytes, and verifies driver
joins and reservation lifetime. Clone-only corruption fixtures qualify refusal,
not registration or policy activation. This is data loading, not installed Sync.

Ordinary `sync.selectors` configuration still needs explicit versioned accepted
policy integration. Registration adds no fields/defaults to existing v1/v2
documents. Before execution, the trusted consumer must enforce both complete
accepted documents, full collection/request membership, current source authority
and committed visibility. Finite jobs, progress/effect closure, installed entrypoints and complete M1
remain selected implementation work; the initial admission bridge is below.


The initial admission bridge now accepts a borrowed `AcquisitionEventContext`.
It retains the loaded scope and the actual request predicate through the whole
reservation/dispatch operation. Before staging it checks exact endpoint/access/
full visibility, full collection membership and actual request membership;
collection exclusion, ephemeral exclusion and request exclusion stay distinct.
The immutable inputs cannot change between these checks and native dispatch.
Current source and scope policy are separately rechecked under relational locks
at both phases. Invalid signed inputs still receive native rejection after the
current relational checks. A scope requires a network source; it cannot be
attached to an import or silently bypassed by omitting that source.

Additive0021 retains AcquisitionScopeId on `producer_requests`, including with
provenance off. An exact16/nonzero nullable bytea plus the complete-relay CHECK
and composite foreign key bind dataset/scope/endpoint/access. Existing rows stay
NULL and cannot be adopted or retried through a changed/omitted scope. Plain bytea
avoids the constrained-domain addition mechanism observed in0020. The actual
populated PostgreSQL upgrade preserved prior rows/xmin and heap filenodes. The ordered
trigger checks current scope epoch after dataset serialization, before Ingest/
producer locks. Unbound imports take no new dataset lock. All prior SQL remains
immutable; native request/receipt/fact formats are unchanged.

Certification shares the request-independent current-source helper and loaded
collection/request context. It neither invents a ProducerRequestId nor checks for
a reservation. Missing0020/0021 prerequisites are explicit, including read-only
calls. Positive original-outcome recovery continues to resolve historical native
input even after current source/scope authority is withdrawn; missing outcomes
remain unresolved. Fresh admission permission is never inferred from recovery.

[Eleven actual PG/Rocks schedules](../implementation/evidence/p03-acquisition-binding-native-20260928.json)
pass at f403813: upgrade/reapply, exact retry, changed/omitted/legacy binding,
membership/source refusals, certification and actual withdrawal, and prerequisites.
Independent byte/state comparisons cover effects and absence of writes.
[Four further refusal schedules](../implementation/evidence/p03-acquisition-binding-refusals-native-20260928.json)
pass at70eea6f: exact rejection/retry, forged certification of a retained body,
invalid input after withdrawal and scope refusal after durable reservation.
A rejected mutation creates an operational outcome while leaving scientific facts
unchanged; its retry writes nothing. [Eight concurrency/recovery schedules](../implementation/evidence/p03-acquisition-binding-recovery-native-20260928.json)
pass at50b4136, retaining original scope/input/receipts after policy advancement and
authenticated withdrawal. The factory rejects stale revisions before the narrower
scope helper. [Raw SQL qualification](../implementation/evidence/p03-acquisition-binding-guards-native-20260928.json)
at9ce248f separately covers23 nested cases and5 helper schedules, including both
lock orders, rollback and isolation refusal. The epoch-only negative clone retains
its original FAIL. No installed Sync or management activation claim follows.
The first finite socket consumer can use the registered full selector. Generated
page/need-list resumption still requires retaining that execution's exact request
context; the scope ID does not reconstruct a page window. Both complete accepted
documents must additionally authorize event limits, provenance, ephemeral/history
policy, endpoint/bans/route and current deployment roots. Installed consumers may
not substitute the fixture validator or legacy unbound path for those checks.


The concrete static Sync policy consumer restores each complete accepted document
against current deployment roots. It permits explicit selectors whose
history_since is at least the accepted floor; accepted ephemeral Drop requires
scope Drop, while a stricter explicit Drop remains valid under Retain. The loaded
selector is never clamped or replaced. This conservative first profile compares
the explicit floor, without proving equivalent restrictions from filter since.
The pre-request collection checker returns the same restricted DirectPolicy as
the source checker; it supplies no socket, job or enrollment authority.

Prepared admission and certification share the arrival checker. The prepared
verification limit must equal each accepted ingest.max_event_bytes; a caller
cannot manufacture a durable size exclusion by choosing a smaller limit.
Provenance flag and witness presence exactly match the accepted objective.
Conflicting accepted limits/objectives refuse. Origin, full target visibility,
observer, clock, witness and received time must match the receiving owner's
retained observation. Invalid signatures/IDs remain attributable native
rejections after these checks. Collection/request membership remains owned by
AcquisitionEventContext, whose same loaded selector/visibility must construct
the checker. Historical outcome recovery keeps its original authority semantics.

The receiving owner still must establish actual socket custody and committed
witness linkage: the endpoint descriptor equals the actual normalized source,
access is the exact no-auth Public/null-label/target-visibility descriptor, and
observer/clock/method references are the trusted committed mappings. Operational
IDs are not archive QualifiedRefs. Provenance off creates no endpoint/access/method
dictionaries or witnesses; operational source binding and receive context remain.
This checker alone does not close those integration obligations or installed Sync.

[Mac components](../implementation/evidence/p03-sync-arrival-policy-mac-20260928.json)
and [nine native cases](../implementation/evidence/p03-sync-arrival-policy-native-20260928.json)
qualify this static arrival consumer at1978574. Actual PostgreSQL/RocksDB uses
independently encoded accepted documents with provenance off. It exercises exact
admission/replay, certification, forged-signature handling, withdrawal and five
policy refusals, including only the second accepted document. Original native
bytes, inputs and unchanged failed-operation state are independently compared.
No actual socket, provenance-on mapping, management transition or M1 claim follows.

## Finite job acceptance profile (implementation in progress)

P03-T01 Steps3/4 use the existing finite `jobs` identity, management command/audit
receipts and committed admission ordering. The first command names one registered
scope, a positive `deadline_us` duration of at most24h, and expected configuration
and policy revisions. All numeric wire fields are canonical decimal strings.
The CLI's duration syntax resolves to this exact value; retries retain the original
scope, duration, revisions and command key. The finite duration starts at durable
acceptance; it bounds this run, not future coverage or a restrictive action lease.
The server samples acceptance time before COMMIT, so waiting for COMMIT also
consumes the duration. A future acceptance timestamp is refused. A delayed
dispatch cannot reset the deadline; checked arithmetic is required.

The fingerprint is SHA256 over ASCII `bigbrotr/acquisition-job/1` plus NUL,
dataset16, authenticated actor16, command-key16, scope-ID16, duration-us u64,
configuration revision u64 and policy revision u64, all integers big endian.
A digest is neither acceptance nor source authority. Authenticated exact historical
retry returns the original receipt before checking permission for new work; it
cannot restart the job, refresh its duration or change its initial inputs.

The existing dataset `roles.control` action permits requesting bounded work from
an already configured Synchronizer. It does not permit enrollment, new network
routes, a signer, reading collected data or publishing it. Enrollment retains its
`policy.change` requirement. `history.read` and export grants do not substitute
for this operation. Execution independently rechecks current account/action,
accepted configuration/security documents, scope policy, enrollment, route/bans,
committed visibility and the actual Control backend fence. Closing the submitting
HTTP connection or logging out does not cancel an accepted job; cancellation is a
separate durable command. Account/permission withdrawal still stops fresh work.

Additive0022 marks participating jobs explicitly and attaches their immutable
scope/duration and authenticated attribution. Existing bare acquisition
jobs cannot be adopted. Acquisition creates new source facts, so its initial
`input_topology_generation` is NULL; no fake Archive input cut or export pin is
required. Later progress must retain its real event-effect dependencies. Acceptance
is atomic with the command, audit, sealed input and admission ordinal, under
Control → dataset UPDATE → account/session → source/job lock order. The qualified
job path must add actual backend fencing and final expiry checks; the generic
job state trigger alone does not provide them.

Installed integration must deliver requests to the living initial owner. It cannot
claim a second Control or reopen that owner's native store. The present explicit
initial profile rejects administration, and its fresh bootstrap allocates new
visibility namespaces; therefore endpoint enrollment and scope registration must
become reachable after bootstrap on that same owner. These are concrete early
integration steps, not a requirement to complete all relay lifecycle services.
The finite consumer closes its guarded socket/SQL transaction before fresh native
certification/admission. A bounded retained batch keeps the exact request predicate
and trusted observation. EOSE alone never advances progress or finishes coverage.

[Native acceptance evidence](../implementation/evidence/p03-acquisition-jobs-native-20260928.json)
qualifies seven actual adapter schedules at3e10c2d: acceptance/replay/conflict,
absent grant, stale policy, disabled source, elapsed duration, deferred COMMIT
refusal and missing0022. The current/future timestamp pair uses actual final SQL
constraints; removing only the future-time predicate makes the same fixture fail.
All Control tables/xmin are compared, including exact login-session cleanup and
preserved prior commands. The first fixture's expired-session expectation failed;
its original outcome remains. Fixture28a7db9 reuses unchanged3e10c2d product/build
inputs. [Thirteen fault schedules](../implementation/evidence/p03-acquisition-jobs-faults-native-20260928.json)
now cover both lost-COMMIT outcomes, held cancellation with close/release, owner
expiry, delay and both policy/grant/enrollment lock orders. Historical authenticated
retry survives withdrawal while absent intent refuses. Exact writer changes and
all67 tables are independently checked. The original9888ad5 run retains a fixture
identity-collision FAIL after ten completed cases; three corrected3e03ab8 retries
complete the bounded wave with unchanged artifacts. Session/credential races,
remaining raw relational negatives, populated prior-job upgrade, claim/dispatch,
installed commands and signed socket journey remain unqualified. No installed
acquisition capability follows.

## Initial exact-ID claim (implementation in progress)

`claim_acquisition` resolves one durable JobId in the actual Control dataset.
It advances accepted revision1 to queued revision2 with checkpoint0 and the
current owner tuple. It reads actor, scope, accepted time and duration from that
immutable job; a caller cannot supply a replacement actor or extend its deadline.
A same-owner queued revision2 retry rechecks authority and writes nothing.

Additive0023 preserves the applied catalog and all existing rows. Its preflight
refuses advanced or inconsistent qualified acquisition history with an explicit
recovery diagnostic; it never adopts a previously owned job. Once populated,
the acquisition owner tuple is immutable across every state, including paused.
Future successor support must provide native custody exclusion before explicitly
extending this protocol. A fresh Control epoch alone cannot establish that fact.

The claim locks Control, dataset, account, exact roles.control grant, enrollment
and job in that order. It requires a current active account and grant, enabled
source, both accepted v2 Sync policies and the job's original configuration and
policy revisions at the current head. This initial conservative profile refuses
all revision changes; later reauthorization requires an explicit design. The
scope epoch must still match. No browser session or historical credential revision
is required after acceptance. Time is sampled after waits and checked again by
the deferred constraint, against the original job deadline and actual owner lease.
Lost COMMIT remains unknown; cancellation poisons the adapter until actual driver
custody ends. No receipt alone authorizes retrying native effects.

`read_acquisition_job` returns bounded historical metadata under an actual Control
session, including after job expiry, permission withdrawal or owner replacement.
It supplies no execution capability. External status still needs independent
reader authentication/authorization. Shared job states now live in the management
domain; the existing ExportState name remains an API alias for its callers.
Dispatch, source sessions, reservation and certification must each check the job
again. Native recovery, those integrations and installed Sync remain incomplete.

[Native claim evidence](../implementation/evidence/p03-acquisition-claims-native-20260928.json)
now qualifies the initial queued operation at e7d1c8c, including populated upgrade,
logout, withdrawals, expiry and foreign owner refusal. Raw SQL plus a paused-only
negative clone discriminate immutable ownership; incompatible history refuses
without partial migration. [Sixteen claim-fault schedules](../implementation/evidence/p03-acquisition-claim-faults-native-20260928.json)
now distinguish actual COMMIT loss/cancellation/expiry and both policy/grant/source/
account lock orders. The original deadline and immutable job attribution survive
fresh-owner observation. No queued job is adopted. Job-bound socket/reservation/
certification, native successor recovery and installed dispatch remain required.

## Job-bound source and event effects (implementation in progress)

Additive0024 retains the finite JobId on each new relay reservation, with a
composite foreign key to its exact dataset/scope. Native input, receipt and fact
formats do not change. Existing NULL-job reservations remain immutable historical
inputs; no migration or retry adopts them. A populated upgrade adds a nullable
plain bytea and constraints, without changing earlier checksums. Operators must
upgrade before using the new job-bound consumer. Older source writers fail closed
on new relay reservations after24; explicit unbound bootstrap/import still work.

At24, every fresh Network admission and certification requires source, loaded
scope and job, including invalid signatures and requests omitting all source
fields. `AcquisitionEventContext::for_job` retains the original job alongside the
actual collection/request predicate. The jobless constructor supports only earlier
qualified foundations. Both reservation and dispatch compare stored job identity,
including omission, before any native effect. A previously source-bound NULL-job
request cannot authorize fresh dispatch; resolve a possible original outcome
through the unchanged historical recovery protocol.

Socket operations carry the same JobId/scope pair. Each socket, reservation,
dispatch and CertifyExisting rechecks current account/action, exact same-owner
queued/running job, scope/source, original duration and current policies. Queued
eligibility is not a capability and does not claim the unfinished running/progress
lifecycle. Accepted, paused, cancelling, terminal or foreign-owner work refuses.
Both complete accepted documents, committed visibility, route/bans and actual
source observation remain separate required checks.

The job helper takes Control, dataset, actor/grant, enrollment and job locks
before Ingest/producer. The new INSERT trigger precedes the existing source
trigger; deferred INSERT validation checks the original deadline again at ordinary
COMMIT. As with earlier guards, forcing constraints early does not establish a
commit-time guarantee. Returned PostgreSQL remaining time caps the socket/native
lease whose local ticket began before SQL. Waiting, processing and cleanup cannot
reset that duration. Historical positive-outcome lookup/copy does not call this
fresh-authority helper, even after job expiry or revocation.

The initial practical wave is being prepared; no actual0024 engine result is yet
claimed. It covers populated upgrade/reapply, signed admission/retry/certification,
job substitution/omission, origin omission including forged signatures, unclaimed
jobs, wrong scope, expiry, withdrawal, old schema and post-reservation refusal.
Job-bound socket expiry/cleanup, held COMMIT, lock races, historical recovery and
installed integration still require their actual practical evidence.
