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
unchanged; its retry writes nothing. Concurrent policy changes and historical
recovery with this binding still require their own evidence.
The first finite socket consumer can use the registered full selector. Generated
page/need-list resumption still requires retaining that execution's exact request
context; the scope ID does not reconstruct a page window. Both complete accepted
documents must additionally authorize event limits, provenance, ephemeral/history
policy, endpoint/bans/route and current deployment roots. Installed consumers may
not substitute the fixture validator or legacy unbound path for those checks.
