# Operational relay endpoint registration

P02-T01 Step 6 begins with one private immutable identity relation. Enrollment,
source access contexts, route policy and finite network grants are subsequent
consumers; registration alone permits none of them. The first
[no-auth enrollment consumer](relay-enrollment.md) adds0019 without changing0003. This boundary has scoped
native qualification, not an installed acquisition feature. The
[scoped native report](../implementation/evidence/p02-relay-registry-native-20260928.json)
records exact identity, capacity, attribution, expired insertion, SQL guards and
rollback evidence. The [fault report](../implementation/evidence/p02-relay-registry-faults-native-20260928.json)
adds actual lost/held COMMIT, cancellation, current-policy lock waits, fresh-owner
attribution and corrupt-bucket refusals. Load and installed acquisition remain open.

`EndpointId` is a nonzero 16-byte operational identity scoped to the local dataset.
It is not an archival ordinal or `QualifiedRef`. Control's typed get-or-create
operation accepts the single protocol `Endpoint` normalizer's output, preserving
scheme, effective port, path, empty query and query ordering. It stores canonical
UTF-8 bytes in `control.relay_endpoints`; URLs remain private operational data.
No per-event provenance mapping or endpoint edge is created in RocksDB.

Migration 0003 appends after 0816, preserving every old SQL checksum and row. The
relation has an ID primary key, a dataset-qualified ID key for later references,
and a nonunique `(dataset,normalization_version,candidate_digest)` index. Full
URL byte equality determines identity. A generated SHA256 candidate reduces the
search but never proves equality. Bytea avoids collation-dependent equality and
an 8192-byte URL need not fit a B-tree key; PostgreSQL's
[index entry limit](https://www.postgresql.org/docs/16/btree-intro.html) excludes
using the full allowed URL as an ordinary unique B-tree key.

The candidate helper accepts bytes and is immutable. In particular, there is no
false immutable promise around text encoding conversion. The BEFORE trigger
computes the candidate itself because PostgreSQL computes stored
[generated columns afterward](https://www.postgresql.org/docs/16/ddl-generated-columns.html).
Canonicality belongs to the Rust normalizer, just as closed policy semantics
belong to the accepted-policy adapter; SQL enforces exact retained bytes, bounds
and relational authority rather than implementing a competing URL normalizer.

Control retains its actual backend fence, locks the dataset in a separate READ
COMMITTED statement, checks current heads against its fully validated policy,
then probes at most 65 candidates. At most 64 entries may share a candidate. Full
byte comparison occurs in PostgreSQL and only scalar receipt fields return.
An existing URL remains readable at 64; a new one returns `Capacity`. An observed
65-row bucket is inconsistent state and returns `InvalidMetadata`, never absence.
The same locks and fresh volatile-trigger snapshots enforce duplicate refusal
on direct insertion. Row-count bounds do not establish a physical I/O bound;
statement/lock and caller deadlines still apply.

Only an absent identity needs random bytes. A random primary-key collision refuses
the transaction; no broad conflict suppression or unbounded retry occurs. The
row retains original time, config/policy revisions and Control attribution. Those
historical values need not equal the current issuer on a later lookup. Updates,
deletes and truncation refuse. The ordinary role receives SELECT/INSERT and the
three invoker functions, alongside its existing Control prerequisites; no DDL or
mutation privileges are needed on this table.

Insertion uses synchronized COMMIT with the existing deferred owner/policy check.
The adapter does not force constraints early. A received explicit `BB001` expiry
is a known rejected COMMIT; a lost response remains `CommitUnknown`. Any failed or
cancelled operation poisons the session and the transport owner retains its driver
until joined. Fresh ownership plus the same complete canonical URL resolves the
identity. This is pure get-or-create: it requires no separate command key or
mutable command history. `created` describes this transaction only and cannot
reconstruct whether an interrupted earlier caller was the original creator.

An existing-row lookup rolls back its read transaction and has no insertion
expiry trigger. It may return the retained identity after the scheduling lease
expires while the actual backend fence and validated policy heads still match.
This grants no network action: scheduling expiry is distinct from positive owner
exclusion, and `AuthorityExpired` describes the new insertion's commit check.

The [actual-engine recipe](../../experiments/relay-registry/README.md) separates
proposed scenarios from executed evidence. Remaining enrollment/route and network
action authorization preserve their selected task scope; this table cannot stand
in for those grants, public reader visibility or archival dictionary allocation.

## Authenticated registration consumer

The initial private owner exposes `POST /management/v1/relay-endpoints` with
closed JSON `url`, `expected_config_revision` and `expected_policy_revision`.
It requires the existing current session, explicit CSRF, exact Origin and one
canonical Idempotency-Key. Fresh acceptance requires dataset `policy.change` and
both expected heads. This records an immutable identity only; enrollment, roots,
visibility, bans and actual network authorization keep their separate checks.
No DNS, socket, archive dictionary or enrollment work occurs during registration.

Reuse the existing normalizer and exact candidate bucket lookup inside the same
Control/dataset/account/session transaction. A browser precheck followed by the
unauthenticated registry method would lose that transaction boundary; duplicating
the registry would risk divergent normalization/collision behavior. Extract only
the existing transaction-local identity operation for both concrete callers.

The internal get-or-create contract above keeps its original URL-based retry.
The browser consumer adds the existing actor/key management receipt and audit,
plus one immutable registration detail linking the endpoint and accepting
session/account revisions/Control identity. This record supplies attributable
historical retry and deferred acceptance checks; it is not a second endpoint
registry. Append migration0025 after0024, without altering earlier SQL/checksums,
endpoint IDs, existing history or codec. Existing rows are not retrospectively
assigned browser actors. Runtime never migrates implicitly.

The new command digest is SHA256 of `bigbrotr/relay-registration/1\0`, then
bytes16 dataset, authenticated actor and command key, two big-endian uint64
expected config/policy revisions, big-endian uint16 normalization version1,
big-endian uint32 normalized URL byte length and those exact UTF-8 bytes. Text
normalization equivalent under endpoint v1 yields the same command digest;
query bytes/order and explicitly empty query remain significant. IDs, revisions
and URL bytes are independent of JSON formatting. The URL is at most8192bytes;
the HTTP body is at most17408bytes including its closed envelope. Endpoint input
validation does not grant routing or authority.

A fresh accepted command returns202 with `bigbrotr-relay-registration/1`, its
command_key, endpoint_id, decimal-string accepted_at_us, created=true and
`authority=identity-only`. `created` describes the command receipt, not whether
this command originally created the endpoint. Another key may accept an already
registered identity without changing its original registration attribution.
Historical retry authenticates the current actor/CSRF, resolves the exact original
key/digest before fresh grant/CAS checks, and returns200 with created=false. No
idle-session touch or cookies. Permission loss prevents new commands, while a
live actor can resolve the original acceptance. Lost COMMIT resolves with the
same key/body under fresh ownership; it must not invent another endpoint.

Acceptance retains the established lock order and actual Control fence through
synchronized COMMIT. Deferred checks require matching command/detail/audit, current
heads, live accepting account/session revisions and scheduling authority. Known
business refusal rolls back all provisional effects; ambiguous/cancelled backend
work poisons Control until its real driver retires. The startup prerequisite
probe checks required table and directly called function privileges before bind.

P02-T01 Step6/P01-T09 Steps1/2 have [bounded installed evidence](../implementation/evidence/p02-initial-registration-http-native-20261001.json)
on product7011acd/fixture45f7593: explicit populated upgrade/reapply, normalized
create/retry, existing identity under another key, denied session/CSRF/permission/
CAS, maximum and malformed URLs, missing grants/schema before bind, and URL→enroll→
withdraw→historical receipts. Complete Control/native originals and the populated
upgrade source remain unchanged where required. Schema0025 and its table/function
privileges are required before the selected initial administration listener binds.

[Seven actual adapter fault schedules](../implementation/evidence/p02-relay-registration-faults-native-20261001.json)
now qualify lost/held COMMIT, cancellation, owner/session expiry for an existing
endpoint and exact fresh-owner recovery. All68 Control tables and original
attribution were checked. The held actual owner rejects a competing acquisition.
[Eleven authority race schedules](../implementation/evidence/p02-relay-registration-races-native-20261001.json)
qualify both lock orders, writer rollback and exact historical retry after changed
heads/grants under a fresh adapter owner. The initial installed owner still stops
on changed policy. Raw relational negatives and installed HTTP ambiguity remain
NOT_RUN. These small fixtures do not establish installed acquisition, capacity or M1.
