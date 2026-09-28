# Operational relay endpoint registration

P02-T01 Step 6 begins with one private immutable identity relation. Enrollment,
source access contexts, route policy and finite network grants are subsequent
consumers; registration alone permits none of them. This boundary has scoped
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
