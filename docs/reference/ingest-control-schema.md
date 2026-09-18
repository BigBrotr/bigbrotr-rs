# Ingest control schema

Migration `0002_ingest.sql` adds the first PostgreSQL coordination records for
ingest, archive projection and source-range retention. PostgreSQL is the authority
for registration and the ordered state transitions described here. It is not an
admission store and it does not certify external-engine persistence.

## Registered authority

`allocation_domains` binds one dataset/kind/domain to one exact Ingest owner
tuple. The domain can move only from active to closed. Its ordinal high watermark,
dictionary mappings and current-domain marker remain in the synchronized RocksDB
manifest and key space. Closing the PostgreSQL row cannot discard old mappings or
authorize ordinal reuse. At most one domain per dataset, dictionary kind and
allocating resource is active. Separately fenced resources may allocate in
disjoint registered domains; closed domains remain registered and readable.

`source_streams` binds `(dataset_id, shard_id, owner_epoch)` to the exact Ingest
owner tuple used by the topology. A stream moves `open -> closing -> closed` and
records its final complete sequence and closure digest. The final sequence may be
zero: zero means that this registered stream committed no facts, rather than
missing, unknown or a sentinel. A closed bound cannot be lower than an already
published consumer cut, committed projection cut or recorded job input cut.
Migration0008 closes the job/closure race in both directions, including old
topologies with NULL handoffs; its [migration contract](../operations/schema-upgrades.md)
specifies READ COMMITTED, lock ordering and incompatible-history refusal. Each shard has at most one
unclosed stream; a successor uses the exact next epoch only after its predecessor
closed. Live admission sequences and pending facts remain RocksDB-owned.

`producer_slots` are permanent stable identities in this schema version. They
cannot be updated, deleted, reused or created implicitly when an Admit request
names a missing slot. `producer_epochs` start at one, advance without a gap only
after explicit predecessor retirement, bind the producer process incarnation and
Ingest owner, and require an owner-side closure digest. Expiring an owner lease
does not retire an epoch or fence its process.

The contract requires a registry-generation fence before a future slot-removal
feature can exist, but it does not yet define that generation's head, transitions
or portable encoding. This migration therefore does not invent a registry number.
A later real removal consumer must add the authority and version the PostgreSQL
and RocksDB manifest formats together.

`producer_checkpoints` store the producer's durable contiguous checkpoint with a
strict CAS revision. `complete_through_request = 0` is an explicit empty prefix.
The Ingest owner's ACK and receipt-GC watermark remains in the synchronized
RocksDB batch. No PostgreSQL checkpoint row, including a higher counter, can
manufacture that owner ACK or resolve an ambiguous Admit.

## Complete cuts and projection attestations

A consumer has an immutable sequence of `consumer_frontier_revisions`. Each
revision contains one `consumer_frontiers` bound for every stream in one sealed
topology. A zero bound explicitly means that the registered stream has no
committed input and therefore carries no fabricated receipt. Every positive bound
carries an archive-batch or projection-chunk attestation digest;
the application must resolve the named receipt and verify that it covers the
exact tuple before sealing. The database rejects omitted streams, numeric
regression and unsafe topology handoff. Publishing `consumers.current_revision`
is a one-revision CAS performed by the exact live Archive or Index owner. A
draining owner may publish a revision it sealed while active, but cannot assemble
or seal another revision.

`projection_generations` fixes the dataset, serving storage incarnation, table
identity and Index owner namespace. It is a registry identity, not an active or
complete serving-generation pointer. `projection_chunks` uses the deterministic
profile-1 chunk digest as `chunk_id`, preserves the versioned target text,
`u64` chunk ordinal and row count, and normalizes the complete source cut through
`projection_chunk_bounds`. Target text is deliberately versioned and open under a
bounded ASCII spelling; the schema does not invent a closed target-family list.
A zero-row committed chunk is valid. Its engine receipt is an attestation that the
connected owner must resolve. It does not prove ClickHouse bytes survived an
unclean storage loss, and this migration publishes no serving generation.

## Source-range leases and claims

The first concrete `snapshot_leases` variant pins one sealed consumer source cut.
It has a job or role-instance holder, a monotone lease revision, an explicit
update time and finite expiry. Renewal must happen before the prior expiry and
extend it. Release is explicit. Lease expiry affects retention coordination only;
it does not fence a writer, extend policy authorization or release another row.

`retention_claims` protect a nonempty inclusive source range contained by that
leased cut. A claim remains active after its lease expires or is released until
the claim itself is explicitly released. Reclamation code must lock and test all
applicable active claims at its actual delete boundary. Absence of a row, elapsed
wall time or the latest catalog membership alone is not delete authority.

Artifact/file and read-cursor references do not yet have owning relations. This
migration intentionally omits generic blob/JSON references for them. Their real
consumer migrations add FK-backed typed variants; the current source-range form
does not claim all P02-T07 or P06 retention work is complete.

## Mutation boundary

All owner-controlled mutations use the full
`(dataset_id, resource_id, epoch, role, process_incarnation)` tuple. Inserts and
new work require its active state. Defined draining transitions may finish
closure and publish already sealed work, but a released tuple cannot mutate state. No trigger treats
`lease_expires_at_us` as positive old-process exclusion.
The owner row itself cannot become released while one of its allocation domains,
source streams or producer epochs remains unresolved; an Index owner likewise
must resolve every prepared chunk first. The application drains those rows before
recording the independently established exclusion on `owner_epochs`. Draining
forbids new projection chunks but permits an already prepared chunk to become
committed or invalid so the owner cannot deadlock its own release. Archive and
Index owners must also publish any next sealed consumer frontier they created;
an unpublished revision cannot be stranded behind a released owner tuple.

The connected application is responsible for four checks that PostgreSQL cannot
derive from an opaque digest:

1. Verify allocation and source closure receipts against the synchronized RocksDB
   owner state.
2. Verify every frontier attestation covers its exact source tuple before sealing
   and head publication.
3. Verify a projection chunk digest from the deterministic encoding and resolve
   its engine receipt under the recorded storage incarnation/table identity.
4. Lock claims again at physical reclamation and apply current policy, engine and
   storage-protection checks.

The independent fixture
`experiments/control-migrations/ingest_schema_cases.sql` exercises positive
zero/maximum-shape semantics, invalid transitions, deferred cut completeness and
whole-transaction rollback. It must run after the real migration command applies
both migrations to a fresh PostgreSQL 16.15 database, with independently computed
source checksums supplied as `expected_migration_1_sha256` and
`expected_migration_2_sha256`. A local SQL grammar parse is only a source check;
it is not PostgreSQL execution evidence.

Migration 0001 could create topology membership before a source registry existed.
Migration 0002 refuses with SQLSTATE `55000` if any such rows already exist: their
owner and closure state cannot be inferred safely. The current unshipped upgrade
path requires explicit review/reconstruction rather than installing prospective
triggers over silently incompatible rows. Fresh databases and databases without
topology members can apply 0002 normally.
