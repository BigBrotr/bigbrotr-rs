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
Its prepared commands are not execution evidence; revision-bound outcomes belong
in implementation evidence. Product Control bootstrap, durable policy projection,
native activation, successor/recovery registration and first signed-event
admission remain separate work.
