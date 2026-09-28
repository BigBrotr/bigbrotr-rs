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
