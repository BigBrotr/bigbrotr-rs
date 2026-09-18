# Initial-owner signed event admission

This P02-T02 slice connects verified owned events to the existing native owner
and dedicated Control backend. It is not an installed Ingest runtime, network
service, complete retirement protocol or release capability. The first archive,
read/export and successor recovery journeys remain required.

`PreparedEvent::new` consumes the exact `EventIntent` and its data/item grant.
It counts retained buffer capacities, including empty tag vectors, before
hashing the bound request. Input storage is capped at 16 MiB. The existing
protocol verifier checks the actual supplied ID and BIP340 signature, without
reparsing or copying the event. Verification can run before entering the
serialized native owner. A failed signature is never a known-ID shortcut.
The original event buffers move into the new payload when needed; duplicate
payloads are compared by all authenticated fields, not only a digest.

The trusted factory supplies the current maximum event size. The connected policy
validator must compare `verification_limit_bytes()` with current configuration,
validate both complete documents against static roots, and authorize the exact
source, scope, origin, lineage and provenance choice. `PreparedEvent` exposes the
verified event and attribution for this check. It grants no policy authority.
The isolated client uses an explicit frozen import fixture grant; installed
source-policy evaluation remains separate work.

`ControlSession::admit_initial_event` checks the configured native root, complete
initial registration/manifest, matching current policy, actual Control fence and
open producer epoch. It bounds the requested outstanding window by the registered
limit. It shares the existing initial owner coordination code with dictionary
bootstrap, retaining registry row locks through the native call and checking the
suspend-aware lease before dispatch and delivery. A scheduling lease alone is
not exclusion. Run on a retained bounded native worker, and join it and its PG
driver through close. Cleanup/output failure after dispatch is DeliveryUnknown.

## Scientific transaction

The native owner serializes final inventory inspection and one synchronized,
WAL-enabled batch. Owner and producer epochs are currently restricted to one;
new requests must be the next contiguous producer sequence. At most 64 requests
may remain outstanding (a deployment can select a smaller window). The local
producer state from bootstrap is unchanged in format. ACK, reclamation, producer
replacement and successor recovery are not implemented; reaching the window
returns WindowFull and never discards detail to make progress.

A first event writes its event dictionary, complete shared payload, first scoped
eligibility and, for a network observation, scoped receive and optional witness.
A later authorized scope gets its own actual supplied signature and eligibility
time. Same-scope later observations retain the original representative. Strictly
earlier receives/witnesses append corrections and replace only their compact
selected-fact indexes. Import adds no local receive; subsequent network intake
keeps the import representative and adds actual local observations. Provenance
off allocates no endpoint/access/method mapping or witness inventory.

Each new source identity, event ordinal, dictionary mapping/reverse candidate,
source/allocation counter, fact, pending marker, producer state and request detail
is included in that single batch. Event mappings cannot precede their verified
payload. Existing descriptors and values are compared completely. The source
counter requires its actual last retained fact; an occupied next fact/ordinal or
detected inconsistent payload/dictionary/scope index is refused before commit.
These bounded checks are not a full corruption scrub; coordinated deletion of
all references to a scoped history cannot be distinguished from its absence.

NewFacts and AlreadyDurable use the unchanged [receipt format](ingest-receipt-format.md).
Their private dependency sets include the sufficient body, selected scoped
facts, required input dictionaries, complete dictionary closure and all new batch
members. A correction also retains the prior selected fact it replaces. Existing
immutable correction history remains retained; each duplicate does not traverse
or copy an unbounded history into its receipt. Backup/source-cut closure must
retain that history independently.

Strict replay compares the exact request digest and returns the original receipt,
context, dependencies and batch. It verifies each retained dependency's full fact
bytes and the complete pending marker. It does not re-reduce today's minima or
replace the original owner time. Missing detail at/below issued is corruption,
not a new request. Missing dependencies fail closed. This first adapter requires
its source facts still in native storage; archive-backed resolution and pending
reclamation are not enabled.

## New persistent values

Existing key, fact, receipt, dictionary, counter and pending formats are unchanged.
The event payload, eligibility, receive and provenance index values are each:

```text
01 <matching key-domain byte> || SourceRecordId (36 fixed bytes) || fact digest (32 bytes)
```

The indexed fact supplies all typed metadata and minima. Reads bind its full source
ID, digest, kind, event ID, scope and logical witness key before use. These are
compact references, not duplicate signed bodies or global signature selection.

An invalid/excluded accepted request retains only operational rejection detail
under its existing Request key. Its value is:

```text
ASCII("bigbrotr/rejection/1") || 00 ||
producer_slot_id:bytes16 || producer_epoch:BE64 || request_sequence:BE64 ||
request_digest:bytes32 || dataset_id:bytes16 || policy_epoch:BE64 || reason:u8
```

The suffix after the literal prefix is exactly 89 bytes. Closed reason codes are
1 empty tag, 2 mismatched event ID, 3 invalid public key, 4 invalid signature, and
5 excluded by the configured normalized event size. Unknown versions/codes and
wrong length/key/dataset are errors. It retains the original policy epoch; replay
does not reinterpret that stored decision. Changed intent conflicts even when a
valid replacement would otherwise succeed. No hostile event body, source fact,
sequence or pending batch is stored for a rejection. This value is not a
scientific receipt or an event-progress basis. Old receipt-only inspection fails
closed on this distinct prefix; it cannot mistake it for absence or success.

## Bounds, errors and evidence

Batch bytes are positive and at most 4 MiB; each encoded fact is at most 2 MiB.
Dependency sets are bounded (8–256 entries, default 64). Point reads have both an
aggregate byte allowance (eight batch limits) and an operation count allowance.
The reverse hash bucket is scanned only within those bounds and always compared
by full descriptor bytes. No unbounded whole-store startup scan is added.
These local checks do not replace eventual disk/backlog admission or aged-state
capacity qualification.

The input grant stays with the prepared value. A separate transferred operation
grant covers staged bytes, fallible vector reservations, decoded lookup state,
returned receipt and the native WriteBatch. Its allocation floor is eight batch
limits plus entry/dependency/typed bookkeeping; native allowance is two batch
limits plus 1024 bytes. Decode allocation/tag limits also cover the admitted typed
input. The returned outcome retains its operation grant. These are requested
allocation bounds, not measured process RSS guarantees.

The shared batch builder checks serialized bounds before native insertion, checks
authority immediately before dispatch, poisons the handle before FFI and clears
that state only on successful synchronized completion. Native write failure is
CommitUnknown. None of cancellation, I/O failure, a missing store or an expired
lease proves rollback.

[Mac checks](../implementation/evidence/p02-event-admission-mac-20260918.json)
exercise the actual engine with the independently signed
[fifteen-operation fixture](../../tests/fixtures/ingest/README.md), exact reopen
replay, rejection detail, missing receipt/dependency, inconsistent inventory,
last-check cancellation and a full outstanding window. The counter-corruption
test first reproduced an accepted inconsistent counter and then passed after the
writer verified its retained last fact.

[Native Linux amd64 evidence](../implementation/evidence/p02-event-admission-native-20260918.json)
qualifies candidate `12f936d`: eleven actual writer tests, seven connected signed
import cases and eight bootstrap regression cases. Exact Git/source/tool/ELF
identities, all native bytes and Control relationships were independently audited.
Before-dispatch SIGKILL retains bootstrap only; after-commit/before-delivery
SIGKILL retains the complete event batch. These are process crashes around
dispatch, not power loss or faults inside FFI. Complete retirement, concurrent
closing races, ENOSPC, archive-backed recovery, aged-state capacity and installed
release behavior remain required.
