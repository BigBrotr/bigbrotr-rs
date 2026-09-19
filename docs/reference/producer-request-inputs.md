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
