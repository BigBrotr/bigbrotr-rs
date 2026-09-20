# Private pending batch handoff

Status: P02-T04 prerequisite in progress. `bigbrotr-protocol::archive_batch`
transfers one retained complete admission batch from the Rocks owner to a finite
native worker. `PendingBatch::write_archive_batch` retains the pending-data
permit while writing. Transfer and private preparation never acknowledge an
archive receipt, advance a source cursor or reclaim pending facts.

This private framing is not a portable archive/export manifest, an admission
request or a public endpoint. It reuses existing canonical fact preimages and the
`bigbrotr/admission-batch/1` semantic digest. It does not substitute that digest
for the separately defined multi-range archive-batch identity.

## Version 1 bytes

Integers below are unsigned big-endian binary values. The header is 80 bytes:

| Offset | Bytes | Meaning |
|---|---|---|
| 0 | 8 | Literal `42 42 41 50 00 00 00 01` |
| 8 | 16 | Nonzero dataset ID |
| 24 | 4 | Source shard |
| 28 | 8 | Nonzero owner epoch |
| 36 | 8 | Nonzero first sequence |
| 44 | 4 | Nonzero record count |
| 48 | 32 | Complete admission-batch digest |

Exactly `count` frames follow. Each is `payload_digest[32]`, `preimage_length:u32`,
then those exact canonical preimage bytes. Sequence `first + index` must fit u64
and match the decoded fact's complete source identity. No trailing bytes are
permitted. The existing closed decoder verifies canonical representation, version,
structural references and digest; it does not grant policy/allocation authority
or reverify signatures.

The encoder checks the supplied record count, lengths and hashes against the
contiguous membership before writing the header, borrowing the retained
preimages without copying or decoding them. The reader additionally verifies
each decoded source identity. A transport failure can still leave a partial
output. The reader checks header
count/overflow before reserving reference storage and frame size before allocating
or reading its payload. Separate bounds cover record count, aggregate preimage
bytes and existing per-fact decoding limits. Framing overhead is 80 + 36 * count
bytes. The application allocation floor is the bounded reference vector, reusable
maximum preimage buffer and one transient decoded fact/allocation budget; it is
not process RSS, an Arrow grant or native memory.

Any failed read poisons the reader. `next_fact` may yield a prefix for private
staging; only `finish` validates complete membership/digest and EOF. Callers must
drop each fact after staging to preserve the one-fact allocation bound. A missing,
altered, foreign, reordered, truncated or extra frame cannot complete. Resource
limits remain distinguishable from integrity and compatibility failures.
Cancellation is checked between bounded records; synchronous I/O itself can
block. The private application parent described below owns the pipes and positive
worker termination. Whole-role crash/recovery remains required; a dropped future
is not termination.

## Private native preparation

`PreparedBatch` consumes an isolated `Engine`. It creates five private scientific
staging tables plus an empty technical receipt table with explicit typed schemas
and feeds each decoded fact through the
[row adapter](archive-format.md), checking native append and flush results.
Only after complete frame validation does it return prepared input. Any failure
drops the engine and partial in-memory staging; job-owned spill cleanup still
belongs to the positively terminated parent. There is no DuckLake attachment,
permanent catalog mutation, publication receipt or source acknowledgement.

The initial path appends one fact at a time. This bounds retained application
input independently of the native aggregate staging but has per-fact appender
overhead. It is not a throughput-qualified batching strategy or a replacement for
P02-T04's file/group/timer/resource acceptance. A future publisher consumes the
staged families under one positively fenced catalog transaction; a diagnostic
count cannot establish full content or reference closure.

The private executable command is:

```text
bigbrotr-archive-worker inspect-batch BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS MAX_RECORDS MAX_ENCODED_BYTES
```

It reads one framed batch from stdin through EOF and emits one bounded diagnostic
result after successful preparation. Per-fact limits currently use
`FactDecodeLimits::default()`: 2 MiB encoded, 8 MiB requested allocation, 65,536
tag nodes and 256 KiB per retained string. Staged counts are ordered dictionary,
event, eligibility, receive, provenance. Source u64 wrappers retain their existing
decimal-string JSON representation. The result is explicitly
`bigbrotr-private-preparation/1`, never an archive receipt. No partial stdout
result is emitted for invalid input. The application parent now supervises this
command; full publisher admission, process-crash exclusion and catalog recovery
remain separate required work.

The independent [fixture author](../../crates/bigbrotr-protocol/tests/fixtures/archive/write_fixture.py)
uses literal canonical CBOR membership and standard SHA-256/struct encoding,
without the Rust codec. Tests retain all truncation positions, digest/source/
version/trailing-byte controls, exact limits and poisoned-reader behavior. The
native staging and executable tests use a synthetic unsigned-extreme model fact;
they do not claim new signed admission or publication evidence. The Rocks test
checks a real synchronized bootstrap/reopen/transfer with unchanged native
sequence. Full signed catalog/body/reference readback remains required.

The [native qualification at `0cdd80f`](../implementation/evidence/p02-pending-handoff-native-20260918.json)
records ten scoped Rust cases and two separately executed packaged-binary cases
on Tux Linux amd64. It checks every frozen-wire truncation, typed staging and real
Rocks reopen/transfer. Those adjacent boundaries were exercised separately. The
later [supervision qualification at `c365053`](../implementation/evidence/p02-private-supervision-native-20260918.json)
connects actual signed admission/reopen/pending to the exact packaged worker twice,
with unchanged full Rocks state and bounded failure controls. It does not qualify
publication or full archive content/reference readback.

## Supervised private inspection

`storage::archive::Preparation::inspect_pending` connects a retained Rocks batch
to the actual worker. It admits one complete native-memory/item grant plus a
128 KiB control grant without waiting while holding another scarce grant. The
pending owner retains its existing data credit. The configured engine memory and
explicit extra native reserve fit the admitted total; the extra reserve must
cover decoding, Arrow, extension startup/threads, stacks and allocator cost.
Validation checks the decoder floor, not a measured process RSS ceiling. CPU/PID
composition, filesystem reservations and maximum-unit memory qualification still
gate the deployed worker role. The caller owns its temporary-disk reservation.

Before exec, the parent hashes the expected executable and the compiled target's
library/extensions. The expected executable identity must come from an
authenticated release or a trusted exact-source build; the API never reads its
expected hash from an untrusted adjacent file. Runtime release authentication is
still a P01/P08 gate. The installation must remain immutable to the runtime
identity; hashing cannot exclude a concurrent privileged filesystem writer.
Canonical absolute paths reject symlink substitution. The job root is separate,
empty, owned by the effective user and inaccessible to group/others.

The child receives an absolute executable path and a cleared environment, with
the verified library search path, fixed locale and `BIGBROTR_ARCHIVE_PARENT_PID`
set to the spawning process's positive PID. Before opening the engine, the
worker refuses missing, malformed or foreign parent identity. On Linux it sets
`PR_SET_PDEATHSIG` to SIGKILL and checks parentage again, closing the race where
the parent dies before signal registration. The synchronous spawning thread must
retain ownership through reap: Linux associates this signal with that thread,
not the lifetime of its entire thread group. The worker must not fork, exec or
change credentials after registration. Mac development checks initial parentage
only and does not claim this Linux termination guarantee.

The parent sets its three pipe ends nonblocking, drains at most 4 KiB from each
output, and checks a shared
cancellation flag/deadline between bounded I/O operations. Output floods fail;
complete-looking stdout followed by a failed exit or stderr is not success.
The parent checks returned membership/digest/counts against its retained batch.
An input limit remains a resource refusal, even when the child subsequently
rejects the incomplete input. Diagnostics are private preparation results only;
counts are not a full content-read oracle or durable publication receipt.

After those checks, the parent also returns the schema-1 semantic archive input
digest for the explicitly supplied topology generation. It borrows the retained
complete admission membership, without copying references or fact bodies. This
uses the separate `bigbrotr/archive-batch/1` domain and maximal source ranges; it
does not reuse the transport/admission digest. The caller must resolve the actual
retained topology and complete admission authority before future publication;
the Rocks creation manifest is not an implicit current-topology source. The
private child framing/result remain version 1 and contain no topology authority.
This input digest is not a stage-content readback, commit or protection receipt.

On error/drop, the parent sends SIGKILL and polls for reaping for 300 ms. Native
and control grants remain owned until exit is observed. If the OS cannot establish
exit, `TerminationUnknown` takes precedence: preserve the job root and exclude
old activity before retry. Dropping an unresolved handle permanently charges its
capacity in that parent process, preventing timeout from freeing possibly live
native work. This is fail-closed accounting, not a claim that every kernel task
is killable on a wall-clock deadline. File verification and process creation use
trusted local filesystem/OS calls, not an interruptible filesystem sandbox.

This transport is restricted to the trusted worker, which must not fork child
processes. [Linux amd64 qualification](../implementation/evidence/p02-parent-binding-native-20260918.json)
at `e4e2a30` verifies positive exit and reap after killing the actual parent of an
initialized SIGSTOPped packaged worker (2.99 ms in this case). The retained
old-worker negative control stops the initialized worker before killing its parent; that worker survives
until the harness separately kills and reaps it. A signal does not prove positive
exit or allow immediate cleanup/retry. Host loss, persisted job recovery and
remote PG backend termination still require their own exclusion. In particular,
child process exit alone cannot resolve a catalog commit. The worker has no catalog
credentials in this inspection path and leaves pending untouched. No P02 task or
M1 gate is completed by this slice.


## Finite supervised catalog bootstrap

`Preparation::bootstrap_catalog` uses the same verified installation, empty private
temporary root, whole-job grants and nonblocking supervised process. It streams a
closed `{dsn,data_root}` JSON value directly onto private stdin, without a retained
plaintext JSON buffer. Counting the exact escaped representation before spawn
rejects more than32KiB; individual DSN/root limits are16KiB/4KiB with no NUL/CR/LF
and a canonical lexical absolute local root without native glob characters `*`,
`?`, `[`. This boundary performs no network
policy discovery: the operator caller must already validate the trusted endpoint,
TLS identity and credential authority. Credentials never enter argv/environment.

The child uses the existing finite `bootstrap-catalog` command. Only its exact
`bigbrotr-native-bootstrap/1` initialized response plus successful exit/EOF permits
success. Direct closed-object decoding rejects duplicate/unknown fields and
trailing JSON; the same rule applies to `bigbrotr-native-initialize/1`.
Initial creation and attach to an existing native catalog can both
succeed; neither is proof of freshness, guard installation, an archive receipt or
launch authorization. The privileged installer independently inspects the resulting
catalog after the child is joined. Cancellation/unknown completion must preserve
the prior credential/transaction recovery obligations. The [credential report](../implementation/evidence/p02-catalog-credentials-native-20260918.json)
at `d2b2609` qualifies actual PG/native bootstrap and fresh-login attach through
this parent, with positive reap and permit return. The 16 native Rust cases also
exercise the existing signed Rocks-to-private-worker path after the shared launch
refactor. No scientific publisher is advertised from a successful bootstrap.

## Private scientific publication candidate

`Preparation::publish_pending` consumes one fresh guarded credential and one
complete retained admission batch. It verifies the scientific catalog identity,
explicit topology and schema1 archive digest before spawning the finite worker.
The selected route is a pinned IP plus validated local-plaintext boundary or
verify-full server name/CA; the libpq DSN goes only through the private input pipe.
The shared publisher/receipt DSN requires a completed SCRAM-SHA-256 exchange and
disables ambient client certificates and GSS transport. All database/login/password/
application-name strings are bounded before DSN allocation. The raw operator
bootstrap DSN remains a separately validated caller responsibility. This route
also accepts the shared sealed-descriptor client identity prepared from protected
configuration. The [native TLS/mTLS qualification](../implementation/evidence/p02-native-mtls-20260919.json)
at `50d9496` covers installed initialization and receipt observation; signed mTLS
publication remains required before advertising that consumer.
The [native authentication report](../implementation/evidence/p02-native-catalog-auth-20260919.json)
qualifies actual receipt reads under SCRAM, trust, incomplete exchanges and
verify-full TLS, with old-client negative controls. It does not qualify mutual
TLS or the installed initializer.
This private candidate is not yet a qualified Archive role or recovery path.

The private `publish-batch` command takes the same limits as `inspect-batch`.
Its stdin begins with a big-endian u32 JSON length (1..32768), then closed JSON
containing `catalog:{dsn,data_root}`, `topology_generation`, `batch_digest`, a
nonzero 16-byte `attempt_id` array and positive `publication_ordinal`; exactly one
existing admission frame follows. Strong topology/digest wrappers preserve their
existing JSON representation. Full framing and semantic identity verification
precede catalog attachment. No partial input grants publication authority.

The worker stages the technical receipt, then checks existing receipt/source
membership and dictionary closure. It COPYs each populated family and receipt to
caller-owned Parquet below an exclusive attempt directory and registers all files
in one native transaction. The [archive contract](../specification/archive-storage-contract.md#actual-publication-and-protection-boundaries)
owns placement, cleanup and ambiguity guarantees. Current preparation is one
bounded admission/one file per populated table; 256MiB file formation, multi-batch
membership, timer behavior, disk accounting and aged costs remain qualification
work. This candidate does not claim those settings are implemented.

A successful child emits exactly `bigbrotr-native-publication/1` and its
`batch_digest`. The parent also requires successful exit, bounded output, EOF and
positive reap. Any launched failure or malformed response is outcome-unknown;
unknown termination takes precedence and retains capacity/cleanup obligations.
Even success does not authorize protection, a source ACK, retry or reclamation.
The caller must retain the durable attempt and pending source through fresh
receipt/file resolution and positive exclusion. The
[signed native qualification](../implementation/evidence/p02-signed-publication-native-20260919.json)
at `2075a39` exercises this actual parent against guarded PG after connected signed
Rocks admission/reopen. All five current families and exact receipt survive fresh
full readback, including a completed native COMMIT whose response was lost. A
pre-forward disconnect leaves only bootstrap visible and retains the event's
prepared files. Both fault outcomes stay unknown at the parent boundary; the lab
reader separately observes the committed state. No source ACK occurs and all 41
Rocks key/value records remain identical. The literal corpus includes one public
network arrival with provenance, invalid ID/signature, missing closure and duplicate
publication controls. [Later signed native fencing](../implementation/evidence/p02-signed-fencing-native-20260919.json)
at `b12c902` qualifies three actual publisher/activation races: owner-first and
old-native-transaction-first with COMMIT/ROLLBACK. Direct PG blocker chains and
exact40001→55000 attribution discriminate the old writer refusal. Positive reap,
historical sealing and separate full readback precede successor launch; committed
work refuses duplicate replay and absent work publishes once. All pending and
orphan files remain retained. Full schema variants, installed source policy,
product receipt/read services and persistent lifecycle recovery remain P02-T04
acceptance; no public command advertises this candidate.

The [installed manager consumer](../implementation/evidence/p02-installed-publication-native-20260919.json)
at `61271e2` runs the actual initializer followed by these six signed schedules as
the configured non-superuser CatalogManager, with PUBLIC CONNECT/USAGE revoked.
It qualifies explicit manager delegation, writer access, two isolated refusal
controls and the same full-body/receipt/fencing oracles. All 22 generated writer
lifetimes finish sealed; 41 Rocks rows remain unchanged per journey. This closes
the fixture's earlier implicit PUBLIC/privileged-manager dependency, while source
policy, persistent service dispatch and M1 still require their real consumers.

## Fresh receipt observation candidate

`Preparation::inspect_receipt` consumes one bounded job and the complete retained
schema1 `ReceiptExpectation`: dataset, schema, topology, semantic batch digest,
record count, maximal input ranges, original attempt ID and publication ordinal.
It observes that original identity, never a newly invented retry attempt. The
operator supplies a separate SELECT-only catalog login and validated pinned route.
No credential enters argv/environment. Parent and child use a closed private JSON
request of at most32KiB, with at most1024 ranges. These are implementation resource
bounds; a larger valid batch is refused, not reclassified as corrupt.

```text
bigbrotr-archive-worker inspect-receipt BUNDLE_DIR EMPTY_TEMP_DIR MEMORY_BYTES TEMP_BYTES THREADS MAX_RECEIPTS
```

Each invocation opens a new native engine. It checks the immutable installation
and scientific binding against the retained dataset/catalog/root and compiled
checksums through a READ_ONLY PostgreSQL attachment, then opens the existing
DuckLake catalog READ_ONLY and begins a new transaction without a historical
snapshot override. Both attachments disable native secret-storage tables; automatic
creation, migration, extension installation and persistent secrets stay disabled.
The caller still owns full privileged guard inspection and the operation's
compatibility boundary. This narrow reader does not perform an owner-row lock or
acquire publication authority. SELECT-only grants and a read-only data mount are
separate practical qualification requirements, not consequences of this API type.

The initial implementation reads **every receipt's actual fields** up to the
configured `PreparationLimits.max_records` scan bound plus one detection row.
It deliberately uses no target filter/count: native metadata statistics can
otherwise skip an unreadable receipt object and falsely suggest absence. Every
decoded row must be a well-formed schema1 receipt for the expected dataset. A row
matching any of digest, attempt or ordinal must match every field exactly; a
second matching row refuses. Present and Absent require successful stream EOF,
read-transaction completion, native client closure, successful child exit and
positive reap. Missing/corrupt objects, late fetch panic, incompatible rows,
target-identity duplicates, cancellation and truncation cannot become Absent.
This is not a global uniqueness audit of unrelated receipt identities. The worker uses a
fixed panic diagnostic because the pinned binding can panic on late native fetch.

The normalized row is bounded at256KiB. Parent admission reserves an additional
16MiB plus three bounded row representations per native vector (at most2048 rows
for the pinned engine, further limited by the scan cap plus one). This is a
conservative allocation floor, not an RSS guarantee; native metadata loading,
startup, disk and deadline remain separately bounded/qualified. Work is linear
in retained receipt rows/files. This first consumer is not qualified for an
unbounded or aged archive; a cap refusal requires more budget or a separately
qualified query design, never a partial result.

The successful reply has exactly five fields: format
`bigbrotr-native-receipt/1`, `status` (`present` or `absent`), `batch_digest`,
original `attempt_id` and `publication_ordinal`. The parent uses closed typed
decoding, rejecting duplicate/unknown fields and trailing JSON. Any launched
failure remains ObservationUnknown; TerminationUnknown takes precedence and
retains prior capacity/cleanup obligations. Pre-launch identity and resource
refusals remain distinct.

Neither observation grants replay, protection, source ACK or reclamation. Before
interpreting absence for replay, the caller must positively exclude old lifetimes
and resolve their catalog transactions in the owning order. Presence verifies the
receipt, not full scientific file membership or a durability class. The signed
publication fixture now wires this actual parent after positive reap and sealing;
its independent full-body reader remains a separate oracle. The
[Linux amd64 qualification at 0b3a427](../implementation/evidence/p02-archive-receipts-native-20260919.json)
passes all six signed schedules and eighteen separate receipt cases using the
actual parent/worker, SELECT-only PG role and readonly data mount. Real PG lock
waits precede deadline/cancellation; missing or corrupt receipt files cannot become
absence for an unrelated target. Same-input retry succeeds after exact restoration.
All source bytes remain retained. The two-receipt workload and bounded full scan
do not establish aged capacity, persistent lifetime recovery or protection.

## Bounded native batch membership candidate

`Preparation::inspect_membership` extends the retained receipt observer with a
source/digest census in one native read transaction. `MembershipRead` names the
complete original receipt expectation and optionally an exact nonnegative BIGINT
snapshot. Historical selection uses the existing READ_ONLY ATTACH path; the
actual transaction snapshot must match before receipt or scientific reads. None
selects the current transaction snapshot. There is no fallback from an unavailable
or mismatching snapshot, and no successful Absent result.

The same bounded full receipt scan must finish with Present. The reader then
examines source identity, stored digest, schema/kind and native filename columns
across all five scientific families. It includes both the expected source ranges
and every row attributed to that attempt's original family filenames. Thus a
target copied to another file and an extra foreign/out-of-range row in an original
file both refuse. Total membership has one detection row beyond the expected
count; sorting must yield exactly the retained maximal ranges, without duplicates
or holes. The canonical archive digest over those references must equal the
retained digest. Maximal ranges do not reconstruct complete admission boundaries;
the original retained `.inputs` file remains that separate evidence.

Each populated family must use its original
`main/<family>/prepared-<attempt>/data.parquet`; the receipt must be the sole row
in its original file. Paths must be canonical regular nonempty files with one
link, including canonical parents. A compacted/replaced layout that changes those
paths refuses until a replacement-membership path is separately qualified. It
does not defend against
privileged concurrent namespace changes or establish historical inode custody.
Unregistered orphan files do not form native membership or protection evidence.

The private response `bigbrotr-native-membership/1` binds snapshot, batch digest,
attempt and ordinal and reports counts in dictionary/payload/eligibility/receive/
provenance order, plus populated family files and one receipt file. Typed closed
parent decoding checks those bindings and counts after positive worker reap.
The input remains at most32KiB/1024 ranges. `max_facts` bounds complete membership;
allocation admission charges two overlapping reference buffers, bounded native/
Arrow path representations and the receipt scan floor. Filenames are capped at
8192 bytes and native vectors at2048 rows. These are allocation/refusal bounds,
not a measured whole-process RSS or aged-catalog capacity guarantee. The selected
filename predicates avoid the all-files materialization of `ducklake_list_files`;
native metadata costs remain subject to worker limits and later capacity evidence.

This observer compares **stored digest columns**, not recomputed scientific
payloads. Same-cut dictionary/reference closure, qualified persistence and root
provisioning, exclusion of reclamation, actual Archive ownership, sealed complete
frontiers and retention pins remain separate requirements. No receipt or census
retroactively certifies archives written before file-sync qualification. The
administrative `archive batch-inspect` command is its first consumer; it retains
the existing stopped-coordinator custody, Control supervision, SELECT-only reader,
bounded diagnostic output and cleanup rules. No public data delivery or frontier
authority follows from its report.

[Native3369091 qualification](../implementation/evidence/p02-archive-membership-native-20260920.json)
passes28 selected tests, including13 native membership controls, and23 actual CLI
cases plus two blocked-reader signal/deadline cases with pidfd reap. Independent
signed normal/history fixtures retain all8/11 Parquet files and native metadata
across two PG crashes/restarts; current4 and exact3 differ for the later corrections.
The executed harness labels both restored blocked-case retries `proxy-retry` due
to a loop-variable error. Their order, complete commands and results remain
recorded; the follow-up changes only that label variable, with no product/Tux
source edit. This evidence does not qualify payload corruption outside inspected
columns, protected frontiers, aged capacity or Linux arm64.

## Persistent local attempt custody candidate

`LocalAttempt::create` takes the actual immutable Control attempt and scientific
catalog binding before a fresh writer is provisioned. The configured coordinator
journal root is canonical, owner-only mode0700 and separate from archive data and
job spill. Creation reserves a new directory named by the attempt's lowercase
bytes16 hex. Existing directories, including interrupted partial creation, refuse;
there is no retry cleanup or silent reuse.

The permanent `custody.lock` is an empty mode0600 regular file, opened readonly
and exclusively locked with nonblocking `flock`. The owner takes that lock before
writing the complete `intent.json`. The closed compact JSON format
`bigbrotr-local-archive-custody/1` binds boot ID, lock device/inode, dataset/catalog/
root, attempt, coordinator and worker incarnations, issuance ordinal, topology and
schema1 semantic digest. It omits the mutable `created`/`latest_issued` observations.
The lock, intent and both directory levels are synchronized before returning.
Intent bytes are capped at32KiB; recovery compares the exact encoded immutable
identity, rejecting altered/extra fields, partial bytes and substituted inodes.
This is operational custody state, not another archive transaction authority.

The initial entrypoint accepts the current Linux boot and an actual local ext4
mount. Restored/copied journals, missing records, another boot, other filesystems
and another host require separate qualified recovery; none becomes an absent
attempt automatically. Keep the journal root/inodes stable for the lifetime of
retained obligations. Do not use NFS/CIFS, unlink/replace a lock, copy its bytes to
claim ownership, or infer shared storage from matching path names.

The publication parent consumes the matching fresh custody value together with
FreshWriter. `command-fds` preserves one duplicate of the same locked open file
description across exec before credentials enter stdin. It preserves the existing
FD number and leaves all three bounded transport pipes unchanged. The trusted
worker must keep inherited custody descriptors open until process exit and must
never unlock them. A recovered value cannot launch, and even a failed fresh spawn
consumes that value's launch allowance. Worker exit, grants and catalog outcomes
keep their existing separate rules.

Every dedicated provisioning catalog Connection must be wrapped with
`retain_catalog_connection` before it is polled/spawned. Its clone remains until
transport destruction, including cancellation before first poll. Dropping the
provisioning future or detaching its JoinHandle is insufficient: queued PostgreSQL
COMMIT may continue. The signed fixture now uses such a dedicated driver and joins
it before handing the credential to the native publisher. This does not grant
ordinary workers the fixture's privileged manager authority.

`LocalAttempt::recover` locks first, then validates the exact journal, retaining
the lock through the caller's catalog resolution. Occupied is a retryable lack of
local exclusion, never permission to force replacement. An unrelated concurrent
fork may briefly inherit CLOEXEC descriptors until exec and conservatively delay
recovery. A failed/time-limited lock attempt cannot prove death. Successful custody
reacquisition excludes registered descriptor holders under this trusted protocol;
it is not whole-process reap, persisted resource reconciliation, backend rollback,
receipt absence or file protection. Seal and resolve native catalog activity,
inspect the fresh receipt and its required closure before deciding replay or ACK.
Missing imported catalog attempts need their own nonmutating resolution; a seal
Conflict is not a successful exclusion receipt.

The recovery fixture now obtains the original request from the bounded
`ControlSession::read_archive_attempt` using only the configured catalog and
attempt ID. It releases and joins that fresh Control session before acquiring
local custody. No fixture input sidecar or registration retry reconstructs the
request. The committed Control row is still only an intent; the exact local
journal, actual catalog resolution and subsequent receipt checks remain separate.
[Native qualification at `d86e147`](../implementation/evidence/p02-retained-attempt-read-native-20260919.json)
passes the actual current/historical read, fresh session, absence, wrong catalog,
nonmutation and blocked cancellation/poison/retry probe. Three custody and two
missing-import schedules with ten clone controls use fresh observer processes;
all five exact journals remain unchanged and no input sidecar exists. This is
still a fixture consumer, not the installed recovery lifecycle.

`resolve_after_local_custody` requires explicitly recovered custody and complete
request/catalog identity before database work. A consumed launch allowance alone
is insufficient. The fresh dedicated manager driver must also retain custody.
Under the same owner FOR UPDATE lock, the adapter checks the full installed guard
and scientific binding. An exact imported lifetime is sealed through the existing
path; its stored login OID must still match. `disabled_current=false` also includes
a current owner already disabled, not only historical imports.

When no row has the intended ordinal, the nonmutating branch refuses any matching
attempt ID, worker incarnation or login, and any orphan target-name role, including
NOLOGIN roles. Only successful rollback returns `NotImported`. Coordinator and
batch digest are deliberately not unique lifetime keys: another attempt may have
published the same batch. This observation neither reissues credentials nor reuses
the journal, authorizes replay/ACK, or changes another current owner. No migration
or persistent journal format changes.

The exact tokio-postgres0.7.18 adapter fully consumes the owner query through
ReadyForQuery before queuing provisioning mutations. The native baseline at
`e571002` held the actual Bind/Execute/Sync request. A live driver retained custody
and later provisioned; destroying the driver permitted local reacquisition and
the released late request created no role/import. The baseline's seal-only
resolver returned Conflict in both missing observations, so its report remains
FAIL. [Qualification at770c485](../implementation/evidence/p02-missing-import-native-20260919.json)
passes both corrected schedules and ten independent collision/retained-role
controls. The actual late query blocks a later resolver on a PostgreSQL transaction
ID; releasing that surviving backend lets resolution return NotImported without
mutation. This is a directly observed blocker pair, distinct from a relation
RowShareLock. A renamed role isolates the historical login-name collision, and
an unrelated same-batch owner retains LOGIN, password presence and owner/role xmin.
All29 scoped native Rust cases and six signed publication/receipt regressions pass;
24 exact journals and23 imported identities/OID bindings were independently checked.

[Native qualification at 5ad77af](../implementation/evidence/p02-local-custody-native-20260919.json)
passes 28 scoped Rust cases, three actual custody/crash/queued-COMMIT schedules and
all six signed publication/receipt schedules. The actual worker alone retains the
lock after coordinator descriptors close; its existing parent-death SIGKILL still
terminates it when the spawning parent dies. Dropped operations/detached handles
leave the actual PG driver holding custody. After driver destruction or manager
crash, local recovery succeeds while sealing still waits on the unresolved server
transaction. All 25 immutable journal byte sequences match independent identities,
and all 41 retained Rocks rows remain unchanged in each journey.

This is the same-boot ext4 custody prerequisite. Cross-boot recovery, installed
lifecycle/resource reconciliation and incomplete-journal
recovery, aged journal/receipt costs, protection and complete M1 remain open.
