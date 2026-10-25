# Ingest store initialization and reopening

`IngestStore` owns the first private native RocksDB handle for P02-T02. It exposes
explicit `initialize` and `reopen` operations, the bound manifest, read-only native
counters, a [bounded complete pending-batch feed](ingest-pending-format.md), and
[retained request-detail inspection](ingest-receipt-format.md#native-detail-inspection),
and [bounded producer-input recovery](producer-request-inputs.md).
There is no generic put/delete, exported DB/CF handle, admission,
receipt or policy grant. Opening a compatible directory is not proof of current
control authority or permission to accept events.

The stored layout is exactly `default`, `facts` and `receipts`, under the
[ingest key format](ingest-key-format.md), [manifest format](ingest-manifest-format.md)
and [native option profile](ingest-native-profile.md). The selected binding is
`rocksdb 0.25.0`, default features disabled, with `zstd` and `bindgen-runtime`;
`Cargo.lock` pins `librocksdb-sys 0.19.0+11.8.1`. No platform is qualified merely
because this dependency resolves or links.

## Entry conditions and observable effects

Both operations take an absolute directory path, a fully specified expected
`IngestManifest`, checked `RocksNativeLimits` and an owned runtime permit covering
at least `native_allowance_bytes`. The permit is checked before path creation or
native allocation. A control-lane or insufficient native permit is refused and
released. An adequate permit remains owned through every synchronous native call,
manifest check and database close, including error paths.

`initialize` requires an existing parent directory and a target entry that does
not exist, including no dangling symlink. It creates only that target directory,
with private Unix permissions, after reserving its canonical parent/name. Before
native creation, it opens the existing parent as a directory without following a
final symlink and synchronizes that descriptor. RocksDB's synchronization inside
the new DB/WAL directory does not persist the new directory entry in its parent.
An open/sync failure returns `ParentDirectorySync`, retains the partial directory
and performs no native initialization; both reservations remain held through the
call. The filesystem and device must honor the synchronization operation. This
does not qualify power-loss behavior on every filesystem or device.

It does not adopt an empty directory or recursively create missing parents.
Native create and missing-CF creation are enabled only in this path, with
error-if-exists set.
The initial application write is a single batch containing only the manifest at
`01 01`, with WAL enabled, `sync=true` and missing-CF writes forbidden.

A native initialization failure can leave engine files without a manifest. A
manifest write error is `ManifestCommitUnknown`: the write may have committed.
Neither failure deletes files, retries with new identity, or calls repair. Inspect
and resolve the existing state; `initialize` will refuse the now-existing path.
A successful matching reopen can identify the manifest, but does not by itself
supply missing control authority or qualify a failed storage device.

`reopen` requires an existing directory and a regular, bounded `CURRENT` pointer
to a native `MANIFEST-<decimal>` file. It refuses missing, malformed and symlink
CURRENT entries before calling the binding. It then checks the exact CF set,
opens all three explicit descriptors with both creation options disabled, reads
the manifest through a pinned native value and requires exact equality with the
expected manifest. The codec refuses values longer than 266 bytes before field
processing. It makes no additional Rust copy of an oversized native value; native
recovery, block reads and allocations are still subject to engine/OS limits.

The [native profile](ingest-native-profile.md#selected-options) selects absolute
WAL consistency. Detected checksum damage and incomplete records refuse native
open; no automatic point-in-time salvage occurs. Preserve the directory for
explicit recovery. This stricter acceptance can make an interrupted trailing
append unavailable. It neither repairs already lost history nor makes successful
open an authoritative noncommit or backup-validation result.

Unknown versions, malformed or absent manifest, a different dataset, owner
resource, source shard, owner epoch, process incarnation, topology, recovery or
policy coordinate, and any changed current namespace are refused. No mismatch
rewrites the manifest. Native read-write open may perform recovery and update
RocksDB metadata/LOG/OPTIONS before manifest validation; refusal is not a promise
that all filesystem bytes remain unchanged. The application record is preserved.

## Lifetime, exclusion and path boundary

An in-process reservation uses the canonical path before native open. It prevents
ordinary path/symlink aliases from reentering the same native owner. The native
RocksDB `LOCK` supplies the separate operating-system lock between processes.
Neither is a distributed lease or a fence for a host that still owns the storage.
Storage roots and their parents must remain operator-controlled and stable while
open; hostile rename, mount/device aliases, manually replaced LOCK files and
independently writable copies are not qualified by canonical path comparison.

The struct owns the native DB before its retained cache/WBM, permit and path
reservation in field destruction order. The DB destructor closes CFs and joins
its native background work before those reservations are released. The binding's
Drop does not report native close errors and must not be used as a durable ACK.
Initial durability depends on the parent directory synchronization and explicit
synchronized write result.

These operations and destruction are synchronous and can wait on native I/O.
The eventual runtime consumer must use its bounded owner worker, keep the permit
through completion and join it before replacement. Timing out or dropping an
async join handle cannot release these resources or prove native exclusion.
There is no unsafe interrupt or fictitious cancellation guarantee in this API.

### Historical manifest inspection

The installed recovery consumer needs the original manifest from the actual store,
without manufacturing an expected old process incarnation. The selected bounded
`IngestRecovery::inspect` operation takes the configured dataset and existing
native path, checked native limits and a transferred permit. It uses the same
path reservation, CURRENT/column-family checks and exclusive native open as strict
reopen. Under that exclusion it decodes the complete retained manifest, refuses a
different dataset and keeps the handle through subsequent registry validation.
It does not rewrite a manifest, create a missing store or expose an admission
handle. Existing strict `reopen` and `IngestRecovery::open` still compare the full
caller-supplied identity.

The [historical registry loader](ingest-registration.md#historical-registration-lookup)
compares original registration coordinates and retained policy digests. Existing
closing/resolution methods still revalidate them under current Control authority.
Neither inspection nor a matching historical row permits successor admission, infers a
missing request's outcome or certifies recovery provenance. Native engine open
may still update housekeeping files as described above. The
[native inspection evidence](../implementation/evidence/p02-ingest-store-linux-20260918.json)
records 12 actual RocksDB tests, build and Clippy at `20e0e5e` on Linux amd64.
This includes independent second-process lock refusal/release, full historical
identity and unchanged logical rows. The connected registry loader has separate
bounded evidence. The explicit [positive recovery command](producer-request-inputs.md#explicit-installed-positive-recovery)
now consumes both under a joined native owner; source closure, successor and full
restart remain unfinished.

A new process obtains a fresh incarnation, so its new expected manifest does not
match an old one automatically. Restart, handoff, restore and policy/topology
changes require their explicit authorized transition and old-owner exclusion;
ordinary reopen must not impersonate the previous process or update the tuple to
make the comparison succeed. Retained source facts, allocation/producer watermarks
and causal backup closure remain obligations of those later consumers.

Native allowance accounting is a reservation floor, not an RSS ceiling. Cache,
WBM, per-family buffers and native background work have overlapping and additional
costs. Use the profile's counters as observations and retain OS process limits.
The current wrapper's CF-list and native manifest replay APIs do not impose a hard
CPU/I/O/allocation bound on a damaged database's physical metadata. Such a refusal
is not evidence that arbitrarily corrupt metadata is cheap to inspect.

## Validation boundary

`rocks_store` exercises actual bindings and local engine files: explicit creation,
external exact manifest/CF inspection, reopen, every identity/namespace mismatch,
missing/malformed CURRENT and manifest, missing/extra CFs, permit lifetime/refusal,
canonical aliases, concurrent initialization and a real second process denied
while the first is open and accepted after it closes. The profile's unit tests
retain the positive shared-WBM
accounting oracle and the CF-only negative control against the same library module.
The Mac-specific case-alias scenario requires an actual case-insensitive temporary
filesystem; absence fails that fixture rather than claiming unexecuted evidence.

Execution identities and results belong to the slice evidence. These tests do not
finish P02-T02: signed-event atomic admission, receipt ambiguity and replay,
producer retirement, crash/ENOSPC/fsync faults, positive stale-owner exclusion,
backup/restore, mixed/aged workloads and Linux target qualification remain separate
practical gates. The explicit first-lifetime CLI below is under qualification;
ordinary restart/handoff and partial-initialization repair are not implemented.
The separate `ingest recover` command closes an initial producer and resolves its
retained positive outcomes; it cannot resume fresh admission. The explicit
[empty-attempt retirement mode](ingest-registration.md#installed-empty-attempt-retirement-under-implementation)
is a separate existing-preparation consumer, qualified on eleven actual installed
commands at788f971. It retires an empty attempted owner; it does not activate one.

## Explicit initial owning lifetime

`bigbrotr serve --file ROOT --roles control,ingest --initialize-ingest` explicitly
selects fresh-only initialization and retains that original Control identity until
stopped. A separate finite initializer cannot hand this initial writer to a new
Control process. Ordinary Serve still never creates an Ingest directory. Static
composition must declare the Archive counterpart; this process selection does
not start it. Configured archive reader, export destination, cursor keyring or
private asset prerequisites are refused by this narrow path.
Both complete accepted documents must also permit the selected profile.

Accepted administration enables login, self inspection, logout,
[URL registration](relay-endpoint-registry.md#authenticated-registration-consumer),
the initial [relay enrollment command](relay-enrollment.md#initial-owner-http-consumer),
and [immutable acquisition scope registration](acquisition-scopes.md#authenticated-scope-registration-consumer-implementation-in-progress),
finite job acceptance and status on the configured loopback listener.
This reuses the ordinary Management dispatcher;
migration0026, including session/enrollment prerequisites and required table/
function privileges, is required before binding. Initial Ingest has no reauthentication,
grant, history or asset routes yet. The initialized record identifies
`initial-source-commands`; session access and enrollment acceptance do not imply
native admission permission. A disabled managed seed cannot override the two
accepted documents. Changed accepted snapshots stop this initial lifetime.

The worker first refuses existing targets and absent/symlink parents, then claims
Control. A bounded fenced basis read obtains the real accepted snapshot, server
clock/deadline and absent or sealed empty topology. Registration independently
rechecks that basis. Fresh OS identities establish the first shard0/resource,
producer and nine empty allocation namespaces. The producer outstanding limit is
the smaller ceiling of the two accepted documents, never the startup file seed.
The existing native initializer commits its complete manifest; the existing
connected bootstrap retains the original public1/private2 mappings and request1
unchanged. A separate fixed-reference append2 creates private3 with its own private
fact envelope. Only after its durable PG/native outcome is delivered does the
initialized report publish `private_scope` and dictionary_count=3; accepted
scopes/jobs are never silently rebound. Both complete accepted documents must
permit at least two outstanding producer requests before any registration/native
initialization. Deployment roots are checked separately; a statically valid managed
file seed of one does not override accepted limits. Declarative file changes also
remain staged until explicit activation. This corrects the earlier local-seed floor
without changing any stored policy, producer identity, migration or existing
directory. Three actual installed comparisons at `14d18e1` qualify managed seed 1
with accepted limit 32 through renewal/shutdown, and rejection when either
accepted document permits only one. Original native inputs and rows survive;
fresh adoption still refuses. This role selection collects no events. The
[private append evidence](../implementation/evidence/p02-dictionary-append-native-20261002.json)
retains the nine original lifetime/window cases, the three corrected window
comparisons and nineteen startup COMMIT cuts;
older evidence below still refers to the original two-map lifetime. Existing
directories are not upgraded or adopted. The three-case installed session
regression also passes with explicit current-schema preparation; full acquisition,
restart and M1 remain unfinished.

One dedicated blocking worker exclusively owns store and Control. The parent
current-thread runtime drives the actual PG connection, signals and phase
watchdog. Only a closed terminal report leaves the worker. Native store destruction
precedes Control destruction on normal errors and panic; graceful shutdown closes
native before explicitly releasing Control, then joins the driver. Cancellation
does not detach or abort native work. If the worker cannot join within the
configured shutdown bound, the process exits4 without first aborting the driver,
releasing the fence or claiming graceful release. Interrupted/ambiguous state is
preserved. Diagnostics use the existing bounded, nonblocking lifecycle channel.

HTTP runs on the parent runtime; one additional blocking slot holds at most one
password hash. Parent cancellation revokes the socket delivery gate and stops new
connections even while native work is blocked. The worker joins HTTP/password work
before normal native close and explicit Control release. A fixed renewal deadline
wins a ready command queue; processing requests cannot postpone it. Both renewal
responses consume the request-start authority window, and only a timely pair may
renew delivery. Commands and socket writes independently check remaining authority.

Between operations the same worker refreshes Control, then renews initial Ingest.
Every native operation still needs its own fresh authority; scheduling is not a
grant. Expired Ingest cannot be revived. Changed accepted snapshots stop the
initial lifetime. An initialized report explicitly has `ready=false` and
`acquisition=not-started`; maintained dictionaries are not complete Ingest/Sync
readiness. SIGTERM does not close the source stream, acknowledge producers or
qualify successor admission.

Adding explicit Sync selects the finite initial executor:

```sh
bigbrotr serve --file ROOT --roles control,ingest,sync --initialize-ingest
```

Declare local Sync placement, configure the numeric `network.dns.resolver` and
any required DER `network.tls.extra_roots`, and accept `sync.enabled=true` in
both stored documents. This profile requires at least four outstanding producer
slots and `ingest.max_event_bytes` at most 256 KiB. It checks execution migrations
and privileges even when administration is disabled. The initialized record
then reports `acquisition=finite-initial, ready=false`. Configured enablement
alone never starts an omitted role.

Use the published private3 scope for an enrolled private relay. The same owner
nominates accepted finite jobs, resolves current source authority and receives
bounded WebSocket quanta. It interns committed attribution and admits valid
missing effects with original producer requests. Invalid signatures, malformed
events, duplicates and events outside the exact selector do not add effects.
Each quantum retains at most three events and examines at most 32 frames; idle
receive has a soft cutoff at most 500 ms. DNS/connect/TLS and native work retain
the independent hard deadline. A stall can stop this initial process.

Failed Sync dispatches append `sync_dispatch` JSON with format
`bigbrotr-initial-sync-diagnostic/1`; the parent watchdog emits the same format
through the bounded lifecycle channel. Ordinary success output is unchanged.
The record contains the selected renewal/authority/due window, actual parent,
dispatch and outer deadlines, last caller stage, zero-based frame ordinal,
request sequence and last locally delivered Durable admission sequence.
Times are saturating monotonic microsecond offsets from dispatch selection.
`returned_us=null` means completion was not observed; a returned error has a
return marker. Neither a stage nor a missing marker proves SQL/native noncommit.
The watchdog freezes its observation before cancellation/grace cleanup; output
is best-effort after cancellation and admission stop. No event, URL, credential
or principal is retained in the diagnostic record. It does not renew authority,
retry an uncertain effect or widen any deadline. Preserve ambiguous state under
the existing recovery contract.

At `b2e3194`, the [recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json)
records 28 focused tests, build/Clippy and one real signed-sample preparation
journey with unchanged success output, complete independent readback and joined
custody. The six new controls exercise diagnostic distinctions; no real timeout
was induced. The earlier a99 timeout cause remains unresolved.

The [nine-case native first wave](../implementation/evidence/p03-initial-sync-native-20261002.json)
qualifies signed admission, duplicate/refusal behavior, quiet renewal/shutdown,
public-envelope isolation and headless missing-prerequisite refusal. Two focused
four-slot cases also qualify deferral of new events and source attribution while
existing events, renewal and HTTP remain usable. Complete scheduling,
failure/recovery and capacity work remain open.
Receipts do not free outstanding slots. Jobs remain queued with no EOSE coverage,
checkpoint advance or completion claim. This path still cannot restart or adopt
an existing store, serve archive reads or export; M1 remains incomplete.

The explicit small native profile uses32MiB cache,16MiB WBM,2MiB per-CF buffers,
16MiB WAL target,64 open files and two background jobs, within a128MiB native
reservation. Bootstrap reserves8MiB payload/4MiB native plus24MiB retained inputs;
the shared configured application envelope also covers the existing Control
connection reservation. No second process allowance is borrowed. This ledger is
shared with session request/connection/hash reservations; startup refuses before
binding if one complete login cannot coexist with retained native/input/Control
work. The item ceiling is36 (three retained owners,16 connections,16 requests and
one hash), plus 16 slots when Sync is explicitly selected; it is a simultaneous
ownership ceiling, not a throughput guarantee.
These are reservations/targets, not measured RSS or capacity. No hot identity cache exists
in this initialization-only path.

Existing/partial native state or a populated source registry is never adopted,
deleted or reinitialized. The in-memory attempt is preserved through the current
worker/driver lifetime, but this slice does not promise crash-resumable setup.
Registry-only interruption can lack the chosen manifest/recovery metadata; retain
it for explicit recovery and do not generate another initial writer. A bounded
loader over existing registry, manifest and staged-input records must precede
restart acceptance. No existing SQL checksum or native format is changed.

The [executed first wave](../implementation/evidence/p01-initial-ingest-native-20260928.json)
qualifies six actual CLI/PostgreSQL/RocksDB cases, including renewal past the
original deadline, graceful release, sealed zero topology and retained-state
refusals. The [native interruption wave](../implementation/evidence/p01-initial-ingest-interruption-native-20260928.json)
holds actual open/close syscalls while observing the retained Control fence,
competing-claim refusal and bounded process death. Close(LOCK) occurs after the
native OS lock is released; this tests an unfinished destructor and driver
custody. The [SQL interruption wave](../implementation/evidence/p01-initial-ingest-commit-native-20260928.json)
executes twelve actual startup COMMIT/cancellation cases with complete SQL/native
and original-input observations. Before/after response loss preserves the exact
committed boundary; no retry regenerates initial native state. Held cancellation
also demonstrates that the deliberate proxy's server transaction can outlive the
terminated CLI, until that upstream connection ends. Client death alone does not
prove server rollback. Other close stages, worker panic, renewal-specific commit
cuts and crash-resumable initialization remain pending; this does not complete the
role or M1.

The [initial private-session first wave](../implementation/evidence/p01-initial-sessions-native-20261001.json)
qualifies actual installed login/logout and incorrect credentials, renewals under
12 concurrent self-inspection clients beyond the original scheduling expiry,
shared-memory refusal before binding, and SIGTERM during observed password work.
Native/input/receipt and64-table comparisons preserve the original owner and
bootstrap. Fixture failures and corrected independent audit expectations remain
recorded. The [installed session fault wave](../implementation/evidence/p01-initial-session-faults-native-20261001.json)
also qualifies a slow valid initial renewal consuming3.003s of5s request-start
authority, actual disconnection during password work without late issuance, and
session HTTP shutdown during held native close. In that close schedule HTTP was
observed stopped5.45ms after SIGTERM and process death after2.017s; Control still
refused the competing owner before process exit. Original bootstrap and all
unrelated SQL rows remain unchanged. These bounded observations do not qualify
worker panic, renewal-specific ambiguous COMMIT, successor recovery or capacity.

## Explicit initial successor (under implementation)

After stopping and fully retiring the initial signed lifetime, the selected
successor command is:

```sh
bigbrotr serve --file ROOT --roles control,ingest,sync --activate-initial-successor
```

Omit `sync` for ownership and private administration without acquisition. The
deployment must assign the selected roles and retain its Archive counterpart;
this command cannot select Archive. It conflicts with `--initialize-ingest`.
Keep the existing native directory and every original producer input. Startup
checks exclusive native custody before opening protected PostgreSQL credentials,
positively replays the predecessor and uses the current accepted policy to prepare
and activate epoch2. It requires no caller-supplied target identities. An existing
epoch2, missing retirement proof or competing native owner refuses startup.

This lifetime starts producer request1 without another visibility bootstrap.
Finite Sync needs two current outstanding slots: attribution dictionaries and one
new event. It uses retained explicitly selected visibility scopes and fresh
attribution domains. Current policy and scheduling renewal govern new work;
historical registration remains evidence of the predecessor. Provenance-off keeps
local receive evidence while suppressing new provenance. The detailed
[activation and acceptance contract](ingest-registration.md#installed-live-initial-successor-under-implementation)
owns this bounded consumer. Connected61a3de1 qualifies the installed off/on network
journeys, renewal and the three startup refusals, retaining original signed data.
General activated-owner recovery, repeated lifetimes,
Archive restart, coverage and ACK/GC remain unimplemented here.

## Explicit later successor (under implementation)

After epoch2 or a later generation is fully retired, select the repeatable
owning lifetime explicitly:

```sh
bigbrotr serve --file ROOT --roles control,ingest,sync --activate-successor
```

Omit `sync` for ownership and private administration without acquisition. Select
local Control/Ingest and retain the deployment's Archive counterpart; this
command cannot select Archive. It conflicts with both `--initialize-ingest` and
`--activate-initial-successor`. Apply migration0038 explicitly and retain the
application's existing Control/Ingest and recovery privileges plus the lineage
privileges listed in the [qualification grant set](../../experiments/ingest-succession/schema_grants.sql).
Run with the application credential, not the provisioning credential.

Retain the entire native directory and every original producer input. Startup
opens the existing native store before reading protected PostgreSQL credentials,
positively verifies the complete bounded history, and prepares a fresh candidate
under current Control and accepted policy. Both genuinely retired epoch2 entry
shapes and later retired generations use this same path. Pending preparations,
activated but unretired heads, unresolved sidecars or missing history refuse.
It does not resume a recorded writer or choose caller-supplied identities.

The delivered writer remains in its original native worker through private HTTP,
optional finite Sync, renewal and joined shutdown. Its own generation and accepted
policy govern new requests; historical registrations are used only for replay.
Sync requires two outstanding slots and retains the existing finite network
limits. Old facts can be referenced only through the checked historical frontier.
Provenance-off suppresses new provenance while preserving local receive evidence.
Existing input-spool and lineage limits apply across lifetimes; activation frees
no space.

Activation reports `bigbrotr-ingest-successor/1` with the actual generation,
preparation and predecessor coordinates, `acquisition=finite-successor` when Sync
is selected, and `ready=false`. Graceful SIGTERM stops admission and joins HTTP,
native ownership and the PostgreSQL driver before the stopped report. Shutdown
does not close the source, abandon jobs, retire this generation or acknowledge
inputs. Preserve all state after an interruption or unknown outcome. Installed
recovery of this new lifetime remains a separate responsibility; do not retry
activation against an unretired head. The [repeatable succession contract](ingest-registration.md#repeatable-ingest-succession-selected-not-yet-implemented)
and existing recovery evidence track implementation and qualification separately.
The exact4cfac91 build passes20 focused Serve/entrypoint tests, CLI/probe
compilation and Clippy. Installed9359721 qualifies three PostgreSQL/HTTP/Sync
lifetimes from both terminal epoch2 shapes and terminal epoch3, with signed
new/historical/retried events, provenance off/on, renewal and joined SIGTERM.
Its sources remain open and jobs queued; it does not qualify their full recovery
and retirement cycle.

## Recovering a later activated successor (under implementation)

After stopping the epoch3-or-later owner, select one finite operation per process:

```sh
bigbrotr ingest recover --file ROOT --successor
bigbrotr ingest recover --file ROOT --close-successor-source
bigbrotr ingest recover --file ROOT --abandon-successor-acquisition
bigbrotr ingest recover --file ROOT --retire-successor
```

These modes require migration0038, the existing application recovery grants,
local Control/Ingest and the entire native store. The ordinary basis requires
every original input. Each
opens the native store before protected connection setup and acquires fresh
Control authority under current accepted policy. They select the retained
activated head, without caller-supplied identities or a resumed writer.

`--successor` closes that producer and resolves positive outcomes while its
source remains open. It can copy missing current outcomes; historical copies
must already be exact. It preserves unresolved reservations and staged inputs.
Completion acknowledges only the current typed PostgreSQL checkpoint, which may
be below the issued prefix; unresolved reservations or a retained staged tail
prevent complete recovery and ACK.

The explicit compacted positive branch accepts an initial closed-producer floor
under a later activated head. It authenticates the initial checkpoint and canonical
closure separately, then replays every required original from epochs2 through
the current head. Initial originals may be absent; any retained residual must
still authenticate. Required later originals cannot be replaced by those residuals.
The complete mixed input census, native root/sequence, current Control authority
and cancellation are rechecked through the final ACK, including an unchanged ACK
watermark. The owned ACK token retains fixed per-epoch input bounds without a
second lineage allocation. Selecting the actual retained basis only chooses the
reservation envelope before Control acquisition; it grants no recovery authority
and malformed compacted state never falls back to the ordinary reader.

Installed qualification covers a three-request head at checkpoint/ACK1, a quiet
head at checkpoint/ACK0, exact retries and missing required-original refusals.
Authentic outcome COMMIT-response loss before forwarding also qualifies fresh
missing-copy repair without inventing a checkpoint. Loss after server completion
qualifies ACK advancement from the retained checkpoint; exact retries add no
native writes. These fault results use product837 and measured COMMIT11. The path adds
`recovery_basis = closed-producer-floor-v3` and
`compacted_requests = N` to the existing successful report; `resolved_requests`
still counts current-head original requests. That qualification does not establish
the Source extension below, acquisition abandonment, retirement or Archive
consumption on this basis. Fault/Sync, physical
loss/restore and capacity qualification remain separate gates.

`--close-successor-source` proves the complete ordered history and publishes the
source cut and digest. A closed retry requires existing exact outcome copies.
Its compacted extension is component- and installed-qualified for finite and
quiet closure, exact retries and required-input refusal. It selects the actual
floor with the same required-original and
residual checks, verifies initial H/D, and publishes only the current Source cut
and digest under fresh Control authority. A current head with three issued
requests, Fact cut4 and typed checkpoint1 closes at cut4 and acknowledges only
checkpoint1. An exact closed retry preserves the closure timestamp, digest and
row version. The report adds the same compacted basis/count fields as positive
recovery; it confers no abandonment, retirement or Archive capability. Installed
closed-source positive recovery refuses at the SQL open-source guard, while
installed abandonment still refuses at its strict factory. Source-publication
response loss and final-ACK fault schedules remain separate gates.

Source publication spends its owner before fallible work and retains native and
proof custody through known publication, final ACK and shutdown. The public
acquisition-abandonment consumer separately refuses a compacted Source proof
before SQL effects and leaves that attempted owner spent. Ordinary Source and
abandonment keep their original-input readers.

`--abandon-successor-acquisition` requires the source already closed and verifies
it again in a fresh process. It records whole-intent coverage gaps for the
original activator's queued acquisition jobs, preserving unclaimed jobs.
`--retire-successor` requires completed source closure and workflow abandonment;
it verifies existing copies and retires the producer, its nine allocation domains
and Ingest owner. An exact retry preserves original attribution and row versions.
Unresolved import obligations and unbound historical Events refuse retirement.

Retry a mode only while its prerequisites remain valid: positive recovery is
source-open-only, and terminal retirement has its own retry. No mode starts
acquisition, resolves an unactivated preparation or reclaims space. On the compacted
basis, positive recovery and the explicit Source path perform their final
typed-checkpoint ACK. A successful
`bigbrotr-ingest-recovery/1` report contains the actual epoch,
request bounds and verified history; it is published after native closure,
Control release and driver join. Preserve state after errors or unknown outcomes.
Once retirement is complete, `serve --activate-successor` can prepare the next
generation. Exact04e41c1 qualifies27actual processes over three fresh copies:
first/retry recovery, source closure, abandonment and retirement, then continuous
Serve4/4/5 with renewal and joined shutdown. The final Serve processes issue no
new jobs or input; this does not qualify prepared attempts, tails, import closure
or Archive restart. The [recovery evidence](../implementation/evidence/p02-producer-recovery-native-20260920.json)
binds full SQL/native/input preservation and distinguishes the earlier live-writer
journey from this installed finite-operation cycle.

## Recovering an unactivated later preparation (under implementation)

Stop the attempted owner and retain its complete store and original inputs, then
use the explicit finite command:

```sh
bigbrotr ingest recover --file ROOT --retire-empty-successor
```

This mode requires migration0038, the existing recovery grants and a recorded
unactivated preparation for epoch3 or later. Fresh Control authority verifies
the complete predecessor history and exact existing outcome copies. It selects
the action from the retained PostgreSQL and native state:

| Retained state | Recovery action |
| --- | --- |
| SQL prepared, native still at predecessor | Complete the exact recorded handoff, inspect and replay the resulting state, then retire the empty attempt |
| Prepared native OPEN0 | Close the empty producer and publish its complete SQL retirement |
| Prepared native CLOSING0, SQL not terminal | Finish the SQL retirement without another native write |
| Exact unactivated terminal head | Verify the complete history through that head and preserve its terminal timestamp, attribution and row versions |

An activated head requires the activated recovery modes even when it has issued
zero requests. Missing original inputs, missing outcome copies, unresolved claims
and inconsistent state refuse recovery. The command preserves every input and
creates no writer, candidate or new preparation. Native epoch2 is accepted only
as the predecessor of a genuine recorded epoch3 preparation gap. Recovery needs
capacity for the existing attempt, without reserving another generation.

The successful report distinguishes the verified predecessor prefix of a first
retirement from the full terminal history checked on retry. It is emitted only
after native closure, Control release and driver join. Exact `f4f23db` CLI/probe
compilation, 22 affected tests and Clippy PASS. Installed `6a3fa53` exercises five
fresh copies and twelve processes: genuine SQL/native gap and CLOSING setups,
first retirement and terminal retries, fresh Serve epochs4/5, activated OPEN0
and legacy-terminal refusal. Complete SQL/native/input states are compared;
originals remain unchanged. The two setup calls retain their CommitUnknown
outcomes. Successful installed reports and expected refusals are separate results.
This qualifies the selected recovery path, without claiming unresolved-tail
repair, input reclamation, Archive restart or capacity/RSS bounds.

## Recovering the activated successor

After stopping the epoch2 owner, the selected finite recovery command is:

```sh
bigbrotr ingest recover --file ROOT --activated-successor
```

Apply migrations through0034 explicitly first and retain the native directory and original
inputs. Using the provisioning credential, grant the configured PostgreSQL
application role `EXECUTE` on `control.require_activated_initial_recovery(bytea)`
and retain its existing Control/Ingest grants. For the qualified application role:

```sql
GRANT EXECUTE ON FUNCTION control.require_activated_initial_recovery(bytea) TO bigbrotr_control;
```

Run recovery with the application credential. The command requires local
Control/Ingest, exclusive native custody and
the exact retained preparation/activation. It uses fresh Control and current
accepted policy; the old Ingest scheduling lease may already have expired. It
closes producer2 and verifies retained positive outcomes, copying a missing
PostgreSQL outcome only after positive native proof. Existing outcomes retain their
original attribution. An identical retry observes the same closed producer.

This mode conflicts with the initial source-closure and retirement flags. It
leaves source2 open, starts no listener or acquisition, and returns no writer.
Missing inputs, incompatible identities or unresolved reservations refuse; preserve
the state and retry this same mode after resolving the reported prerequisite.
An interrupted or unknown outcome is never permission to reinitialize the store.
The [recovery contract](ingest-registration.md#activated-initial-producer-recovery-under-implementation)
owns qualification. The selected boundary passes 27 actual CLI invocations:
eight positive/retry/missing-copy cases, four COMMIT losses with four fresh
recoveries, two recoveries of a real rejected request with zero source facts, and
nine corrupt-input/identity/dependency or held-native refusals. One finite typed
producer supplies the rejected-request seed; this does not qualify installed
import. Current policy advancement and expired historical scheduling preserve
original inputs and outcomes. Source2 closure, retirement, repeated lifetimes and
Archive restart remain separate responsibilities.

## Closing the activated source (under qualification)

After stopping the epoch2 owner, select the finite source closure explicitly:

```sh
bigbrotr ingest recover --file ROOT --close-activated-source
```

Apply migration0035 and, using the provisioning credential, grant the configured
application role its additional validator permission:

```sql
GRANT EXECUTE ON FUNCTION control.require_activated_initial_source(bytea) TO bigbrotr_control;
```

Retain existing Control/Ingest privileges, the native directory and every original
input. This mode conflicts with all other recovery flags. It resolves every
reserved request in order and verifies the complete source2 prefix before recording
its cut and digest. It can operate after the historical Ingest lease expires, using
fresh Control and current accepted policy. An open source may receive missing
PostgreSQL outcome copies after positive native verification. Once closed, the
source requires all exact copies to remain present; a retry preserves its original
closure row and timestamp. Missing or corrupt input, unresolved reservations,
inconsistent source counters or incomplete terminal state refuse.

The successful report has `state=activated-source-closed`, decimal request counts,
and `source_closure` with `last_complete_sequence` and `sha256`. Native close,
committed Control release and joined transport precede delivery. Preserve all
retained state after an interrupted or unknown outcome and retry the same mode
in a fresh process. This command leaves workflow and producer retirement for their
separate operation; it starts no acquisition, acknowledges no receipts, reclaims
no inputs and keeps `ready=false`. Qualification is tracked in the
[source closure contract](ingest-registration.md#activated-initial-source-closure-under-implementation)
and existing recovery evidence. The first installed wave passes 13 invocations:
off/on closure and exact retry, empty and rejected-only prefixes, legacy recovery
with its original grants, and missing-input or inconsistent terminal refusals.
The on case uses an isolated privileged draining baseline; it does not qualify
the transition into draining. Four further invocations exercise final-COMMIT loss
before forwarding and after server completion, followed by fresh-process recovery;
the earlier repaired outcome and any completed closure retain their original
attribution. A genuine authenticated network resend also qualifies an
AlreadyDurable-only prefix, followed by installed source closure and exact retry
without new facts or retry writes. Its job remains queued and its session remains
unrevoked. Both concurrent job/source orders also pass: the second incompatible
commit refuses. These use a privileged relational job with all product guards
enabled; authenticated job acceptance is a separate journey. Three further installed
refusals cover a real reserved/unissued request after lost COMMIT completion, a
retained native source tail and a conflicting terminal outcome copy. They preserve
all retained data and perform no speculative repair. The selected source boundary
now has24 installed invocations and two finite seeds; workflow abandonment and
retirement remain separate.

## Abandoning activated acquisition (under qualification)

After stopping the epoch2 owner, retain its native directory and every original
input, apply migration0036, and select:

```sh
bigbrotr ingest recover --file ROOT --abandon-activated-acquisition
```

Keep the source-closure privileges above and the existing job update privileges.
Using the provisioning credential, grant the recovery application role:

```sql
GRANT SELECT, INSERT ON control.acquisition_gaps TO bigbrotr_control;
GRANT EXECUTE ON FUNCTION control.guard_acquisition_gap(),
    control.require_acquisition_gap_complete() TO bigbrotr_control;
```

The command first verifies and closes source2, then cancels the queued acquisition
jobs owned by the Control that activated epoch2. It records whole-intent coverage
as unknown, including claimed jobs with no mutation requests. Accepted jobs that
were never claimed remain unchanged. The combined number of retained gaps and
remaining claims must be at most64; advanced jobs or inconsistent history refuse.
Sessions and original job details remain intact. The source may already be closed
when a later abandonment step refuses; preserve that state.

A successful report has `state=activated-acquisition-abandoned`, the source cut
and digest, and `acquisition_abandonment` containing decimal `jobs`, reason
`activated_initial_producer_abandoned` and `coverage=unknown`. An identical retry
returns the retained total without rewriting its original attribution. After an
interrupted or unknown outcome, inspect retained state and retry the same command
in a fresh process. This operation leaves producer2 closing and its Ingest lifetime
unretired; it starts no acquisition and keeps `ready=false`.

The selected qualification covers first/retry on two closed sources, final
abandonment COMMIT reply loss with fresh recovery, genuine extra claims and an
untouched accepted job, raw SQL guards, ordinary HTTP with recovery-only privileges
removed, the combined64 bound and full-batch refusal. Actual installed SIGTERM
and a finite adapter with real Control UTC expiry both exercise the final job wait
without partial terminal writes. The HTTP evidence retains a missing first
schema32 migration-response limitation; later ledger and reapply captures remain.
See the [activated abandonment contract](ingest-registration.md#activated-initial-acquisition-abandonment-under-implementation)
and its linked evidence for exact sources, retained failures and workload bounds.

## Retiring the activated initial lifetime (under qualification)

After explicit source2 closure and acquisition abandonment, preserve the native
directory and every original input, apply migration0037, and select:

```sh
bigbrotr ingest recover --file ROOT --retire-activated-initial
```

Retain the existing recovery privileges. Using the provisioning credential, grant
the configured recovery application role:

```sql
GRANT EXECUTE ON FUNCTION control.lock_activated_initial_retirement(bytea),
    control.require_activated_initial_retirement(bytea),
    control.require_activated_initial_retirement_transition(bytea,bytea,bigint)
    TO bigbrotr_control;
```

This command observes the already closed native producer and positively verifies
every original request against its existing PostgreSQL copy. It does not create
missing copies or complete source closure or abandonment. It atomically retires
producer2, closes its nine allocation domains, releases its Ingest owner with
storage fencing and retires its role. Source2 keeps its original closure time,
cut, digest and row version. Native state, original inputs, receipts, jobs, gaps
and browser sessions remain intact.

Missing or conflicting inputs/copies, incomplete closure, unresolved acquisition
claims, import bindings/checkpoints or mixed terminal history refuse. Every native
Event needs its acquisition job and completed gap, including a rejected or already
durable Event. A historical issuer need not remain live; current recovery uses a
fresh actual Control owner and accepted policy. An exact retry verifies the same
proofs and preserves the original terminal timestamp and row versions.

Success reports `state=activated-initial-retired`, the source cut/digest and decimal
request counts. `ingest_retirement` contains decimal `allocation_domains` and
`acquisition_gaps`, `owner_release=committed`, `native_acknowledged=0` and
`native_retired=0`. Native close, current Control release and joined transport
precede delivery; `ready=false`. Preserve all state after an interrupted or unknown
outcome and retry this mode in a fresh process. It conflicts with every other
recovery flag, reclaims no capacity and activates no replacement writer.
Implementation and actual qualification remain tracked under the
[retirement contract](ingest-registration.md#activated-initial-producer-retirement-under-implementation).

## Exact dependency source basis

The pinned Rust [open implementation](https://github.com/rust-rocksdb/rust-rocksdb/blob/e4aac69f55021e619566b6ca5902665e0ea2af4e/src/db.rs#L630)
creates directories even when native create-if-missing is false, motivating the
application preflight. Its DB destructor closes CF handles before the native DB.
The native [POSIX lock](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/env/fs_posix.cc#L858)
combines a process-local filename registry and `fcntl`; aliases must not bypass
the local reservation. Native [close](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/db_impl/db_impl.cc#L797)
waits for scheduled background jobs. These source guarantees do not substitute
for the process and failure scenarios above.

Native [directory creation](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/env/fs_posix.cc#L756)
does not synchronize its parent. The
[CURRENT installation](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/file/filename.cc#L429)
and [first synchronized WAL](https://github.com/facebook/rocksdb/blob/abeebd9630f11bd08c28b7bd43c7bdfc62050654/db/db_impl/db_impl_write.cc#L2394)
sync their DB/WAL directories. The wrapper therefore synchronizes the parent of
its newly created target separately, before native initialization.
