# Authority identity and lifetime

P01-T06 currently provides a local identity/timing and policy-lease core. It does
not authenticate principals, persist owner epochs, acquire engine locks, fence
commits, or authorize takeover. The [operations contract](../specification/operations-contract.md#dependency-recovery-lifecycle-and-operator-procedure)
and [restrictive activation barrier](../product/private-administration.md#restrictive-change-activation-barrier)
remain the owning product contracts. Real storage, stream delivery, grant and
old-owner exclusion acceptance is still NOT_RUN.

## Writer identity

[`ProcessIncarnation`](../../crates/bigbrotr-model/src/identity.rs) is a distinct
nonzero bytes16 portable identity. Parsing it establishes structure only.
[`fresh_process_incarnation`](../../apps/bigbrotr/src/runtime/ownership.rs) obtains
16 bytes from the pinned getrandom OS provider. An error, including partial
output, or an all-zero draw fails closed with a static redacted reason. There is
no PID/hostname fallback. The OS entropy call can block during early boot; obtain
the identity before acquiring protected work or writer authority. Obtain a new
identity on each process start; inherited state after fork must not become a new
owner under the parent's incarnation.

`WriterIdentity::require_same` checks dataset, storage/control responsibility,
owner epoch and process incarnation independently. The current responsibilities
are Control, Ingest, Archive and Index. Algorithm/protocol roles do not acquire
independent writable engine directories through this value. An equal tuple is
only an identity comparison; the storage/control adapter must establish the
expected identity from real ownership and validate it at the commit boundary.
Neither a nonce nor an incremented epoch excludes a paused old writer.

`OwnershipTiming` requires positive TTL, heartbeat, renewal timeout, safety margin
and graceful timeout, and checks `heartbeat + renewal < TTL - safety` without
wrapping. The initial 60/20/5/20/30-second values pass. These scheduling/drain
settings remain independent of policy authorization and its five-second maximum.

## Restrictive policy lifetime

[`PolicyLeaseTracker`](../../apps/bigbrotr/src/runtime/policy_authority.rs) has one
known policy epoch, at most one installed lease and at most one owned refresh.
It is not cloneable or deserializable. A local opaque refresh ticket binds the
tracker allocation, dataset, process incarnation and checked request sequence.
Two trackers in the same process cannot accept one another's sequence-1 ticket.
The ticket keeps its allocation identity alive, preventing address reuse while
old tickets remain. There is one small shared allocation per tracker; refreshes
clone that identity reference without allocating another tracker or queue.

Begin refresh records a local clock reading before the adapter starts its
request. A successful response installs only a positive TTL no greater than the
configured ceiling, which itself cannot exceed five seconds. The deadline is
**request start plus TTL**. Network/control delay consumes this interval: a reply
arriving at or after the deadline cannot renew authority. Unknown/foreign
requests, replayed replies, another dataset or process incarnation cannot install.
Response data is not a grant or proof that Control is authentic; the future
bounded authenticated adapter must establish that boundary before submitting it.

A trusted newer epoch immediately invalidates the old lease. Older notifications
cannot reduce the known epoch, and a late old refresh cannot restore it. Even a
timed-out matching reply that reveals a newer epoch invalidates older authority,
while remaining ineligible to install a new lease. Queued output carries its
admitted epoch: obtaining a new lease does not relabel old output under new policy.

A caller timeout marks the refresh unusable but retains the occupied slot. New
refresh attempts coalesce into an explicit in-flight refusal. Only terminal driver
completion/failure, join, or positive exclusion permits the owning adapter to
release that slot. Timeout does not extend the existing lease; only its original
unexpired remainder can continue. The core has no driver and does not itself
prove terminal exclusion. A dropped ticket cannot silently free the slot.

`check_delivery` reads the clock anew and fails closed for absent, expired or
wrong-epoch authority. Consumers must call it immediately before every bounded
output chunk, signing/send attempt and retry, including previously queued work.
Its successful unit result is not a reusable authorization token. Actual output
adapters still need action/resource grants, cancellation, temporal eligibility,
and bounded in-flight drain; these are not supplied by a lifetime check alone.

## Clock selection and failure

The tracker owns a sealed clock provider; callers cannot supply cached timestamps,
UTC values or a custom production clock. Rust's
[`Instant`](https://doc.rust-lang.org/std/time/struct.Instant.html) does not promise
that host suspension counts as elapsed time. It is therefore unsuitable as the
sole restrictive-authority deadline clock, although it remains useful for ordinary
process bookkeeping and bounded test waits.

With rustix 1.1.5's fallible `clock_gettime_dynamic`, the provider selects Linux
`CLOCK_BOOTTIME`, which includes suspend time and excludes discontinuous UTC
steps, and macOS `CLOCK_MONOTONIC`, whose platform contract also includes sleep.
These are different platform semantics for similarly named clocks. Unsupported
platforms fail closed. The selected API does not use a raw FFI wrapper or expose
wall-clock conversion. Read failure, invalid clock representation or a detected
regression invalidates the tracker permanently; recovery creates a new tracker
with no lease and requires fresh authenticated authority. Deadline/sequence
arithmetic is checked and never wraps.

The basis is the [Linux clock contract](https://man7.org/linux/man-pages/man2/clock_gettime.2.html)
and Apple's pinned
[clock manual](https://github.com/apple-oss-distributions/Libc/blob/71bbe350ab79eef58113991d817ccc6165061a64/gen/clock_gettime.3)
and [implementation](https://github.com/apple-oss-distributions/Libc/blob/71bbe350ab79eef58113991d817ccc6165061a64/gen/clock_gettime.c).
Exact rustix/getrandom source paths and hashes are retained in the
[scoped evidence](../implementation/evidence/p01-authority-lifetime-20260917.json).
This source audit and ordinary Mac clock execution do not claim a physical
suspend/resume, virtual-machine pause/migration, hostile privileged clock change,
or native Linux experiment. Target qualification must preserve the stated
provider semantics and test the relevant deployment failure boundary.

## Integration and remaining acceptance

The focused tests exercise real OS nonce generation and actual Mac clock expiry,
plus controlled schedules for delayed replies, expiry equality, restrictions,
foreign correlation, outage/timeout coalescing, clock faults and exhaustion.
Simulated suspend and UTC jumps include negative interpretations that would
incorrectly keep a lease alive; they are not host-clock mutation experiments.

The integrating owner must wire the runtime modules and replace configuration's
inline ownership arithmetic with `OwnershipTiming::new(Duration::from_micros(...))`
for the five named settings, plus `PolicyLeaseTtl::new` for
`runtime.authorization_ttl`. Preserve the existing smaller read-authorization
ceiling check. The lease tracker then needs actual Control and output consumers;
no currently shipped command advertises live authority from this core.

Manual failover still requires stopping or positively excluding the old owner,
recovering and verifying its data, advancing the persisted epoch under fenced
control, and only then enabling its successor. Lease expiry, health failure or
this identity comparator cannot substitute for that sequence. The required
P01-T06 drill must resume a paused old writer and inspect actual commits and
client-delivered bytes across revocation before completion is claimed.
