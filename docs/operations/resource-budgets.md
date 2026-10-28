# Resource budget ownership

The first runtime budget primitive is a synchronous, process-local admission
boundary. It makes ownership explicit: a successful request returns an
`OwnedPermit`, and the charged bytes and items stay live until that permit is
dropped. A caller-side reply timeout does not release work that still owns its
permit.

`ProcessEnvelope` validates this byte equation with checked arithmetic before a
budget can be built:

```text
baseline + data + native + control <= process memory maximum
```

The terms correspond to the static configuration's `baseline_bytes`,
`payload_bytes`, `native_bytes`, `completion_reserve_bytes`, and
`memory_max_bytes`. Item count is a separate finite admission dimension chosen
by the consumer. Replacing live limits repeats the byte-envelope validation. A
reduction does not revoke existing ownership; when current use exceeds a new
limit, the affected lane issues no new permits until use falls within the
limit. A replacement also charges the maximum of live use and the proposed
limit for each memory dimension. Capacity retained by an older permit cannot be
reassigned to another lane before that permit releases it.

## Admission lanes

Data requests atomically reserve data bytes, item count, and native bytes.
Control requests reserve only the protected control bytes. Each class has its
own bounded FIFO waiter lane, so a blocked data request cannot occupy the
control queue or consume its reserve. Immediate acquisition does not bypass an
existing waiter in the same lane. Every blocking acquisition has an explicit
monotonic deadline; expiry removes that request from the queue.

The waiter limits bound threads already admitted to a condition-variable wait.
They do not create threads or an executor. Callers must also bound the work
that reaches `acquire_until`; a full lane is refused with `WaitQueueFull`.
`acquire_until` is a blocking standard-library call and must not run on an
async executor worker. An async owner can first use immediate admission or add
a bounded adapter that preserves the same permit lifetime.

Closing a budget is irreversible. It prevents new admission and wakes queued
waiters. `drain_until` is valid only after close and succeeds only after all
owned permits have actually dropped and every queued waiter has left. A caller
deadline, cancellation request, or lost response is not evidence of drain.

## Consumer obligations

A storage or network owner must acquire before accepting the bounded work and
move the permit into the object or task that retains the resources. The permit
must cover the full uncertain outcome interval. For example, a PostgreSQL
operation retains it through transaction outcome resolution, and a RocksDB
write retains it through the synced batch result and any required reopen
resolution. Dropping a client reply future alone must not drop the permit while
the backend operation continues.

The native-byte field is accounting, not control of an allocator or child
process. A native engine owner must configure its own cache and memory limits,
charge a conservative amount before invoking it, and retain the permit until
the engine releases the associated work. Native jobs still require explicit
process lifecycle, cgroup or equivalent enforcement, and kill/reap behavior.

Control reserve is for bounded completion and recovery actions whose progress
must remain possible under data saturation. Consumers must not classify normal
data work as control work or borrow the reserve for throughput.

## Finite operation reservations

`try_reserve` atomically charges one complete operation's data, native, item and
completion allowances to its existing parent budget. An existing waiter in either
requested lane retains precedence. Only temporary `Unavailable` permits deferral;
closed, impossible or overflowing requests remain errors. The funded budget has
fixed limits and no waiter lane. It cannot reserve another operation or move
capacity between dimensions.

The reservation owner closes child admission when dropped. Unused parent capacity
then returns immediately, while each surviving child permit retains its exact
parent charge until destruction. A retained Archive input therefore keeps its own
allowance without pinning the completed worker's entire allowance. A closed budget
clone alone owns no parent credit. While the operation remains open, freed child
credit stays funded for its later phases, including progress COMMIT and native ACK.
Closing the parent prevents new parent admissions; an already funded operation
may finish until its reservation owner closes child admission.

The initial combined owner uses these reservations after bounded read-only Sync
nomination and before its actual claim, or before an Archive publication. A Sync
scan has its own64KiB grant; temporary scan pressure leaves its position unchanged
and creates no full-work debt. An actual nominee retains its exact job identity
across deferral, and claim rechecks its original authority and expiry. Empty Sync
scans and fresh empty Archive polls cannot interrupt an Index readiness sweep.
Archive reserves publication and complete-cut capture
together, including the separate native worker and application completion lane.
Sync's direct transport uses the funded scope while sharing its original DNS gate
and TLS owner. Ordinary Control renewal and status retain their existing independent
ownership. No issued operation's error is converted into a resource retry.
After a joined Index operation releases its physical credit, deferred work retries
before an inactive readiness sweep can yield metadata. Successful admission keeps
the remaining verifier and oldest-read clock; only another failed application
reservation may discard idle metadata, without touching active/unknown work or
refreshing a verified observation.

## Current boundary

The executable's configuration resolver and concrete initial PostgreSQL, native,
network and Index owners use this checked process envelope. Primitive controls
do not qualify installed shared-budget overlap. This accounting does not enforce
host-wide memory, RSS, CPU, disk or I/O limits or choose shares across unrelated
work classes. Those claims require the concrete process boundaries and recorded
operating evidence.
