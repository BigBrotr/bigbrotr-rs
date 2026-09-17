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

## Current boundary

The executable's offline configuration resolver uses the same checked process
envelope. This slice also supplies the ownership primitive and pressure tests;
PostgreSQL, RocksDB and supervisor consumers remain unimplemented. It does not
enforce host-wide memory, RSS, CPU, disk, or
I/O limits; choose shares across unrelated work classes; or define an async
scheduling policy. Those claims require the concrete owners and process
boundaries that consume this primitive.
