# Initial native dictionary batch

This is the bounded initial write slice of P02-T02. The selected event admission,
producer retirement, archive, handoff and installed runtime remain separate work.
No public endpoint or `serve` role exposes this operation yet.

`ControlSession::bootstrap_initial_dictionaries` connects the actual dedicated
Control backend to an exclusively open `IngestStore`. The actual locked directory
must match the trusted factory's selected absolute static root; canonical aliases
are compared after filesystem resolution. Roots must remain operator-controlled
and stable while open. It checks the complete
initial registration against the native manifest, including dataset, resource,
incarnation, owner epoch, shard, topology, policy and allocation domains. It
validates both complete current policy documents through the trusted caller's
root validator, requires the actual PostgreSQL Control fence and retains the
registry row locks through native completion. The producer epoch must still be
open. A plain registration observation or manifest cannot invoke the internal
native primitive from outside the storage module.

The suspend-aware policy deadline starts before PostgreSQL I/O and cannot exceed
five seconds or either owner's remaining scheduling lease. Checks occur before
native inspection, immediately before synchronized dispatch and before receipt
delivery. The native directory lock and serialized mutable owner, rather than a
scheduling timeout, supply local writer exclusion. This first-source operation
does not authorize another directory, successor, restored generation or takeover.
The caller must run it on a retained bounded owner worker and join that worker,
its database driver and native close before releasing resources or replacing it.
Dropping an async future cannot cancel native FFI.

## Closed initial state

Only producer epoch one, request sequence one, owner epoch one and admission
policy version one are accepted. The store must contain exactly its manifest,
or exactly the complete retained initial batch being retried. Dictionaries are
caller ordered, with ordinals contiguous from one in each registered namespace.
The first visibility mapping may reference itself. Every other envelope or
descriptor dependency must precede its consumer in the batch. Duplicate complete
descriptors and event dictionaries are refused; event identity must be admitted
with its verified payload by the later event operation.
Endpoint descriptors must exactly equal the existing protocol normalizer's output;
the storage owner rejects structurally valid but noncanonical URL identities.

One WAL-enabled `WriteBatch`, with synchronous commit and missing-CF refusal,
contains every dictionary fact, mapping, reverse candidate, allocation/source
counter, producer state, pending marker and request receipt. It also rewrites the
unchanged manifest. There is no partial write before this batch. `recorded_at` is
the trusted owner's supplied time; retry preserves the originally recorded time,
context, dependencies and receipt bytes.

A retry reconstructs and compares the **entire** initial key/value state before
returning the retained result. Missing or changed members and unexpected keys
fail closed. In particular, missing receipt detail with other persisted effects
is not rollback. Later populated stores require the general receipt-resolution
operation, not this bounded initializer. A changed digest for the same retained
request returns RequestConflict.

## Persistent values added by this slice

Integers are unsigned big endian. All keys retain the
[version-one key format](ingest-key-format.md). Existing manifest, fact, pending
and receipt formats are unchanged.

| Domain | Value bytes |
|---|---|
| Allocation stream | `01 03` + highest committed ordinal BE64 |
| Dictionary mapping | `01 04` + full SourceRecordId (36 bytes) + fact payload digest (32 bytes) |
| Reverse candidate | `01 05` + complete canonical descriptor bytes |
| Producer state | `01 31` + highest issued sequence BE64 + contiguous acknowledged-through BE64 + retired-through BE64 |

The initial producer state is issued=1, acknowledged=0, retired=0. These fields
do not implement ACK, GC or epoch retirement. An absent allocation counter means
no mapping has yet been committed in that registered namespace, not an available
unregistered namespace or permission to infer rollback from a damaged store.

Canonical descriptor bytes are deterministic CBOR
`[normalization_version, descriptor_schema, descriptor_version, descriptor]`,
using exactly the closed descriptor encodings in the
[dictionary contract](../specification/dictionary-descriptors.md). They omit the allocated
reference, unlike the existing dictionary fingerprint. The reverse candidate
key hashes these bytes with SHA-256 only as an operational accelerator; complete
descriptor equality is always required. No scientific fingerprint changes.
The public visibility descriptor literal is
`84016a7669736962696c69747901a165636c617373667075626c6963`.
Future interpretations of these persistent layouts require an explicit version
and migration, including any future producer-state extension.

## Resource and outcome boundary

The caller reserves its input buffers and bounded PostgreSQL documents. A
separate transferred permit covers staged values, transient encodings, returned
receipt and native WriteBatch allocation. Limits are positive and at most 256
mappings and 16 MiB per serialized batch; deployments may select smaller limits.
These are implementation limits, not throughput or RSS claims. Key/value sizes
and conservative native framing are summed before any native batch insertion;
the binding's capacity argument is not treated as an enforced maximum.

The permit remains retained by the returned receipt. Native read pins and raw
iterators never escape the call. An engine write error poisons further writes on
that handle and returns CommitUnknown. Failure/cancellation after dispatch or
during PostgreSQL cleanup never becomes a negative durability result. A failed
operation after coordination begins poisons the Control session; a fresh runtime owner/recovery operation
must establish its own authority. Durable initial replay alone does not supply
that successor protocol.

The isolated `ingest_bootstrap_probe` invokes the connected adapter and separately
dumps actual native key/value bytes. It validates a frozen policy fixture; it is
not the installed root validator or a product startup entrypoint. Executed
evidence must separately qualify this client, actual engines, crash schedules
and external byte oracle. Compilation or Mac unit tests do not qualify those
native scenarios, power loss, disk exhaustion, maximum load or Linux arm64.
