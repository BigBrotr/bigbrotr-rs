# Original requests and import positions

P03-T10 step3 and P02-T02 step4 require a producer to retain the logical work
associated with an original mutation before dispatch. Additive migration0013
introduces the initial event-only JSONL binding consumer. It is an internal
consistency adapter under the real Control/native owner, not installed import,
operator job acceptance, current source-root authorization or a checkpoint.

An `ImportBatch` describes one contiguous sequence of complete verified JSONL
records in one source shard. Its existing job identity, request digest, expected
job revision, configuration and policy are compared with the running job under
current Control ownership. The source namespace, shard, parser profile1
(`nostr-jsonl/1`), complete source size/SHA256, each logical line, exact half-open
byte range and raw-record SHA256 remain immutable. A namespace digest identifies
an externally retained source manifest; it does not substitute for that manifest
or authorize resume. Full installed job acceptance still needs its complete
source/shape/limits/scope/policy manifest and grants.

This first batch has exactly one strict event-Admit obligation per record.
The complete ordered list is retained, including batch first line/count and
zero-based obligation ordinal. Claim/proof/terminal-rejection variants are not
accepted by this API. Consequently it cannot represent a source requiring a
sidecar or treat its body alone as a complete import record. Such consumers and
multi-obligation record closure remain required before logical checkpointing.
Identical signed events at different source positions remain distinct obligations;
an AlreadyDurable Admit still uses its original mutation request.

The connected adapter stages every original canonical request under the native
owner, then commits all bindings and producer reservations in one PG transaction.
Each binding retains exact producer slot/epoch/sequence, digest, profile/byte
count and native manifest hash; complete recovery bytes remain in the synchronized
co-located input spool. Deferred foreign keys and a complete-batch constraint
refuse missing members, holes, partial reservations and conflicting context.
The initial relation admits at most64 retained bindings per job and the existing
producer/spool window and byte bounds still apply. This is a finite prerequisite,
not the selected large-import capacity or a reclaiming production ledger. The
bridge reserves64KiB additional temporary metadata memory before staging; this
does not reserve physical PG storage or establish peak native memory usage.

Control fence, job, Ingest owner and producer locks follow one order. Source
identity, byte ordering and the finite per-job ledger are serialized by the job
row. Ordinary connected admission checks a bound request's current running job
in both its reservation and native transactions. Cancellation/pause/owner changes
cannot be ignored merely because its reservation already exists. Initial native
operations therefore require migration13 explicitly. Historical unbound bootstrap
and other initial consumers remain distinct; bootstrap request1 never becomes
imported line1.

Exact batch retry compares every retained field. An old reservation lacking its
original binding is refused even when its PG outcome is absent: native completion
may already have occurred. A failed attempt can leave synchronized unreserved
input files; preserve them and retry only exact bytes. A lost binding COMMIT reply
is unknown and poisons the current Control session; no native dispatch has begun
through that call. Resolve through the retained original batch and current owner,
never replace an ambiguous request with certification or a new identity.

All bindings and original input/outcome detail remain retained. No logical
imported prefix, job success, dependency closure, producer checkpoint, ACK, GC or
successor authority follows from these rows. Further work must connect complete
Receipt/ExistingProof and separate claim/exclusion obligations before advancing
whole logical positions, including proof-only progress with a zero or unchanged
mutation watermark. The bare producer_checkpoints digest is insufficient.

The current slice is under implementation. Local shape checks are not native SQL,
connected failure/recovery, upgrade or release qualification. Its practical report
will bind exact source/artifact identities and exercised limits when available.
