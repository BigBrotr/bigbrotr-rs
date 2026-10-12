# Ingest manifest value format

The singleton `01 01` [ingest key](ingest-key-format.md) in `default` names this
closed binary value. The manifest binds an Ingest store to an exact control
identity and current allocation namespaces. It does not grant ownership, qualify
a protection class or prove that any event was admitted. The owner must compare
it with current control authority before enabling work.

All offsets are zero-based, lengths are bytes, and integers are unsigned
big-endian. There is no padding, text encoding or optional trailing extension.

| Offset | Width | Meaning |
|---|---|---|
| 0 | 1 | Value format: `1` |
| 1 | 1 | Value type: Ingest manifest, `1` |
| 2 | 2 | Key schema: `1` initially; `2` for an explicitly prepared successor |
| 4 | 2 | Value schema: `1` |
| 6 | 2 | Fingerprint profile: `1` |
| 8 | 2 | Native option profile: `1` |
| 10 | 16 | Nonzero dataset ID |
| 26 | 8 | Topology generation |
| 34 | 16 | Nonzero owner resource ID, matching `control.owner_epochs.resource_id` |
| 50 | 4 | Source shard ID |
| 54 | 8 | Nonzero owner epoch |
| 62 | 16 | Nonzero process incarnation |
| 78 | 16 | Recovery generation |
| 94 | 8 | Policy epoch |
| 102 | 1 | Protection spelling: `1` for LocalSynced |
| 103 | 1 | Current allocation namespace count, `0..9` |
| 104 | 18 × count | Repeated dictionary kind BE16 and nonzero allocation domain bytes16 |

The complete length is `104 + 18 × count`, at most **266 bytes**. Allocation
kinds use the assigned values 1 through 9 from the
[identity contract](../specification/identity-and-encoding.md), in strictly
increasing order. Unknown kinds, duplicate kinds and reordered entries fail.
An empty set is distinct from a fabricated allocation domain. Each kind has at
most one current domain within this store; independently fenced owners may use
disjoint domains. Old domains and their mappings remain retained under their own
keys and control records across handoff or restore.

Topological and policy coordinates can encode zero; the recovery identity accepts
all bytes16 values under the portable model. Encoding these values is not evidence
that control has registered or authorized them. The source owner epoch starts at
one. No integer is narrowed through signed arithmetic.

Source high watermarks and allocation ordinals belong to their domain-specific
keys and the same synchronized batch as their effects. They are not duplicated
in this manifest. Producer registry generation is not invented here: slots cannot
be removed or reused until a real registry-fence transition is implemented and
the affected control/local schemas are versioned together.

## Compatibility and validation

The decoder requires exact length and all five version/profile coordinates plus
the type byte. Unknown protection spellings and zero allocated identities fail.
Both the manifest and its encoded output use fixed maximum storage; decoding
rejects oversized input before processing entries and allocates no heap buffer.
Future formats require explicit migration; reopening must never rewrite an
unknown manifest as the current one.

Key schema 2 adds the immutable owner-handoff key below. Existing keys retain
their exact version-1 bytes. Changing this coordinate in the same synchronized
batch as the handoff fences old implementations before they can open for work;
it does not grant the new incarnation admission. Fresh initialization retains
schema 1. A successor manifest is created only by the explicit handoff protocol.

## Initial owner handoff receipt

The default-family key is `01 0a || dataset_id[16] || owner_resource_id[16] ||
predecessor_owner_epoch:BE64` (42 bytes). Its immutable value is:

| Order | Width | Meaning |
|---|---|---|
| 1 | 2 | Value version/type `01 0a` |
| 2 | 16 | Stable producer slot ID |
| 3 | 8 | Predecessor producer epoch |
| 4 | 8 | Closed producer issued-through request |
| 5 | 8 | Closed source last-complete sequence, including zero |
| 6 | 32 | Positive initial source-closure digest |
| 7 | 2 + length | Previous manifest byte length BE16 and exact bytes |
| 8 | 2 + length | Successor manifest byte length BE16 and exact bytes |

Maximum value length is 610 bytes; no trailing extension is permitted. This
initial-only transition retains dataset/resource/shard/recovery identity and
producer slot, advances owner/producer epochs 1→2 and topology by exactly one,
and uses a fresh incarnation and fresh distinct allocation domains for the same
kind set. PostgreSQL checks freshness against retained registry identities,
including historical domains. Old manifests remain byte-exact because original
producer reservations bind their digest.

After confirmed PostgreSQL preparation under current Control authority, the
exclusive native recovery owner writes exactly one synchronized batch: this
receipt, the new singleton manifest, an explicit zero successor source counter
(`01 02` plus BE64 zero), and an explicit OPEN successor producer marker
(`01 31` plus three BE64 zeros). All predecessor keys and original inputs remain
unchanged. Preparation does not activate a writer, rerun dictionary bootstrap,
acknowledge requests or reclaim anything.

The native plan is constructed only from the positive initial source witness;
it is not an externally decoded authorization. Its local directory device/inode
binding and Rocks sequence guard reject a different store and detect intervening
writes between proof and application. These local guards are not receipt fields;
operator-controlled roots must remain stable. Reapplying the same
retained plan requires the exact receipt, manifest and complete empty successor
state and performs no write; conflicting or partial state refuses. This local
plan can survive closing/reopening the native handle within the same caller.
The bounded historical adapter below reconstructs only an observed completed
preparation. Completing an abandoned empty incarnation and activating a fresh
actual writer remain separate required adapters. An
observer or a durable prepared row cannot impersonate that writer. Until explicit
activation is implemented, PostgreSQL refuses successor reservations and input
bindings. This format is an implementation contract, not installed qualification.

`HistoricalInitial` is an opaque borrowed read context. Decoding the retained
handoff grants no authority: the actual singleton, handoff, explicit zero source,
OPEN/zero producer, empty target namespaces and absent target input sidecars must
all match. Reads use the retained predecessor manifest only for historical
interpretation. Every positive request witness is bound to its actual native
directory device/inode and Rocks sequence before entering a source accumulator.
Matching portable identities do not permit combining observations across stores.

Reconstruction replays every original input and effect, permits precisely the
original and prepared producer markers, then checks the recomputed closure/count/
cut before rebinding a local guard. It writes no native key or input. A changed
sequence invalidates a previously read witness even when current bytes match.
Its bounded decoder and close/reopen tests are candidate implementation, pending
execution; separate-process and connected authority evidence are tracked in the
[recovery report](../implementation/evidence/p02-producer-recovery-native-20260920.json).

`apps/bigbrotr/tests/rocks_manifest.rs` uses independently specified literal
bytes, nonuniform identities, unsigned boundary coordinates, empty and full
namespace sets, every truncation, extra bytes and changed-field refusal cases.
These codec checks do not establish the store's open, exclusion, commit or
recovery guarantees. Those require the actual private engine owner and practical
acceptance in [P02-T02](../implementation/02-storage.md#p02-t02--atomic-rocksdb-admission-and-exact-identity-inventory).
