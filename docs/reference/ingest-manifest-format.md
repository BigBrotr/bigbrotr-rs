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
| 2 | 2 | Key schema: `1` |
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

`apps/bigbrotr/tests/rocks_manifest.rs` uses independently specified literal
bytes, nonuniform identities, unsigned boundary coordinates, empty and full
namespace sets, every truncation, extra bytes and changed-field refusal cases.
These codec checks do not establish the store's open, exclusion, commit or
recovery guarantees. Those require the actual private engine owner and practical
acceptance in [P02-T02](../implementation/02-storage.md#p02-t02--atomic-rocksdb-admission-and-exact-identity-inventory).
