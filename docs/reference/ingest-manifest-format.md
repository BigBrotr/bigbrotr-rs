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
The bounded historical adapter below reconstructs an observed completed
preparation. The existing-gap adapter also reconstructs a positive old-state plan
from original history and exact PostgreSQL intent. Both native preflight and
dispatch reject retained successor input files, including files added without a
Rocks sequence change. Retiring an abandoned empty incarnation and activating a
fresh actual writer remain separate required adapters. An
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
Seven focused decoder/close-reopen tests pass in exact-source build39e7468, with
affected original replay regressions. Harness e92f3d7 additionally qualifies a
genuinely new process reconstructing the prepared signed history under fresh
actual Control, with no native read-phase writes. Its seven connected cases and
remaining activation gates are tracked in the
[recovery report](../implementation/evidence/p02-producer-recovery-native-20260920.json).
Build162caeb adds two independent native gap tests: fresh complete original replay,
literal four-write completion, immutable completed retry, and target-input refusal
before proof, after proof and during completed replay. Connected51a6b86 qualifies
completion of an actual PG/native gap in a new process and repeats the affected
completed-history cases. No writer activation or installed restart follows.

`apps/bigbrotr/tests/rocks_manifest.rs` uses independently specified literal
bytes, nonuniform identities, unsigned boundary coordinates, empty and full
namespace sets, every truncation, extra bytes and changed-field refusal cases.
These codec checks do not establish the store's open, exclusion, commit or
recovery guarantees. Those require the actual private engine owner and practical
acceptance in [P02-T02](../implementation/02-storage.md#p02-t02--atomic-rocksdb-admission-and-exact-identity-inventory).

## Repeatable owner handoff (selected, not yet implemented)

The [repeatable succession protocol](ingest-registration.md#repeatable-ingest-succession-selected-not-yet-implemented)
selects manifest key schema3 for owner epochs3 and later. Its manifest has the
same exact field widths and maximum266 bytes; all other version/profile coordinates
stay1. Fresh initialization stays schema1. The initial1-to2 receipt and schema2
manifest retain their exact bytes and decoders. Earlier schema1/2-only binaries
refuse schema3; opening a supported manifest still grants no admission authority.

Use the existing42-byte owner-handoff key `01 0a || dataset_id[16] ||
owner_resource_id[16] || predecessor_owner_epoch:BE64`. Its identity and ordering
do not change. The key with predecessor1 retains value version1. Keys with
predecessor n>=2 use the following closed version2 value, never reinterpretation
of an existing value:

| Order | Width | Meaning |
| --- | --- | --- |
| 1 | 2 | Value version/type `02 0a` |
| 2 | 1 | Predecessor terminal kind:1 activated and retired;2 prepared, unactivated and retired empty |
| 3 | 16 | Stable nonzero producer slot ID |
| 4 | 8 | Predecessor producer epoch n, matching the key's owner epoch |
| 5 | 8 | Predecessor issued-through request count |
| 6 | 8 | Predecessor closed source last-complete sequence |
| 7 | 32 | Predecessor's positive terminal closure digest |
| 8 | 32 | SHA-256 of the exact incoming handoff value that introduced predecessor n |
| 9 | 2 + length | Previous manifest length BE16 and exact bytes |
| 10 | 2 + length | Next manifest length BE16 and exact bytes |

The fixed portion is111 bytes; the total is `111 + previous_len + next_len`,
between319 and643 bytes. Reject unknown kinds, truncation, extensions and lengths
outside the closed manifest bounds before allocation. Both codecs use fixed
maximum storage. A decoded record is data only, with no conversion to a native
proof, preparation authority or writer.

Require n>=2, matching predecessor owner/producer epochs and exact checked n+1
for the target. Previous key schema is2 exactly when n=2, otherwise3; next schema
is3. Topology advances exactly one without overflow. Dataset/resource/shard/recovery
identity and producer slot remain unchanged. The target incarnation differs from
the predecessor; allocation kind sets match and target domains are distinct from
one another and every predecessor domain. The connected lineage validator also
checks incarnation/role/domain freshness against all earlier retained identities.
The format can encode the existing namespace subsets; the installed profile
independently requires its complete nine-kind registry shape.

Issued count is at most64 in this profile. Terminal kind2 requires issued0 and
source0, absent activation and absent target input/detail; kind1 permits both zero
and nonzero counts, but issued0 requires source0. The scalar bytes do not prove
those absences or the terminal digest. Validate them against the real native state,
exact registry and positive input/effect replay. Legacy epoch2 uses its existing
activated or empty digest unchanged.

For2-to3, the incoming hash names the exact retained `01 0a` value at predecessor1.
For n-to(n+1) with n>=3, it names the exact `02 0a` value at predecessor n-1.
Adjacent manifests must match byte-for-byte: a parent's next is the child's
previous. Verify the whole contiguous chain and complete key census; a matching
hash at the head is not sufficient. The compact frontier is reconstructed from
all retained terminal cuts, including zeros. It is not duplicated in every
receipt. The [profile limits](ingest-registration.md#resource-admission-and-qualification)
bound lineage memory and retained topology growth independently of the scalar
integer encoding.
Require the complete key-epoch/value-version matrix, not a scan filtered to one
value version: predecessor1 is exactly the legacy version1 record; every later
predecessor is version2. A schema1/2 singleton with a later native receipt is an
incompatible downgrade, never a shorter valid history. A new SQL lineage tail
also refuses except for the explicit prepared/native-old gap: exactly one
immediately following preparation, no activation, no target native marker/receipt/
input, and native state exactly at the positively verified predecessor. This
exception belongs only to the new-family gap loader, including native2 with
prepared3; legacy loaders still refuse that new SQL shape. The same exact pending
gap rule applies at later schema3 heads. Longer, activated or mixed tails refuse.
Parent hashes cover the complete retained value bytes; permissive decode/re-encode
is not an alternative representation for hashing.

For activated sources at epochs>=3, the new closure digest is SHA-256 over:
`bigbrotr/ingest-successor-source-closure/1` followed by NUL, the SHA-256 of the
exact incoming handoff value, the SHA-256 of the exact current manifest, slot16,
epoch BE64, issued count BE64, then each ordered original request's sequence BE64,
input digest32 and SHA-256 of its exact native outcome-copy bytes, finally the
closed source cut BE64. The reducer positively validates each original effect and
all dependencies before incorporating it; issued and resolved counts must match.

For an unactivated empty attempt at epochs>=3, use SHA-256 over
`bigbrotr/ingest-successor-empty-closure/1` followed by NUL, the SHA-256 of the exact
incoming handoff value, the SHA-256 of the exact target manifest, slot16, epoch
BE64, issued0 BE64 and source0 BE64. Recompute only after positive predecessor
lineage proof and exact empty target inspection. Empty and activated-zero digests
have distinct domains. This definition adds no ACK/GC or permission to reuse an
incarnation.

After confirmed PostgreSQL preparation, the native operation writes exactly the
new receipt, schema3 singleton and explicit zero source/producer markers in one
WAL-enabled synchronized batch. Existing-plan retry requires the exact empty
target state and makes no write. Root device/inode, native sequence and separately
checked retained input census remain local proof guards, never portable receipt
fields. A fresh process reconstructs the recorded attempt from positive history
and resolves it; it cannot activate the lost candidate. Formats and profile limits
are selected here, while implementation, native/SQL boundaries and installed
qualification remain NOT_RUN.
