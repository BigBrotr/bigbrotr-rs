# Import format contract and Pensieve source inspection

Status: selected adapter design; product adapters and interoperability acceptance
are not implemented. P03-T08/T09/T10 own import preflight, decoding and recovery;
P08-T04 owns export. The [data contract](data-contracts.md) remains authoritative
for the admitted event, clocks, claims and signed fidelity.

## Selected formats

| Format identity | Interpretation | Recovery unit |
|---|---|---|
| `nostr-jsonl/1` | One complete NIP-01 event object per UTF-8 line; no float conversion of integers | File digest and completed line boundary; compressed streams may require replay from a declared compression unit |
| `bigbrotr-archive/1` | Versioned manifest, typed Parquet families, dictionary/document references and source identities | Immutable manifest/file digest plus complete row group; canonical receipt before advancing |
| `pensieve-delimited/a89cec2` | Unsigned varint byte length followed by one protobuf event; repeated until exact boundary EOF | File digest plus byte offset immediately after a complete admitted message |
| `bigbrotr-analysis/1` | Explicitly projected analytical rows, including omitted signed fields | Named analytical dataset only; never a full-event admission path |

Adapters yield the same bounded untrusted signed-event candidate and then invoke
the independent canonical-ID and Schnorr verifier. A parsed format is not proof
of a valid event. No adapter trusts an archive's stored verification flag.
Reimport preserves external source claims as claims and separately records local
first acquisition; it never invents a local relay witness. Disabled evidence
families follow `import.claims` and provenance policy before their interpretation.

## Pensieve: inspected source, not assumed compatibility

[Source hashes and read scope](../research/evidence/pensieve-format-source.json)
identify the complete files read without building or running them.
The inspected producer revision is
`a89cec26ec250ba3a23da7d6a9b1efaf70bce838`. See its
[schema](https://github.com/andotherstuff/pensieve/blob/a89cec26ec250ba3a23da7d6a9b1efaf70bce838/docs/nostr.proto)
and [decoder](https://github.com/andotherstuff/pensieve/blob/a89cec26ec250ba3a23da7d6a9b1efaf70bce838/crates/pensieve-core/src/proto.rs).
The selected adapter follows these wire facts:

| Field number | Meaning | Wire/schema type and admission rule |
|---|---|---|
| 1 | Event ID | String; decode exact 32-byte hex ID |
| 2 | Author | String; decode exact 32-byte hex public key |
| 3 | Created time | Signed int64, ordinary protobuf varint; accept 0 through i64 maximum, reject negative values without wrapping |
| 4 | Kind | Signed int32; require the selected NIP-01 kind range before conversion |
| 5 | Ordered tags | Repeated nested message; each has repeated string values at field 1; preserve order, duplicates, short tags and empty strings |
| 6 | Content | UTF-8 string; empty is a legitimate signed value |
| 7 | Signature | String; decode exact 64-byte signature, then verify |

The producer's convenience batch message is not its primary storage framing.
The adapter does not guess a batch by trying several parsers until one succeeds.
Format identity is explicit and unsupported future changes fail preflight.

The producer uses proto3 implicit scalar presence. An omitted content field and
an encoded empty string both decode to an empty string; the ordinary decoded
message does not preserve their distinction. The default value is valid if the
reconstructed event's ID and signature verify. Missing/default ID, public key or
signature fails the required identity lengths. Empty tags and zero timestamp
are likewise valid values when signed correctly. JSONL still requires its signed
field keys: do not extrapolate protobuf default rules to other formats. This
follows the [protobuf presence contract](https://protobuf.dev/programming-guides/field_presence/).

BigBrotr's canonical timestamp supports u64, but this producer format's positive
int64 domain is smaller. Export or conversion to this format must refuse values
above i64 maximum with a precise incompatibility; it cannot clamp or silently
reinterpret them. Ordinary full Parquet/JSONL exports preserve the larger domain.

## Framing and resource rules

1. EOF before the first byte of the next length is clean completion. EOF after
   any prefix byte is a truncated source, not successful end of file.
2. Bound a u64 prefix to ten bytes and reject overflow in its last byte. Validate
   conversion to platform size and the configured message-byte limit before
   allocating. A valid length prefix is not permission to allocate arbitrary RAM.
3. A short payload is a failed/incomplete unit. The last committed source offset
   remains before that message; never advance progress past a corrupt tail.
4. Bound nested tags, decoded strings, aggregate candidate memory and decompressed
   input before admission. Unknown protobuf fields may be skipped within the
   bounded message; they are not silently appended to signed canonical fields.
5. Apply protobuf scalar/merge semantics through the selected decoder, then verify
   the reconstructed signed event. Reject invalid UTF-8 and malformed wire data;
   do not repair a signed value. Upstream decoder behavior is not the verifier.

Source inspection found a useful negative control: the inspected producer's
length decoder maps `UnexpectedEof` from prefix parsing to normal EOF and allocates
from the decoded length without a local cap. BigBrotr must exercise a partial
prefix and huge declared length through its actual adapter. This is an observation
about this pinned decoder, not an executed finding about the evolving Go project.

## Reuse and distribution boundary

The inspected Pensieve repository declares
[PolyForm Noncommercial 1.0.0](https://github.com/andotherstuff/pensieve/blob/a89cec26ec250ba3a23da7d6a9b1efaf70bce838/LICENSE).
Do not vendor its Rust implementation or schema into BigBrotr under an assumed
permissive license. Select an independently authored interoperability adapter
and fixtures over the wire contract above, using the separately licensed protobuf
runtime. Any proposed upstream code/schema/generated artifact reuse requires
compatible permission in P01's dependency/license inventory before inclusion.
This design does not assert that the upstream license permits such redistribution.

## Required practical evidence

P03-T09's real import suite must contain independently signed fixtures for empty
content, empty/one-element/repeated tags, zero and int64-maximum time, invalid
negative time/kind, and malformed identities. Include empty file, truncated
prefix, truncated body, overlong/overflow prefix and oversized declared message.
The full-format suite separately tests u64 maximum where representable. Run import
through the shipped binary, interrupt after a durable receipt before progress,
resume, and compare full signed fields with an independent verifier/export reader.
An actual upstream-produced permitted fixture establishes interoperability with
that producer version; a self-roundtrip alone does not. No such product result
is claimed by this planning record.
