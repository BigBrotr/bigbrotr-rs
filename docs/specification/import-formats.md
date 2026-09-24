# Import format contract and Pensieve source inspection

Status: selected adapter design with a bounded captured-JSONL source/reader
prerequisite; full product import and interoperability acceptance remain open.
P03-T08/T09/T10 own import preflight, decoding and recovery;
P08-T04 owns export. The [data contract](data-contracts.md) remains authoritative
for the admitted event, clocks, claims and signed fidelity.

## Selected formats

| Format identity | Interpretation | Recovery unit |
|---|---|---|
| `nostr-jsonl/1` | One complete NIP-01 event object per UTF-8 line; no float conversion of integers | File digest and completed line boundary; compressed streams may require replay from a declared compression unit |
| `bigbrotr-archive/1` | Versioned manifest, event-payload-value/1 plus permitted event-eligibility/1, typed evidence families, dictionary/document references and source identities | Immutable manifest/file digest plus complete row group; Receipt or sufficient ExistingProof dependency checkpoint before advancing |
| `pensieve-delimited/a89cec2` | Unsigned varint byte length followed by one protobuf event; repeated until exact boundary EOF | File digest plus byte offset immediately after a complete admitted message |
| `bigbrotr-analysis/1` | Explicitly projected analytical rows, including omitted signed fields | Standalone DuckDB analysis/export only; canonical importer rejects it |

The three full-event import adapters yield a bounded untrusted signed-event
candidate and then invoke
the independent canonical-ID and Schnorr verifier. A parsed format is not proof
of a valid event. No adapter trusts an archive's stored verification flag.
Reimport preserves external source claims as claims and separately records local
first acquisition; it never invents a local relay witness. Disabled evidence
families follow `import.claims` and provenance policy before their interpretation.

The native full-event value has no original internal event ordinal/admission
envelope; its body digest plus permitted scope representative reassembles all
seven Nostr fields. A public export must not disclose private-first signature,
time, envelope or dictionary through payload reuse. Import maps source visibility
under explicit local policy, retains attributed source eligibility as a claim,
and establishes local eligibility independently. Unknown required eligibility
or value schemas fail preflight before any progress; see
[identity and encoding](identity-and-encoding.md#export-imports-replay-and-recovery-closure).
CertifyExisting may cover an already retained event's dependencies; separate
sidecar claims still require their own AdmitEvidence outcome.

Projected artifacts do not create a second in-product dataset catalog or importer.
P08-T04 owns their standalone DuckDB guide. Research imports retain kind-5
deletion requests and original targets as signed events; deployment administrative
controls belong to the distinct backup/restore manifest. Unknown mandatory
control-bearing formats fail compatibility preflight. The destination always
enforces its current admission restrictions.

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

The JSONL unit is a LF-terminated line or the nonempty final unit at exact file
EOF. CR in CRLF remains JSON whitespace and counts toward the wire cap; positions
and raw-record digests include the complete delimiters. Blank lines are invalid
records and an empty file has zero records. A safely framed oversized/malformed
unit may be classified while continuing; I/O failure cannot become a completed
rejection or advance a checkpoint. [Captured-source implementation](../reference/import-source.md)
defines the current ephemeral storage and allocation boundary. Full job authority,
destination policy and durable obligation closure remain separate prerequisites.

The initial exact-ID signed export writes `nostr-jsonl/1` as a compact object
with fields in `id`, `pubkey`, `created_at`, `kind`, `tags`, `content`, `sig` order,
followed by exactly one LF. Integers retain their full unsigned domain; tag order,
duplicates, empty strings and Unicode spelling remain unchanged. JSON escaping
preserves values, without Unicode normalization. It uses the selected scope's
verified signature representative, never a payload's unrelated first signature.
No acquisition, receipt, provenance, deployment or scope fields are added to the
signed event object. Those interpretations and excluded families belong in the
artifact manifest. An authorized absence at the selected cut produces an explicit
zero-row, zero-byte file and its SHA-256, not an omitted or failed output.

The byte limit includes LF and is checked before writing the row. Encoding streams
the already verified event without another complete JSON buffer and hashes the
bytes actually accepted by the output writer. Cancellation or an output failure
returns no successful file description, even after a prefix was written. The
file description is only an encoding result: the executor retains custody and
budgets through file/directory synchronization and final manifest/checkpoint
publication. A successful encoder test does not establish those durable effects.

For the length-delimited protobuf adapter:

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

### Initial signed JSONL artifact

The private exact-ID artifact uses manifest identity
`bigbrotr-signed-events-export/1`, distinct from a full native archive, analytical
projection or deployment backup. Its `ArtifactId` is a newly allocated nonzero
bytes16 identity, stored with the job's execution unit before any output dispatch;
restart retains it. It is not a permission, digest, job ID or user-supplied path.
The [closed JSON Schema](../../contracts/artifacts/v1/export.schema.json) owns
the emitted field shape; its runtime-check descriptions are part of the profile.

`manifest.json` describes one `events.jsonl` file (`file_id="events"`,
`format="nostr-jsonl/1"`). The manifest includes artifact/job identities, complete
source cut including zero prefixes, original catalog/frontier/snapshot, exact
event filter and qualified visibility reference, and the accepted configuration
and policy revisions. Accepted revisions describe original intent; they are not
current execution authority. Counters and revisions use canonical decimal
strings; the native snapshot is positive and bounded by signed BIGINT, while
source counters retain their unsigned domain. The signed Nostr integers inside
JSONL remain JSON numbers. File length,
row count and conventional SHA-256 cover the exact bytes including LF. The
manifest does not include its own digest; the artifact inventory retains that
digest over the complete original manifest bytes.

This profile states `visibility="private"`, `fidelity="full-signed-events"` and
`origin="retained-scoped-archive"`. It includes complete signed events only.
It explicitly excludes internal payload envelopes, eligibility facts, first-local
receive metadata, relay provenance, imported claims, measurements, documents,
dictionaries, coverage, derived results and deployment state. Its qualified
visibility reference attributes the source selection; it is not a standalone
dictionary inventory or evidence of acquisition origin. Verifying the complete
Nostr objects requires no external dictionary or document. Other export formats
still require their selected dependency closure.

The initial manifest is bounded to 256 KiB and 1024 source streams. It names exactly
zero or one complete row, with an explicit empty file digest for zero rows. The
executor must keep the final manifest unavailable until the data file and its
directory entries are synchronized. It then publishes the manifest atomically,
synchronizes its directory and records the same file/manifest identities in the
job checkpoint. File existence alone is neither SQL completion nor public
publication. Recovery resolves the same unit against retained bytes and current
authority; it never replaces a conflicting completed artifact with new intent.
The filesystem unit implements the following bounded local protocol; the SQL
execution claim, inventory checkpoint and supervised runtime consumer remain
separate integration requirements, not guarantees supplied by the encoder.

#### Local output and retry

Before dispatch, persist the original unit's artifact identity, destination
binding and retained byte reservation. Resolve any ambiguous SQL completion
before resuming: a completed unit must not be rewritten. Reconstruct expected
bytes from the original pinned cut under current authority, including an actual
authorized absence for an empty export. A self-consistent old manifest is not
the expected result.

The private destination holds three fixed names per artifact: its final hex
identity directory, `.artifact-<id>.partial` and `.artifact-<id>.lock`. The empty
lock file is a permanent mutex, never authority, and is not renamed or removed.
Acquire its exclusive nonblocking lock before output; retain its descriptor and
the original destination beyond cancellation through all actual worker and SQL
driver joins. Only one write handle can exist under that custody. A failed
acquisition can create an empty lock but never repairs existing contents.

Validate both existing directories before any removal. The final must contain
exactly the two complete expected files. Staging may contain either, both or
neither; each retained file must be a byte prefix of the same expected output.
An incomplete manifest is never a receipt. Refuse unknown names, differing bytes,
links, nonregular files, foreign ownership, permission conflicts or aliases,
preserving those objects. Directories are private `0700`; files are `0600` with
one link. Linux additionally requires descriptor-based `statx` mount identities
to refuse same-filesystem bind aliases; missing mount identity fails closed.
This local output capability requires that kernel/filesystem support (Linux
5.8 or later for `STATX_MNT_ID`), independent of the rest of the product's kernel
requirements. See [statx](https://man7.org/linux/man-pages/man2/statx.2.html).

Under the durably reserved namespace and exclusive custody, remove only the
verified staging entries and directory, then synchronize the destination. Create
a fresh staging directory and new files, write the complete data and manifest,
synchronize both files and staging directory, and publish with atomic
`NOREPLACE` directory rename. If a prior nonterminal final exactly matches the
independently reconstructed output, use `EXCHANGE` instead, leaving the old
directory at the staging name. Synchronize the destination before returning a
file result. An error after either rename retains the same identity for retry;
rebuilding on fresh inodes reissues the writes instead of merely repeating fsync
on an inode that may have suffered writeback failure. Neither operation permits
automatic repair of differing bytes or arbitrary corruption.

There is at most one staging copy, including after interrupted cleanup or
exchange. Reserve two complete data-plus-manifest logical byte counts; include
filesystem metadata/allocation overhead and retained output in the executor's
disk accounting. This bound is not a physical quota. The manifest allocation is
at most 256 KiB, I/O buffering is 64 KiB plus an 8 KiB comparison scratch buffer;
the executor also retains the verified event/native custody. It never collects
another complete JSONL byte buffer. Cancellation gates each new writer call,
including short-write retries during buffer flush, and withholds the result even
after a complete rename. The executor also supplies its nonrenewable action/pin
window to those synchronous checks: Control renewal or a delayed supervisor
must not extend authorization of the next write. Recheck after a potentially
blocking destination verification before rename. A call admitted before expiry
may still complete afterwards; refuse subsequent payload writes, new manifest
publication and successful completion. Synchronization of an already admitted
namespace change may finish while custody remains held. A blocked kernel call still requires supervised process
termination and retained obligations, not an assertion that AtomicBool aborts I/O.

Normalize permissions only on newly created names under private namespace
custody; do not change process-global umask or repair an existing nonconforming
path. A crash before this normalization can require explicit operator recovery.
Concurrent hostile mutation by the same OS user/root is outside the local
custody boundary. Kernel syscall, crash and restart qualification must exercise
these paths on the supported installation filesystem; Mac file tests alone do
not establish native Linux or power-loss durability.

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
