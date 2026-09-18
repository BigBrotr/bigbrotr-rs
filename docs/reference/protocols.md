# Signed-event verification

The current standalone command verifies one UTF-8 Nostr event read from standard
input through EOF and writes its complete normalized JSON to standard output:

```sh
cargo run --quiet --locked -- event verify < event.json > verified.json
```

It creates no state and makes no network request. A successful signature does not
establish source policy, visibility, durable admission or a local observation.
The process waits for the input producer to close stdin; Ctrl-C terminates it.
Failures produce a bounded reason on stderr without echoing hostile input. Exit
codes are 0 verified, 1 I/O failure, 2 command usage, 3 invalid event and 4 excluded
by an input limit. A failed output can be partial and must never be published as
an export; callers must check the exit status.

The pinned [NIP baseline](../../contracts/protocol/versions.json) is
`a2494f4f81d46684e5814a9bf35e2b1df978f955`, verifier
`bigbrotr-nip01-verifier/1`. Only event verification is advertised. Transport,
filters, endpoints, cache, durable admission and general NIP conformance remain
separate P01/P02 responsibilities.

The parser requires an object with all seven fields in the
[structural schema](../../contracts/protocol/nip01-event.json). It rejects duplicate
signed fields, including escaped spellings of the same key, non-UTF-8 text,
non-scalar Unicode in signed strings, missing fields, wrong types, integer
overflow/fraction/exponent tokens and non-lowercase or wrong-length hex. NIP-01
requires each tag to contain at least one string; an empty tag list is valid,
but `[[]]` is not. Empty names/values, one-string tags, duplicate tags and their
order are preserved. Content remains text even when it looks like JSON.

Unknown top-level values are bounded and skipped, as selected by the
[identity contract](../specification/identity-and-encoding.md#compatibility-before-writes).
They are absent from output and hashing. Duplicate unknown names are not given
domain semantics; their escaped contents are skipped rather than interpreted as
domain strings. Exact wire retention is a later separately enabled facility.
JSON Schema is a structural description, not a signature or duplicate-key oracle.

Before Serde expands containers, an allocation-free scan checks the 2 MiB wire
cap, depth 32, 65,536 nodes and 2 MiB encoded-string cap. Keys, containers and
scalars count, including unknown values. The parser working estimate is
`3 * wire_bytes + 96 * nodes`, checked against 8 MiB with checked arithmetic.
It covers owned strings, escaped-string scratch and Vec minimum/geometric growth
for this exact model on 64-bit targets; it is not an RSS or cgroup limit. Changes
to representation/parser require revisiting it. The future P01-T05 runtime must
also reserve wire, allocator/native overhead, concurrency and completion budgets.

The complete normalized seven-field event must fit 256 KiB. Verification streams
the six-element NIP-01 preimage into SHA-256, compares the supplied ID, then checks
the actual signature/public key against the computed 32-byte digest using
`secp256k1` 0.33.1. No event-ID cache shortcut exists. A private `VerifiedEvent`
constructor prevents deserialization from manufacturing that marker. Two valid
signatures over the same payload both verify; later admission owns scope-specific
representative selection. Expiry, replacement and deletion do not reject retained
historical originals at this boundary.

Internal consumers that already own a `SignedEvent` can call
`verify_owned(event, max_event_bytes)`. It moves the existing buffers into the
verified result and uses the same normalized-size, ID and signature checks as
the JSON entrypoint. A checked lower bound on content, tag containers and strings
precedes JSON escaping and hashing; even empty elements consume the work bound.
The positive byte limit applies to normalized event JSON, including escaping.
This interface does not parse a wire message or reserve memory for an input that
already exists. Its caller retains the resource grant for all owned capacities,
including excess Vec/String capacity, through verification and downstream work.
It creates no source, visibility or admission authority.

The [frozen historical corpus](../../experiments/identity-contract/signed-fixture.json)
provides 15 variants/14 IDs, including two valid signatures for one ID. The
[extreme fixtures](../../tests/fixtures/protocol/signed-extremes.json) add timestamp
zero/u64 maximum, kind 65535, Unicode/control characters and a cryptographically
valid but structurally invalid empty tag. Their independent Python author/check
script uses python-secp256k1 0.14.0 and the separate research BIP340 equation
checker; it is test tooling, never product cryptography. Ordinary verification
does not regenerate expected bytes:

```sh
python3 tests/fixtures/protocol/signed_extremes.py
cargo test --locked --test protocol_boundary -p bigbrotr
cargo test --locked -p bigbrotr-protocol
```

These tests exercise the real process plus literal canonical byte comparisons,
known-ID tampering, malformed fields, unknown extensions and resource exclusions.
The controlled WebSocket peer scenario P01-T03 step 7 is still pending, as are
P01-T02's actual storage/columnar mappings and P02's durable hostile-input replay.
