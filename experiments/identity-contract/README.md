# Identity and encoding contract probe

Isolated paper-specification research for
[identity and encoding](../../docs/specification/identity-and-encoding.md).
It writes no product code, starts no engine and does not contact Tux or the legacy
repository. Python 3.9+, Node.js and the system `openssl` command are sufficient;
there are no downloaded packages or generated corpus files.

Run from the repository root:

```sh
python3 experiments/identity-contract/probe.py --evidence docs/research/evidence/identity-contract.json
```

Omit `--evidence` to inspect results without changing recorded evidence. The
checked-in [evidence](../../docs/research/evidence/identity-contract.json) records
tool versions, source digests, scenario counts and proof limitations. A failed
assertion exits nonzero before writing a PASS result.

`golden-vectors.json` freezes literal preimage bytes and SHA-256 for all eight
BigBrotr fingerprint domains, the separate Nostr preimage and raw document
digest, plus scalar examples. The certification intent uses the producer-request
domain's expressly reserved proof-only identity. Primitive vectors cover u64
maximum, i64 minimum, every integer width, UTF-8, null/empty distinctions, map
ordering and ordered duplicate tags. `write_vectors.py` documents their explicit
hex assembly and OpenSSL digest authoring; the runner **never regenerates** its
expected values. A Python CBOR encoder and independently authored Node encoder
must both match the frozen bytes; Python, Node and OpenSSL must match digests.
These small research encoders are not proposed product libraries.

The incremental model stores body/eligibility/receive/witness facts. Its separate
whole-arrival-history oracle does not call the model's reducers, effects or
encoding functions. It enumerates both provenance settings, all 24 orders of
four encounters and every committed prefix for public/private/combined reads.
Additional cases cover import→network, correction cuts, strict request conflict,
retirement, nonmutating duplicate certification, proof dependency loss, scoped
restriction/re-enable, authorized-before-winner, private deletions and expiry.
Fixture arrival history is test input for the oracle, **not** proposed permanent
delivery data; with provenance off the scientific model's facts and proof intent
contain no endpoint pair.

Negative controls evaluate incorrect global signature/receive selection,
winner-before-authorization, private deletion influence, expiry-before-winner,
ID-only verification, incomplete proof closure and event-ID-only request
deduplication. Encoding controls detect lossy floats, reordered/deduplicated
signed tags, wrong endian, null/empty conflation and digest-domain aliasing.
Separate checks exercise qualified-dictionary aliasing, restore counters, whole
batch overflow refusal, topology handoff and incompatible peers before writes.

Signatures/event IDs in these model/vector fixtures are **synthetic format
values**, not cryptographically valid Nostr examples. A validity flag models the
already-independent verifier's result. The test does not prove Schnorr security,
protocol serializer interoperability, filesystem synchronization, engine writes,
owner fencing, process crash recovery, physical backup closure or fleet capacity.
Those need the real producer/engine/read/export adapters in the implementation
tasks; the paper contract's new choices are not shipped features.

The initial run found a test expectation incorrectly comparing entire proofs
after an unused relay label changed with provenance off. The corrected model
binds only semantic intent (excluding owner-assigned admission time, verifier
status and disabled witness data); the fixed comparison confirms no scientific
effect or source dependency change. This was an experiment fixture correction,
not a claim of finding a bug in the unimplemented product.

## Real signed cross-component fixture

The separate [signed-fixture.json](signed-fixture.json) is a real signed corpus,
not the synthetic values in the encoding/model probe. Verify its fixed values:

```sh
python3 experiments/identity-contract/signed_fixture.py
```

This command requires the locally recorded `secp256k1==0.14.0` Python binding.
It signs with maintained libsecp256k1 only when explicitly run with `--generate`;
ordinary verification reads the frozen fixture. The equation checker is a small,
separate, slow research implementation of BIP340 group verification, anchored by
official test vector 0 and a corrupted-message negative control. It is not
constant-time or proposed product cryptography. Every event is also checked by
the maintained binding; independent Node JSON serialization/SHA-256 checks IDs.
Keys 1/2/3 are public test fixtures, never credentials for a real account.

The corpus has 15 signed variants representing 14 unique event IDs. E has two
different valid signatures over identical signed fields. Twenty ordered actions
cover private→public, ordinary duplicate, earlier correction, import→network,
private newer replacement, private deletion, duplicate follow tags, empty list
replacement, deletion before target arrival, expiry, equal-time replacement
tie-break, public deletion, a new public provenance edge and an invalid signature.
Named expected query sets and graph edges are manually enumerated before the
model runs. Fifteen views run with both provenance settings; history exports
retain 12 public and 3 private event representatives, including deleted/expired
signed originals. The public E signature is the public encounter's signature.

After the empty A list, the fixed A/B/C graph has only B→C. Its indegrees are
A=0, B=0, C=1; max-normalized values are 0,0,1. This is the explicit graph-fixture
statistic, not a claim about a production trust/PageRank score. Expiry is 250
Nostr seconds and the two views evaluate 249/250; all admissions precede it.
At 250 the winning A profile disappears without reviving A_old. A private newer
profile or private deletion cannot affect a public view.

The fixture provides all event IDs, signatures, times, expected exports and
complete SourceCut arrays after each action. A bootstrap batch preallocates 18
dictionary mappings with provenance off and 24 with it on in shard0/epoch1;
event batches append to that same single stream. The final complete bound is
63 without provenance or 86 with it. Event batches contribute 45
facts with provenance off or 62 with it on: 14 bodies, 15 eligibility facts,
16 receive facts and optionally 17 witness facts. These are reference-model
identities/bounds for future adapter acceptance, not a claim that product stores
already contain those rows. The disabled recipe contains no endpoint/access
dictionary or scientific edge inventory.

[signed-fixture-evidence.json](signed-fixture-evidence.json) records the actual
signature/ID checks, 30 public query/graph cases, four reverified full signed
export roundtrips, source cuts and immutable source hashes. It does not establish
engine admission, native archive export, process fencing or crash recovery.

Primary crypto references:
[BIP340 algorithm](https://github.com/bitcoin/bips/blob/master/bip-0340.mediawiki),
[official vector 0](https://github.com/bitcoin/bips/blob/master/bip-0340/test-vectors.csv),
and [libsecp256k1 auxiliary signing parameters](https://github.com/bitcoin-core/secp256k1/blob/v0.6.0/include/secp256k1_schnorrsig.h).
