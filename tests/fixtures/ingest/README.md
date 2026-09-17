# Signed event admission fixture

`signed-event-admission.json` is an independent semantic oracle for the first
RocksDB event-admission owner. It is intended for P02-T02 steps 2–5 and the
practical cases in step 7. The fixture does not qualify the RocksDB binding,
process fencing, crash recovery, receipt retirement, or power-loss behavior.

The fixture references, rather than copies, three events from
`experiments/identity-contract/signed-fixture.json`. That corpus contains two
different valid BIP340 signatures over the same event body and one independently
signed import event. Run its verifier before this oracle:

```sh
python3 experiments/identity-contract/signed_fixture.py
```

The verifier checks the literal event IDs and signatures with both
python-secp256k1/libsecp256k1 and a separate BIP340 equation implementation. The
invalid case in this fixture changes one byte of a real signature and must fail
cryptographic verification; it is never accepted through a fake verifier.
The two literal body-fingerprint preimages were authored from the profile-1
recipe, independently of the admission reducer, and let a consumer check each
expected body digest directly.

`import-event.jsonl` contains the exact signed `I` event as one UTF-8 JSON line.
Its complete bytes, including the final LF, supply the import-lineage digest.
JSONL has no source dataset identity, so the descriptor keeps that field null.
The historical research fixture's synthetic `fixture-import` digest and source
dataset are not reused as evidence of this artifact. Endpoint descriptors include
the root slash required by `bigbrotr-endpoint/1`; the harness must still check
exact equality with the actual protocol normalizer before admission.

## Harness contract

Each scenario starts from an empty scientific event inventory and a fresh
producer-request namespace. The harness installs the named descriptor bindings
before admission. Their descriptor schemas, versions and scalar values match
`docs/specification/dictionary-descriptors.md` at the exact candidate and digest
recorded in `dictionary_descriptor_contract`. The fixture's
`$binding:<name>` spelling is only a placeholder for a QualifiedRef-valued
descriptor field. The harness must allocate the named same-dataset mapping,
substitute its complete QualifiedRef, construct the typed descriptor, and retain
that resolved value for the scenario. The placeholder is never a persistent
descriptor value or wire spelling.

`base_only_no_witness_bindings` installs visibility, observer and import-lineage
mappings. `base_plus_witness` additionally installs endpoint, access-scope and
event-verification mappings. Observer clock resolution belongs to the occurrence's
`ClockQuality`, not to the observer descriptor. Null descriptor attribution stays
null rather than being invented by the fixture. The two access descriptors point
to their named visibility mappings after reference substitution. This deliberately
does not prescribe dictionary allocation order, ordinals, dictionary source-record
order, event ordinals, fact source-record IDs, contiguous sequence endpoints,
receipt bytes, or batch digests. Those are admission-owner outputs and must still
satisfy the identity and durability contracts.

No event mapping is preinstalled. An operation whose expected dictionary delta
creates one uses the closed `event` descriptor with normalization and descriptor
version 1. The admission owner assigns its QualifiedRef and writes that mapping
with the first accepted payload batch. A later scope for the same event reuses the
mapping and does not allocate another event identity.

For every operation, resolve `event_ref` from the signed corpus and construct the
exact intent described by the fixture. `signature_override` replaces only the
signature. A network observation uses the named observer and clock; when
provenance is enabled it also uses the endpoint, access scope, and verification
method. When provenance is disabled the witness context is absent, and the
adapter must allocate no endpoint/access/method mapping or provenance inventory
on behalf of the request.

`owner_recorded_at_us` is a deterministic owner-clock input for the test. It is
used for the fact envelope and first eligibility time, but it is not part of the
producer request intent or request digest. Replaying a request cannot replace
the originally committed owner time.

`expected.scientific_deltas` lists only immutable scientific facts. Request
detail and receipts are operational effects named separately. A correction's
`corrects` value names the prior selected fact; the harness must substitute and
check the actual earlier `SourceRecordId`. `same_stored_result_as` requires the
original durable result and dependency closure, including its original batch
when the result was `NewFacts`; it does not authorize a second batch.
`request_detail: created` requires durable resolution detail even for a no-fact
Admit outcome; it does not turn `Invalid` into a scientific receipt.

After every step, compare the complete relevant inventory with the named entry
in `expected_inventory_states`. Absence is meaningful. In particular, an invalid
signature, later same-scope delivery, or conflicting reuse of a request ID
cannot alter minima or add scientific facts. Exact request replay returns the
stored result. Changed signature or observation under the same retained request
ID is `RequestConflict`, even though the event body would otherwise deduplicate.

The fixture is bounded to five scenarios, fifteen operations, three source
events, and at most four scientific facts per operation. The referenced corpus
must be no larger than the declared byte bound and match its SHA-256 before use.
The admission test should add its ordinary wire, intent, batch, and resource
limits; these fixture bounds are not deployment limits.
