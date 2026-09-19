"""Independent complete-byte oracle for the connected signed import workload.

The input is the frozen JSONL event and fixed descriptor corpus. No native output
or product codec is an expected-value template.
"""
import hashlib
import json
from pathlib import Path
from oracle import cbor


def expected_event(dataset, committed, repetitions=1):
    source_root = Path(__file__).resolve().parents[2]
    artifact = (source_root / 'tests/fixtures/ingest/import-event.jsonl').read_bytes()
    assert repetitions in (1, 2)
    artifact_digest = bytes.fromhex('c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae')
    assert hashlib.sha256(artifact).digest() == artifact_digest
    event = json.loads(artifact)
    if repetitions == 2:
        artifact_digest = bytes.fromhex("ce439b495fd2f99031706e9625529e264cd3043ffc16af4fd6002243503e90fc")
        assert hashlib.sha256(artifact * 2).digest() == artifact_digest
    be = lambda n, width=8: n.to_bytes(width, 'big')
    sha = lambda b: hashlib.sha256(b).digest()
    source = lambda n: [dataset, 0, 1, n]
    source_key = lambda n: dataset + be(0, 4) + be(1) + be(n)
    domains = {4: bytes([64]) * 16, 8: bytes([68]) * 16, 9: bytes([69]) * 16}
    reference = lambda kind, n=1: [dataset, kind, domains[kind], n]
    ref_key = lambda kind, n=1: dataset + be(kind, 2) + domains[kind] + be(n)
    scope = reference(4)
    slot = bytes([66]) * 16
    manifest = (bytes.fromhex('01010001000100010001') + dataset + be(0)
                + bytes([61]) * 16 + be(0, 4) + be(1) + bytes([63]) * 16
                + bytes([67]) * 16 + be(1) + bytes([1, 3])
                + b''.join(be(k, 2) + v for k, v in domains.items()))
    rows = {('default', b'\x01\x01'): manifest}
    facts, refs = {}, {}
    def put(cf, key, value): rows[cf, key] = value
    def fact(sequence, kind, payload, at):
        value = b'bigbrotr/fact/1\0' + cbor([source(sequence), kind, 1, scope, at, payload])
        facts[sequence] = value
        refs[sequence] = [source(sequence), sha(value)]
        put('facts', b'\x01\x20' + source_key(sequence), b'\x01\x20' + sha(value) + value)
        return value
    def index(domain, key, sequence):
        put('default', bytes([1, domain]) + key, bytes([1, domain]) + source_key(sequence) + refs[sequence][1])
    def mapping(sequence, kind, ordinal, schema, descriptor, at):
        fields = [1, schema, 1, descriptor]
        payload = [reference(kind, ordinal), *fields]
        fact(sequence, 'dictionary', payload, at)
        index(4, ref_key(kind, ordinal), sequence)
        canonical = cbor(fields)
        put('default', b'\x01\x05' + dataset + be(kind, 2) + sha(canonical) + ref_key(kind, ordinal), b'\x01\x05' + canonical)
        put('default', b'\x01\x03' + dataset + be(kind, 2) + domains[kind], b'\x01\x03' + be(ordinal))
        return ['dictionary', 1, payload]
    def finish(sequence, operation, intent, batch_sequences, dependencies):
        request = [slot, 1, sequence]
        digest = sha(b'bigbrotr/producer-request/1\0' + cbor([request, operation, 1, intent]))
        batch = [refs[n] for n in batch_sequences]
        receipt = b'bigbrotr/receipt/1\0' + cbor([1, request, digest, [dataset, 0, 1, bytes([67]) * 16, 1], batch, [refs[n] for n in dependencies]])
        first, last = batch_sequences[0], batch_sequences[-1]
        batch_digest = sha(b'bigbrotr/admission-batch/1\0' + cbor([dataset, 0, 1, first, last, batch]))
        put('facts', b'\x01\x21' + source_key(first), b'\x01\x21' + be(last) + batch_digest)
        put('default', b'\x01\x02' + dataset + be(0, 4) + be(1), b'\x01\x02' + be(last))
        put('receipts', b'\x01\x30' + slot + be(1) + be(sequence), receipt)
        put('receipts', b'\x01\x31' + slot + be(1), b'\x01\x31' + be(sequence) + be(0) + be(0))
        return receipt
    dictionaries = [mapping(1, 4, 1, 'visibility', {'class':'public'}, 123),
                    mapping(2, 4, 2, 'visibility', {'class':'private'}, 123),
                    mapping(3, 9, 1, 'dataset-lineage', {'format':'nostr-jsonl', 'format_version':1, 'source_dataset_id':None, 'source_artifact_digest':artifact_digest}, 123)]
    finish(1, 'admit-evidence', [1, scope, dictionaries], [1, 2, 3], [1, 2, 3])
    receipt = None
    if committed:
        body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'], event['kind'], event['tags'], event['content']]
        body_digest = sha(b'bigbrotr/body/1\0' + cbor(body))
        assert body_digest.hex() == '1e05f420946c84c3ee8b178daf05dd564c9904c7ed18f7ab99874aec878337fb'
        signature = bytes.fromhex(event['sig'])
        mapping(4, 8, 1, 'event', {'event_id':body[0]}, 456)
        fact(5, 'event-payload', [*body, reference(8)], 456)
        index(6, dataset + body[0], 5)
        fact(6, 'event-eligibility', [body[0], body_digest, signature, 456, 'import', reference(9)], 456)
        index(7, dataset + body[0] + ref_key(4), 6)
        receipt = finish(2, 'admit', [1, scope, [*body, signature], 'import', reference(9), None], [4, 5, 6], [1, 3, 4, 5, 6])
    assert len(rows) == (26 if committed else 16)
    return sorted((cf, key, value) for (cf, key), value in rows.items()), receipt, [facts[n] for n in [4, 5, 6]] if committed else []
