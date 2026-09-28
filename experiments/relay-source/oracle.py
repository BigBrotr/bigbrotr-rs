"""Independent complete-byte oracle for the fixed provenance-off signed network workload.

The input is the frozen JSONL event and fixed descriptor corpus. No native output
or product codec is an expected-value template.
"""
import hashlib
import json
from pathlib import Path
from bootstrap_oracle import cbor


def expected_source(dataset, committed):
    source_root = Path(__file__).resolve().parents[2]
    artifact = (source_root / 'tests/fixtures/ingest/import-event.jsonl').read_bytes()
    artifact_digest = bytes.fromhex('c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae')
    assert hashlib.sha256(artifact).digest() == artifact_digest
    event = json.loads(artifact)
    be = lambda n, width=8: n.to_bytes(width, 'big')
    sha = lambda b: hashlib.sha256(b).digest()
    source = lambda n: [dataset, 0, 1, n]
    source_key = lambda n: dataset + be(0, 4) + be(1) + be(n)
    domains = {2: bytes([65]) * 16, 4: bytes([64]) * 16, 8: bytes([68]) * 16}
    reference = lambda kind, n=1: [dataset, kind, domains[kind], n]
    ref_key = lambda kind, n=1: dataset + be(kind, 2) + domains[kind] + be(n)
    scope = reference(4)
    slot = bytes([66]) * 16
    manifest = (bytes.fromhex('01010001000100010001') + dataset + be(0)
                + bytes([61]) * 16 + be(0, 4) + be(1) + bytes([63]) * 16
                + bytes([67]) * 16 + be(1) + bytes([1, 3])
                + b''.join(be(k, 2) + v for k, v in domains.items()))
    rows = {('default', b'\x01\x01'): manifest}
    facts, refs, inputs = {}, {}, {}
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
    def finish(sequence, operation, intent, batch_sequences):
        request = [slot, 1, sequence]
        inputs[sequence] = b'bigbrotr/producer-request/1\0' + cbor([request, operation, 1, intent])
        batch = [refs[n] for n in batch_sequences]
        first, last = batch_sequences[0], batch_sequences[-1]
        batch_digest = sha(b'bigbrotr/admission-batch/1\0' + cbor([dataset, 0, 1, first, last, batch]))
        put('facts', b'\x01\x21' + source_key(first), b'\x01\x21' + be(last) + batch_digest)
        put('default', b'\x01\x02' + dataset + be(0, 4) + be(1), b'\x01\x02' + be(last))
    dictionaries = [mapping(1, 4, 1, 'visibility', {'class':'public'}, 123),
                    mapping(2, 4, 2, 'visibility', {'class':'private'}, 123),
                    mapping(3, 2, 1, 'observer', {'clock':'system-utc', 'vantage':'source-fixture', 'software':None, 'method_id':None, 'public_key':None, 'route_class':None, 'software_version':None}, 123)]
    finish(1, 'admit-evidence', [1, scope, dictionaries], [1, 2, 3])
    if committed:
        body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'], event['kind'], event['tags'], event['content']]
        body_digest = sha(b'bigbrotr/body/1\0' + cbor(body))
        assert body_digest.hex() == '1e05f420946c84c3ee8b178daf05dd564c9904c7ed18f7ab99874aec878337fb'
        signature = bytes.fromhex(event['sig'])
        mapping(4, 8, 1, 'event', {'event_id':body[0]}, 1234)
        fact(5, 'event-payload', [*body, reference(8)], 1234)
        index(6, dataset + body[0], 5)
        fact(6, 'event-eligibility', [body[0], body_digest, signature, 1234, 'network', None], 1234)
        index(7, dataset + body[0] + ref_key(4), 6)
        clock = [reference(2), 1, None, 0]
        fact(7, 'event-receive', [body[0], 456, reference(2), clock, None], 1234)
        index(8, dataset + body[0] + ref_key(4), 7)
        finish(2, 'admit', [1, scope, [*body, signature], 'network', None,
            [456, reference(2), clock, None]], [4, 5, 6, 7])
    assert len(rows) == (25 if committed else 14)
    return sorted((cf, key, value) for (cf, key), value in rows.items()), inputs


def complete_source(dataset, committed, closing=False):
    """Include literal operational receipts/producer marker for recovery checks."""
    rows, inputs = expected_source(dataset, committed)
    be = lambda n: n.to_bytes(8, 'big')
    slot = bytes([66]) * 16
    references = {}
    for cf, key, value in rows:
        if cf == 'facts' and key[:2] == b'\x01\x20':
            n = int.from_bytes(key[-8:], 'big')
            references[n] = [[dataset, 0, 1, n], value[2:34]]
    receipts = {}
    batches = [(1, [1, 2, 3], [1, 2, 3])]
    if committed:
        batches.append((2, [4, 5, 6, 7], [1, 3, 4, 5, 6, 7]))
    for sequence, batch, dependencies in batches:
        receipt = b'bigbrotr/receipt/1\0' + cbor([
            1, [slot, 1, sequence], hashlib.sha256(inputs[sequence]).digest(),
            [dataset, 0, 1, bytes([67]) * 16, 1],
            [references[n] for n in batch], [references[n] for n in dependencies]])
        receipts[sequence] = receipt
        rows.append(('receipts', b'\x01\x30' + slot + be(1) + be(sequence), receipt))
    marker = (b'\x02\x31' if closing else b'\x01\x31') + be(2 if committed else 1) + be(0) + be(0)
    if closing:
        marker += b'\x01'
    rows.append(('receipts', b'\x01\x31' + slot + be(1), marker))
    return sorted(rows), inputs, receipts
