"""Literal private append after the original installed two-map bootstrap.

No Rust codec or native dump supplies expected bytes. The input sequence and
timestamps are fixed by the isolated probe, not inferred from its receipt.
"""
import hashlib
from bootstrap_oracle import expected as bootstrap


def expected(dataset, cbor, *, interned=False):
    be = lambda n, width=8: n.to_bytes(width, 'big')
    sha = lambda value: hashlib.sha256(value).digest()
    domains = {1: 70, 2: 65, 3: 71, 4: 64, 5: 72, 8: 68}
    ref = lambda kind, n=1: [dataset, kind, bytes([domains[kind]]) * 16, n]
    ref_key = lambda kind, n=1: dataset + be(kind, 2) + bytes([domains[kind]]) * 16 + be(n)
    src = lambda n: [dataset, 0, 1, n]
    src_key = lambda n: dataset + be(0, 4) + be(1) + be(n)
    rows, first_receipt, _ = bootstrap(dataset, True,
        namespaces=[(kind, bytes([domain]) * 16) for kind, domain in sorted(domains.items())])
    rows = {(cf, k): v for cf, k, v in rows}
    slot, scope = bytes([66]) * 16, ref(4, 3)
    initial = [['dictionary', 1, [ref(4, n), 1, 'visibility', 1, {'class': label}]]
               for n, label in [(1, 'public'), (2, 'private')]]
    first_input = b'bigbrotr/producer-request/1\0' + cbor([[slot, 1, 1], 'admit-evidence', 1,
        [1, ref(4), initial]])
    definitions = [(4, 3, 'visibility', {'class': 'private'}),
        (2, 1, 'observer', {'clock': 'system-utc', 'vantage': 'source-fixture', 'software': None,
            'method_id': None, 'public_key': None, 'route_class': None, 'software_version': None}),
        (1, 1, 'endpoint', {'url': 'ws://127.0.0.1:7447/private'}),
        (3, 1, 'access-scope', {'class': 'public', 'label': None, 'visibility_scope_id': scope}),
        (5, 1, 'event-verification', {'input': 'nip01-event/1', 'algorithm': 'sha256-bip340',
            'capability': 'event-id-and-signature', 'output_unit': 'verification-result',
            'failure_taxonomy': 'bigbrotr-nip01-verifier/1', 'algorithm_version': 1,
            'protocol_revision': 'a2494f4f81d46684e5814a9bf35e2b1df978f955'})]
    refs, entries = [], []
    for sequence, (kind, ordinal, schema, value) in enumerate(definitions, 3):
        descriptor = [1, schema, 1, value]
        mapping = [ref(kind, ordinal), *descriptor]
        fact = b'bigbrotr/fact/1\0' + cbor([src(sequence), 'dictionary', 1, scope, 234, mapping])
        digest, canonical = sha(fact), cbor(descriptor)
        refs.append([src(sequence), digest])
        entries.append(['dictionary', 1, mapping])
        rows['facts', b'\x01\x20' + src_key(sequence)] = b'\x01\x20' + digest + fact
        rows['default', b'\x01\x04' + ref_key(kind, ordinal)] = b'\x01\x04' + src_key(sequence) + digest
        rows['default', b'\x01\x05' + dataset + be(kind, 2) + sha(canonical) + ref_key(kind, ordinal)] = b'\x01\x05' + canonical
        rows['default', b'\x01\x03' + dataset + be(kind, 2) + bytes([domains[kind]]) * 16] = b'\x01\x03' + be(ordinal)
    rows['default', b'\x01\x02' + dataset + be(0, 4) + be(1)] = b'\x01\x02' + be(7)
    inputs, receipts = [first_input], [first_receipt]
    plans = ([(2, entries[:1], refs[:1]), (3, entries[1:], refs[1:]), (4, entries[1:], None)]
             if interned else [(2, entries, refs), (3, entries, None)])
    for sequence, selected, created in plans:
        dependencies = refs[:1] if interned and sequence == 2 else refs
        request = b'bigbrotr/producer-request/1\0' + cbor([[slot, 1, sequence], 'admit-evidence', 1, [1, scope, selected]])
        receipt = b'bigbrotr/receipt/1\0' + cbor([1 if created else 2, [slot, 1, sequence], sha(request),
            [dataset, 0, 1, bytes([67]) * 16, 1], created, dependencies])
        if created:
            first, last = created[0][0][3], created[-1][0][3]
            rows['facts', b'\x01\x21' + src_key(first)] = b'\x01\x21' + be(last) + sha(
                b'bigbrotr/admission-batch/1\0' + cbor([dataset, 0, 1, first, last, created]))
        rows['receipts', b'\x01\x30' + slot + be(1) + be(sequence)] = receipt
        inputs.append(request)
        receipts.append(receipt)
    rows['receipts', b'\x01\x31' + slot + be(1)] = b'\x01\x31' + be(4 if interned else 3) + be(0) + be(0)
    assert len(rows) == (36 if interned else 34)
    return sorted((cf, k, v) for (cf, k), v in rows.items()), inputs, receipts
