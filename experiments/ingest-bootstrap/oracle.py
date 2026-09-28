"""Independent fixed bootstrap corpus; no Rust codecs or observed value templates."""
import hashlib

# identity-and-encoding.md and dictionary-descriptors.md assign visibility=4.
VISIBILITY_KIND = 4


def head(major, value):
    if value < 24:
        return bytes([major * 32 + value])
    for width, tag in [(1, 24), (2, 25), (4, 26), (8, 27)]:
        if value < 1 << (width * 8):
            return bytes([major * 32 + tag]) + value.to_bytes(width, "big")
    raise ValueError("integer exceeds u64")


def cbor(value):
    if value is None:
        return b"\xf6"
    if isinstance(value, int):
        assert value >= 0
        return head(0, value)
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, str):
        data = value.encode()
        return head(3, len(data)) + data
    if isinstance(value, list):
        return head(4, len(value)) + b"".join(map(cbor, value))
    if isinstance(value, dict):
        entries = sorted((cbor(k), cbor(v)) for k, v in value.items())
        return head(5, len(entries)) + b"".join(k + v for k, v in entries)
    raise TypeError(type(value))


def expected(dataset, committed, policy=1, *, domain=bytes([64])*16,
             slot=bytes([66])*16, owner=bytes([61])*16, incarnation=bytes([63])*16,
             recovery=bytes([67])*16, topology=0, namespaces=None, recorded_at=123):
    assert len(dataset) == 16 and any(dataset)
    be = lambda n, width=8: n.to_bytes(width, "big")
    namespaces = [(VISIBILITY_KIND, domain)] if namespaces is None else namespaces
    source = lambda n: [dataset, 0, 1, n]
    source_key = lambda n: dataset + be(0, 4) + be(1) + be(n)
    reference = lambda n: [dataset, VISIBILITY_KIND, domain, n]
    ref_key = lambda n: dataset + be(VISIBILITY_KIND, 2) + domain + be(n)
    manifest = (bytes.fromhex("01010001000100010001") + dataset + be(topology)
                + owner + be(0, 4) + be(1) + incarnation
                + recovery + be(policy) + bytes([1, len(namespaces)])
                + b''.join(be(kind, 2)+value for kind, value in namespaces))
    rows = [("default", b"\x01\x01", manifest)]
    if not committed:
        return rows, None, []
    assert policy == 1
    facts, refs, entries = [], [], []
    for n, label in enumerate(["public", "private"], 1):
        descriptor = [1, "visibility", 1, {"class": label}]
        mapping = [reference(n), *descriptor]
        entries.append(["dictionary", 1, mapping])
        fact = b"bigbrotr/fact/1\0" + cbor([source(n), "dictionary", 1, reference(1), recorded_at, mapping])
        digest = hashlib.sha256(fact).digest()
        facts.append(fact)
        refs.append([source(n), digest])
        canonical = cbor(descriptor)
        rows.extend([
            ("facts", b"\x01\x20" + source_key(n), b"\x01\x20" + digest + fact),
            ("default", b"\x01\x04" + ref_key(n), b"\x01\x04" + source_key(n) + digest),
            ("default", b"\x01\x05" + dataset + be(VISIBILITY_KIND, 2) + hashlib.sha256(canonical).digest() + ref_key(n), b"\x01\x05" + canonical),
        ])
    request_id = [slot, 1, 1]
    request = b"bigbrotr/producer-request/1\0" + cbor([request_id, "admit-evidence", 1, [1, reference(1), entries]])
    receipt = b"bigbrotr/receipt/1\0" + cbor([1, request_id, hashlib.sha256(request).digest(),
        [dataset, topology, 1, recovery, 1], refs, refs])
    batch_digest = hashlib.sha256(b"bigbrotr/admission-batch/1\0" + cbor([dataset, 0, 1, 1, 2, refs])).digest()
    rows.extend([
        ("default", b"\x01\x02" + dataset + be(0, 4) + be(1), b"\x01\x02" + be(2)),
        ("default", b"\x01\x03" + dataset + be(VISIBILITY_KIND, 2) + domain, b"\x01\x03" + be(2)),
        ("facts", b"\x01\x21" + source_key(1), b"\x01\x21" + be(2) + batch_digest),
        ("receipts", b"\x01\x30" + slot + be(1) + be(1), receipt),
        ("receipts", b"\x01\x31" + slot + be(1), b"\x01\x31" + be(1) + be(0) + be(0)),
    ])
    assert len(rows) == 12 and len(set((cf, key) for cf, key, _ in rows)) == 12
    return sorted(rows), receipt, facts
