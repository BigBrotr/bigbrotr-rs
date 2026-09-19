#!/usr/bin/env python3
"""Independent exact native rows for staged-input process-kill boundaries."""
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'experiments/ingest-bootstrap'))
from oracle import cbor


def main():
    be = lambda n, width=8: n.to_bytes(width, 'big')
    dataset, domain, slot = bytes([1])*16, bytes([2])*16, bytes([5])*16
    reference = [dataset, 4, domain, 1]
    source = [dataset, 0, 1, 1]
    source_key = dataset + be(0, 4) + be(1) + be(1)
    ref_key = dataset + be(4, 2) + domain + be(1)
    manifest = (bytes.fromhex('01010001000100010001') + dataset + be(0)
                + bytes([3])*16 + be(0, 4) + be(1) + bytes([4])*16
                + bytes([6])*16 + be(1) + bytes([1, 1]) + be(4, 2) + domain)
    before = [('default', b'\x01\x01', manifest)]
    descriptor = [1, 'visibility', 1, {'class': 'public'}]
    mapping = [reference, *descriptor]
    request_id = [slot, 1, 1]
    request = b'bigbrotr/producer-request/1\0' + cbor([request_id, 'admit-evidence', 1,
        [1, reference, [['dictionary', 1, mapping]]]])
    fact = b'bigbrotr/fact/1\0' + cbor([source, 'dictionary', 1, reference, 123, mapping])
    digest = hashlib.sha256(fact).digest()
    refs = [[source, digest]]
    receipt = b'bigbrotr/receipt/1\0' + cbor([1, request_id, hashlib.sha256(request).digest(),
        [dataset, 0, 1, bytes([6])*16, 1], refs, refs])
    batch = hashlib.sha256(b'bigbrotr/admission-batch/1\0' + cbor([dataset, 0, 1, 1, 1, refs])).digest()
    canonical = cbor(descriptor)
    after = before + [
        ('default', b'\x01\x02' + dataset + be(0, 4) + be(1), b'\x01\x02' + be(1)),
        ('default', b'\x01\x03' + dataset + be(4, 2) + domain, b'\x01\x03' + be(1)),
        ('default', b'\x01\x04' + ref_key, b'\x01\x04' + source_key + digest),
        ('default', b'\x01\x05' + dataset + be(4, 2) + hashlib.sha256(canonical).digest() + ref_key, b'\x01\x05' + canonical),
        ('facts', b'\x01\x20' + source_key, b'\x01\x20' + digest + fact),
        ('facts', b'\x01\x21' + source_key, b'\x01\x21' + be(1) + batch),
        ('receipts', b'\x01\x30' + slot + be(1) + be(1), receipt),
        ('receipts', b'\x01\x31' + slot + be(1), b'\x01\x31' + be(1) + be(0) + be(0)),
    ]
    rows = lambda values: [[cf, key.hex(), value.hex()] for cf, key, value in sorted(values)]
    result = dict(format='bigbrotr-staged-bootstrap-crash/1', before=rows(before), after=rows(after), request_hex=request.hex())
    Path(__file__).with_name('staged-bootstrap-crash.json').write_text(json.dumps(result, indent=2)+'\n')


if __name__ == '__main__':
    main()
