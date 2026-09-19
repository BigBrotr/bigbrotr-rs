#!/usr/bin/env python3
"""Independent definite-CBOR authoring; no Rust codec/verifier imported."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
EVENT = ROOT / 'tests/fixtures/ingest/import-event.jsonl'

def head(major, value):
    if value < 24:
        return bytes([(major << 5) | value])
    for width, tag in [(1, 24), (2, 25), (4, 26), (8, 27)]:
        if value < 1 << (8 * width):
            return bytes([(major << 5) | tag]) + value.to_bytes(width, 'big')
    raise ValueError(value)

def encode(value):
    if value is None:
        return b'\xf6'
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, str):
        raw = value.encode('utf-8')
        return head(3, len(raw)) + raw
    if isinstance(value, int):
        return head(0, value) if value >= 0 else head(1, -1-value)
    if isinstance(value, list):
        return head(4, len(value)) + b''.join(map(encode, value))
    raise TypeError(value)

def main():
    event_bytes = EVENT.read_bytes()
    event = json.loads(event_bytes)
    request = [bytes([3])*16, 1, 2]
    def ref(kind):
        return [bytes([1])*16, kind, bytes([kind])*16, 1]
    signed = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']),
              event['created_at'], event['kind'], event['tags'], event['content'],
              bytes.fromhex(event['sig'])]
    intent = [1, ref(4), signed, 'import', ref(9), None]
    preimage = b'bigbrotr/producer-request/1\0' + encode([request, 'admit', 1, intent])
    output = {
        'format': 'bigbrotr-signed-request-fixture/1',
        'authoring': 'Independent Python CBOR encoder; complete existing independently signed import event.',
        'source': 'tests/fixtures/ingest/import-event.jsonl',
        'source_sha256': hashlib.sha256(event_bytes).hexdigest(),
        'preimage_hex': preimage.hex(),
        'sha256': hashlib.sha256(preimage).hexdigest(),
    }
    path = Path(__file__).with_name('signed-request-v1.json')
    path.write_text(json.dumps(output, indent=2)+'\n')
    print(json.dumps({'path': str(path), 'bytes': len(preimage), 'sha256': output['sha256']}))

if __name__ == '__main__':
    main()
