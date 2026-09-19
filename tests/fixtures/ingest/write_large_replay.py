#!/usr/bin/env python3
"""Independent signed compact recipe around the native fact decoding boundary."""
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'experiments/identity-contract'))
sys.path.insert(0, str(ROOT / 'experiments/ingest-bootstrap'))
from signed_fixture import sign, verified
from oracle import cbor

event = sign(3, 1, 1, [], 'x' * 2_096_947)
assert verified(event)
dataset = bytes([1]) * 16
reference = lambda kind: [dataset, kind, bytes([kind]) * 16, 1]
body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), 1, 1, [], event['content']]
request = b'bigbrotr/producer-request/1\0' + cbor([
    [bytes([5]) * 16, 1, 1], 'admit', 1,
    [1, reference(4), [*body, bytes.fromhex(event['sig'])], 'import', reference(9), None],
])
fact = b'bigbrotr/fact/1\0' + cbor([
    [dataset, 0, 1, 6], 'event-payload', 1, reference(4), 1, [*body, reference(8)],
])
assert len(fact) <= 2 * 1024 * 1024 < len(request)
recipe = dict(authoring='python-secp256k1 signing plus independent BIP340 equation verification; public test key 3',
              content_repeat='x', content_count=len(event.pop('content')),
              event=event, payload_fact_bytes=len(fact), request_preimage_bytes=len(request),
              payload_fact_sha256=hashlib.sha256(fact).hexdigest(),
              request_preimage_sha256=hashlib.sha256(request).hexdigest())
(Path(__file__).with_name('large-replay.json')).write_text(json.dumps(recipe, indent=2) + '\n')
print(json.dumps(dict(status='PASS', fact_bytes=len(fact), request_bytes=len(request))))
