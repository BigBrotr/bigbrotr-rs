"""Independent expected digest for the real signed Rocks-to-worker scenario.

Reuses only the independent Python CBOR oracle. Inputs are the frozen E_private
signed event and literal test admission envelope, never observed Rust output.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(root / 'experiments/ingest-bootstrap'))
from oracle import cbor

event = json.loads((root / 'experiments/identity-contract/signed-fixture.json').read_text())['events']['E_private']
dataset = bytes([1]) * 16
reference = lambda kind, ordinal=1: [dataset, kind, bytes([kind]) * 16, ordinal]
body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'], event['kind'], event['tags'], event['content']]
body_digest = hashlib.sha256(b'bigbrotr/body/1\0' + cbor(body)).digest()
clock = [reference(2), 1, None, 0]
payloads = [
    ['dictionary', [reference(8), 1, 'event', 1, {'event_id': body[0]}]],
    ['event-payload', [*body, reference(8)]],
    ['event-eligibility', [body[0], body_digest, bytes.fromhex(event['sig']), 10_000_000, 'network', None]],
    ['event-receive', [body[0], 10_000_000, reference(2), clock, None]],
]
records = []
for sequence, (kind, payload) in enumerate(payloads, 5):
    source = [dataset, 0, 1, sequence]
    preimage = b'bigbrotr/fact/1\0' + cbor([source, kind, 1, reference(4, 2), 10_000_000, payload])
    records.append([source, hashlib.sha256(preimage).digest()])
fixtures = []
for topology in [0, 2**64 - 1]:
    encoded = b'bigbrotr/archive-batch/1\0' + cbor([dataset, 1, topology, [[0, 1, 5, 8]], records])
    digest = subprocess.check_output(['openssl', 'dgst', '-sha256', '-binary'], input=encoded).hex()
    fixtures.append({'topology_generation': str(topology), 'preimage_hex': encoded.hex(), 'sha256': digest})
Path(__file__).with_name('archive-signed-pending.json').write_text(json.dumps({
    'format': 'bigbrotr-archive-signed-pending-vector/1',
    'authoring': 'Independent Python CBOR from frozen signed E_private and literal admission envelope; OpenSSL SHA-256; no Rust output used.',
    'event_id': event['id'], 'vectors': fixtures,
}, indent=2) + '\n')
