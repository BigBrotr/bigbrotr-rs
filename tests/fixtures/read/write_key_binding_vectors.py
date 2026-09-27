#!/usr/bin/env python3
"""Public registration hashes independent of the Rust keyring implementation."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
fixture = json.loads((root / 'keyring-vectors.json').read_text())
keyfile = bytes.fromhex(fixture['cases'][0]['hex'])
count = int.from_bytes(keyfile[40:42], 'big')
assert count == 2 and len(keyfile) == 42 + 48 * count + 32
assert hashlib.sha256(keyfile[:-32]).digest() == keyfile[-32:]
rows = []
for index in range(count):
    record = keyfile[42 + index * 48:90 + index * 48]
    rows.append(dict(key_id=record[:16].hex(), digest=hashlib.sha256(
        b'bigbrotr/cursor-key-material/1\0' + keyfile[8:40] + record).hexdigest()))
(root / 'key-binding-vectors.json').write_text(json.dumps(rows, indent=2) + '\n')
