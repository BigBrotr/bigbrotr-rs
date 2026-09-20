#!/usr/bin/env python3
"""Frozen independent signed body that exceeds a diagnostic pipe response."""
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'experiments/identity-contract'))
from signed_fixture import sign, verified

repeat, count = 'archive\x00event é\n', 4096
event = sign(3, 1, 1, [['x'], ['x', ''], ['t', 'é'], ['t', 'é']], repeat * count)
assert verified(event)
encoded = json.dumps(event, ensure_ascii=False, separators=(',', ':')).encode()
assert 4096 < len(encoded) < 256 * 1024
fields = list(event)
event.pop('content')
recipe = dict(authoring='python-secp256k1 plus independent BIP340 equation verifier; public test key3',
              content_repeat=repeat, content_count=count, event=event, serialization_fields=fields,
              signed_event_bytes=len(encoded), signed_event_sha256=hashlib.sha256(encoded).hexdigest())
Path(__file__).with_name('archive-event-response.json').write_text(json.dumps(recipe, indent=2) + '\n')
print(json.dumps(dict(status='PASS', bytes=len(encoded), sha256=recipe['signed_event_sha256'])))
