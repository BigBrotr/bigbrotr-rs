#!/usr/bin/env python3
"""Public exact envelope answers from libsodium, independent of BigBrotr/RustCrypto."""
import base64
import hashlib
import json
from pathlib import Path
import struct

import nacl
import nacl._sodium
from nacl.bindings import crypto_aead_xchacha20poly1305_ietf_encrypt as encrypt

dataset, ring = bytes([0x11])*16, bytes([0x22])*16
resource = b'management.jobs/1'
cases = []
for ordinal, length in enumerate([0, 1, 15, 16, 17, 63, 64, 65, 255, 3015]):
    key_id = bytes([0x33 if ordinal % 2 == 0 else 0x44])*16
    key = bytes(range(1, 33)) if ordinal % 2 == 0 else bytes(range(33, 65))
    nonce = bytes([ordinal])+bytes(range(1, 24))
    header = b'\x01'+key_id
    aad = b'BigBrotr cursor\x00'+dataset+ring+header+struct.pack('>H', len(resource))+resource
    plaintext = bytes((i*17+ordinal) % 256 for i in range(length))
    wire = header+nonce+encrypt(plaintext, aad, nonce, key)
    encoded = base64.urlsafe_b64encode(wire).rstrip(b'=')
    assert len(wire) == 57+length and len(encoded) <= 4096
    cases.append(dict(name='length-'+str(length), key_id=key_id.hex(), nonce=nonce.hex(),
                      aad=aad.hex(), plaintext=plaintext.hex(), token=encoded.decode()))
value = dict(format='bigbrotr-cursor-envelope-public-fixtures/1',
             warning='Public fixed keys/nonces only; never provision these keys.',
             dataset_id=dataset.hex(), keyring_id=ring.hex(), resource=resource.decode(),
             oracle=dict(pynacl=nacl.__version__, sodium_module_sha256=hashlib.sha256(
                 Path(nacl._sodium.__file__).read_bytes()).hexdigest()), cases=cases)
Path(__file__).with_name('cursor-vectors.json').write_text(json.dumps(value, indent=2)+'\n')
