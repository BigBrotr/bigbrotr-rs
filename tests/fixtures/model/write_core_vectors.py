"""Explicit one-time CBOR fixture authoring; tests never regenerate these bytes.

No product/research encoder is imported. SHA-256 is independently run by OpenSSL.
These synthetic signatures test field encoding, never cryptographic validity.
"""
import json
import subprocess
from pathlib import Path

D, A, G, E, P, S = '01'*16, '02'*16, '03'*16, 'aa'*32, 'bb'*32, 'cc'*64
SOURCE1 = '8450' + D + '000101'
SOURCE2 = '8450' + D + '000102'


def ref(kind_hex):
    return '8450' + D + kind_hex + '50' + A + '01'


SCOPE, OBSERVER, LINEAGE = ref('04'), ref('02'), ref('09')
BODY_FIELDS = '5820' + E + '5820' + P + '1bffffffffffffffff0183816178826178608261786063c3a90a'
BODY_DIGEST = '0e6fa8588956533ae2408caf6fa304437b7a0b8fd216a0e11a10cf276704f760'
CLOCK = '84' + OBSERVER + '01f600'
MIN_TIME = '3b7fffffffffffffff'
WITNESS = '83' + ref('01') + ref('03') + ref('05')
SIGNED = '87' + BODY_FIELDS + '5840' + S
vectors = []


def add(name, domain, value):
    raw = f'bigbrotr/{domain}/1\0'.encode() + bytes.fromhex(value)
    digest = subprocess.check_output(['openssl', 'dgst', '-sha256', '-binary'], input=raw).hex()
    vectors.append(dict(name=name, preimage_hex=raw.hex(), sha256=digest))


def fact(name, kind_hex, payload):
    # Source sequence 2, recorded_at i64::MIN; correction target sequence 1.
    add(name, 'fact', '86' + SOURCE2 + kind_hex + '01' + SCOPE + MIN_TIME + payload)


fact('event-payload', '6d6576656e742d7061796c6f6164', '87' + BODY_FIELDS + ref('08'))
fact('event-receive-correction', '6d6576656e742d72656365697665', '855820' + E + '20' + OBSERVER + CLOCK + SOURCE1)
fact('provenance-correction', '6a70726f76656e616e6365', '885820' + E + ref('01') + OBSERVER + ref('03') + '20' + CLOCK + ref('05') + SOURCE1)
fact('eligibility-import', '716576656e742d656c69676962696c697479', '865820' + E + '5820' + BODY_DIGEST + '5840' + S + '2066696d706f7274' + LINEAGE)
REQUEST = '848350' + G + '01016561646d697401'
add('request-import', 'producer-request', REQUEST + '8601' + SCOPE + SIGNED + '66696d706f7274' + LINEAGE + 'f6')
add('request-network-provenance', 'producer-request', REQUEST + '8601' + SCOPE + SIGNED + '676e6574776f726bf6' + '8420' + OBSERVER + CLOCK + WITNESS)

(Path(__file__).parent / 'core-fingerprints.json').write_text(json.dumps({
    'purpose': 'Frozen independently assembled core fact/request preimages; synthetic signatures, no validity claim.',
    'fields': 'D=01x16, allocation=02x16, producer=03x16, event=AAx32, pubkey=BBx32, signature=CCx64; body uses u64::MAX, kind1, ordered duplicate tags and é newline; refs ordinal1; owner/producer epoch1; request sequence1. Facts source sequence2 and recorded_at i64::MIN; observations/eligibility -1, clock resolution1/uncertainty null/flags0; corrections source sequence1.',
    'expected_bytes': 'Explicit hex assembly in write_core_vectors.py; tests only read frozen results.',
    'expected_digests': 'System OpenSSL SHA-256.',
    'vectors': vectors,
}, ensure_ascii=False, indent=2) + '\n')
