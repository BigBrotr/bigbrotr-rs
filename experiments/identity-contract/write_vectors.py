"""One-time fixture authoring: explicit CBOR bytes, no import from either encoder.

Kept to make the hand-assembled structure reviewable. The test runner reads only
frozen golden-vectors.json and never regenerates expectations. SHA uses OpenSSL.
"""
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent

def blob(hexstr):
    return {'$bytes': hexstr}

def sha(hexstr):
    return subprocess.check_output(['openssl', 'dgst', '-sha256', '-binary'], input=bytes.fromhex(hexstr)).hex()

vectors = []
def add(name, domain, value, encoded, mode='cbor'):
    prefix = (f'bigbrotr/{domain}/1\0'.encode().hex()) if domain else ''
    preimage = prefix + encoded.replace(' ', '')
    v = dict(name=name, domain=domain, mode=mode, preimage_hex=preimage, sha256=sha(preimage))
    v['raw_hex' if mode == 'raw' else 'value'] = encoded if mode == 'raw' else value
    vectors.append(v)
    return v['sha256']

D = '01' * 16
A = '02' * 16
G = '03' * 16
E = 'aa' * 32
P = 'bb' * 32
S = 'cc' * 64
source = [blob(D), 0, 1, 1]
source_hex = '8450' + D + '000101'
ref = [blob(D), 4, blob(A), 1]
ref_hex = '8450' + D + '0450' + A + '01'
observer = [blob(D), 2, blob(A), 1]
observer_hex = '8450' + D + '0250' + A + '01'
body = [blob(E), blob(P), {'$int': str(2**64-1)}, 1, [['x'], ['x',''], ['x','']], 'é\n']
body_hex = '865820' + E + '5820' + P + '1bffffffffffffffff0183816178826178608261786063c3a90a'
body_digest = add('body-u64-ordered-tags', 'body', body, body_hex)
# Primitive bytes fixed separately from domain examples.
add('integer-boundaries', None, [0, 23, 24, 255, 256, 65535, 65536, {'$int':'4294967296'}, {'$int':str(2**64-1)}, -1, {'$int':str(-(2**63))}],
    '8b0017181818ff19010019ffff1a000100001b00000001000000001bffffffffffffffff203b7fffffffffffffff')
add('presence-and-utf8', None, [None, '', blob(''), [], {}, False, True, 'é'], '88f6604080a0f4f562c3a9')
add('closed-map-order', None, {'aa': 2, 'z': 1}, 'a2617a0162616102')
add('raw-document', None, None, '000affc3a9', 'raw')
# Nostr encoding is a separate protocol JSON path, never CBOR.
nostr = ('[0,"' + P + '",18446744073709551615,1,[["x"],["x",""],["x",""]],"é\\n"]').encode().hex()
add('nostr-canonical-preimage', None, None, nostr, 'raw')

dictionary = [ref, 1, 'visibility', 1, {'class':'public'}]
dictionary_hex = '85' + ref_hex + '016a7669736962696c69747901a165636c617373667075626c6963'
add('dictionary', 'dictionary', dictionary, dictionary_hex)

elig = [blob(E), blob(body_digest), blob(S), -1, 'network', None]
elig_hex = '865820' + E + '5820' + body_digest + '5840' + S + '20676e6574776f726bf6'
fact = [source, 'event-eligibility', 1, ref, -1, elig]
fact_hex = '86' + source_hex + '716576656e742d656c69676962696c69747901' + ref_hex + '20' + elig_hex
fact_digest = add('fact-eligibility', 'fact', fact, fact_hex)

clock = [observer, 1, None, 0]
clock_hex = '84' + observer_hex + '01f600'
observation = [-1, observer, clock, None]
observation_hex = '8420' + observer_hex + clock_hex + 'f6'
signed = body + [blob(S)]
signed_hex = '87' + body_hex[2:] + '5840' + S
intent = [1, ref, signed, 'network', None, observation]
intent_hex = '8601' + ref_hex + signed_hex + '676e6574776f726bf6' + observation_hex
request = [[blob(G), 1, 1], 'admit', 1, intent]
request_hex = '848350' + G + '01016561646d697401' + intent_hex
request_digest = add('producer-request', 'producer-request', request, request_hex)
certification_request = [[blob('00'*16), 0, 0], 'certify-existing', 1, intent]
certification_request_hex = '848350' + '00'*16 + '000070636572746966792d6578697374696e6701' + intent_hex
intent_digest = add('certification-intent', 'producer-request', certification_request, certification_request_hex)

deps = [[source, blob(fact_digest)]]
deps_hex = '8182' + source_hex + '5820' + fact_digest
admission = [blob(D), 0, 1, 1, 1, deps]
admission_hex = '8650' + D + '00010101' + deps_hex
add('admission-batch', 'admission-batch', admission, admission_hex)
archive = [blob(D), 1, 1, [[0, 1, 1, 1]], deps]
archive_hex = '8550' + D + '0101818400010101' + deps_hex
add('archive-batch', 'archive-batch', archive, archive_hex)
cut = [blob(D), 1, [[0,1,1]]]
cut_hex = '8350' + D + '018183000101'
projection = [blob(G), 'event_eligibility', 1, cut, 0, [[source, ['event',0], elig]]]
projection_hex = '8650' + G + '716576656e745f656c69676962696c69747901' + cut_hex + '008183' + source_hex + '82656576656e7400' + elig_hex
add('projection-chunk', 'projection-chunk', projection, projection_hex)
proof = [blob(D), 1, 'local-synced', blob(G), 1, blob(intent_digest), deps]
proof_hex = '8750' + D + '016c6c6f63616c2d73796e63656450' + G + '015820' + intent_digest + deps_hex
add('existing-proof', 'existing-proof', proof, proof_hex)
(HERE/'golden-vectors.json').write_text(json.dumps(dict(
    purpose='Frozen hand-assembled canonical byte examples; signatures are synthetic format values.',
    expected_bytes='Explicit hex assembly in write_vectors.py, independently of both encoders.',
    expected_digests='System OpenSSL SHA-256 of frozen preimage bytes.',
    vectors=vectors), ensure_ascii=False, indent=2) + '\n')
print(f'Wrote {len(vectors)} frozen vectors')
