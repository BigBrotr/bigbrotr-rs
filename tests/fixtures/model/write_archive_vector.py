"""Literal closed archive recipe; no production/research encoder imports."""
import json
import subprocess
from pathlib import Path

dataset = '01' * 16
maximum = '1b' + 'ff' * 8
# Three maximal ranges: shard0/epoch1 sequences23..24,30; then maximum stream/id.
ranges = '83' + '840001171818' + '840001181e181e' + '841affffffff' + maximum * 3
records = ('84'
           + '828450' + dataset + '0001175820' + '11' * 32
           + '828450' + dataset + '000118185820' + '22' * 32
           + '828450' + dataset + '0001181e5820' + '33' * 32
           + '828450' + dataset + '1affffffff' + maximum * 2 + '5820' + '44' * 32)
preimage = b'bigbrotr/archive-batch/1\0'.hex() + '8550' + dataset + '19ffff' + maximum + ranges + records
sha = subprocess.check_output(['openssl', 'dgst', '-sha256', '-binary'], input=bytes.fromhex(preimage)).hex()
Path(__file__).with_name('archive-multiple-ranges.json').write_text(json.dumps({
    'format': 'bigbrotr-archive-vector/1',
    'authoring': 'Literal CBOR heads/field order and OpenSSL SHA-256. Synthetic source references, no commit or signature claim.',
    'archive_schema_version': 65535, 'topology_generation': str(2**64 - 1),
    'preimage_hex': preimage, 'sha256': sha,
}, indent=2) + '\n')
