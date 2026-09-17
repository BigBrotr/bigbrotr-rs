#!/usr/bin/env python3
"""Author/check frozen test events with independent Python crypto; never product code."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'experiments/identity-contract'))
from signed_fixture import sign, verified, canonical

PATH = Path(__file__).with_name('signed-extremes.json')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true', help='explicitly regenerate frozen fixtures')
    args = parser.parse_args()
    if args.write:
        events = {
            'unsigned-maximum': sign(1, 2**64-1, 65535,
                [[''], ['x'], ['x', ''], ['t', 'é', 'a,b'], ['t', 'e\u0301'], ['t', 'é']],
                '{"value":"still text"}\n"\\/\b\t\n\f\r\0\x01é𝄞\u2028'),
            'timestamp-zero': sign(2, 0, 0, [], ''),
        }
        data = {'format': 'bigbrotr-signed-extremes/1',
                'test_keys': 'Public integers 1 and 2; never real accounts',
                'authoring': 'python-secp256k1 ' + importlib.metadata.version('secp256k1'),
                'events': {name: {'event': event, 'canonical_hex': canonical(event).hex()}
                           for name, event in events.items()},
                'invalid_structure': {'empty-tag': sign(1, 1, 1, [[]], '')}}
        PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    data = json.loads(PATH.read_text())
    for row in data['events'].values():
        assert verified(row['event']), 'library and independent BIP340 equation verification'
        assert canonical(row['event']).hex() == row['canonical_hex']
        assert hashlib.sha256(bytes.fromhex(row['canonical_hex'])).hexdigest() == row['event']['id']
    assert verified(data['invalid_structure']['empty-tag'])
    assert data['invalid_structure']['empty-tag']['tags'] == [[]]
    print('PASS: two valid signed extremes and one signed empty-tag negative; independent crypto checks')

if __name__ == '__main__':
    main()
