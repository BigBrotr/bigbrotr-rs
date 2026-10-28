#!/usr/bin/env python3
"""Author/check public fixture keys1 and3; the original event uses key3."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'experiments/identity-contract'))
from signed_fixture import sign, verified, canonical

PATH = Path(__file__).with_name('retained-requests.json')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    assert importlib.metadata.version('secp256k1') == '0.14.0'
    if args.write:
        first = json.loads((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_text())
        events = [first] + [sign(1, 1000+n, 1, [], f'retained request boundary {n:02}')
                            for n in range(2, 63)]
        PATH.write_text(json.dumps(dict(format='bigbrotr-retained-request-events/1',
            test_keys='Public integers1 and3 only; preserved first event uses3; never real accounts',
            authoring='python-secp256k1 0.14.0', events=events), indent=2)+'\n')
    corpus = json.loads(PATH.read_text())
    events = corpus['events']
    assert len(events) == len({e['id'] for e in events}) == 62
    assert events[0] == json.loads((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_text())
    assert events[0]['pubkey'] == 'f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9'
    assert {event['pubkey'] for event in events[1:]} == {
        '79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798'}
    for n, event in enumerate(events, 1):
        assert event['kind'] == 1 and event['tags'] == []
        assert verified(event), 'library and independent group-equation verification'
        assert hashlib.sha256(canonical(event)).hexdigest() == event['id']
        assert not verified(dict(event, content=event['content']+'!'))
        assert not verified(dict(event, sig='00'*64))
        if n > 1:
            assert event['created_at'] == 1000+n and event['content'] == f'retained request boundary {n:02}'
    print(json.dumps(dict(status='PASS', events=62, admitted=61, refused=1,
        artifact_sha256=hashlib.sha256(PATH.read_bytes()).hexdigest(),
        verification=['libsecp256k1', 'independent BIP340 group equation', 'changed body/signature refusals'])))


if __name__ == '__main__':
    main()
