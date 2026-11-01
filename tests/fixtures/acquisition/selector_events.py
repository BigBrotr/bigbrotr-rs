#!/usr/bin/env python3
"""Author/check frozen selector events with independent crypto and literal outcomes."""
import argparse
import importlib.metadata
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'experiments/identity-contract'))
from signed_fixture import sign, verified, canonical

PATH = Path(__file__).with_name('selector-events.json')


def frozen(name, author, time, kind, tags):
    event = sign(author, time, kind, tags, name)
    return dict(name=name, event=event, canonical_hex=canonical(event).hex())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--linear-window', action='store_true',
                        help='author/check the bounded window and same-second corpus')
    args = parser.parse_args()
    if args.linear_window:
        path = PATH.with_name('linear-window-events.json')
        assert importlib.metadata.version('secp256k1') == '0.14.0'
        if args.write:
            events = [frozen('same-second-alternate', 1, 23, 1, [['t', 'allowed']]),
                      frozen('island-first', 3, 172801, 1, []),
                      frozen('island-second', 3, 172801, 1, [])]
            path.write_text(json.dumps(dict(format='bigbrotr-linear-window-events/1',
                test_keys='Public integers 1 and 3 only; never real accounts',
                authoring='python-secp256k1 0.14.0', events=events), indent=2)+'\n')
        data = json.loads(path.read_text())
        assert data['format'] == 'bigbrotr-linear-window-events/1'
        assert [(row['name'], row['event']['created_at']) for row in data['events']] == [
            ('same-second-alternate', 23), ('island-first', 172801), ('island-second', 172801)]
        assert len({row['event']['id'] for row in data['events']}) == 3
    else:
        data = None
    if args.write and not args.linear_window:
        rows = [
            ('included', 1, 23, 1, [['t', 'allowed']], 'included', True),
            ('ephemeral', 1, 23, 22222, [['t', 'allowed']], 'ephemeral-excluded', True),
            ('wrong-author', 2, 23, 1, [['t', 'allowed']], 'outside-selector', False),
            ('wrong-first-tag', 1, 23, 1, [['t', 'different', 'allowed']], 'outside-selector', False),
            ('below-floor', 1, 22, 1, [['t', 'allowed']], 'outside-selector', False),
            ('outside-request-window', 1, 24, 1, [['t', 'allowed']], 'included', False),
        ]
        events = []
        for name, author, time, kind, tags, collection, request in rows:
            events.append(dict(frozen(name, author, time, kind, tags),
                               collection=collection, request=request))
        public = '79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798'
        collection = dict(version=1, filters=[dict(authors=[public], **{'#t': ['allowed']})],
                          history_since=23, ephemeral='drop')
        request = dict(version=1, filters=[dict(authors=[public], **{'#t': ['allowed']}, since=23, until=23)],
                       history_since=0, ephemeral='retain')
        PATH.write_text(json.dumps(dict(format='bigbrotr-selector-signed-fixture/1',
            test_keys='Public integers 1 and 2 only; never real accounts',
            authoring='python-secp256k1 ' + importlib.metadata.version('secp256k1'),
            collection=collection, request=request, events=events), indent=2)+'\n')
    if data is None:
        data = json.loads(PATH.read_text())
        assert len(data['events']) == 6
    for row in data['events']:
        assert verified(row['event'])  # native library AND independent group equation
        assert canonical(row['event']).hex() == row['canonical_hex']
        bad = dict(row['event'], content=row['event']['content']+'!')
        assert not verified(bad)
        bad = dict(row['event'], sig='00'*64)
        assert not verified(bad)
    print(f"PASS: {len(data['events'])} signed events, declared corpus and crypto negatives")


if __name__ == '__main__':
    main()
