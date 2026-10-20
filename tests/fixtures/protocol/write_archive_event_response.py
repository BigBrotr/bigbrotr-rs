#!/usr/bin/env python3
"""Author/check frozen signed inputs with independent crypto; public test key3."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
CASES = ('archive-sample', 'content-heavy', 'tag-heavy')


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def supported(event):
    """Independent shape arithmetic for the finite installed Sync input profile.

    Seven closed event fields contribute15 lexical nodes; EVENT/subscription and
    the outer array add3. Each nonempty tag contributes its array and strings.
    This is a frozen-fixture constraint, not execution of the production parser.
    """
    assert set(event) == {'id', 'pubkey', 'created_at', 'kind', 'tags', 'content', 'sig'}
    assert all(type(tag) is list and tag and all(type(value) is str for value in tag)
               for tag in event['tags']), 'nonempty string tags'
    size = len(encoded(event))
    nodes = 18 + sum(1 + len(tag) for tag in event['tags'])
    assert size <= 262144, 'installed normalized event byte bound'
    assert nodes <= 65536, 'whole EVENT envelope node bound'
    return dict(signed_event_bytes=size, envelope_nodes=nodes,
                tags=len(event['tags']), tag_strings=sum(map(len, event['tags'])),
                content_utf8_bytes=len(event['content'].encode()))


def build(case):
    # Import the existing authoring dependency only for explicit author/check work.
    sys.path.insert(0, str(ROOT / 'experiments/identity-contract'))
    from signed_fixture import sign, verified, canonical
    if case == 'archive-sample':
        tags, content = [['x'], ['x', ''], ['t', 'é'], ['t', 'é']], 'archive\x00event é\n' * 4096
    elif case == 'content-heavy':
        tags, content = [], 'a' * 261811
    else:
        assert case == 'tag-heavy'
        tags = [['x'] for _ in range(32756)] + [['x', ''], ['t', 'é']]
        content = 'a' * 65256
    event = sign(3, 1, 1, tags, content)
    assert verified(event), 'library and independent BIP340 equation verification'
    metrics = supported(event)
    assert metrics['signed_event_bytes'] == (94577 if case == 'archive-sample' else 262144)
    assert metrics['envelope_nodes'] == {'archive-sample': 29, 'content-heavy': 18, 'tag-heavy': 65536}[case]
    body = encoded(event)
    if case == 'archive-sample':
        recipe = dict(authoring='python-secp256k1 plus independent BIP340 equation verifier; public test key3',
            content_repeat='archive\x00event é\n', content_count=4096,
            event={key: value for key, value in event.items() if key != 'content'},
            serialization_fields=list(event), signed_event_bytes=len(body),
            signed_event_sha256=hashlib.sha256(body).hexdigest())
        raw = (json.dumps(recipe, indent=2) + '\n').encode()
    else:
        raw = body
    return raw, dict(case=case, event_id=event['id'], **metrics,
        signed_event_sha256=hashlib.sha256(body).hexdigest(),
        canonical_sha256=hashlib.sha256(canonical(event)).hexdigest(),
        fixture_sha256=hashlib.sha256(raw).hexdigest(),
        verification='python-secp256k1 plus independent BIP340 equation; public test key3')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=CASES, default='archive-sample')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--check', action='store_true', help='reconstruct and compare without writing')
    args = parser.parse_args()
    path = args.output or Path(__file__).with_name('archive-event-response.json'
        if args.case == 'archive-sample' else 'archive-event-' + args.case + '.json')
    raw, evidence = build(args.case)
    if args.check:
        assert path.read_bytes() == raw, 'complete frozen fixture bytes'
    elif args.case == 'archive-sample':
        path.write_bytes(raw)  # Preserve the original explicit generator behavior.
    else:
        with path.open('xb') as stream:
            stream.write(raw)
    print(json.dumps(dict(status='PASS_OFFLINE_SIGNED_INPUT', **evidence)))


if __name__ == '__main__':
    main()
