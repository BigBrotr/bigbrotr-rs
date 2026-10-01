"""Literal scope command bytes for clone-only relational negative controls."""
import hashlib
from pathlib import Path
from scopes import CANONICAL

CASES = {'scope-command-relations'}
CHECKS = [
    ('valid-before', 'ZX001'), ('missing-detail', '23514'),
    ('missing-command', '23503'), ('missing-audit', '23503'),
    ('wrong-scope', '23514'), ('wrong-audit-time', '23514'),
    ('extra-audit', '23514'), ('old-owner', '23514'), ('absent-session', '23514'),
    ('revoked-session', 'BB001'), ('credential-revision', 'BB001'),
    ('authentication-revision', 'BB001'), ('grant-revision', 'BB001'),
    ('mutate-detail', '23514'), ('delete-detail', '23514'),
    ('valid-new-scope', 'ZX001'), ('new-scope-missing-detail', '23514'),
    ('new-scope-revoked-session', 'BB001'), ('missing-grant', 'BB001'),
    ('valid-after', 'ZX001'),
]
ALTERNATE = b'{"version":1,"filters":[{"kinds":[3]}],"history_since":0,"ephemeral":"retain"}'


def render(state):
    scope, = state['acquisition_scopes']
    selector, = state['acquisition_selectors']
    assert selector['document'] == '\\x' + CANONICAL.hex()
    assert scope['visibility_kind'] == 4 and scope['visibility_ordinal'] == 2
    dataset = bytes.fromhex(scope['dataset_id'][2:])
    prefix = (b'bigbrotr/acquisition-scope-registration/1\0' + dataset
        + bytes.fromhex('10' * 16 + '88' * 16)
        + bytes.fromhex(scope['endpoint_id'][2:] + scope['access_scope_id'][2:])
        + dataset + b'\0\4' + bytes.fromhex(scope['visibility_domain'][2:])
        + b''.join(n.to_bytes(8, 'big') for n in (2, 1, 1)))
    digest = lambda document: hashlib.sha256(prefix + len(document).to_bytes(4, 'big') + document).hexdigest()
    replacements = {
        '@fresh_digest@': digest(CANONICAL),
        '@alternate_digest@': digest(ALTERNATE),
        '@alternate_document@': ALTERNATE.hex(),
    }
    fixture = Path(__file__).with_suffix('.sql').read_text()
    for token, value in replacements.items():
        assert fixture.count(token) == 1
        fixture = fixture.replace(token, value)
    return fixture, replacements['@fresh_digest@']
