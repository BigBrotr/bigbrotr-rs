"""Explicit independent identity expectations for the existing Archive clients.

Optional fixture-profile.json in their supplied directory has exactly these keys:
  format: bigbrotr-installed-archive-fixture/1
  product_binary: absolute executable path
  password_ref: file:/absolute/protected-password (exact UTF-8, no newline)
  expected_principal_id: independent nonzero lowercase bytes16 hex
  expected_actions: independent exact sorted unique action strings

For a genuinely enrolled owner, freeze actions from the confirmed ADMIN_ACTIONS
in apps/bigbrotr/src/storage/postgres/enrollment.rs, never from observed /me.
The caller supplies the enrollment receipt's principal independently of /me.
Absent profiles retain the clients' legacy fixtures. Username alice, ports,
barriers and all nonidentity protocol/scenario expectations remain unchanged.
"""
import json
import os
from pathlib import Path
import re
import stat


def absolute_path(value):
    assert isinstance(value, str) and value.startswith('/') and not value.endswith('/'), 'absolute fixture path required'
    assert len(value) <= 8192 and not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value), 'invalid fixture path'
    assert not any(part in ('.', '..') for part in value.split('/')), 'relative fixture path component'
    return Path(value)


def password_file(reference):
    assert isinstance(reference, str) and reference.startswith('file:'), 'local password reference required'
    assert len(reference) <= 8192, 'password reference bound'
    path = absolute_path(reference[5:])
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    parent = os.open('/', flags)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, flags, dir_fd=parent)
            os.close(parent)
            parent = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
            dir_fd=parent)
    finally:
        os.close(parent)
    with os.fdopen(descriptor, 'rb') as stream:
        metadata = os.fstat(stream.fileno())
        assert stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.geteuid(), 'protected password owner/type required'
        assert stat.S_IMODE(metadata.st_mode) in (0o400, 0o600), 'protected password mode required'
        assert metadata.st_size <= 1024, 'password byte bound'
        raw = stream.read(1025)
    assert len(raw) <= 1024, 'password byte bound'
    password = raw.decode('utf-8')
    assert len(password) >= 12 and not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in password), 'invalid fixture password'
    return password


def load(directory):
    path = directory / 'fixture-profile.json'
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    assert stat.S_ISREG(metadata.st_mode), 'regular fixture profile required'
    with path.open('rb') as stream:
        raw = stream.read(16385)
    assert len(raw) <= 16384, 'fixture profile byte bound'
    profile = json.loads(raw)
    assert isinstance(profile, dict) and set(profile) == {
        'format', 'product_binary', 'password_ref', 'expected_principal_id', 'expected_actions'}, 'invalid fixture profile fields'
    assert profile['format'] == 'bigbrotr-installed-archive-fixture/1', 'unknown fixture profile'
    binary = absolute_path(profile['product_binary'])
    assert binary.is_file() and os.access(binary, os.X_OK), 'executable product binary required'
    principal = profile['expected_principal_id']
    assert isinstance(principal, str) and re.fullmatch('[0-9a-f]{32}', principal) and int(principal, 16), 'invalid expected principal'
    actions = profile['expected_actions']
    assert isinstance(actions, list) and actions and all(isinstance(action, str) and action for action in actions), 'explicit expected actions required'
    assert actions == sorted(set(actions)), 'expected actions must be unique and sorted'
    # Validate the protected input before launching the CLI. Never serialize it.
    password_file(profile['password_ref'])
    return profile
