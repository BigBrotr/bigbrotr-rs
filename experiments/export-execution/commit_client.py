#!/usr/bin/env python3
"""Installed automatic completion through one selected, bounded SQL fault proxy."""
import fcntl
import json
import os
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http


def emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def lock_available():
    descriptor = os.open('/out/signed_normal/export-destination', os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True
    finally:
        os.close(descriptor)


def main(mode, port, policy_revision=6):
    assert mode in ('before-forward', 'after-complete', 'restart')
    assert policy_revision in (4, 6)
    original = http.CONFIG
    config = original.read_text()
    if mode != 'restart':
        assert 0 < port < 65536
        needle = '[postgres.control]\nendpoint="127.0.0.1:5432"'
        assert config.count(needle) == 1
        http.CONFIG = http.ROOT / ('executor-commit-' + mode + '.toml')
        http.CONFIG.write_text(config.replace(needle, f'[postgres.control]\nendpoint="127.0.0.1:{port}"'))
    server = http.Server('executor-commit-' + mode)
    try:
        if mode == 'restart':
            owner = server.ready(policy_revision)
            assert owner['listeners'] == []
            emit('ready', owner=owner)
            assert sys.stdin.readline() == 'stop\n'
            server.stop()
            assert lock_available()
            emit('terminal', exit=0, lock_available=True)
            return
        # Automatic execution may finish before ready() can sample a live parent.
        # Collect the actual first runtime record after its finite failure instead.
        code = server.process.wait(timeout=15)
        stderr = server.error.read_text()
        lines = [json.loads(line) for line in server.output.read_text().splitlines()]
        assert code == 4 and 'CommitUnknown' in stderr, (code, stderr)
        assert lines and lines[0]['state'] == 'idle' and lines[0]['policy_revision'] == str(policy_revision)
        assert lines[0]['listeners'] == [] and all(v.get('release') != 'committed' for v in lines)
        assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
        assert not list((http.ROOT / 'scratch').iterdir())
        emit('failed', exit=code, stderr=stderr, owner=lines[0], parent_absent=True, lock_available=True)
    finally:
        server.cleanup()
        http.CONFIG = original


if __name__ == '__main__':
    assert len(sys.argv) in (3, 4)
    main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]) if len(sys.argv)==4 else 6)
