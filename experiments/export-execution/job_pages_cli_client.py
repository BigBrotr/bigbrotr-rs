#!/usr/bin/env python3
"""Invoke the installed CLI; credentials and raw cursors never leave private files."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
import private_grant_http as http
from commit_client import emit, lock_available


def main():
    directory = http.ROOT / 'cli-sessions'
    directory.mkdir(mode=0o700)
    password = directory / 'password.private'
    with os.fdopen(os.open(password, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write('Independent owner password 2026')
    server = None
    try:
        for line in sys.stdin:
            request = json.loads(line)
            name, op = request['request'], request['operation']
            if op == 'start':
                assert server is None
                server = http.Server('job-pages-cli-' + name)
                emit(name, operation=op, owner=server.ready(3))
                continue
            if op in ('pause', 'stop'):
                assert server is not None
                server.stop()
                assert not Path(f'/proc/{server.process.pid}').exists() and lock_available()
                server.cleanup(); server = None
                emit(name, operation=op, parent_absent=True, lock_available=True)
                if op == 'stop': return
                continue
            user = request['username']
            assert user in ('alice', 'bob')
            session = directory / (user + '.private')
            if op == 'login':
                args = ['access', 'login', '--origin', 'http://127.0.0.1:18080',
                    '--username', user, '--password-ref', 'file:' + str(password),
                    '--session-output', str(session)]
            elif op == 'logout':
                args = ['access', 'logout', '--session-ref', 'file:' + str(session)]
            else:
                assert op == 'page'
                args = ['jobs', 'list', '--session-ref', 'file:' + str(session), '--page-size', '2']
                for field in ('state', 'created-from-us', 'created-before-us'):
                    if field in request: args.extend(['--' + field, request[field]])
                if 'continue' in request:
                    cursor = directory / (request['continue'] + '.cursor.private')
                    assert cursor.is_file()
                    args.extend(['--cursor-ref', 'file:' + str(cursor)])
            prior = session.read_bytes() if session.exists() else None
            started = time.monotonic()
            process = subprocess.run([str(http.BINARY), *args], env={}, capture_output=True, timeout=10)
            observation = json.loads(process.stdout) if process.returncode == 0 else None
            if process.returncode:
                assert process.stdout == b''
            if observation and op == 'page':
                token = observation.pop('next_cursor')
                observation['has_next'] = token is not None
                if token is not None:
                    with os.fdopen(os.open(directory / (name + '.cursor.private'),
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
                        stream.write(token)
                    observation['cursor_sha256'] = hashlib.sha256(token.encode()).hexdigest()
            current = session.read_bytes() if session.exists() else None
            if prior is not None: assert current == prior
            if current is not None:
                saved = json.loads(current)
                assert set(saved) == {'format', 'origin', 'session', 'csrf'}
                assert saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == 'http://127.0.0.1:18080'
                assert session.stat().st_mode & 0o777 == 0o600 and session.stat().st_nlink == 1
                for secret in (saved['session'], saved['csrf']):
                    assert secret.encode() not in process.stdout + process.stderr
            assert password.read_bytes() not in process.stdout + process.stderr
            emit(name, operation=op, exit=process.returncode, observation=observation,
                stderr=process.stderr.decode(), elapsed_seconds=time.monotonic()-started,
                credential_file_unchanged=prior is not None and current == prior,
                credential_file_present=current is not None)
        raise AssertionError('missing explicit stop')
    finally:
        if server is not None: server.cleanup()


if __name__ == '__main__':
    main()
