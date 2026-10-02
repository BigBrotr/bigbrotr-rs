#!/usr/bin/env python3
"""Independent installed login and registration; secret bytes never enter output."""
import http.client
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import time

def read_outcome(connection, mode, *, kind='endpoint'):
    observed = {}
    response = http.client.HTTPResponse(connection)
    wanted = 202 if mode == 'pass' else 200 if mode == 'disconnect' else 503
    try:
        response.begin()
        # A success status remains a false success even if its body is lost.
        # Check it before any body read can fail and retain it in the evidence.
        observed['status'] = response.status
        assert response.status == wanted, 'unexpected command response status'
        data = response.read(4097)
        assert len(data) <= 4096 and response.read(1) == b''
        # Bounded HTTPResponse.read deliberately allows premature fixed-length
        # EOF. An outstanding declared length still means an interrupted body.
        if response.length is not None and response.length > 0:
            raise http.client.IncompleteRead(data, response.length)
        observed['body'] = json.loads(data)
    except (http.client.RemoteDisconnected, http.client.IncompleteRead,
            ConnectionResetError, BrokenPipeError) as error:
        # Socket closure is observable ambiguity, never inferred rollback.
        # begin() can parse the status and then lose the connection in headers.
        if isinstance(response.status, int):
            observed['status'] = response.status
            assert response.status == wanted, 'unexpected command response status'
        observed['closed'] = type(error).__name__
    if mode in ('pass', 'disconnect'):
        assert observed['status'] == (202 if mode == 'pass' else 200)
        receipt = observed['body']
        formats = dict(endpoint='bigbrotr-relay-registration/1', scope='bigbrotr-acquisition-scope-registration/1', job='bigbrotr-job-acceptance/1')
        assert receipt['format'] == formats[kind]
        assert receipt['authority'] == ('acceptance-only' if kind=='job' else 'identity-only')
        assert receipt['command_key'] == '31' * 16
        if kind=='job': assert receipt['execution']=='unobserved'
        assert receipt['created'] == (mode == 'pass')
    else:
        assert 'closed' in observed or observed['status'] == 503, 'ambiguous command reported success'
    return observed


def main():
    directory, mode = Path(sys.argv[1]), sys.argv[2]
    kind = sys.argv[3] if len(sys.argv)==4 else 'endpoint'
    assert len(sys.argv) in (3,4) and kind in ('endpoint','scope','job')
    assert directory.is_absolute() and mode in ('pass', 'before', 'after', 'disconnect', 'signal')
    origin = 'http://127.0.0.1:18081'


    def emit(event, **fields):
        print(json.dumps(dict(event=event, **fields)), flush=True)


    def gate(name):
        deadline = time.monotonic() + 20
        while not (directory / (name + '.go')).exists():
            assert time.monotonic() < deadline, 'controller barrier deadline'
            time.sleep(.01)


    password = directory / 'password.private'
    with os.fdopen(os.open(password, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write('Independent owner password 2026')
    session_file = directory / 'session.private'
    login = subprocess.run(['/build/bigbrotr', 'access', 'login', '--origin', origin,
        '--username', 'alice', '--password-ref', 'file:' + str(password),
        '--session-output', str(session_file)], capture_output=True, env={}, timeout=5)
    assert login.returncode == 0 and not login.stderr, 'actual CLI login failed'
    saved = json.loads(session_file.read_text())
    assert saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == origin
    assert session_file.stat().st_mode & 0o777 == 0o600
    assert all(saved[k].encode() not in login.stdout for k in ('session', 'csrf'))
    if kind=='job':
        from job_http_client import prepare
        command = prepare(directory, origin, saved)
        path = 'jobs'
    elif kind=='scope':
        from scope_http_client import prepare
        command = prepare(directory, origin, saved)
        path = 'acquisition-scopes'
    else:
        command = dict(url='WS://127.0.0.1:7447/a/../installed-fixture',
            expected_config_revision='1', expected_policy_revision='1')
        path = 'relay-endpoints'
    body = json.dumps(command).encode()
    header = (f'POST /management/v1/{path} HTTP/1.1\r\nHost: 127.0.0.1:18081\r\n'
        f'Origin: {origin}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n'
        f'Cookie: BigBrotrLocalSession={saved["session"]}\r\nX-CSRF-Token: {saved["csrf"]}\r\n'
        f'Idempotency-Key: {"31" * 16}\r\nConnection: close\r\n\r\n').encode()
    emit('authenticated')
    gate('send')
    connection = socket.create_connection(('127.0.0.1', 18081), 3)
    connection.settimeout(12)
    try:
        connection.sendall(header + body)
        if mode == 'disconnect':
            gate('disconnect')
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            connection.close()
            emit('disconnected')
            gate('retry')
            connection = socket.create_connection(('127.0.0.1', 18081), 3)
            connection.settimeout(5)
            connection.sendall(header + body)
        emit('done', outcome=read_outcome(connection, mode, kind=kind))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
