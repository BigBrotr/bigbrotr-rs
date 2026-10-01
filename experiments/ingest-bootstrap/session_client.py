#!/usr/bin/env python3
"""Independent HTTP and actual CLI clients inside the isolated engine container."""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

directory, mode = Path(sys.argv[1]), sys.argv[2]
assert directory.is_absolute() and mode in ('journey', 'hash-stop')
origin = 'http://127.0.0.1:18081'
password = 'Independent owner password 2026'


def request(method, path, body=None, headers=None):
    connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=3)
    try:
        fields = {'Origin': origin, **(headers or {})}
        if body is not None:
            fields['Content-Type'] = 'application/json'
        connection.request(method, path, None if body is None else json.dumps(body), fields)
        response = connection.getresponse()
        data = response.read()
        return response.status, data
    finally:
        connection.close()


def cli(*args):
    result = subprocess.run(['/build/bigbrotr', 'access', *args],
                            env={}, capture_output=True, timeout=5)
    assert result.returncode == 0, (result.returncode, result.stderr.decode())
    assert password.encode() not in result.stdout + result.stderr
    return json.loads(result.stdout)


def journey():
    credential = directory / 'password.private'
    with os.fdopen(os.open(credential, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write(password)
    session = directory / 'session.private'
    logged_in = cli('login', '--origin', origin, '--username', 'alice',
                    '--password-ref', 'file:' + str(credential), '--session-output', str(session))
    saved = json.loads(session.read_text())
    assert saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == origin
    assert session.stat().st_mode & 0o777 == 0o600
    auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
    status, body = request('GET', '/management/v1/me', headers=auth)
    assert status == 200
    observed = json.loads(body)
    assert observed['format'] == 'bigbrotr-session/1'
    assert observed['principal_id'] == '10' * 16 and observed['authority'] == 'observation-only'
    assert observed['stored_actions'] == ['policy.change']
    assert request('GET', '/management/v1/me')[0] == 401
    assert request('POST', '/management/v1/sessions',
                {'username': 'alice', 'password': 'deliberately incorrect'})[0] == 401
    for method, path, status in [
        ('GET', '/v1/resources/event_history', 404),
        ('POST', '/management/v1/jobs', 405),
        ('POST', '/management/v1/sessions/current/reauthenticate', 405),
    ]:
        assert request(method, path, headers=auth)[0] == status

    started = time.monotonic()
    deadline = started + 8
    def reader():
        counts = {}
        while time.monotonic() < deadline:
            status, body = request('GET', '/management/v1/me', headers=auth)
            assert status in (200, 429, 503), (status, body)
            if status == 200:
                current = json.loads(body)
                assert all(current[k] == observed[k] for k in (
                    'format', 'principal_id', 'credential_revision', 'grant_revision',
                    'config_revision', 'policy_revision', 'stored_actions', 'authority'))
            counts[status] = counts.get(status, 0) + 1
        return counts
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        counts = list(pool.map(lambda _: reader(), range(12)))
    elapsed = time.monotonic() - started
    assert all(c.get(200, 0) > 0 for c in counts), counts
    assert sum(c.get(200, 0) for c in counts) >= 24
    logged_out = cli('logout', '--session-ref', 'file:' + str(session))
    assert json.loads(session.read_text()) == saved
    assert request('GET', '/management/v1/me', headers=auth)[0] == 401
    assert all(secret not in json.dumps([logged_in, logged_out]) for secret in (saved['session'], saved['csrf']))
    return dict(login=logged_in, logout=logged_out, readers=counts, elapsed_seconds=elapsed,
                unauthenticated=401, incorrect_password=401, revoked_session=401)


def hash_stop():
    identity = json.loads(Path(str(directory) + '.pid.json').read_text())
    process = Path('/proc') / str(identity['pid'])
    assert process.joinpath('stat').read_text().split()[21] == identity['start_ticks']
    assert process.joinpath('cmdline').read_bytes().split(b'\0')[:4] == [
        b'/build/bigbrotr', b'serve', b'--file', identity['config'].encode()]
    before = {p.name for p in process.joinpath('task').iterdir()}
    body = json.dumps({'username': 'alice', 'password': password}).encode()
    connection = socket.create_connection(('127.0.0.1', 18081), 2)
    connection.sendall((f'POST /management/v1/sessions HTTP/1.1\r\nHost: 127.0.0.1:18081\r\n'
                        f'Origin: {origin}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n').encode() + body)
    deadline = time.monotonic() + 3
    sample = None
    while sample is None:
        assert time.monotonic() < deadline, 'no running password worker observed'
        for task in process.joinpath('task').iterdir():
            if task.name in before:
                continue
            stat = task.joinpath('stat').read_text().split()
            if stat[1] == '(tokio-runtime-w)' and stat[2] == 'R' and int(stat[13]) > 0:
                # The initial runtime has exactly two blocking slots: its
                # permanent native owner and this newly started password worker.
                sample = dict(tid=int(task.name), state=stat[2], user_ticks=int(stat[13]),
                              start_ticks=stat[21], prior_tasks=sorted(before))
                os.kill(identity['pid'], signal.SIGSTOP)
                break
        time.sleep(.001)
    try:
        assert process.joinpath('stat').read_text().split()[21] == identity['start_ticks']
        os.kill(identity['pid'], signal.SIGTERM)
    finally:
        os.kill(identity['pid'], signal.SIGCONT)
    connection.settimeout(4)
    try:
        received = connection.recv(65536)
    except ConnectionResetError:
        received = b''
    finally:
        connection.close()
    assert received == b'', 'credentials delivered after interrupted hash'
    return dict(worker=sample, socket_bytes=0, signal='SIGTERM during running password work')


result = journey() if mode == 'journey' else hash_stop()
with (directory / 'client-result.json').open('x') as stream:
    json.dump(dict(status='PASS', mode=mode, **result), stream, indent=2)
print(json.dumps(dict(status='PASS', mode=mode)))
