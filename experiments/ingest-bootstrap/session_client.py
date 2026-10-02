#!/usr/bin/env python3
"""Independent HTTP and actual CLI clients inside the isolated engine container."""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import time

directory, mode = Path(sys.argv[1]), sys.argv[2]
assert directory.is_absolute() and mode in ('journey', 'hash-stop', 'hash-disconnect', 'renewal', 'enrollment', 'registration', 'scope', 'jobs', 'export-compat', 'installed-sync')
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


def login(expected_actions=('policy.change',), *, username='alice', principal='10'*16, stem=''):
    credential = directory / (stem + 'password.private')
    with os.fdopen(os.open(credential, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write(password)
    session = directory / (stem + 'session.private')
    logged_in = cli('login', '--origin', origin, '--username', username,
                    '--password-ref', 'file:' + str(credential), '--session-output', str(session))
    saved = json.loads(session.read_text())
    assert saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == origin
    assert session.stat().st_mode & 0o777 == 0o600
    auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
    status, body = request('GET', '/management/v1/me', headers=auth)
    assert status == 200
    observed = json.loads(body)
    assert observed['format'] == 'bigbrotr-session/1'
    assert observed['principal_id'] == principal and observed['authority'] == 'observation-only'
    assert observed['stored_actions'] == list(expected_actions)
    return logged_in, session, saved, auth, observed


def logout(session, saved, auth):
    logged_out = cli('logout', '--session-ref', 'file:' + str(session))
    assert json.loads(session.read_text()) == saved
    assert request('GET', '/management/v1/me', headers=auth)[0] == 401
    return logged_out


def journey():
    logged_in, session, saved, auth, observed = login()
    assert request('GET', '/management/v1/me')[0] == 401
    assert request('POST', '/management/v1/sessions',
                {'username': 'alice', 'password': 'deliberately incorrect'})[0] == 401
    for method, path, status in [
        ('GET', '/v1/resources/event_history', 404),
        ('POST', '/management/v1/jobs', 400),  # Selected route, missing command/key.
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
    logged_out = logout(session, saved, auth)
    assert all(secret not in json.dumps([logged_in, logged_out]) for secret in (saved['session'], saved['csrf']))
    return dict(login=logged_in, logout=logged_out, readers=counts, elapsed_seconds=elapsed,
                unauthenticated=401, incorrect_password=401, revoked_session=401)


def interrupt_hash(disconnect=False):
    identity = json.loads(Path(str(directory) + '.pid.json').read_text())
    process = Path('/proc') / str(identity['pid'])
    assert process.joinpath('stat').read_text().split()[21] == identity['start_ticks']
    assert process.joinpath('cmdline').read_bytes().split(b'\0')[:4] == [
        b'/build/bigbrotr', b'serve', b'--file', identity['config'].encode()]
    before = {p.name for p in process.joinpath('task').iterdir()}
    def tasks():
        return {p.name: p.joinpath('stat').read_text().split()[:22]
                for p in process.joinpath('task').iterdir()}
    initial_tasks = tasks()
    new_tasks = {}
    body = json.dumps({'username': 'alice', 'password': password}).encode()
    connection = socket.create_connection(('127.0.0.1', 18081), 2)
    connection.sendall((f'POST /management/v1/sessions HTTP/1.1\r\nHost: 127.0.0.1:18081\r\n'
                        f'Origin: {origin}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n').encode() + body)
    deadline = time.monotonic() + 3
    sample = None
    while sample is None:
        if time.monotonic() >= deadline:
            with (directory / 'hash-observation.json').open('x') as stream:
                json.dump(dict(initial=initial_tasks, new=new_tasks, terminal=tasks()), stream, indent=2)
            raise AssertionError('no running password worker observed')
        for task in process.joinpath('task').iterdir():
            if task.name in before:
                continue
            stat = task.joinpath('stat').read_text().split()
            new_tasks[task.name] = stat[:22]
            # Pinned Tokio1.53.1 Builder default, independently observed in /proc.
            if stat[1] == '(tokio-rt-worker)' and stat[2] == 'R' and int(stat[13]) > 0:
                # The initial runtime has exactly two blocking slots: its
                # permanent native owner and this newly started password worker.
                sample = dict(tid=int(task.name), name=stat[1], state=stat[2], user_ticks=int(stat[13]),
                              start_ticks=stat[21], prior_tasks=sorted(before))
                os.kill(identity['pid'], signal.SIGSTOP)
                break
        time.sleep(.001)
    try:
        assert process.joinpath('stat').read_text().split()[21] == identity['start_ticks']
        if disconnect:
            # Reset the actual in-flight credential request while its hash and
            # server are paused. The original request cannot receive a reply.
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            connection.close()
        else:
            os.kill(identity['pid'], signal.SIGTERM)
    finally:
        os.kill(identity['pid'], signal.SIGCONT)
    if disconnect:
        deadline = time.monotonic() + 2
        while True:
            stat = process.joinpath('task', str(sample['tid']), 'stat').read_text().split()
            assert stat[21] == sample['start_ticks']
            if stat[2] == 'S' and int(stat[13]) > sample['user_ticks']:
                break
            assert time.monotonic() < deadline, 'password work did not become terminal'
            time.sleep(.001)
        # A new successful login proves the one hash slot was actually collected.
        # The host requires exactly one issued/revoked session and its two audits,
        # so late issuance for the abandoned first request cannot be hidden.
        logged_in, session, saved, auth, _ = login()
        logged_out = logout(session, saved, auth)
        return dict(worker=sample, disconnected=True, login=logged_in, logout=logged_out)
    connection.settimeout(4)
    try:
        received = connection.recv(65536)
    except ConnectionResetError:
        received = b''
    finally:
        connection.close()
    assert received == b'', 'credentials delivered after interrupted hash'
    return dict(worker=sample, socket_bytes=0, signal='SIGTERM during running password work')


def enrollment():
    inputs = json.loads((directory / 'command-input.json').read_text())
    permitted = inputs['permitted']
    logged_in, session, saved, auth, _ = login(
        ('policy.change',) if permitted else ('principals.manage',))
    command = inputs['command']
    observations = []
    def change(value, key, status):
        code, body = request('POST', '/management/v1/relay-enrollments', value,
                             {**auth, 'Idempotency-Key': key})
        answer = json.loads(body)
        observations.append(dict(status=code, body=answer))
        assert code == status, (code, answer)
        return answer
    if permitted:
        private = json.loads(json.dumps(command))
        private['route']['cidr'] = '127.0.0.0/8'
        change(private, '81' * 16, 403)
        public = dict(command, route={'class': 'public'})
        change(public, '82' * 16, 403)  # second accepted policy disables public
        first = change(command, '83' * 16, 202)
        assert first['format'] == 'bigbrotr-relay-enrollment/1'
        assert first['endpoint_id'] == command['endpoint_id']
        assert first['generation'] == '1' and first['created'] and first['enabled']
        assert first['authority'] == 'acceptance-only' and first['application'] == 'unobserved'
        historical = change(command, '83' * 16, 200)
        assert historical == dict(first, created=False)
        change(command, '84' * 16, 409)  # new key cannot ignore current generation
        withdrawn = dict(command, enabled=False, expected_generation='1')
        last = change(withdrawn, '85' * 16, 202)
        assert last['generation'] == '2' and last['created'] and not last['enabled']
        assert all(last[k] == first[k] for k in ('endpoint_id', 'access_scope_id', 'route_id'))
        def barrier(name):
            (directory / (name + '.json')).write_text(json.dumps(observations, indent=2))
            deadline = time.monotonic() + 5
            while not (directory / (name + '.continue')).exists():
                assert time.monotonic() < deadline, 'independent enrollment observer'
                time.sleep(.01)
        barrier('before-replay')
        assert change(command, '83' * 16, 200) == historical
        assert change(withdrawn, '85' * 16, 200) == dict(last, created=False)
        barrier('after-replay')
    else:
        change(command, '83' * 16, 403)
    logged_out = logout(session, saved, auth)
    return dict(permitted=permitted, observations=observations, login=logged_in, logout=logged_out)


if mode == 'journey':
    result = journey()
elif mode == 'renewal':
    logged_in, session, saved, auth, _ = login()
    result = dict(login=logged_in, logout=logout(session, saved, auth))
elif mode == 'installed-sync':
    from installed_sync_client import run
    result = run(directory, request, login, logout)
elif mode == 'jobs':
    from job_client import run
    result = run(directory, request, login, logout)
elif mode == 'export-compat':
    from control_status_client import run
    result = run(directory, request, login, logout)
elif mode == 'scope':
    from scope_client import run
    result = run(directory, request, login, logout)
elif mode == 'registration':
    from registration_client import run
    result = run(directory, request, login, logout)
elif mode == 'enrollment':
    result = enrollment()
else:
    result = interrupt_hash(disconnect=mode == 'hash-disconnect')
with (directory / 'client-result.json').open('x') as stream:
    json.dump(dict(status='PASS', mode=mode, **result), stream, indent=2)
print(json.dumps(dict(status='PASS', mode=mode)))
