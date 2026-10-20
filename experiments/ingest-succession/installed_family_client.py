"""Installed repeatable successor process and authenticated finite wire client.

The default relay sends a distinct signed event, the retained predecessor event,
then the distinct event again. The opt-in progress transport sends the resend in
a later real REQ, after a host capture gate. Neither mode reads SQL or supplies
product receipts; the gate is fixture coordination, not checkpoint authority.
"""
import base64
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys
import threading
import time

from installed_sync_client import exact, publish, read_frame, relay_listener, send_frame

ENDPOINT = 'ws://127.0.0.1:7447/installed-successor-fixture'
ORIGIN = 'http://127.0.0.1:18081'
EVENT_IDS = ('8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849',
    '57b20f44c0095dfc789765ed38caa8dd54ae37635dab32237c563146c8865b02')


def serve(config, prefix):
    assert config.is_absolute() and prefix.is_absolute()
    command = ['/build/bigbrotr', 'serve', '--file', str(config),
        '--activate-successor', '--roles', 'control,ingest,sync']
    started = time.monotonic()
    with Path(str(prefix) + '.stdout').open('xb') as out, Path(str(prefix) + '.stderr').open('xb') as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env={})
        forced = False
        identity = dict(pid=child.pid, config=str(config), prefix=str(prefix), command=command)
        try:
            identity['start_ticks'] = int(Path(f'/proc/{child.pid}/stat').read_text().split()[21])
            publish(prefix.parent, prefix.name + '.pid.json', identity)
            child.wait(timeout=55)
        except BaseException:
            forced = True
            child.kill()
            child.wait(timeout=5)
            raise
        finally:
            publish(prefix.parent, prefix.name + '.terminal.json', dict(identity,
                exit_code=child.returncode, reaped=child.returncode is not None,
                forced=forced, elapsed_seconds=time.monotonic() - started))


def request(method, path, body=None, headers=None):
    connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=3)
    try:
        fields = {'Origin': ORIGIN, **(headers or {})}
        if body is not None:
            fields['Content-Type'] = 'application/json'
        connection.request(method, path, None if body is None else json.dumps(body), fields)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def access(*arguments):
    result = subprocess.run(['/build/bigbrotr', 'access', *arguments], env={},
        capture_output=True, timeout=5)
    assert result.returncode == 0 and not result.stderr, (result.returncode, result.stderr)
    return json.loads(result.stdout)


def progress_release(directory, mutation_request):
    """Read only the bounded host gate; the offline reader binds its capture."""
    path = directory / 'progress-release.json'
    if not path.exists():
        return None
    assert stat.S_ISREG(path.lstat().st_mode) and path.stat().st_size <= 1024
    raw = path.read_bytes()
    assert len(raw) <= 1024
    def unique(pairs):
        value = {}
        for key, item in pairs:
            assert key not in value, 'duplicate progress gate key'
            value[key] = item
        return value
    gate = json.loads(raw, object_pairs_hook=unique)
    assert set(gate) == {'format', 'mutation_request', 'observation_sha256'}
    assert gate['format'] == 'bigbrotr-progress-transport-release/1'
    assert gate['mutation_request'] == mutation_request
    assert re.fullmatch('[0-9a-f]{64}', gate['observation_sha256'])
    return dict(gate, release_sha256=hashlib.sha256(raw).hexdigest())


def relay(directory, listener, finished, errors, progress=False, hole=False, ack=False):
    assert type(progress) is type(hole) is bool and (not hole or progress)
    assert type(ack) is bool and (not ack or (progress and not hole))
    if ack:
        from installed_progress_transport import ack_events
        selected_events = ack_events(directory,successor=True)
        assert selected_events is not None
        assert json.loads((directory/'progress-context.json').read_text()) == dict(
            format='bigbrotr-ack-context/1',kind='successor')
    old, new = json.loads((directory / 'events.json').read_text())
    assert (old['id'], new['id']) == EVENT_IDS
    for event in (old, new):
        canonical = json.dumps([0, event['pubkey'], event['created_at'], event['kind'],
            event['tags'], event['content']], ensure_ascii=False, separators=(',', ':')).encode()
        assert hashlib.sha256(canonical).hexdigest() == event['id']
        assert event['kind'] == 1 and len(bytes.fromhex(event['sig'])) == 64
    nonces = set()
    mutation_request, proof_sent = None, False
    try:
        number = 0
        while not finished.is_set():
            try:
                connection, remote = listener.accept()
            except socket.timeout:
                continue
            number += 1
            assert number <= 128 and remote[0] == '127.0.0.1'
            trace = dict(number=number, connected_at_us=time.time_ns() // 1000,
                clock_resolution_us=math.ceil(time.clock_getres(time.CLOCK_REALTIME) * 1_000_000))
            with connection:
                connection.settimeout(3)
                header = bytearray()
                while not header.endswith(b'\r\n\r\n'):
                    assert len(header) < 4096
                    header.extend(exact(connection, 1))
                lines = header.decode('ascii').split('\r\n')
                assert lines[0] == 'GET /installed-successor-fixture HTTP/1.1'
                fields = {}
                for line in lines[1:]:
                    if line:
                        key, value = line.split(':', 1)
                        assert key.lower() not in fields
                        fields[key.lower()] = value.strip()
                assert fields['host'] == '127.0.0.1:7447'
                assert fields['upgrade'].lower() == 'websocket'
                assert fields['connection'].lower() == 'upgrade'
                assert fields['sec-websocket-version'] == '13'
                key = fields['sec-websocket-key']
                assert len(base64.b64decode(key, validate=True)) == 16
                accepted = base64.b64encode(hashlib.sha1(
                    (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
                connection.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                    b'Connection: Upgrade\r\nSec-WebSocket-Accept: ' + accepted + b'\r\n\r\n')
                opcode, raw = read_frame(connection)
                command = json.loads(raw)
                assert opcode == 1 and len(command) == 3 and command[0] == 'REQ'
                ident = command[1]
                assert re.fullmatch('[0-9a-f]{48}', ident) and int(ident[:32], 16)
                assert ident[:32] not in nonces
                nonces.add(ident[:32])
                assert command[2] == {'kinds': [1], 'limit': 3}
                trace.update(request=command, request_at_us=time.time_ns() // 1000,
                    request_monotonic_ns=time.monotonic_ns())
                publish(directory, f'relay-open-{number:03}.json', trace)
                release = (progress_release(directory, mutation_request)
                    if progress and number > 1 and not proof_sent else None)
                if progress and (number == 1 or release is not None):
                    if number == 1:
                        mutation_request = ident
                        phase, values, labels = 'mutation', (new, old), ['distinct', 'predecessor']
                        if hole:
                            # Correlated valid JSON that cannot decode as an
                            # EventIntent. Later real signed frames must still
                            # reach ordinary admission, without prefix progress.
                            values, labels = ({}, new, old), ['preparation-hole', *labels]
                    else:
                        # Select once before sending. A partial write/refused
                        # close cannot silently retry this phase on another REQ.
                        proof_sent = True
                        phase, values, labels = 'proof-only', (new,), ['distinct-resend']
                    if ack:
                        values, labels = selected_events, ['signed']*3
                    trace.update(progress_phase=phase, frames=labels + ['EOSE'],
                        event_ids=[value.get('id') for value in values],
                        event_json_sha256=[hashlib.sha256(json.dumps(value,
                            ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
                            for value in values],
                        sent_at_us=time.time_ns() // 1000)
                    if release is not None:
                        trace['progress_release'] = release
                    for value in values:
                        send_frame(connection, ['EVENT', ident, value])
                    send_frame(connection, ['EOSE', ident])
                    opcode, raw = read_frame(connection)
                    assert opcode == 1 and json.loads(raw) == ['CLOSE', ident]
                    opcode, raw = read_frame(connection)
                    assert opcode == 8 and raw == b''
                    connection.sendall(b'\x88\x00')
                    connection.shutdown(socket.SHUT_WR)
                    assert connection.recv(1) == b''
                    trace['completion'] = 'request-and-websocket-close'
                elif number == 1:
                    trace.update(frames=['distinct', 'predecessor', 'distinct-resend', 'EOSE'],
                        event_ids=[new['id'], old['id'], new['id']], sent_at_us=time.time_ns() // 1000)
                    for value in (new, old, new):
                        send_frame(connection, ['EVENT', ident, value])
                    send_frame(connection, ['EOSE', ident])
                    opcode, raw = read_frame(connection)
                    assert opcode == 1 and json.loads(raw) == ['CLOSE', ident]
                    opcode, raw = read_frame(connection)
                    assert opcode == 8 and raw == b''
                    connection.sendall(b'\x88\x00')
                    connection.shutdown(socket.SHUT_WR)
                    assert connection.recv(1) == b''
                    trace['completion'] = 'request-and-websocket-close'
                else:
                    trace['frames'] = []
                    assert connection.recv(1) == b'', 'quiet receive did not close its actual socket'
                    trace['completion'] = 'quiet-eof'
                trace['closed_at_us'] = time.time_ns() // 1000
                trace['closed_monotonic_ns'] = time.monotonic_ns()
                assert trace['closed_at_us'] - trace['request_at_us'] < 3_000_000
                publish(directory, f'relay-closed-{number:03}.json', trace)
    except BaseException as error:
        errors.append(type(error).__name__ + ': ' + str(error))
        publish(directory, 'relay-error.json', dict(error=errors[-1]))
    finally:
        listener.close()


def journey(directory, progress=False, hole=False, commit_loss=False, ack=False):
    assert type(progress) is type(hole) is bool and (not hole or progress)
    assert type(commit_loss) is bool and (not commit_loss or (progress and not hole))
    assert type(ack) is bool and (not ack or (progress and not hole and not commit_loss))
    if progress:
        assert not any((directory / name).exists() or (directory / name).is_symlink() for name in (
            'progress-release.json', 'progress-mutation-observation.json',
            'progress-proof-only-observation.json')), 'progress transport requires fresh capture paths'
    fixture = json.loads((directory / 'fixture.json').read_text())
    assert set(fixture) == {'config_revision', 'policy_revision', 'visibility'}
    password = 'Independent owner password 2026'
    secret = directory / 'password.private'
    with os.fdopen(os.open(secret, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write(password)
    session = directory / 'session.private'
    logged_in = access('login', '--origin', ORIGIN, '--username', 'alice',
        '--password-ref', 'file:' + str(secret), '--session-output', str(session))
    saved = json.loads(session.read_text())
    assert saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == ORIGIN
    assert session.stat().st_mode & 0o777 == 0o600
    auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
    status, body = request('GET', '/management/v1/me', headers=auth)
    me = json.loads(body)
    assert status == 200 and me['principal_id'] == '10' * 16 and me['authority'] == 'observation-only'
    assert me['config_revision'] == fixture['config_revision']
    assert me['policy_revision'] == fixture['policy_revision']
    assert me['stored_actions'] == ['data.read', 'policy.change', 'principals.manage',
        'roles.control', 'status.read']
    assert request('GET', '/management/v1/me')[0] == 401
    observations = []

    def call(path, value, key):
        status, body = request('POST', '/management/v1/' + path, value,
            {**auth, 'Idempotency-Key': key})
        answer = json.loads(body)
        observations.append(dict(path=path, input=value, key=key, status=status, body=answer))
        publish(directory, f'command-{len(observations)}.json', observations[-1])
        assert status == 202 and answer['created'] and answer['command_key'] == key, answer
        return answer

    finished, errors = threading.Event(), []
    visibility = fixture['visibility']
    revisions = dict(expected_config_revision=fixture['config_revision'],
        expected_policy_revision=fixture['policy_revision'])
    endpoint = call('relay-endpoints', dict(url=ENDPOINT, **revisions), 'e1' * 16)
    enrollment = call('relay-enrollments', dict(endpoint_id=endpoint['endpoint_id'], visibility=visibility,
        route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True,
        expected_generation=None, **revisions), 'e2' * 16)
    assert enrollment['generation'] == '1' and enrollment['enabled']
    scope = call('acquisition-scopes', dict(endpoint_id=endpoint['endpoint_id'],
        access_scope_id=enrollment['access_scope_id'], visibility=visibility,
        selector=dict(version=1, filters=[{'kinds': [1]}], history_since=0, ephemeral='retain'),
        **revisions), 'e3' * 16)
    worker = threading.Thread(target=relay,
        args=(directory, relay_listener(), finished, errors, progress, hole, ack))
    worker.start()

    def barrier(name):
        publish(directory, name + '.json', observations)
        deadline = time.monotonic() + 25
        while not (directory / (name + '.continue')).exists():
            assert not errors, errors
            assert time.monotonic() < deadline, name
            time.sleep(.01)

    try:
        barrier('prepared')
        receipt = call('jobs', dict(kind='acquisition', scope_id=scope['scope_id'],
            deadline_us='120000000', **revisions), 'e4' * 16)
        assert receipt['execution'] == 'unobserved' and receipt['authority'] == 'acceptance-only'
        barrier('submitted')
        observed = logged_out = None
        if not commit_loss:
            status, raw = request('GET', '/management/v1/jobs/' + receipt['job_id'], headers=auth)
            observed = json.loads(raw)
            assert status == 200 and observed == dict(format='bigbrotr-job-status/1', job_id=receipt['job_id'],
                kind='acquisition', state='queued', job_revision='2', checkpoint_revision='0',
                created_at_us=receipt['accepted_at_us'], completed_at_us=None,
                observed_at_us=observed['observed_at_us'], reason_code=None, authority='observation-only')
            logged_out = access('logout', '--session-ref', 'file:' + str(session))
            assert json.loads(session.read_text()) == saved
            assert request('GET', '/management/v1/me', headers=auth)[0] == 401
        barrier('commands-finished')
    finally:
        finished.set()
        worker.join(4)
        assert not worker.is_alive(), 'relay thread not joined'
    assert not errors, errors
    digests = {key: hashlib.sha256(domain + bytes.fromhex(saved[key])).hexdigest()
        for key, domain in [('session', b'bigbrotr/browser-session/1\0'),
            ('csrf', b'bigbrotr/browser-csrf/1\0')]}
    result = dict(status='PASS', login=logged_in, logout=logged_out, me=me, digests=digests,
        observations=observations, job_status=observed, relay_joined=True, unauthenticated=401)
    if progress:
        closed = [json.loads(path.read_text()) for path in sorted(directory.glob('relay-closed-*.json'))]
        phases = [value['progress_phase'] for value in closed if 'progress_phase' in value]
        assert phases == ['mutation', 'proof-only'], 'both separate transport phases must complete'
        result.update(status='CAPTURED_PROGRESS_TRANSPORT_ONLY',
            checkpoint_validation='NOT_RUN', native_proof_validation='NOT_RUN')
        if ack:
            result['native_ack_validation']='NOT_RUN'
    assert all(secret not in json.dumps(result) for secret in (saved['session'], saved['csrf'], password))
    publish(directory, 'client-result.json', result)
    return result


if __name__ == '__main__':
    assert len(sys.argv) in (3, 4)
    if sys.argv[1] == 'serve':
        assert len(sys.argv) == 4
        serve(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        assert sys.argv[1] in ('journey', 'journey-progress', 'journey-progress-hole', 'journey-progress-loss', 'journey-progress-ack') and len(sys.argv) == 3
        journey(Path(sys.argv[2]), progress=sys.argv[1] != 'journey',
            hole=sys.argv[1] == 'journey-progress-hole', commit_loss=sys.argv[1] == 'journey-progress-loss',
            ack=sys.argv[1] == 'journey-progress-ack')
