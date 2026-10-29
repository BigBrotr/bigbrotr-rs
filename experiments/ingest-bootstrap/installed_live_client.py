"""Independent installed successor process and authenticated finite wire client.

The relay sends a distinct signed event, the retained predecessor event, then
the distinct event again. It never reads SQL or supplies product receipts.

Both commands accept --profile DIRECTORY containing the existing strict
fixture-profile.json. An explicit profile must exist; omitting the option keeps
the legacy binary and owner fixture. Journey revisions default to 2 and can be
supplied independently as canonical positive u64 decimal strings.
Only --live-index-proof adds a separate fixed45s phase after the queued-job
observation and before logout; ordinary source/client barriers remain25s.
"""
import argparse
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
import threading
import time

from installed_sync_client import exact, publish, read_frame, relay_listener, send_frame

ENDPOINT = 'ws://127.0.0.1:7447/installed-live-fixture'
ORIGIN = 'http://127.0.0.1:18081'
FIXTURE_SHA = '64f3d2ad64cf6960aaa9908e8b85b21205ca89786cb2c21dacb3c4e8e4d1938f'
LIVE_INDEX_PROOF_SECONDS = 45


def barrier(directory, name, observations, errors, *, phase_clock=False):
    """The child owns each fixed deadline, including its release decision."""
    assert name in ('prepared', 'submitted', 'commands-finished') or (phase_clock and name == 'index-proof')
    if phase_clock:
        started = time.monotonic_ns()
        seconds = LIVE_INDEX_PROOF_SECONDS if name == 'index-proof' else 25
        deadline = started + seconds * 1_000_000_000
        phase = dict(format='bigbrotr-installed-live-phase/1', phase=name,
            started_monotonic_ns=started, deadline_monotonic_ns=deadline)
        publish(directory, name + '-phase.json', phase)
    publish(directory, name + '.json', observations)
    if not phase_clock:
        # The ordinary finite client's existing25s starts after publication.
        deadline = time.monotonic_ns() + 25_000_000_000
    while True:
        assert not errors, errors
        assert time.monotonic_ns() < deadline, name
        if (directory / (name + '.continue')).exists():
            accepted = time.monotonic_ns()
            assert accepted < deadline, name
            if phase_clock:
                release = dict(phase, format='bigbrotr-installed-live-phase-release/1',
                    accepted_monotonic_ns=accepted)
                publish(directory, name + '-phase-released.json', release)
                return release
            return None
        time.sleep(.01)


def explicit_profile(directory):
    if directory is None:
        return None
    # Legacy labs copy only this file and installed_sync_client.py.
    from installed_archive_profile import load
    profile = load(directory)
    assert profile is not None, 'explicit fixture profile required'
    return profile


def revision(value):
    if (not isinstance(value, str) or re.fullmatch('[1-9][0-9]{0,19}', value) is None
            or int(value) > 2**64 - 1):
        raise argparse.ArgumentTypeError('canonical positive u64 revision required')
    return value


def serve(config, prefix, profile_directory=None):
    profile = explicit_profile(profile_directory)
    binary = profile['product_binary'] if profile else '/build/bigbrotr'
    assert config.is_absolute() and prefix.is_absolute()
    command = [binary, 'serve', '--file', str(config),
        '--activate-initial-successor', '--roles', 'control,ingest,sync']
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


def access(*arguments, binary='/build/bigbrotr', failure_directory=None):
    parent = None
    try:
        if failure_directory is not None:
            assert arguments[0] in ('login', 'logout')
            parent = os.open(failure_directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            metadata = os.fstat(parent)
            assert metadata.st_uid == os.geteuid() and stat.S_IMODE(metadata.st_mode) == 0o700, 'private evidence directory required'

        def retain(stdout, stderr, exit_code, timed_out=False):
            if parent is None:
                return
            descriptor = os.open(arguments[0] + '-failure.private',
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent)
            with os.fdopen(descriptor, 'w') as stream:
                json.dump(dict(exit_code=exit_code, timed_out=timed_out,
                    stdout_base64=base64.b64encode(stdout or b'').decode('ascii'),
                    stderr_base64=base64.b64encode(stderr or b'').decode('ascii')), stream)

        try:
            result = subprocess.run([binary, 'access', *arguments], env={},
                capture_output=True, timeout=5)
        except subprocess.TimeoutExpired as error:
            retain(error.stdout, error.stderr, None, timed_out=True)
            raise AssertionError('private CLI command timed out') from None
        if result.returncode != 0 or result.stderr:
            retain(result.stdout, result.stderr, result.returncode)
            raise AssertionError('private CLI command refused')
        return json.loads(result.stdout)
    finally:
        if parent is not None:
            os.close(parent)


def relay(directory, listener, finished, errors):
    old = json.loads((directory / 'event.jsonl').read_bytes())
    artifact = (directory / 'selector-events.json').read_bytes()
    assert hashlib.sha256(artifact).hexdigest() == FIXTURE_SHA
    selected, = [e for e in json.loads(artifact)['events'] if e['name'] == 'included']
    new = selected['event']
    canonical = json.dumps([0, new['pubkey'], new['created_at'], new['kind'], new['tags'], new['content']],
        ensure_ascii=False, separators=(',', ':')).encode()
    assert canonical.hex() == selected['canonical_hex']
    assert hashlib.sha256(canonical).hexdigest() == new['id'] != old['id']
    split = None
    if (directory / 'progress-context.json').exists():
        from installed_progress_transport import SplitEvents, ack_events
        split = SplitEvents(directory, new, 'activated', event_sequence=ack_events(directory))
    nonces = set()
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
                assert lines[0] == 'GET /installed-live-fixture HTTP/1.1'
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
                selected = split.select(ident, number) if split is not None else None
                if selected is not None:
                    values = split.describe(trace, selected)
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
                elif split is None and number == 1:
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


def journey(directory, profile_directory=None, expected_config_revision='2', expected_policy_revision='2',
            *, live_index_proof=False):
    assert type(live_index_proof) is bool
    expected_config_revision = revision(expected_config_revision)
    expected_policy_revision = revision(expected_policy_revision)
    profile = explicit_profile(profile_directory)
    binary = profile['product_binary'] if profile else '/build/bigbrotr'
    if profile is None:
        password = 'Independent owner password 2026'
        secret = directory / 'password.private'
        with os.fdopen(os.open(secret, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
            stream.write(password)
        password_ref = 'file:' + str(secret)
    else:
        from installed_archive_profile import password_file
        password_ref = profile['password_ref']
        # Retain sensitive bytes only to check final result redaction.
        password = password_file(password_ref)
    session = directory / 'session.private'
    logged_in = access('login', '--origin', ORIGIN, '--username', 'alice',
        '--password-ref', password_ref, '--session-output', str(session), binary=binary, failure_directory=directory)
    saved = json.loads(session.read_text())
    assert saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == ORIGIN
    assert session.stat().st_mode & 0o777 == 0o600
    auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
    status, body = request('GET', '/management/v1/me', headers=auth)
    me = json.loads(body)
    principal = profile['expected_principal_id'] if profile else '10' * 16
    actions = profile['expected_actions'] if profile else ['data.read', 'policy.change',
        'principals.manage', 'roles.control', 'status.read']
    assert status == 200 and me['principal_id'] == principal and me['authority'] == 'observation-only'
    assert me['config_revision'] == expected_config_revision and me['policy_revision'] == expected_policy_revision
    assert me['stored_actions'] == actions
    assert request('GET', '/management/v1/me')[0] == 401
    observations = []

    def call(path, value, key):
        status, body = request('POST', '/management/v1/' + path, value,
            {**auth, 'Idempotency-Key': key})
        answer = json.loads(body)
        observations.append(dict(path=path, input=value, key=key, status=status, body=answer))
        publish(directory, f'command-{len(observations)}.json', observations[-1])
        assert status == 202 and answer['created'] and answer['command_key'] == key, 'management command refused'
        return answer

    finished, errors = threading.Event(), []
    visibility = json.loads((directory / 'visibility.json').read_text())
    revisions = dict(expected_config_revision=expected_config_revision, expected_policy_revision=expected_policy_revision)
    endpoint = call('relay-endpoints', dict(url=ENDPOINT, **revisions), 'f1' * 16)
    enrollment = call('relay-enrollments', dict(endpoint_id=endpoint['endpoint_id'], visibility=visibility,
        route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True,
        expected_generation=None, **revisions), 'f2' * 16)
    assert enrollment['generation'] == '1' and enrollment['enabled']
    scope = call('acquisition-scopes', dict(endpoint_id=endpoint['endpoint_id'],
        access_scope_id=enrollment['access_scope_id'], visibility=visibility,
        selector=dict(version=1, filters=[{'kinds': [1]}], history_since=0, ephemeral='retain'),
        **revisions), 'f3' * 16)
    worker = threading.Thread(target=relay,
        args=(directory, relay_listener(), finished, errors))
    worker.start()

    def wait(name):
        return barrier(directory, name, observations, errors, phase_clock=live_index_proof)

    try:
        wait('prepared')
        receipt = call('jobs', dict(kind='acquisition', scope_id=scope['scope_id'],
            deadline_us='120000000', **revisions), 'f4' * 16)
        assert receipt['execution'] == 'unobserved' and receipt['authority'] == 'acceptance-only'
        wait('submitted')
        status, raw = request('GET', '/management/v1/jobs/' + receipt['job_id'], headers=auth)
        observed = json.loads(raw)
        assert status == 200 and observed == dict(format='bigbrotr-job-status/1', job_id=receipt['job_id'],
            kind='acquisition', state='queued', job_revision='2', checkpoint_revision='0',
            created_at_us=receipt['accepted_at_us'], completed_at_us=None,
            observed_at_us=observed['observed_at_us'], reason_code=None, authority='observation-only')
        proof_phase = wait('index-proof') if live_index_proof else None
        logged_out = access('logout', '--session-ref', 'file:' + str(session), binary=binary, failure_directory=directory)
        assert json.loads(session.read_text()) == saved
        assert request('GET', '/management/v1/me', headers=auth)[0] == 401
        wait('commands-finished')
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
    if live_index_proof:
        result['live_index_proof'] = proof_phase
    serialized = json.dumps(result)
    assert all(json.dumps(secret)[1:-1] not in serialized
        for secret in (saved['session'], saved['csrf'], password)), 'private result contains sensitive bytes'
    publish(directory, 'client-result.json', result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    serving = commands.add_parser('serve')
    serving.add_argument('config', type=Path)
    serving.add_argument('prefix', type=Path)
    traveling = commands.add_parser('journey')
    traveling.add_argument('directory', type=Path)
    for command in (serving, traveling):
        command.add_argument('--profile', type=Path, help='directory containing required fixture-profile.json')
    traveling.add_argument('--expected-config-revision', type=revision, default='2')
    traveling.add_argument('--expected-policy-revision', type=revision, default='2')
    traveling.add_argument('--live-index-proof', action='store_true',
        help='opt in to the separate45s live Index observation phase before logout')
    args = parser.parse_args(argv)
    if args.command == 'serve':
        serve(args.config, args.prefix, args.profile)
    else:
        options = dict(live_index_proof=True) if args.live_index_proof else {}
        journey(args.directory, args.profile, args.expected_config_revision, args.expected_policy_revision, **options)


if __name__ == '__main__':
    main()
