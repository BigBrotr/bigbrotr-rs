"""Independent private HTTP client and literal WebSocket relay inside the lab."""
import base64
import hashlib
import json
import math
import re
import socket
import struct
import threading
import time


REFUSAL_SHA256 = '64f3d2ad64cf6960aaa9908e8b85b21205ca89786cb2c21dacb3c4e8e4d1938f'
REFUSED_VISIBILITIES = {'sync-public-visibility': 1, 'sync-absent-visibility': 99,
    'sync-bootstrap-private-visibility': 2}


def refusal_frames(event, artifact):
    """Frozen signed kind22222 is outside kinds1, even with ephemeral retain."""
    assert hashlib.sha256(artifact).hexdigest() == REFUSAL_SHA256
    fixture = json.loads(artifact)
    assert fixture['format'] == 'bigbrotr-selector-signed-fixture/1'
    selected, = [entry for entry in fixture['events'] if entry['name'] == 'ephemeral']
    outside = selected['event']
    canonical = json.dumps([0, outside['pubkey'], outside['created_at'], outside['kind'],
        outside['tags'], outside['content']], ensure_ascii=False, separators=(',', ':')).encode()
    assert canonical.hex() == selected['canonical_hex']
    assert hashlib.sha256(canonical).hexdigest() == outside['id']
    assert outside['kind'] == 22222 and outside['id'] != event['id']
    assert len(bytes.fromhex(outside['sig'])) == 64
    return ({'id': event['id']}, outside, event)


def publish(directory, name, value):
    partial = directory / (name + '.partial')
    with partial.open('x') as stream:
        json.dump(value, stream, indent=2)
    partial.rename(directory / name)


def exact(connection, count):
    value = bytearray()
    while len(value) < count:
        part = connection.recv(count-len(value))
        if not part:
            raise EOFError('peer closed')
        value.extend(part)
    return bytes(value)


def read_frame(connection):
    first, second = exact(connection, 2)
    assert first in (0x81, 0x88), ('unexpected client frame', first)
    assert second & 128, 'client frame must be masked'
    size = second & 127
    assert size != 127, '64-bit command length exceeds fixture profile'
    if size == 126:
        size = int.from_bytes(exact(connection, 2), 'big')
        assert size >= 126
    assert size < 4096, 'client command bound'
    mask = exact(connection, 4)
    value = bytes(b ^ mask[i % 4] for i, b in enumerate(exact(connection, size)))
    return first & 15, value


def send_frame(connection, value):
    body = json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()
    assert len(body) < 65536
    header = bytes([0x81, len(body)]) if len(body) < 126 else b'\x81\x7e'+struct.pack('!H', len(body))
    connection.sendall(header+body)


def relay(directory, listener, finished, errors, signed, refusals=False, window=False,
          event_sender=None):
    assert event_sender is None or (signed and not refusals and not window)
    split = None
    if (directory / 'progress-context.json').exists():
        assert signed and not refusals and not window and event_sender is None
        from installed_progress_transport import SplitEvents, ack_events
        split = SplitEvents(directory, json.loads((directory / 'event.jsonl').read_bytes()), 'initial',
            event_sequence=ack_events(directory))
    request_ids = set()
    try:
        number = 0
        window_rounds = 0
        while not finished.is_set():
            try:
                connection, remote = listener.accept()
            except socket.timeout:
                continue
            number += 1
            assert number <= 128 and remote[0] == '127.0.0.1'
            trace = dict(number=number, connected_at_us=time.time_ns()//1000,
                clock_resolution_us=math.ceil(time.clock_getres(time.CLOCK_REALTIME)*1_000_000))
            with connection:
                connection.settimeout(3)
                header = bytearray()
                while not header.endswith(b'\r\n\r\n'):
                    assert len(header) < 4096
                    header.extend(exact(connection, 1))
                lines = header.decode('ascii').split('\r\n')
                assert lines[0] == 'GET /installed-fixture HTTP/1.1'
                fields = {}
                for line in lines[1:]:
                    if line:
                        key, value = line.split(':', 1)
                        assert key.lower() not in fields
                        fields[key.lower()] = value.strip()
                assert fields['host'] == '127.0.0.1:7447'
                assert fields['upgrade'].lower() == 'websocket'
                assert fields['connection'].lower() == 'upgrade' and fields['sec-websocket-version'] == '13'
                key = fields['sec-websocket-key']
                assert len(base64.b64decode(key, validate=True)) == 16
                accept = base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
                connection.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n')
                opcode, raw = read_frame(connection)
                command = json.loads(raw)
                assert opcode == 1 and len(command) == 3 and command[0] == 'REQ'
                ident = command[1]
                assert re.fullmatch('[0-9a-f]{48}', ident) and int(ident[:32], 16) != 0
                assert ident[:32] not in request_ids, 'connection nonce reused'
                request_ids.add(ident[:32])
                assert command[2] == {'kinds': [1], 'limit': 3}
                trace.update(request=command, request_at_us=time.time_ns()//1000)
                if split is not None:
                    trace['request_monotonic_ns'] = time.monotonic_ns()
                publish(directory, f'relay-open-{number:03}.json', trace)
                window_ready = window and (directory/'window-release').exists() and window_rounds < 2
                selected = split.select(ident, number) if split is not None else None
                if (selected is not None if split is not None else signed and (number <= 2 or window_ready)):
                    event = json.loads((directory/'event.jsonl').read_bytes())
                    if split is not None:
                        values = split.describe(trace, selected)
                    elif event_sender is not None:
                        trace['sent_at_us'] = time.time_ns()//1000
                        trace['frames'] = event_sender(connection, ident, number)
                        values = ()
                    elif window_ready and number > 2:
                        from installed_sync_window import window_frames
                        window_rounds += 1
                        values, labels = window_frames(event, (directory/'selector-events.json').read_bytes(),
                            distinct=directory.name == 'sync-window-event' and window_rounds == 1)
                        trace.update(frames=labels+['EOSE'], window_sequence=window_rounds,
                            window_event_ids=[value['id'] for value in values])
                    elif refusals:
                        values = refusal_frames(event, (directory/'selector-events.json').read_bytes())
                        trace['frames'] = ['malformed-event', 'signed-outside-selector', 'signed', 'EOSE']
                        trace['outside_event_id'] = values[1]['id']
                        trace['selector_fixture_sha256'] = REFUSAL_SHA256
                    else:
                        values = (event, dict(event, sig='00'*64), event)
                        trace['frames'] = ['signed', 'forged-signature', 'duplicate', 'EOSE']
                    if event_sender is None and split is None:
                        trace['sent_at_us'] = time.time_ns()//1000
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
                else:
                    trace['frames'] = []
                    # Deliberately no EVENT or EOSE. A bounded cancelled receive
                    # must destroy this connection; it cannot reuse the session.
                    assert connection.recv(1) == b'', 'quiet quantum did not destroy socket'
                    trace['completion'] = 'quiet-eof'
                trace['closed_at_us'] = time.time_ns()//1000
                if split is not None:
                    trace['closed_monotonic_ns'] = time.monotonic_ns()
                assert trace['closed_at_us']-trace['request_at_us'] < 3_000_000
                publish(directory, f'relay-closed-{number:03}.json', trace)
    except EOFError as error:
        if (directory/'stop-requested').exists():
            publish(directory, 'relay-shutdown.json', dict(error=str(error), phase='incomplete handshake during owned signal'))
        else:
            errors.append(type(error).__name__+': '+str(error))
            publish(directory, 'relay-error.json', dict(error=errors[-1]))
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error))
        publish(directory, 'relay-error.json', dict(error=errors[-1]))
    finally:
        listener.close()


def no_connections(directory, listener, finished, errors):
    """Keep a real listening socket throughout the refused job and owned shutdown."""
    started = time.time_ns()//1000
    connections = 0
    publish(directory, 'relay-monitor-started.json', dict(started_at_us=started))
    try:
        while not finished.is_set():
            try:
                connection, remote = listener.accept()
            except socket.timeout:
                continue
            connections += 1
            connection.close()
            raise AssertionError('source with refused visibility dialed '+remote[0])
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error))
        publish(directory, 'relay-error.json', dict(error=errors[-1]))
    finally:
        listener.close()
        publish(directory, 'relay-monitor-ended.json', dict(started_at_us=started,
            finished_at_us=time.time_ns()//1000, connections=connections))


def relay_listener(address=('127.0.0.1', 7447)):
    listener = socket.socket()
    try:
        # Completed rounds can leave accepted sockets in TIME_WAIT across cases
        # sharing this namespace. REUSEADDR permits restart, not a second live
        # listener; deliberately do not enable REUSEPORT or retry another port.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(address)
        listener.listen(2)
        listener.settimeout(.1)
        return listener
    except BaseException:
        listener.close()
        raise


def run(directory, request, login, logout):
    refusals = directory.name == 'sync-peer-refusals'
    expiry = directory.name == 'sync-expired-before-valid'
    window = directory.name in ('sync-window-event', 'sync-window-source')
    signed = directory.name == 'sync-signed' or refusals or expiry or window
    visibility_refusal = directory.name in REFUSED_VISIBILITIES
    assert signed or visibility_refusal or directory.name == 'sync-quiet'
    observations = []
    logged_in, session, saved, auth, _ = login(('policy.change', 'roles.control', 'status.read'))

    def call(path, value, key):
        status, body = request('POST', '/management/v1/'+path, value, {**auth, 'Idempotency-Key': key})
        answer = json.loads(body)
        observations.append(dict(path=path, input=value, key=key, status=status, body=answer))
        assert status == 202 and answer['created'] and answer['command_key'] == key, (status, answer)
        return answer

    def barrier(name, deadline=None):
        publish(directory, name+'.json', observations)
        deadline = time.monotonic()+22 if deadline is None else deadline
        while not (directory/(name+'.continue')).exists():
            assert not errors, errors
            assert time.monotonic() < deadline, name
            time.sleep(.01)

    visibility = json.loads((directory/'visibility.json').read_text())
    assert visibility['ordinal'] == str(REFUSED_VISIBILITIES.get(directory.name, 3))
    revisions = dict(expected_config_revision='1', expected_policy_revision='1')
    endpoint = call('relay-endpoints', dict(url='WS://127.0.0.1:7447/a/../installed-fixture', **revisions), '91'*16)
    enrollment = call('relay-enrollments', dict(endpoint_id=endpoint['endpoint_id'], visibility=visibility,
        route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True, expected_generation=None, **revisions), '92'*16)
    assert enrollment['generation'] == '1' and enrollment['enabled'] is True
    scope = call('acquisition-scopes', dict(endpoint_id=endpoint['endpoint_id'], access_scope_id=enrollment['access_scope_id'],
        visibility=visibility, selector=dict(version=1, filters=[{'kinds': [1]}], history_since=0, ephemeral='retain'), **revisions), 'a1'*16)
    listener = relay_listener()
    finished, errors = threading.Event(), []
    worker = threading.Thread(target=no_connections if visibility_refusal else relay,
        args=(directory, listener, finished, errors) if visibility_refusal else (directory, listener, finished, errors, signed, refusals, window))
    worker.start()
    deferred_worker = None
    try:
        barrier('prepared')
        if expiry:
            from installed_sync_expiry import DURATION_US, KEY, suspended_cli
            call('jobs', dict(kind='acquisition', scope_id=scope['scope_id'],
                deadline_us=str(DURATION_US), **revisions), KEY)
            with suspended_cli(directory, publish) as deadline:
                barrier('expiry-accepted', deadline)
            barrier('expiry-continued')
        receipt = call('jobs', dict(kind='acquisition', scope_id=scope['scope_id'], deadline_us='120000000', **revisions), 'b1'*16)
        assert receipt['execution'] == 'unobserved' and receipt['authority'] == 'acceptance-only'
        barrier('submitted')
        status, raw = request('GET', '/management/v1/jobs/'+receipt['job_id'], headers=auth)
        observed = json.loads(raw)
        assert status == 200 and observed == dict(format='bigbrotr-job-status/1', job_id=receipt['job_id'],
            kind='acquisition', state='queued', job_revision='2', checkpoint_revision='0',
            created_at_us=receipt['accepted_at_us'], completed_at_us=None, observed_at_us=observed['observed_at_us'],
            reason_code=None, authority='observation-only')
        assert int(observed['observed_at_us']) >= int(receipt['accepted_at_us'])
        if window:
            deferred_receipt = None
            if directory.name == 'sync-window-source':
                from installed_sync_window import SECOND_ENDPOINT
                monitored = directory/'deferred-source'; monitored.mkdir()
                deferred_listener = relay_listener(('127.0.0.1', 7448))
                deferred_worker = threading.Thread(target=no_connections,
                    args=(monitored, deferred_listener, finished, errors))
                deferred_worker.start()
                other = call('relay-endpoints', dict(url=SECOND_ENDPOINT, **revisions), '93'*16)
                other_enrollment = call('relay-enrollments', dict(endpoint_id=other['endpoint_id'], visibility=visibility,
                    route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True,
                    expected_generation=None, **revisions), '94'*16)
                other_scope = call('acquisition-scopes', dict(endpoint_id=other['endpoint_id'],
                    access_scope_id=other_enrollment['access_scope_id'], visibility=visibility,
                    selector=dict(version=1, filters=[{'kinds': [1]}], history_since=0, ephemeral='retain'),
                    **revisions), 'a2'*16)
                deferred_receipt = call('jobs', dict(kind='acquisition', scope_id=other_scope['scope_id'],
                    deadline_us='120000000', **revisions), 'b2'*16)
            barrier('window-commands')
            if deferred_receipt:
                status, raw = request('GET', '/management/v1/jobs/'+deferred_receipt['job_id'], headers=auth)
                deferred_status = json.loads(raw)
                assert status == 200 and deferred_status == dict(observed,
                    job_id=deferred_receipt['job_id'], created_at_us=deferred_receipt['accepted_at_us'],
                    observed_at_us=deferred_status['observed_at_us'])
                assert int(deferred_status['observed_at_us']) >= int(deferred_receipt['accepted_at_us'])
                publish(directory, 'window-job-status.json', deferred_status)
        logged_out = logout(session, saved, auth)
        barrier('commands-finished')  # Host stops Serve while our relay remains available.
    finally:
        finished.set()
        worker.join(4)
        assert not worker.is_alive(), 'relay worker did not join'
        if deferred_worker is not None:
            deferred_worker.join(1)
            assert not deferred_worker.is_alive(), 'deferred-source monitor did not join'
    assert not errors, errors
    return dict(login=logged_in, logout=logged_out, observations=observations, job_status=observed)
