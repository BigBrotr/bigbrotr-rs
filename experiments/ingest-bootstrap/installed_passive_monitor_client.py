"""One literal passive peer and real owner enrollment; no Sync scope or job."""
import base64
import errno
import hashlib
import json
import math
import re
import socket
import threading
import time

from installed_sync_client import exact, publish, read_frame, relay_listener, send_frame
from linear_capped_transport import accept_nowait

SIGNED_SHA256 = 'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'
ENDPOINT = 'ws://127.0.0.1:7447/installed-fixture'


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def framed_bytes(payload, masked=False):
    assert len(payload) < 65536
    return len(payload) + (2 if len(payload) < 126 else 4) + (4 if masked else 0)


def exchange(connection, event):
    connection.settimeout(3)
    trace = dict(connected_at_us=time.time_ns() // 1000,
        clock_resolution_us=math.ceil(time.clock_getres(time.CLOCK_REALTIME)*1_000_000))
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
    assert fields['upgrade'].lower() == 'websocket' and fields['connection'].lower() == 'upgrade'
    assert fields['sec-websocket-version'] == '13'
    assert 'sec-websocket-extensions' not in fields and 'sec-websocket-protocol' not in fields
    key = fields['sec-websocket-key']
    assert len(base64.b64decode(key, validate=True)) == 16
    accept = base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
    connection.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n')
    opcode, request_raw = read_frame(connection)
    request = json.loads(request_raw)
    assert opcode == 1 and len(request) == 3 and request[0] == 'REQ'
    ident = request[1]
    assert re.fullmatch('[0-9a-f]{48}', ident) and int(ident[:32], 16)
    assert ident[32:] == '0000000000000001'
    assert request[2] == dict(kinds=[1], limit=1)
    assert request_raw == json_bytes(request)
    trace.update(request=request, request_at_us=time.time_ns() // 1000,
        sent_at_us=time.time_ns() // 1000, event_id=event['id'])
    messages = [['EVENT', ident, event], ['EOSE', ident]]
    for message in messages:
        send_frame(connection, message)
    opcode, close_raw = read_frame(connection)
    assert opcode == 1 and json.loads(close_raw) == ['CLOSE', ident]
    assert close_raw == json_bytes(['CLOSE', ident])
    opcode, payload = read_frame(connection)
    assert opcode == 8 and payload == b''
    connection.sendall(b'\x88\x00')
    connection.shutdown(socket.SHUT_WR)
    assert connection.recv(1) == b'', 'additional AUTH/PUBLISH/REQ/bytes after own teardown'
    trace.update(closed_at_us=time.time_ns() // 1000, completion='request-and-websocket-close',
        rx_physical_frames=3, rx_messages=3, event_candidates=1,
        rx_ws_bytes=sum(framed_bytes(json_bytes(value)) for value in messages)+2,
        rx_payload_bytes=sum(len(json_bytes(value)) for value in messages),
        tx_ws_bytes=framed_bytes(request_raw, True)+framed_bytes(close_raw, True)+6,
        client_commands=['REQ', 'CLOSE', 'websocket-close'], server_messages=['EVENT', 'EOSE', 'websocket-close'])
    assert 0 <= trace['closed_at_us'] - trace['request_at_us'] < 3_000_000
    return trace


def relay(directory, listener, finished, errors):
    connections, completed, final_queue_empty = 0, False, False
    try:
        original = (directory / 'event.jsonl').read_bytes()
        assert hashlib.sha256(original).hexdigest() == SIGNED_SHA256
        event = json.loads(original)
        assert event['kind'] == 1
        publish(directory, 'listener-started.json', dict(fileno=listener.fileno(), connections=0))
        while not finished.is_set():
            probe = connections == 1 and not completed
            try:
                connection, remote = accept_nowait(listener) if probe else listener.accept()
            except OSError as error:
                if probe:
                    assert error.errno in (errno.EAGAIN, errno.EWOULDBLOCK)
                    assert listener.fileno() >= 0 and not finished.is_set()
                    publish(directory, 'relay-complete.json', dict(format='bigbrotr-passive-peer-completion/1',
                        connections=1, listening=True, eof=True, completed_at_us=time.time_ns() // 1000))
                    completed = True
                elif not isinstance(error, socket.timeout):
                    raise
                continue
            connections += 1
            with connection:
                assert connections == 1 and remote[0] == '127.0.0.1', 'passive attempt opened another connection'
                trace = exchange(connection, event)
                publish(directory, 'relay-exchange.json', trace)
        # The owner has joined, but a connection made just before its exit may
        # still be queued if this worker was delayed. Check the same live socket
        # with an actual kernel accept before closing it or reporting success.
        try:
            connection, _ = accept_nowait(listener)
        except OSError as error:
            assert error.errno in (errno.EAGAIN, errno.EWOULDBLOCK)
            assert listener.fileno() >= 0
            final_queue_empty = True
        else:
            connections += 1
            with connection:
                raise AssertionError('passive attempt queued another connection before owner join')
    except BaseException as error:
        errors.append(type(error).__name__ + ': ' + str(error))
        publish(directory, 'relay-error.json', dict(error=errors[-1]))
    finally:
        listener.close()
        publish(directory, 'listener-ended.json', dict(connections=connections, completed=completed,
            final_queue_empty=final_queue_empty,
            listener_closed=listener.fileno() == -1, finished_at_us=time.time_ns() // 1000))


def run(directory, request, cli, logged_in, session, saved, auth):
    observations, errors = [], []
    recovery_profile = directory/'monitor-ack-recovery.json'
    recovery = recovery_profile.exists()
    if recovery:
        assert json.loads(recovery_profile.read_text()) == dict(format='bigbrotr-monitor-ack-recovery-peer/1',
            boundary='checkpoint-after-complete',observed_seconds=120)
    visibility = json.loads((directory / 'visibility.json').read_text())
    revisions = dict(expected_config_revision='1', expected_policy_revision='1')
    finished = threading.Event()
    listener = relay_listener()
    worker = threading.Thread(target=relay, args=(directory, listener, finished, errors))
    worker.start()

    def call(path, value, key):
        status, answer = request('POST', '/management/v1/'+path, value, {**auth, 'Idempotency-Key': key})
        observations.append(dict(path=path, input=value, key=key, status=status, body=answer))
        assert status == 202 and answer['created'] and answer['command_key'] == key
        return answer

    def barrier(name, seconds):
        publish(directory, name+'.json', observations)
        deadline = time.monotonic()+seconds
        while not (directory / (name+'.continue')).exists():
            assert not errors and worker.is_alive(), errors
            assert time.monotonic() < deadline, name
            time.sleep(.01)

    try:
        endpoint = call('relay-endpoints', dict(url=ENDPOINT, **revisions), '91'*16)
        enrollment = call('relay-enrollments',
            dict(endpoint_id=endpoint['endpoint_id'], visibility=visibility,
                route={'class':'private', 'cidr':'127.0.0.1/32'}, enabled=True,
                expected_generation=None, **revisions), '92'*16)
        assert enrollment['enabled'] is True and enrollment['generation'] == '1'
        selection = dict(format='bigbrotr-monitor-once-v1', endpoint_id=endpoint['endpoint_id'],
            access_scope_id=enrollment['access_scope_id'], enrollment_generation='1',
            route_id=enrollment['route_id'], visibility='private')
        assert all(re.fullmatch('[0-9a-f]{32}', selection[key]) and int(selection[key],16)
            for key in ('endpoint_id','access_scope_id','route_id'))
        publish(directory, 'source-selection.json', selection)
        barrier('prepared', 15)
        barrier('observed', 120 if recovery else 65)
        assert (directory/'relay-complete.json').exists() and not errors
        if recovery:
            # The original HTTP owner has joined. Keep the same live peer
            # through both recovery invocations; no replacement login or probe.
            logged_out = None
        else:
            logged_out = cli('logout', '--session-ref', 'file:'+str(session))
            assert json.loads(session.read_text()) == saved
            assert request('GET', '/management/v1/me', headers=auth)[0] == 401
            # The owner has the existing30s graceful stop plus finite receipt/driver
            # observation. Keep this actual listener until the host confirms its join.
            barrier('commands-finished', 45)
    finally:
        finished.set()
        worker.join(4)
        assert not worker.is_alive(), 'passive peer did not join'
    assert not errors, errors
    ended = json.loads((directory/'listener-ended.json').read_text())
    assert ended['connections'] == 1 and ended['completed'] and ended['final_queue_empty'] and ended['listener_closed']
    return dict(login=logged_in, logout=logged_out, observations=observations, source=selection,
        peer='one-passive-request-no-auth-no-publish')
