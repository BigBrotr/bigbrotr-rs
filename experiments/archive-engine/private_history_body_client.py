#!/usr/bin/env python3
"""Actual signed history over a deliberately bounded Linux TCP connection.

The host independently observes the socket queue, native child absence and SQL
pin. Credentials remain inside the private fixture root and are never emitted.
"""
import hashlib
from http.cookies import SimpleCookie
import json
import os
from pathlib import Path
import socket
import sys
import time
import private_grant_http as http


def emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def large_event(source):
    recipe = json.loads((source / 'tests/fixtures/protocol/archive-event-response.json').read_text())
    event = dict(recipe['event'], content=recipe['content_repeat'] * recipe['content_count'])
    encoded = json.dumps({key:event[key] for key in recipe['serialization_fields']},
                         ensure_ascii=False, separators=(',', ':')).encode()
    assert len(encoded) == recipe['signed_event_bytes']
    assert hashlib.sha256(encoded).hexdigest() == recipe['signed_event_sha256']
    return event


def setup():
    server = http.Server('body-enrollment')
    try:
        server.ready()
        status, _, headers = http.request('POST', '/management/v1/sessions',
                                          dict(username='alice', password=http.PASSWORD))
        assert status == 201
        cookies = SimpleCookie()
        for name, value in headers:
            if name.lower() == 'set-cookie': cookies.load(value)
        auth = {'Cookie':'BigBrotrLocalSession=' + cookies['BigBrotrLocalSession'].value,
                'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value, 'Idempotency-Key':'c1'*16}
        principal = (http.ROOT / 'principal').read_text()
        command = dict(principal_id=principal, action='history.read', enabled=True,
                       scope=dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1'),
                       expected_config_revision='1', expected_policy_revision='2', expected_grant_revision='1',
                       allocation=dict(catalog_id='5a'*16, frontier_revision='1'))
        status, proof, _ = http.request('POST', '/management/v1/sessions/current/reauthenticate',
                                       dict(password=http.PASSWORD, command=command), auth)
        assert status == 201
        status, grant, _ = http.request('PUT', f'/management/v1/principals/{principal}/grants',
                                       dict(command=command, proof_id=proof['proof_id']), auth)
        assert status == 200 and grant['enabled'] and grant['created'] and grant['scope_generation'] == '1'
        descriptor = os.open(http.ROOT / 'session.private', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            json.dump({k:auth[k] for k in ['Cookie', 'X-CSRF-Token']}, stream)
        server.stop()
        emit('setup', status='PASS')
    finally:
        server.cleanup()


def main(mode, revision):
    assert mode in ('pass', 'action', 'pin')
    expected = large_event(Path(__file__).resolve().parents[2])
    session = http.ROOT / 'session.private'
    assert session.stat().st_mode & 0o777 == 0o600
    auth = json.loads(session.read_text())
    server = http.Server('body-' + mode)
    peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.ready(revision)
        assert http.request('GET', '/management/v1/me', auth=auth)[0] == 200
        peer.settimeout(8)
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
        peer.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 512)
        started = time.monotonic_ns()
        peer.connect(('127.0.0.1', 18080))
        value = dict(version=1, resource='event_history', mode='snapshot',
                     scope=dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1'),
                     filters=[dict(ids=[expected['id']])])
        encoded = json.dumps(value, separators=(',', ':')).encode()
        headers = {'Host':http.ADDRESS, 'Origin':'http://' + http.ADDRESS,
                   'Content-Type':'application/json', 'Content-Length':str(len(encoded)), **auth}
        request = 'POST /v1/resources/event_history HTTP/1.1\r\n'
        request += ''.join(f'{key}: {value}\r\n' for key, value in headers.items()) + '\r\n'
        peer.sendall(request.encode() + encoded)
        head = bytearray()
        while not head.endswith(b'\r\n\r\n'):
            byte = peer.recv(1)
            assert byte and len(head) < 16384, 'missing or oversized HTTP headers'
            head += byte
        lines = head.decode('ascii').split('\r\n')
        assert lines[0] == 'HTTP/1.1 200 OK', lines[0]
        response_headers = {key.lower():value.strip() for key, value in
                            (line.split(':', 1) for line in lines[1:] if line)}
        length = int(response_headers['content-length'])
        assert 90000 < length < 128 * 1024 and response_headers['cache-control'] == 'no-store'
        assert 'transfer-encoding' not in response_headers
        data = bytearray(peer.recv(1))
        assert data
        emit('headers', parent_pid=server.process.pid, client_port=peer.getsockname()[1],
             started_ns=started, header_ms=round((time.monotonic_ns()-started)/1e6, 3),
             content_length=length, receive_buffer=peer.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
             mss=peer.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG), initial_body_bytes=len(data))
        assert sys.stdin.readline() == 'drain\n'
        draining = time.monotonic_ns()
        while True:
            chunk = peer.recv(16384)
            if not chunk: break
            data += chunk
            assert len(data) <= length
        ended = time.monotonic_ns()
        if mode == 'pass':
            assert len(data) == length
            page = json.loads(data)
            assert page['items'] == [expected]
            assert page['view_stamp']['input_generations'] == [dict(kind='archive', catalog_id='5a'*16, frontier_revision='1', snapshot_id='3')]
            assert page['view_stamp']['policy_epoch'] == str(revision)
        else:
            assert 0 < len(data) < length, 'expired authority delivered a complete page'
            # An ordinary5s connection timeout cannot satisfy this short test.
            assert (ended-started)/1e9 < 4
        peer.close()
        assert http.request('GET', '/management/v1/me', auth=auth)[0] == 200
        assert server.process.poll() is None
        assert not list((http.ROOT / 'scratch').iterdir())
        emit('healthy', body_bytes=len(data), content_length=length, complete=len(data)==length,
             signed_event_verified=mode=='pass', request_ms=round((ended-started)/1e6,3),
             drain_started_ms=round((draining-started)/1e6,3), drain_ms=round((ended-draining)/1e6,3))
        assert sys.stdin.readline() == 'stop\n'
        server.stop()
        emit('terminal', parent_exit=server.process.returncode, scratch_entries=0)
    finally:
        peer.close()
        server.cleanup()


if __name__ == '__main__':
    if sys.argv[1:] == ['setup']: setup()
    else: main(sys.argv[1], int(sys.argv[2]))
